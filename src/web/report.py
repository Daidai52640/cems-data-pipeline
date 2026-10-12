# -*- coding: utf-8 -*-
"""报表聚合：分钟/日/月/自由四类时间维度均值统计，聚合全部在 TDengine 侧完成。

返回统一结构：{"unit","start","end","points":[...],"coverage":{...}}
每个 point 含 ts、各测点值、n（条数）、expected、coverage、insufficient、pending。
窗口内无数据时测点值为 null（不是 0），n 为 0。

覆盖率口径（照 docs/adr/0002-告警判据选型.md §3.4）：
  覆盖率 = 实际条数 / 应有条数
  应有条数 = 窗口秒数 / 采集周期（POLL_INTERVAL，默认 5.0s ⇒ 每分钟 12 条）
  覆盖率 < REPORT_COVERAGE_MIN（默认 0.75）⇒ insufficient = true
  insufficient 表示"数据不足以判定"，绝不等于达标。

为什么门限 0.75 而不是 1.0：实测采集周期 5.146s（名义 5.0s），
覆盖率天花板 = 5.0/5.154 = 97.0%。683 个干净窗口 P05=91.7%，最差 83.3%。
门限 0.75（每分钟 9 条）比实测最差干净窗口还低 8.3 个百分点，正常数据不误标。

进行中的窗口：按已过时间算应有条数。还没到期的 expected=0、pending=true，
不判 insufficient；已经过了一部分的照常判（今天缺的今天就要看见）。

本模块 coverage 是到达条数口径（COUNT(*)），告警层 coverage 是有效样本口径，
两者分子不同，不可互相替代。
"""

from __future__ import annotations

import calendar
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final, Optional

import taosrest

# 让 src/common 能被导入：三种启动方式都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import COLUMNS   # noqa: E402
from src.web import cache   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = os.getenv("TD_URL", "http://localhost:6041")
TD_USER: Final[str] = os.getenv("TD_USER", "root")
TD_PASS: Final[str] = os.getenv("TD_PASS", "taosdata")
TD_DB: Final[str] = os.getenv("TD_DB", "cems")
TD_STABLE: Final[str] = os.getenv("TD_STABLE", "cems_data")

# ---- 报表规模限制 ----
# 报表点数上限：语义是"聚合窗口数"，和实时接口的 QUERY_LIMIT（原始点数）不是一回事
REPORT_MAX_POINTS: Final[int] = int(os.getenv("REPORT_MAX_POINTS", "5000"))
# 自由报表最大跨度（天）：按小时聚合，92 天 ≈ 2208 个窗口
CUSTOM_MAX_DAYS: Final[int] = int(os.getenv("CUSTOM_MAX_DAYS", "92"))

# ---- 聚合窗口与格式 ----
WINDOW_MINUTE: Final[timedelta] = timedelta(minutes=1)
WINDOW_HOUR: Final[timedelta] = timedelta(hours=1)
WINDOW_DAY: Final[timedelta] = timedelta(days=1)
UNIT_MINUTE: Final[str] = "1m"
UNIT_HOUR: Final[str] = "1h"
UNIT_DAY: Final[str] = "1d"
TS_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
# 允许的入参时间格式：表单里给的是不带秒的，API 直接调可能带秒
TS_INPUT_FORMATS: Final[tuple[str, ...]] = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
)
ROUND_DIGITS: Final[int] = 2      # 聚合值保留几位小数
COVERAGE_DIGITS: Final[int] = 4   # 覆盖率保留几位小数

# ---- 覆盖率 / 样本不足 ----
# 采集周期：与网关的 POLL_INTERVAL 必须一致。是覆盖率分母的唯一来源。
# 这是名义周期；实测真实周期 5.154s，覆盖率天花板 97.0%，门限按此现实定。
_raw_interval: Final[float] = float(os.getenv("POLL_INTERVAL", "5.0"))
POLL_INTERVAL_SECONDS: Final[float] = _raw_interval if _raw_interval > 0 else 5.0

# 覆盖率门限（0~1）。默认 0.75 = 告警层 ALARM_COVERAGE_MIN，同口径同取值。
REPORT_COVERAGE_MIN: Final[float] = float(os.getenv("REPORT_COVERAGE_MIN", "0.75"))

# 缺口清单条数上限：整段断档时清单会与窗口数同阶，必须设上限否则响应体翻倍。
# 超上限只给前 N 条并置 gaps_truncated=true，判定与计数仍按全部窗口给。
REPORT_GAP_MAX: Final[int] = int(os.getenv("REPORT_GAP_MAX", "200"))

# 在途样本宽限（秒）：只作用于进行中的窗口。网关补传最大滞后 32.1s，取 60s = 1.9 倍余量。
# 已结束的窗口不设宽限：门限 0.75 本身留了 25% 余量。
REPORT_INFLIGHT_GRACE_SECONDS: Final[float] = float(
    os.getenv("REPORT_INFLIGHT_GRACE_SECONDS", "60")
)

# ---- Redis 缓存 ----
# 只作用于已闭合时间窗的查询；判据见 src/web/cache.py 顶部。
# 开关与环境变量解析的唯一真源在 cache.py，这里只是引用。
CACHE_ENABLED: Final[bool] = cache.CACHE_FLAG

# ---- 日志 ----
# 级别/格式见 src/common/logging.py，本模块不复制那三个常量。
LOGGER: Final[logging.Logger] = logging.getLogger("web.report")


class ReportParamError(ValueError):
    """入参不合法（日期格式错、start>end、跨度超限等）—— 接口层转 400。"""


class ReportQueryError(RuntimeError):
    """TDengine 查询失败 —— 接口层转 503（细节只进日志）。"""


# ==================== 2. 参数解析（拼 SQL 只允许用这里解析出来的对象） ====================

def parse_time(text: Optional[str], label: str, default: datetime) -> datetime:
    """解析时间入参；留空取默认值，格式不对抛 ReportParamError。"""
    if not text:
        return default
    for fmt in TS_INPUT_FORMATS:
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    raise ReportParamError(f"{label} 格式应为 YYYY-MM-DD HH:MM[:SS]，收到 {text!r}")


def parse_date(text: Optional[str], label: str, default: datetime) -> datetime:
    """解析 YYYY-MM-DD 日期入参。"""
    if not text:
        return default
    try:
        return datetime.strptime(text.strip(), "%Y-%m-%d")
    except ValueError as exc:
        raise ReportParamError(f"{label} 格式应为 YYYY-MM-DD，收到 {text!r}") from exc


def parse_int(text: Optional[str], label: str, low: int, high: int, default: int) -> int:
    """解析有范围要求的整数入参，越界或非数字都抛 ReportParamError。"""
    if not text:
        return default
    try:
        value = int(text.strip())
    except ValueError as exc:
        raise ReportParamError(f"{label} 应为整数，收到 {text!r}") from exc
    if not low <= value <= high:
        raise ReportParamError(f"{label} 应在 {low}~{high} 之间，收到 {value}")
    return value


def check_span(
    start: datetime,
    end: datetime,
    window: timedelta,
    label: str,
    max_points: Optional[int] = None,
) -> None:
    """校验时间范围：start 必须早于 end，且窗口数不超过上限。"""
    limit = REPORT_MAX_POINTS if max_points is None else max_points
    if start >= end:
        raise ReportParamError(f"{label}：开始时间必须早于结束时间")
    windows = int((end - start) / window) + 1
    if windows > limit:
        raise ReportParamError(
            f"{label}：预计 {windows} 个窗口，超过上限 {limit} 个，请缩小时间范围"
        )


def floor_to_window(moment: datetime, window: timedelta) -> datetime:
    """把时刻向下对齐到窗口边界（与 TDengine INTERVAL 的对齐口径一致）。"""
    epoch = datetime(1970, 1, 1)
    return epoch + ((moment - epoch) // window) * window


def align_range(start: datetime, end: datetime, window: timedelta) -> tuple[datetime, datetime]:
    """把查询范围向内对齐到窗口边界，只保留完整窗口。

    不对齐的话，首末那半个窗口的均值会被当成整窗均值展示，看着像数据掉了一截。
    """
    aligned_start = floor_to_window(start, window)
    if aligned_start < start:
        aligned_start += window          # 起点向上取整，保证第一个窗口完整
    return aligned_start, floor_to_window(end, window)


# ==================== 3. 查询与结果组装 ====================

def query_aggregate(
    start: datetime,
    end: datetime,
    window: str,
    *,
    fill_null: bool = False,
    kind: str = "aggregate",
    scope: str = "",
) -> list[tuple[Any, ...]]:
    """按窗口聚合求均值；聚合在 TDengine 侧用 INTERVAL + AVG 完成。

    拼进 SQL 的时间由已解析的 datetime 重新格式化，窗口串是代码常量，
    列名来自测点契约白名单——不拼接原始用户输入。
    区间统一用半开 [start, end)：调用方已向内对齐到窗口边界。

    设备维度既进缓存键、也进 SQL tag 过滤：超级表是多设备共用的，
    不过滤会把两台设备的样本一起 AVG（出"混合曲线"）。
    """
    avg_columns = ", ".join(f"AVG({column})" for column in COLUMNS)
    # 请求级设备维度（空 = 本实例配置的那台）
    plant, device = cache.scope_split(scope)
    scope = scope or cache.DEVICE_SCOPE
    sql = (
        f"SELECT _wstart, {avg_columns}, COUNT(*) FROM {TD_DB}.{TD_STABLE} "
        f"WHERE ts >= '{start.strftime(TS_FORMAT)}' AND ts < '{end.strftime(TS_FORMAT)}' "
        f"AND plant = '{plant}' AND device = '{device}' "
        f"INTERVAL({window})"
    )
    if fill_null:
        # 日报表要固定 24 个整点必须加 FILL(NULL)；FILL(0) 在 TDengine 3.x 报 syntax error
        sql += " FILL(NULL)"
    # 没有数据的区间返回 0 行且不报错，原样返回空列表
    return _execute_cached(
        f"{sql} ORDER BY _wstart ASC",
        where=f"aggregate/{kind}",
        context=cache.prepare(kind, window, start, end, device=scope),
    )


def _execute(sql: str) -> list[tuple[Any, ...]]:
    """执行查询（不走缓存）；任何跨系统异常都包装成 ReportQueryError。"""
    conn: Any = None
    try:
        conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
        cursor = conn.cursor()
        cursor.execute(sql)
        return list(cursor.fetchall())
    except Exception as exc:
        LOGGER.error("报表聚合查询失败: %s | SQL: %s", exc, sql)
        raise ReportQueryError(str(exc)) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                LOGGER.debug("关闭 TDengine 连接时出错（忽略）: %s", exc)


def _execute_cached(
    sql: str,
    where: str,
    context: Optional[dict[str, Any]],
) -> list[tuple[Any, ...]]:
    """带缓存的查询出口：上下文为空或缓存关闭时退化为 _execute。

    context is None 是有语义的：调用方明确表示"这次查询不能缓存"（如覆盖当前秒的原始点）。
    设备维度已由 cache.prepare 放进 context["device"] 并烘焙进 context["key"]。
    """
    if context is None or not CACHE_ENABLED:
        return _execute(sql)
    return cache.query_cached(context, where, lambda: _execute(sql))


def format_ts(value: Any) -> str:
    """把 TDengine 返回的时间戳统一成 'YYYY-MM-DD HH:MM:SS'。"""
    if isinstance(value, datetime):
        return value.strftime(TS_FORMAT)
    return str(value)[:19]


def parse_ts(text: Any) -> Optional[datetime]:
    """format_ts 的逆运算：'YYYY-MM-DD HH:MM:SS' -> naive datetime。

    只用于解析本模块自己生成的窗口起点串，解析不了返回 None。
    """
    try:
        return datetime.strptime(str(text)[:19], TS_FORMAT)
    except (ValueError, TypeError):
        return None


def empty_point(ts: str) -> dict[str, Any]:
    """窗口内一条数据都没有：测点值为 null（不能当 0），采样条数 0。"""
    point: dict[str, Any] = {"ts": ts}
    point.update({column: None for column in COLUMNS})
    point["n"] = 0
    return point


def rows_to_points(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    """查询结果转下发结构；NULL 原样保留（前端据此断线/显示 —）。"""
    points: list[dict[str, Any]] = []
    for row in rows:
        point: dict[str, Any] = {"ts": format_ts(row[0])}
        for index, column in enumerate(COLUMNS, start=1):
            value = row[index]
            point[column] = round(float(value), ROUND_DIGITS) if value is not None else None
        count = row[len(COLUMNS) + 1]
        # FILL 出来的空窗口 COUNT(*) 是 NULL，但"这个窗口没有采样"就是 0 条
        point["n"] = int(count) if count is not None else 0
        points.append(point)
    return points


def points_on_grid(rows: list[tuple[Any, ...]], grid: list[datetime]) -> list[dict[str, Any]]:
    """把结果对齐到给定窗口网格，网格上缺的窗口补 null。

    必须补：实测"整个区间一条数据都没有"时，即使加了 FILL(NULL) 也返回 0 行，
    光靠 FILL 保证不了日报表固定 24 点。
    """
    by_start = {format_ts(row[0]): row for row in rows}
    points: list[dict[str, Any]] = []
    for start in grid:
        key = start.strftime(TS_FORMAT)
        row = by_start.get(key)
        points.append(rows_to_points([row])[0] if row is not None else empty_point(key))
    return points


def expected_samples(window_seconds: float) -> int:
    """窗口内"应有条数" = 窗口秒数 ÷ 采集周期（默认 5s ⇒ 1 分钟 12 条）。

    这是名义分母。实测真实周期 5.146s，正常窗口覆盖率 91.7%~97.2% 而非 100%。
    """
    return max(1, int(round(window_seconds / POLL_INTERVAL_SECONDS)))


def coverage_of(count: int, expected: int) -> float:
    """覆盖率 = 实际条数 / 应有条数，封顶 1.0。

    封顶的理由：窗口内条数高于名义周期时，"覆盖率"语义是"到齐了没有"，
    到齐就是 100%；原始条数仍在 n 里，不会被封顶藏起来。
    """
    if expected <= 0:
        return 0.0
    return min(1.0, count / expected)


def annotate_coverage(
    points: list[dict[str, Any]],
    window: timedelta,
    threshold: float,
    now: Optional[datetime] = None,
) -> None:
    """就地给每个窗口加 expected / coverage / insufficient / pending。

    expected 是"该窗口到目前为止应有条数"：
      - 已结束窗口：窗口秒数 / 采集周期；
      - 进行中窗口：已过秒数 − 在途宽限 / 采集周期——还没到的时间不算缺数，
        但已经过去的时间里缺的必须算出来；
      - 还没到期或刚过去不到宽限的窗口：expected=0、pending=true。

    三个要点：
    1. n==0 的窗口照样出现在结果里，coverage=0.0、insufficient=true——缺数不能消失。
    2. 判定用四舍五入后的 coverage，保证"显示的数字"和"判定结果"一致。
    3. pending 的窗口不判 insufficient，仍带 coverage 出现。
    """
    moment = datetime.now() if now is None else now
    window_seconds = window.total_seconds()
    for point in points:
        count = int(point.get("n") or 0)
        window_start = parse_ts(point.get("ts"))
        elapsed = (moment - window_start).total_seconds() if window_start is not None else 0.0
        if window_start is None:
            # 窗口起点解析不了：宁可少报一个结论，也不误报缺口
            LOGGER.warning("窗口起点无法解析，按未到期处理: %r", point.get("ts"))
        if elapsed < window_seconds:
            # 未结束的窗口：只按"已经过去、且已过在途宽限"的部分算应有条数
            due = elapsed - REPORT_INFLIGHT_GRACE_SECONDS
            if due <= 0:
                expected, pending = 0, True
            else:
                expected, pending = max(1, int(round(due / POLL_INTERVAL_SECONDS))), False
        else:
            expected, pending = expected_samples(window_seconds), False
        coverage = round(coverage_of(count, expected), COVERAGE_DIGITS)
        point["expected"] = expected
        point["coverage"] = coverage
        point["pending"] = bool(pending)
        point["insufficient"] = bool(not pending and coverage < threshold)


def coverage_summary(
    points: list[dict[str, Any]],
    window: timedelta,
    threshold: float,
) -> dict[str, Any]:
    """整个响应的覆盖率汇总：窗口总数、样本不足窗口数、整体覆盖率、缺口清单。

    缺口清单让"拔线后前后计数对得上"能被机器核对（对账脚本直接读 gaps 求和）。
    清单按 REPORT_GAP_MAX 截断（只截清单本身，计数不截断）。
    expected_total 是各窗口 expected 之和，"还没到期"的窗口天然不参与整体覆盖率。
    """
    expected_per_window = expected_samples(window.total_seconds())
    judged = [point for point in points if not point.get("pending")]
    pending_windows = len(points) - len(judged)
    actual_total = sum(int(point.get("n") or 0) for point in points)
    expected_total = sum(int(point.get("expected") or 0) for point in points)
    gaps = [
        {
            "ts": point["ts"],
            "n": int(point.get("n") or 0),
            "expected": int(point.get("expected") or 0),
            "coverage": point.get("coverage", 0.0),
            "missing": max(0, int(point.get("expected") or 0) - int(point.get("n") or 0)),
        }
        for point in judged
        if point.get("insufficient")
    ]
    return {
        # 判定参数一起下发，调用方不必猜数字怎么来的
        "threshold": threshold,
        "poll_interval_seconds": POLL_INTERVAL_SECONDS,
        "window_seconds": window.total_seconds(),
        "expected_per_window": expected_per_window,
        # 窗口计数
        "windows": len(points),
        "judged_windows": len(judged),
        "pending_windows": pending_windows,
        "insufficient_windows": len(gaps),
        # 整体覆盖率
        "actual_total": actual_total,
        "expected_total": expected_total,
        # 没有任何"应到"的数据时给 null 而不是 0.0（0/0 不是 0）
        "overall": (
            round(coverage_of(actual_total, expected_total), COVERAGE_DIGITS)
            if judged else None
        ),
        # 缺口清单
        "gap_count": len(gaps),
        "gaps": gaps[:REPORT_GAP_MAX],
        "gaps_truncated": len(gaps) > REPORT_GAP_MAX,
    }


def envelope(
    unit: str,
    start: datetime,
    end: datetime,
    points: list[dict[str, Any]],
    window: Optional[timedelta] = None,
) -> dict[str, Any]:
    """组装统一返回结构。只增加 fields，原来的 unit/start/end/points 不改名不删。"""
    payload: dict[str, Any] = {
        "unit": unit,
        "start": start.strftime(TS_FORMAT),
        "end": end.strftime(TS_FORMAT),
        "points": points,
    }
    if window is not None:
        payload["coverage"] = coverage_summary(points, window, REPORT_COVERAGE_MIN)
    return payload


def clamp_to_now(end: datetime, now: datetime) -> datetime:
    """查询上界统一不超过当前时刻，避免把未来时间戳的脏数据算进报表。"""
    if end > now:
        LOGGER.debug("end=%s 超过当前时刻，已收窄到 %s", end, now)
        return now
    return end


def query_raw(start: datetime, end: datetime, limit: int, scope: str = "") -> list[tuple[Any, ...]]:
    """原始点查询（曲线的短区间用）：不聚合，直接取原始行。

    明确不缓存：原始点区间可能覆盖当前秒，缓存它等于把当前数据冻住。
    按 plant/device tag 过滤：超级表多设备共用，不过滤会把两台设备的原始点混成一条曲线。
    """
    columns = ", ".join(("ts", *COLUMNS))
    plant, device = cache.scope_split(scope)
    sql = (
        f"SELECT {columns} FROM {TD_DB}.{TD_STABLE} "
        f"WHERE ts >= '{start.strftime(TS_FORMAT)}' AND ts <= '{end.strftime(TS_FORMAT)}' "
        f"AND plant = '{plant}' AND device = '{device}' "
        f"ORDER BY ts ASC LIMIT {limit}"
    )
    return _execute(sql)


def aggregate_series(
    start: datetime,
    end: datetime,
    unit: str,
    window: timedelta,
    *,
    label: str,
    pad_grid: bool,
    max_points: Optional[int] = None,
    kind: str = "aggregate",
    scope: str = "",
) -> dict[str, Any]:
    """把一段区间聚合成均值序列——报表页、Excel 导出、曲线三处共用这一份口径。

    做四件事：范围向内对齐到窗口边界、库侧 INTERVAL+AVG 聚合、
    可选按窗口网格补 null、给每个窗口标覆盖率与样本不足。
    kind 只影响缓存键，不改变计算结果。
    """
    aligned_start, aligned_end = align_range(start, end, window)
    if aligned_start >= aligned_end:            # 不足一个完整窗口
        return envelope(unit, aligned_start, aligned_end, [], window=window)
    check_span(aligned_start, aligned_end, window, label, max_points)

    rows = query_aggregate(aligned_start, aligned_end, unit, fill_null=pad_grid, kind=kind, scope=scope)
    if pad_grid:
        count = int((aligned_end - aligned_start) / window)
        grid = [aligned_start + index * window for index in range(count)]
        points = points_on_grid(rows, grid)
    else:
        points = rows_to_points(rows)
    # 标记放最后一步：无论点是查出来的还是网格补出来的，都走同一份判定
    annotate_coverage(points, window, REPORT_COVERAGE_MIN)
    return envelope(
        unit, aligned_start, aligned_end - timedelta(seconds=1), points, window=window,
    )


# ==================== 4. 四类报表 ====================

def minute_report(start_text: Optional[str], end_text: Optional[str], scope: str = "") -> dict[str, Any]:
    """分钟报表：默认最近 1 小时，按分钟聚合。

    必须补网格：不补的话整分钟无数据的窗口会被 INTERVAL(1m) 直接吞掉、从结果里消失。
    补网格后该窗口以 n=0 / null / coverage=0.0 / insufficient=true 出现。
    """
    now = datetime.now()
    start = parse_time(start_text, "start", now - WINDOW_HOUR)
    end = clamp_to_now(parse_time(end_text, "end", now), now)
    return aggregate_series(
        start, end, UNIT_MINUTE, WINDOW_MINUTE, label="分钟报表", pad_grid=True,
        kind="minute", scope=scope,
    )


def day_report(date_text: Optional[str], scope: str = "") -> dict[str, Any]:
    """日报表：默认今天，按小时聚合，固定 24 个整点。

    晚于当前时刻的整点还没到期：n=0、pending=true、不算缺口；
    当前正在过的小时按已过时间判覆盖率，今天缺的数据今天就能看见。
    """
    first = parse_date(date_text, "date", datetime.now())
    first = first.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day = first + WINDOW_DAY
    # 半开区间 [当天 00:00, 次日 00:00)，否则跨到次日会产生第 25 个窗口
    return aggregate_series(
        first, next_day, UNIT_HOUR, WINDOW_HOUR, label="日报表", pad_grid=True,
        kind="day", scope=scope,
    )


def month_report(year_text: Optional[str], month_text: Optional[str], scope: str = "") -> dict[str, Any]:
    """月报表：默认当月，按天聚合；按当月实际天数补齐（闰年 2 月 29）。

    同日报表：还没到的日期 pending=true 不算缺口，但仍带 coverage 出现；
    今天这一天按已过时间判定。
    """
    now = datetime.now()
    year = parse_int(year_text, "year", 1970, 9999, now.year)
    month = parse_int(month_text, "month", 1, 12, now.month)
    first = datetime(year, month, 1)
    days = calendar.monthrange(year, month)[1]      # 闰年自动给 29
    next_month = first + timedelta(days=days)
    return aggregate_series(
        first, next_month, UNIT_DAY, WINDOW_DAY, label="月报表", pad_grid=True,
        kind="month", scope=scope,
    )


def custom_report(start_text: Optional[str], end_text: Optional[str], scope: str = "") -> dict[str, Any]:
    """自由报表：默认最近 24 小时，按小时聚合，可跨天/跨月。

    按小时网格补齐：整段无数据时也返回完整网格（值 null）而不是空数组，
    否则前端分不清"没查到"和"这段确实没数据"。
    """
    now = datetime.now()
    start = parse_time(start_text, "start", now - WINDOW_DAY)
    end = clamp_to_now(parse_time(end_text, "end", now), now)
    if end - start > timedelta(days=CUSTOM_MAX_DAYS):
        raise ReportParamError(
            f"自由报表最多查询 {CUSTOM_MAX_DAYS} 天，当前跨度 {(end - start).days} 天"
        )
    return aggregate_series(
        start, end, UNIT_HOUR, WINDOW_HOUR, label="自由报表", pad_grid=True,
        kind="custom", scope=scope,
    )

#: 区间报表可选的粒度：分钟 / 小时 / 日。键同时是对外的 unit 取值。
RANGE_UNITS: Final[dict[str, timedelta]] = {
    UNIT_MINUTE: WINDOW_MINUTE, UNIT_HOUR: WINDOW_HOUR, UNIT_DAY: WINDOW_DAY,
}


def range_report(
    start_text: Optional[str], end_text: Optional[str], unit: str = UNIT_HOUR, scope: str = "",
) -> dict[str, Any]:
    """区间报表：在给定区间上按指定粒度聚合（分钟 / 小时 / 日）。

    把「区间」与「粒度」解耦：原来四个入口各自绑死默认区间，页面改了起止时间
    就没法跟着变。口径与其它报表共用 aggregate_series。
    """
    if unit not in RANGE_UNITS:
        raise ReportParamError(
            f"unit 应为 {'/'.join(RANGE_UNITS)} 之一，收到 {unit!r}"
        )
    now = datetime.now()
    start = parse_time(start_text, "start", now - WINDOW_DAY)
    end = clamp_to_now(parse_time(end_text, "end", now), now)
    if start >= end:
        raise ReportParamError("start 必须早于 end")
    if end - start > timedelta(days=CUSTOM_MAX_DAYS):
        raise ReportParamError(
            f"区间最多查询 {CUSTOM_MAX_DAYS} 天，当前跨度 {(end - start).days} 天"
        )
    return aggregate_series(
        start, end, unit, RANGE_UNITS[unit], label="区间报表", pad_grid=True,
        kind="range", scope=scope,
    )
