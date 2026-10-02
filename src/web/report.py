# -*- coding: utf-8 -*-
"""报表聚合：分钟/日/月/自由四类时间维度均值统计，聚合全部在 TDengine 侧完成。

对外只暴露四个函数，返回值统一为：
    {"unit": "1m|1h|1d", "start": "...", "end": "...", "points": [{...}, ...],
     "coverage": {...}}
每个 point 形如 {"ts": "...", "so2": 29.87, ..., "n": 7,
                 "expected": 12, "coverage": 0.9167, "insufficient": false,
                 "pending": false}；
窗口内无数据时测点值为 null（不是 0），n 为 0。

====================== 覆盖率与"样本不足"标记（为什么有这一段） ======================
拔线演练暴露的**真实缺陷**：数据缺口在报表里是"消失"的，而不是"被标记"的 ——
`INTERVAL(1m)` 对整分钟无数据的窗口**直接不返回那一行**（实测 2026-10-01 21:26），
看报表的人根本不知道那里缺数。本模块的处置是**把缺的窗口补出来并标记**，
绝不用 0 或推算值掩盖：窗口的测点值仍然是 null，只是多了一个"这一窗覆盖了多少"的结论。

口径（照 `docs/告警判据设计.md` §3.4，不另创）：
    覆盖率 = 该窗口实际条数 / 该窗口应有条数
    应有条数 = 窗口秒数 / 采集周期（`POLL_INTERVAL`，默认 5.0 s ⇒ 每分钟 12 条；
               进行中/未到期的窗口另算，见下）
    覆盖率 < `REPORT_COVERAGE_MIN`（默认 0.75，与 `ALARM_COVERAGE_MIN` 同值）⇒ insufficient = true
    ⚠️ insufficient **绝不等于达标**，它表示"这一窗的数据不足以判定"。

⚠️ 为什么门限必须远低于 100%（实测数据，见 `scripts/measure_report_coverage.py` 的输出）：
    真实采集周期是 **5.146 s**（`docs/性能与可靠性指标.md` §2.1：104.2 min 实测 1216 条，
    名义 720 条/h vs 实测约 700 条/h）。实测在线段相邻间隔均值 **5.154 s**，于是
    覆盖率天花板 = 名义/实测 = 5.0/5.154 = **97.0%**（与那份文档 §6.4 的"名义完整率 97.22%"同一件事）。
    再叠加秒级截断，1 分钟窗口实际只会拿到 11 或 12 条：683 个"干净窗口"（不与任何断档相交、
    且窗口完整落在数据范围内）的覆盖率 **P05 = 11/12 = 91.7%，最差一个 10/12 = 83.3%**。
    门限取 0.75（= 每分钟 9 条），比实测最差干净窗口还低 **8.3 个百分点** ——
    所以**正常数据不会被误标**，而真断档（实测 21:25 只有 5 条 = 41.7%、21:26 为 0 条）必然被标出来。
    门限可用 `REPORT_COVERAGE_MIN` 覆盖，但改小/改大都必须重新跑一遍上面那个标定脚本。

⚠️ 进行中/未到期的窗口（`expected` 的第二种取值）：日报表的整点、月报表的日期要补满网格，
    所以网格里必然包含"今天还没过完的小时"和"这个月还没到的日期"。
    处置是**按已过时间**算应有条数：
      - 还没到期的窗口（未来的整点/日期，以及刚开始不到在途宽限的窗口）→ `expected = 0`、
        `pending = true`，不判 insufficient（它们不是缺口，只是还没到期），
        但仍然出现在 points 里并带 coverage；
      - 已经过了一部分、但整窗未结束的窗口 → `expected = (已过秒数 − 在途宽限) / 周期`，
        **照常判 insufficient** —— 今天已经过去的那 21 个小时里缺的数据必须现在就能看出来，
        否则"今天的月度报表"要等到明天才敢说话，那也是一种静默。
    `overall` / `actual_total` / `expected_total` 都用各窗口自己的 `expected` 求和，
    因此"还没到期"既不虚高也不压低整体覆盖率。没有任何应到数据时 `overall` 给 null（0/0 不是 0）。

⚠️ 与告警层口径的差别（不掩盖）：本模块的 coverage 是**到达条数**口径（COUNT(*)），
    回答"数据有没有到齐"；`docs/告警判据设计.md` §3.4 小时结算的 coverage 是**有效样本**口径
    （折算值非 nan，即 `o2 < 21`），回答"数据能不能用来判定排放"。两者分子不同，
    报表层不重复做有效性判定（那是告警层的职责），因此两者**不可互相替代**。
====================================================================================
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

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
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
# 报表点数上限单独一个变量：它的语义是"聚合窗口数"，和实时接口的 QUERY_LIMIT（原始点数）不是一回事
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
COVERAGE_DIGITS: Final[int] = 4   # 覆盖率保留几位小数（1/12 = 0.0833，4 位足够分辨单条差异）

# ---- 覆盖率 / 样本不足（缺数要标出来，不许当达标） ----
# 采集周期：与网关的 POLL_INTERVAL 必须一致（docker-compose 里 gateway 固定 5.0）。
# 它是覆盖率分母的**唯一来源**：应有条数 = 窗口秒数 / POLL_INTERVAL。
# ⚠️ 这个值是**名义**周期。实测真实周期 5.154 s（docs/性能与可靠性指标.md §2.1），
#    所以覆盖率天然到不了 100%（天花板 = 5.0/5.154 = 97.0%），门限必须按这个现实定（见模块头部说明）。
_raw_interval: Final[float] = float(os.getenv("POLL_INTERVAL", "5.0"))
POLL_INTERVAL_SECONDS: Final[float] = _raw_interval if _raw_interval > 0 else 5.0

# 覆盖率门限（0~1）。默认 0.75 = docs/告警判据设计.md §3.4 的 ALARM_COVERAGE_MIN，
# 与告警层同口径同取值；本层只做"到齐了没有"，不替代告警层的有效性判定。
REPORT_COVERAGE_MIN: Final[float] = float(os.getenv("REPORT_COVERAGE_MIN", "0.75"))

# 缺口清单条数上限：整段断档（例如 92 天自由报表）时清单会与窗口数同阶，
# 必须设上限，否则响应体会被清单撑大一倍。超上限时只给前 N 条并置 gaps_truncated=true，
# 判定与计数（insufficient_windows / gap_count）仍然按**全部**窗口给，不受截断影响。
REPORT_GAP_MAX: Final[int] = int(os.getenv("REPORT_GAP_MAX", "200"))

# 在途样本宽限（秒）：只作用于"进行中"的窗口（日报表当前这个小时、月报表今天这一天）。
# 网关补传慢路径实测最大滞后 32.1 s（docs/性能与可靠性指标.md §4.2），这里取 60 s = 1.9 倍余量
# —— 与展示层缓存边界 CACHE_MARGIN_SECONDS 同一依据、同一取值（那边防的是"迟到行落进已闭合窗口"，
# 这边防的是"刚过 10 秒就说这一小时缺数"）。已结束的窗口**不设宽限**：
# 门限 0.75 本身留了 25% 余量，1 分钟里 1 条在途行只占 1/12，翻不动判定。
REPORT_INFLIGHT_GRACE_SECONDS: Final[float] = float(
    os.getenv("REPORT_INFLIGHT_GRACE_SECONDS", "60")
)

# ---- Redis 缓存 ----
# 只作用于 **已闭合时间窗** 的查询；判据与安全边界见 src/web/cache.py 顶部说明。
# 开关与环境变量解析的唯一真源在 cache.py（CACHE_ENABLED=0 即整层停用，
# 行为回到"每次都查库"，用于前后对照测量），这里只是引用，不再解析一遍。
CACHE_ENABLED: Final[bool] = cache.CACHE_FLAG

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

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
    """把查询范围向内对齐到窗口边界，只保留**完整**窗口。

    不对齐的话，首末那半个窗口的均值会被当成整窗均值展示：
    比如查 22:25:15~23:25:15，第一个窗口其实只有 45 秒的数据，
    却和整分钟的窗口画在同一条线上，看着像数据掉了一截。
    """
    aligned_start = floor_to_window(start, window)
    if aligned_start < start:
        aligned_start += window          # 起点向上取整，保证第一个窗口是完整的
    return aligned_start, floor_to_window(end, window)


# ==================== 3. 查询与结果组装 ====================

def query_aggregate(
    start: datetime,
    end: datetime,
    window: str,
    *,
    fill_null: bool = False,
    kind: str = "aggregate",
) -> list[tuple[Any, ...]]:
    """按窗口聚合求均值；聚合在 TDengine 侧用 INTERVAL + AVG 完成，不把原始点拉回 Python。

    拼进 SQL 的时间一律由已解析的 datetime 重新格式化得到，窗口串是代码里的常量，
    列名来自测点契约白名单 —— 没有任何一处拼接原始用户输入。
    区间统一用半开写法 [start, end)：调用方（aggregate_series）已把范围向内对齐到窗口边界，
    所以每个窗口都是完整的，不会出现"半个窗口被当成整窗均值"。

    缓存：`kind` 进缓存键（分钟报表/日报表/曲线各自独立），
    是否真的命中由 cache.query_cached 按"窗口是否已闭合"决定。

    ★ **设备维度**既进缓存键、也进 SQL 的 tag 过滤：
      - 进缓存键：同一时间窗对不同设备是不同的数据，键里少了它，第二台设备查同一时间窗
        就会命中第一台的条目（见 src/web/cache.py 模块头第 6 条）。
      - 进 SQL：`cems_data` 是多设备共用的超级表（TAG = plant/device），**不过滤就会把
        两台设备的样本一起 AVG** —— 出的是"混合曲线"，比缓存串数据更隐蔽。
      两个取值都从 `cache.device_tag_parts()` 引用（唯一真源，且带 tag 白名单校验：
      非法 `TD_PLANT`/`TD_DEVICE` 在导入期就拒绝启动，见 src/common/sql_safety.py）。
    """
    avg_columns = ", ".join(f"AVG({column})" for column in COLUMNS)
    plant, device = cache.device_tag_parts()
    sql = (
        f"SELECT _wstart, {avg_columns}, COUNT(*) FROM {TD_DB}.{TD_STABLE} "
        f"WHERE ts >= '{start.strftime(TS_FORMAT)}' AND ts < '{end.strftime(TS_FORMAT)}' "
        f"AND plant = '{plant}' AND device = '{device}' "
        f"INTERVAL({window})"
    )
    if fill_null:
        # 日报表要固定 24 个整点就必须加 FILL(NULL)：不加只会返回有数据的那些小时。
        # FILL(0) 在 TDengine 3.x 会报 syntax error，别用。
        sql += " FILL(NULL)"
    # 没有数据的区间会返回 0 行且不报错，这里原样返回空列表，由上层组装成空/NULL 序列
    return _execute_cached(
        f"{sql} ORDER BY _wstart ASC",
        where=f"aggregate/{kind}",
        context=cache.prepare(kind, window, start, end, device=cache.DEVICE_SCOPE),
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
    """带缓存的查询出口：上下文为空或缓存关闭时退化为 `_execute`。

    `context is None` 是**有语义的**：调用方明确表示"这次查询不能缓存"（例如覆盖当前秒的原始点），
    这种查询连"试着读缓存"都不做。

    设备维度不需要在这里单独传参：它已经由 `cache.prepare` 放进了 `context["device"]` 并烘焙进
    `context["key"]`，所以下游 `cache.query_cached` 的读写都只发生在同一台设备的命名空间里。
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
    """`format_ts` 的逆运算：'YYYY-MM-DD HH:MM:SS' -> naive datetime。

    只用于解析**本模块自己生成的**窗口起点串，解析不了返回 None（调用方按"判不了"处理）。
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
        # FILL 出来的空窗口 COUNT(*) 是 NULL，但"这个窗口没有采样"就是 0 条，给 0 更好读
        point["n"] = int(count) if count is not None else 0
        points.append(point)
    return points


def points_on_grid(rows: list[tuple[Any, ...]], grid: list[datetime]) -> list[dict[str, Any]]:
    """把结果对齐到给定窗口网格，网格上缺的窗口补 null。

    必须补：实测"整个区间一条数据都没有"时，即使加了 FILL(NULL) 也返回 0 行，
    光靠 FILL 保证不了日报表固定 24 点 / 月报表固定当月天数。
    """
    by_start = {format_ts(row[0]): row for row in rows}
    points: list[dict[str, Any]] = []
    for start in grid:
        key = start.strftime(TS_FORMAT)
        row = by_start.get(key)
        points.append(rows_to_points([row])[0] if row is not None else empty_point(key))
    return points


def expected_samples(window_seconds: float) -> int:
    """窗口内"应有条数" = 窗口秒数 ÷ 采集周期（默认 5 s ⇒ 1 分钟 12 条、1 小时 720 条）。

    ⚠️ 这是**名义**分母（与 docs/告警判据设计.md §3.4 的 720 条/h 同口径）。
    实测真实周期 5.146 s，所以正常窗口的覆盖率是 91.7%~97.2% 而不是 100%，
    门限的取值论证见模块头部。
    """
    return max(1, int(round(window_seconds / POLL_INTERVAL_SECONDS)))


def coverage_of(count: int, expected: int) -> float:
    """覆盖率 = 实际条数 / 应有条数，封顶 1.0。

    封顶的理由：窗口内条数**高于**名义周期（实测存在 4 s 间隔）时，
    "覆盖率"的语义是"数据到齐了没有"，到齐就是 100%，超出部分不是覆盖率要表达的东西；
    原始条数仍然在 `n` 里，不会被这个封顶藏起来。
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
    """就地给每个窗口加 `expected` / `coverage` / `insufficient` / `pending`。

    `expected` 的准确定义是"该窗口**到目前为止**应有条数"：
      - 已结束的窗口：窗口秒数 / 采集周期（正常路径，例：60 s ⇒ 12 条）；
      - 进行中的窗口：**已过秒数 − 在途宽限** / 采集周期 —— 还没到的时间不算缺数，
        但已经过去的时间里缺的必须算出来（否则"今天的月度报表"会一直判不出今天的缺口）；
      - 还没轮到第一个采集周期、或刚过去不到在途宽限的窗口：expected = 0、`pending = true`。

    ⚠️ 三个必须守住的点：
    1. `n == 0`（整窗无数据）**照样出现在结果里**，且 coverage = 0.0、insufficient = true ——
       缺数绝不能"从结果里消失"（这是拔线演练暴露的原问题）。
    2. 判定用的是**四舍五入后**的 coverage，保证"看到的数字"和"判定结果"永远一致，
       不会出现"显示 0.75 却判不足"这种无法复核的组合。
    3. `pending`（expected = 0）的窗口不判 insufficient：它们仍然带 coverage 出现在 points 里，
       汇总里也单列 `pending_windows` 计数 —— 是"还没到期"，不是"被藏起来"。

    在途宽限说明：进行中的窗口在快照那一刻可能有 1 条样本还在补传路上（最大滞后 32.1 s），
    不设宽限就会在每个小时的头几十秒里把"正常"判成"样本不足"。
    """
    moment = datetime.now() if now is None else now
    window_seconds = window.total_seconds()
    for point in points:
        count = int(point.get("n") or 0)
        window_start = parse_ts(point.get("ts"))
        elapsed = (moment - window_start).total_seconds() if window_start is not None else 0.0
        if window_start is None:
            # 窗口起点解析不了（只可能来自被改坏的数据）：宁可少报一个结论，也不误报缺口
            LOGGER.warning("窗口起点无法解析，按未到期处理: %r", point.get("ts"))
        if elapsed < window_seconds:
            # 未结束的窗口：只按"已经过去、且已经过了在途宽限"的那部分时间算应有条数
            due = elapsed - REPORT_INFLIGHT_GRACE_SECONDS
            if due <= 0:
                # 含两种情况：窗口在未来（elapsed < 0），以及窗口刚开始（含在途宽限内）
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

    为什么值得给"缺口清单"（判断：值得）：
    本任务要证明的是拔线后"前后计数对得上、无静默丢数据"。
    只有逐窗列出"哪个窗口缺、缺几条"，这件事才能被**机器核对**（对账脚本直接读 gaps 求和），
    否则只能靠人眼在几百行 points 里找 null。清单带上 missing 条数，等于把"丢了多少"直接给出。
    清单按 REPORT_GAP_MAX 截断（只截清单本身，计数不截断），避免整段断档时响应体翻倍。

    计数口径：`expected_total` 是各窗口 `expected` 之和，因此"还没到期的窗口"（expected = 0）
    天然不参与整体覆盖率 —— 不会把今天还没过完的半天算成缺口，
    也不会把今天的缺口漏掉（已过去的那部分照样计入分母）。
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
        # 判定参数（把门限与分母的口径一起下发，调用方不必猜数字是怎么来的）
        "threshold": threshold,
        "poll_interval_seconds": POLL_INTERVAL_SECONDS,
        "window_seconds": window.total_seconds(),
        # 完整窗口的应有条数；进行中的窗口看每个 point 自己的 expected
        "expected_per_window": expected_per_window,
        # 窗口计数
        "windows": len(points),
        "judged_windows": len(judged),
        "pending_windows": pending_windows,
        "insufficient_windows": len(gaps),
        # 整体覆盖率（分子分母都只算"到目前为止应到"的部分）
        "actual_total": actual_total,
        "expected_total": expected_total,
        # 没有任何"应到"的数据时没有整体覆盖率可言 —— 给 null 而不是 0.0，
        # 免得把"还没到期"读成"覆盖率为零"（0/0 不是 0）。
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
    """组装统一返回结构。

    只**增加** fields：原来的 unit/start/end/points 一个不改名不删。
    `window` 给定时附带整个响应的覆盖率汇总（`coverage`），
    不给就只返回旧结构（保持对老调用方的兼容）。
    """
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


def query_raw(start: datetime, end: datetime, limit: int) -> list[tuple[Any, ...]]:
    """原始点查询（曲线的短区间用）：不聚合，直接取原始行。

    列名来自测点契约白名单，时间来自已解析的 datetime，没有任何原始输入拼接进 SQL。

    ⚠️ **明确不缓存**：原始点区间按定义可能覆盖"当前秒"（短区间默认就含当下），
    缓存它等于把当前数据冻住。这里直接调 `_execute`，连读缓存都不做。

    ★ **按 plant/device tag 过滤**：超级表是多设备共用的，不过滤会把两台设备的原始点
    混成一条曲线（与 `query_aggregate` 同一理由）。取值同源于 `cache.device_tag_parts()`。
    """
    columns = ", ".join(("ts", *COLUMNS))
    plant, device = cache.device_tag_parts()
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
) -> dict[str, Any]:
    """把一段区间聚合成均值序列 —— 报表页、Excel 导出、曲线三处共用这一份口径。

    做四件事：范围向内对齐到窗口边界（只统计完整窗口）、在库侧用 INTERVAL + AVG 聚合、
    可选地按窗口网格补 null（整段无数据时也返回完整网格而不是空数组）、
    给每个窗口标覆盖率与"样本不足"（口径见模块头部）。

    `kind` 只影响缓存键，不改变任何计算结果与返回结构。
    """
    aligned_start, aligned_end = align_range(start, end, window)
    if aligned_start >= aligned_end:            # 不足一个完整窗口
        return envelope(unit, aligned_start, aligned_end, [], window=window)
    check_span(aligned_start, aligned_end, window, label, max_points)

    rows = query_aggregate(aligned_start, aligned_end, unit, fill_null=pad_grid, kind=kind)
    if pad_grid:
        count = int((aligned_end - aligned_start) / window)
        grid = [aligned_start + index * window for index in range(count)]
        points = points_on_grid(rows, grid)
    else:
        points = rows_to_points(rows)
    # 标记放在最后一步：无论点是查出来的还是网格补出来的，都走同一份判定，不会漏标。
    annotate_coverage(points, window, REPORT_COVERAGE_MIN)
    return envelope(
        unit, aligned_start, aligned_end - timedelta(seconds=1), points, window=window,
    )


# ==================== 4. 四类报表 ====================

def minute_report(start_text: Optional[str], end_text: Optional[str]) -> dict[str, Any]:
    """分钟报表：默认最近 1 小时，按分钟聚合（只统计完整分钟窗口）。

    ⚠️ 这里**必须**补网格（`pad_grid=True`）：原先不补，整分钟无数据的窗口会被
    `INTERVAL(1m)` 直接吞掉、从结果里消失（实测 2026-10-01 21:26 整分钟无数据，
    查询结果里那一行**根本不存在**）。补网格后该窗口以 n=0 / 各测点 null /
    coverage=0.0 / insufficient=true 的形式出现 —— 缺数被标出来，而不是消失。
    """
    now = datetime.now()
    start = parse_time(start_text, "start", now - WINDOW_HOUR)
    end = clamp_to_now(parse_time(end_text, "end", now), now)
    return aggregate_series(
        start, end, UNIT_MINUTE, WINDOW_MINUTE, label="分钟报表", pad_grid=True,
        kind="minute",
    )


def day_report(date_text: Optional[str]) -> dict[str, Any]:
    """日报表：默认今天，按小时聚合，固定 24 个整点。

    ⚠️ "今天"的报表里，晚于当前时刻的整点还没到期：它们以 n=0 出现在网格里，
    但 `pending=true`、`expected=0`，不算缺口（否则每天都会凭空多出十几个"样本不足"的假告警）；
    当前这个正在过的小时则按**已过时间**判覆盖率，今天缺的数据今天就能看见。
    """
    first = parse_date(date_text, "date", datetime.now())
    first = first.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day = first + WINDOW_DAY
    # 用半开区间 [当天 00:00, 次日 00:00)，否则跨到次日会产生第 25 个窗口
    return aggregate_series(
        first, next_day, UNIT_HOUR, WINDOW_HOUR, label="日报表", pad_grid=True,
        kind="day",
    )


def month_report(year_text: Optional[str], month_text: Optional[str]) -> dict[str, Any]:
    """月报表：默认当月，按天聚合；按当月实际天数补齐 28/30/31（闰年 2 月 29）。

    同日报表：当月还没到的日期 `pending=true`（`expected=0`）、不算缺口，但仍带 coverage 出现；
    今天这一天按已过时间判定 —— 今天已经缺掉的那几个小时，现在就应该看得见。
    """
    now = datetime.now()
    year = parse_int(year_text, "year", 1970, 9999, now.year)
    month = parse_int(month_text, "month", 1, 12, now.month)
    first = datetime(year, month, 1)
    days = calendar.monthrange(year, month)[1]      # 闰年自动给 29
    next_month = first + timedelta(days=days)
    return aggregate_series(
        first, next_month, UNIT_DAY, WINDOW_DAY, label="月报表", pad_grid=True,
        kind="month",
    )


def custom_report(start_text: Optional[str], end_text: Optional[str]) -> dict[str, Any]:
    """自由报表：默认最近 24 小时，按小时聚合，可跨天/跨月。

    按小时网格补齐：整段无数据时也返回完整网格（值 null）而不是空数组，
    否则前端拿到空列表，分不清"没查到"和"这段确实没数据"。
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
        kind="custom",
    )
