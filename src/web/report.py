# -*- coding: utf-8 -*-
"""报表聚合：分钟/日/月/自由四类时间维度均值统计，聚合全部在 TDengine 侧完成。

对外只暴露四个函数，返回值统一为：
    {"unit": "1m|1h|1d", "start": "...", "end": "...", "points": [{...}, ...]}
每个 point 形如 {"ts": "...", "so2": 29.87, ..., "n": 7}；
窗口内无数据时测点值为 null（不是 0），n 为 0。
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
) -> list[tuple[Any, ...]]:
    """按窗口聚合求均值；聚合在 TDengine 侧用 INTERVAL + AVG 完成，不把原始点拉回 Python。

    拼进 SQL 的时间一律由已解析的 datetime 重新格式化得到，窗口串是代码里的常量，
    列名来自测点契约白名单 —— 没有任何一处拼接原始用户输入。
    区间统一用半开写法 [start, end)：调用方（aggregate_series）已把范围向内对齐到窗口边界，
    所以每个窗口都是完整的，不会出现"半个窗口被当成整窗均值"。
    """
    avg_columns = ", ".join(f"AVG({column})" for column in COLUMNS)
    sql = (
        f"SELECT _wstart, {avg_columns}, COUNT(*) FROM {TD_DB}.{TD_STABLE} "
        f"WHERE ts >= '{start.strftime(TS_FORMAT)}' AND ts < '{end.strftime(TS_FORMAT)}' "
        f"INTERVAL({window})"
    )
    if fill_null:
        # 日报表要固定 24 个整点就必须加 FILL(NULL)：不加只会返回有数据的那些小时。
        # FILL(0) 在 TDengine 3.x 会报 syntax error，别用。
        sql += " FILL(NULL)"
    # 没有数据的区间会返回 0 行且不报错，这里原样返回空列表，由上层组装成空/NULL 序列
    return _execute(f"{sql} ORDER BY _wstart ASC")


def _execute(sql: str) -> list[tuple[Any, ...]]:
    """执行查询；任何跨系统异常都包装成 ReportQueryError，不让第三方异常外泄。"""
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


def format_ts(value: Any) -> str:
    """把 TDengine 返回的时间戳统一成 'YYYY-MM-DD HH:MM:SS'。"""
    if isinstance(value, datetime):
        return value.strftime(TS_FORMAT)
    return str(value)[:19]


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


def envelope(
    unit: str,
    start: datetime,
    end: datetime,
    points: list[dict[str, Any]],
) -> dict[str, Any]:
    """组装统一返回结构。"""
    return {
        "unit": unit,
        "start": start.strftime(TS_FORMAT),
        "end": end.strftime(TS_FORMAT),
        "points": points,
    }


def clamp_to_now(end: datetime, now: datetime) -> datetime:
    """查询上界统一不超过当前时刻，避免把未来时间戳的脏数据算进报表。"""
    if end > now:
        LOGGER.debug("end=%s 超过当前时刻，已收窄到 %s", end, now)
        return now
    return end


def query_raw(start: datetime, end: datetime, limit: int) -> list[tuple[Any, ...]]:
    """原始点查询（曲线的短区间用）：不聚合，直接取原始行。

    列名来自测点契约白名单，时间来自已解析的 datetime，没有任何原始输入拼接进 SQL。
    """
    columns = ", ".join(("ts", *COLUMNS))
    sql = (
        f"SELECT {columns} FROM {TD_DB}.{TD_STABLE} "
        f"WHERE ts >= '{start.strftime(TS_FORMAT)}' AND ts <= '{end.strftime(TS_FORMAT)}' "
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
) -> dict[str, Any]:
    """把一段区间聚合成均值序列 —— 报表页、Excel 导出、曲线三处共用这一份口径。

    做三件事：范围向内对齐到窗口边界（只统计完整窗口）、在库侧用 INTERVAL + AVG 聚合、
    可选地按窗口网格补 null（整段无数据时也返回完整网格而不是空数组）。
    """
    aligned_start, aligned_end = align_range(start, end, window)
    if aligned_start >= aligned_end:            # 不足一个完整窗口
        return envelope(unit, aligned_start, aligned_end, [])
    check_span(aligned_start, aligned_end, window, label, max_points)

    rows = query_aggregate(aligned_start, aligned_end, unit, fill_null=pad_grid)
    if pad_grid:
        count = int((aligned_end - aligned_start) / window)
        grid = [aligned_start + index * window for index in range(count)]
        points = points_on_grid(rows, grid)
    else:
        points = rows_to_points(rows)
    return envelope(unit, aligned_start, aligned_end - timedelta(seconds=1), points)


# ==================== 4. 四类报表 ====================

def minute_report(start_text: Optional[str], end_text: Optional[str]) -> dict[str, Any]:
    """分钟报表：默认最近 1 小时，按分钟聚合（只统计完整分钟窗口）。"""
    now = datetime.now()
    start = parse_time(start_text, "start", now - WINDOW_HOUR)
    end = clamp_to_now(parse_time(end_text, "end", now), now)
    return aggregate_series(
        start, end, UNIT_MINUTE, WINDOW_MINUTE, label="分钟报表", pad_grid=False,
    )


def day_report(date_text: Optional[str]) -> dict[str, Any]:
    """日报表：默认今天，按小时聚合，固定 24 个整点。"""
    first = parse_date(date_text, "date", datetime.now())
    first = first.replace(hour=0, minute=0, second=0, microsecond=0)
    next_day = first + WINDOW_DAY
    # 用半开区间 [当天 00:00, 次日 00:00)，否则跨到次日会产生第 25 个窗口
    return aggregate_series(
        first, next_day, UNIT_HOUR, WINDOW_HOUR, label="日报表", pad_grid=True,
    )


def month_report(year_text: Optional[str], month_text: Optional[str]) -> dict[str, Any]:
    """月报表：默认当月，按天聚合；按当月实际天数补齐 28/30/31（闰年 2 月 29）。"""
    now = datetime.now()
    year = parse_int(year_text, "year", 1970, 9999, now.year)
    month = parse_int(month_text, "month", 1, 12, now.month)
    first = datetime(year, month, 1)
    days = calendar.monthrange(year, month)[1]      # 闰年自动给 29
    next_month = first + timedelta(days=days)
    return aggregate_series(
        first, next_month, UNIT_DAY, WINDOW_DAY, label="月报表", pad_grid=True,
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
    )
