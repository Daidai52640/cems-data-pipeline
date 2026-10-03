# -*- coding: utf-8 -*-
"""实时大屏的自由区间曲线：按跨度自动分级降采样，聚合复用 report.py 的同一套口径。

为什么不能靠调大 QUERY_MINUTES：/api/data 是 `ORDER BY ts DESC LIMIT n` 的原始点查询，
窗口一放大就会被静默截断成"最新 n 个点"，前端看起来像前面那段没数据，且不报错。
所以自由区间走这个接口，长区间在 TDengine 侧按 1 分钟/1 小时/1 天聚合。

聚合粒度下，响应里额外带 report.py 算好的 `coverage` 汇总（见 src/web/report.py 头部），
本模块不重算覆盖率，也不新增窗口语义 —— 覆盖率只有一份口径。
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final, Optional

# 让 src/common 与 src/web 能被导入：三种启动方式都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import COLUMNS   # noqa: E402
from src.web import report   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================

# 按跨度分级选粒度：跨度越大窗口越粗，保证点数可控、聚合都在库侧完成
CURVE_RAW_MAX_HOURS: Final[float] = float(os.getenv("CURVE_RAW_MAX_HOURS", "2"))
CURVE_MINUTE_MAX_HOURS: Final[float] = float(os.getenv("CURVE_MINUTE_MAX_HOURS", "48"))
CURVE_HOUR_MAX_DAYS: Final[float] = float(os.getenv("CURVE_HOUR_MAX_DAYS", "31"))
# 单次返回的点数上限：既当原始点查询的 LIMIT，也当聚合窗口数的兜底上限
CURVE_MAX_POINTS: Final[int] = int(os.getenv("CURVE_MAX_POINTS", "10000"))

UNIT_RAW: Final[str] = "raw"
WINDOW_BY_UNIT: Final[dict[str, timedelta]] = {
    report.UNIT_MINUTE: report.WINDOW_MINUTE,
    report.UNIT_HOUR: report.WINDOW_HOUR,
    report.UNIT_DAY: report.WINDOW_DAY,
}
# 前端要显式告诉用户"当前画的是原始点还是均值"，文案统一放这里
UNIT_LABELS: Final[dict[str, str]] = {
    UNIT_RAW: "原始点",
    report.UNIT_MINUTE: "1 分钟均值",
    report.UNIT_HOUR: "1 小时均值",
    report.UNIT_DAY: "1 天均值",
}

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("web.curve")


class CurveParamError(ValueError):
    """曲线参数不合法（时间格式错、start>=end、点数超上限）—— 接口层转 400。"""


# ==================== 2. 粒度选择与取数 ====================

def pick_unit(span: timedelta) -> str:
    """按跨度选粒度：≤2 小时原始点，≤48 小时 1 分钟，≤31 天 1 小时，再长 1 天。"""
    if span <= timedelta(hours=CURVE_RAW_MAX_HOURS):
        return UNIT_RAW
    if span <= timedelta(hours=CURVE_MINUTE_MAX_HOURS):
        return report.UNIT_MINUTE
    if span <= timedelta(days=CURVE_HOUR_MAX_DAYS):
        return report.UNIT_HOUR
    return report.UNIT_DAY


def columns_from_raw(rows: list[tuple[Any, ...]]) -> dict[str, list[Any]]:
    """原始行 -> 列式结构（与 /api/data 同形状，前端可以复用同一个渲染函数）。"""
    payload: dict[str, list[Any]] = {"ts": [report.format_ts(row[0]) for row in rows]}
    for index, column in enumerate(COLUMNS, start=1):
        payload[column] = [
            round(float(row[index]), report.ROUND_DIGITS) if row[index] is not None else None
            for row in rows
        ]
    return payload


def columns_from_points(points: list[dict[str, Any]]) -> dict[str, list[Any]]:
    """聚合点 -> 列式结构；空窗口的 null 原样保留，前端据此断线。"""
    payload: dict[str, list[Any]] = {"ts": [point["ts"] for point in points]}
    for column in COLUMNS:
        payload[column] = [point.get(column) for point in points]
    return payload


def build_curve(start_text: Optional[str], end_text: Optional[str], scope: str = "") -> dict[str, Any]:
    """取一段区间的曲线数据（列式）+ 粒度元信息。

    参数解析、上界收窄、聚合口径全部复用 report.py，曲线路径不另写一套 SQL 口径。
    聚合粒度下额外带上 report.py 算好的覆盖率汇总（`coverage`）；
    原始点粒度没有"窗口"这个概念，**不加**该字段（字段只加不删，不硬塞 null 占位）。
    """
    now = datetime.now()
    start = report.parse_time(start_text, "start", now - timedelta(hours=1))
    end = report.clamp_to_now(report.parse_time(end_text, "end", now), now)
    if start >= end:
        raise CurveParamError("start 必须早于 end")

    unit = pick_unit(end - start)
    coverage: Optional[dict[str, Any]] = None
    if unit == UNIT_RAW:
        # 原始点区间可能覆盖"当前秒"（跨度 ≤ CURVE_RAW_MAX_HOURS 的默认区间就包含当下），
        # 明确声明不可缓存：kind=None 会让 aggregate/query_raw 走无缓存出口。
        payload = columns_from_raw(report.query_raw(start, end, CURVE_MAX_POINTS, scope))
    else:
        series = report.aggregate_series(
            start, end, unit, WINDOW_BY_UNIT[unit],
            label="曲线", pad_grid=True, max_points=CURVE_MAX_POINTS,
            kind="curve", scope=scope,
        )
        start, end = report.parse_time(series["start"], "start", start), \
            report.parse_time(series["end"], "end", end)
        payload = columns_from_points(series["points"])
        coverage = series["coverage"]

    payload.update({
        "unit": unit,
        "unit_label": UNIT_LABELS[unit],
        "downsampled": unit != UNIT_RAW,
        "points": len(payload["ts"]),
        "start": start.strftime(report.TS_FORMAT),
        "end": end.strftime(report.TS_FORMAT),
    })
    if coverage is not None:
        payload["coverage"] = coverage
    LOGGER.debug(
        "曲线 %s ~ %s 粒度=%s 点数=%d", payload["start"], payload["end"], unit, payload["points"],
    )
    return payload
