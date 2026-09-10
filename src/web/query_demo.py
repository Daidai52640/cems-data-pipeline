# -*- coding: utf-8 -*-
"""TDengine 查询演示脚本：查入库总量、最新明细与各测点的 INTERVAL 时间聚合均值。"""

from __future__ import annotations

import logging
from typing import Any, Final, Optional

import taosrest

# ==================== 1. 配置区（要改参数只动这里） ====================

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = "http://localhost:6041"
TD_USER: Final[str] = "root"
TD_PASS: Final[str] = "taosdata"
TD_DB: Final[str] = "cems"
TD_STABLE: Final[str] = "cems_data"

# ---- 测点列（与超级表列名一致，改动后此处同步即可）----
TD_COLUMNS: Final[tuple[str, ...]] = (
    "so2", "nox", "flow", "dust", "o2", "temp", "humidity", "pressure",
)

# ---- 查询参数 ----
LATEST_LIMIT: Final[int] = 5           # 最新明细取几条
INTERVAL_MINUTES: Final[int] = 30      # 聚合查询的时间范围（分钟）
INTERVAL_WINDOW: Final[str] = "1m"     # 窗口大小：1m=分钟均值，1h=小时均值

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("web.query_demo")


# ==================== 2. 日志 ====================

def setup_logging() -> None:
    """初始化日志：控制台输出，级别由 LOG_LEVEL 统一控制。"""
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        force=True,
    )


# ==================== 3. 查询函数（每个函数各自兜住异常） ====================

def connect_td() -> Optional[Any]:
    """连接 TDengine 并做一次探测查询，失败返回 None（不抛异常）。

    注意：taosrest.connect() 是惰性连接、不发请求，必须探测一次才能确认真的连得上。
    """
    try:
        conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
        cur = conn.cursor()
        cur.execute("SHOW DATABASES")     # 探测：真正打到 taosAdapter 才算连上
        LOGGER.info("TDengine 已连接: %s", TD_URL)
        return conn
    except Exception as exc:
        LOGGER.error("连接 TDengine 失败: %s", exc)
        return None


def run_query(conn: Any, sql: str) -> Optional[list[tuple[Any, ...]]]:
    """执行一条查询 SQL，失败返回 None（不抛异常，异常已记 error）。"""
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return list(cur.fetchall())
    except Exception as exc:
        LOGGER.error("查询失败: %s | SQL: %s", exc, sql)
        return None


def show_total(conn: Any) -> None:
    """打印入库总条数。"""
    rows = run_query(conn, f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE}")
    if rows:
        LOGGER.info("入库总条数: %s", rows[0][0])


def show_latest(conn: Any, limit: int = LATEST_LIMIT) -> None:
    """打印最新 N 条原始数据。"""
    rows = run_query(
        conn,
        f"SELECT ts, {', '.join(TD_COLUMNS)} FROM {TD_DB}.{TD_STABLE} "
        f"ORDER BY ts DESC LIMIT {limit}",
    )
    if rows is None:      # 查询失败（错误已由 run_query 记录），不再误报成"无数据"
        return
    LOGGER.info("最新 %d 条原始数据:", limit)
    for row in rows:
        LOGGER.info("  %s | %s", row[0], _format_values(row[1:]))
    if not rows:
        LOGGER.info("  （库中暂无数据，确认 subscriber_to_td.py 在跑）")


def show_interval_avg(
    conn: Any,
    minutes: int = INTERVAL_MINUTES,
    window: str = INTERVAL_WINDOW,
) -> None:
    """打印 INTERVAL 时间聚合结果（环保平台"分钟均值"曲线的做法）。"""
    averages = ", ".join(f"AVG({column}) AS avg_{column}" for column in TD_COLUMNS)
    rows = run_query(
        conn,
        f"SELECT _wstart, {averages}, COUNT(*) AS n "
        f"FROM {TD_DB}.{TD_STABLE} WHERE ts >= now - {minutes}m INTERVAL({window})",
    )
    if rows is None:      # 查询失败，不再误报成"该时间段无数据"
        return
    LOGGER.info("★ INTERVAL(%s) 各测点均值（最近 %d 分钟，每窗口一条）:", window, minutes)
    for row in rows:
        LOGGER.info("  窗口起点 %s | %s | 原始条数 %s", row[0], _format_values(row[1:-1]), row[-1])
    if not rows:
        LOGGER.info("  （该时间范围内没有数据，确认 subscriber_to_td.py 在跑）")


def _format_values(values: tuple[Any, ...]) -> str:
    """把一行测点值按列名格式化成 "so2=35.2 nox=18.5 ..."，并保留两位小数。"""
    parts = []
    for column, value in zip(TD_COLUMNS, values):
        parts.append(f"{column}={round(value, 2) if value is not None else None}")
    return " ".join(parts)


# ==================== 4. 主流程 ====================

def main() -> None:
    """依次执行三类查询，单项失败不影响其余查询。"""
    setup_logging()

    conn = connect_td()
    if conn is None:
        LOGGER.error("TDengine 不可用，请确认容器已启动（docker start tdengine）后重试")
        return

    try:
        show_total(conn)
        show_latest(conn)
        show_interval_avg(conn)
    except Exception:
        LOGGER.exception("查询演示异常退出")
    finally:
        try:
            conn.close()
        except Exception as exc:
            LOGGER.debug("关闭 TDengine 连接时出错（忽略）: %s", exc)
        LOGGER.info("查询演示结束")


if __name__ == "__main__":
    main()
