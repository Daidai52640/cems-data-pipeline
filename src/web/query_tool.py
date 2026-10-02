# -*- coding: utf-8 -*-
"""TDengine 查询工具：核对入库总量、最新明细，以及各测点的 INTERVAL 时间聚合均值。

用途：不依赖 Web 大屏，直接从命令行确认「采集链路是否真的在入库」——
入库总量长期不涨、最新明细停在同一时刻、或聚合窗口为空，都指向接入层/网关故障。

★ 设备维度（多设备部署必读）
-----------------------------------------------------------------------------
`cems_data` 是多设备共用的超级表（TAG = `plant` / `device`），三条查询**默认全表读**：
看得到"库里现在总共有什么"，这是排障时想要的（能一眼发现多出来一台设备的行）。
但全表读**不能**用来回答"某台设备的采样正不正常"：实测同一分钟混读 24 行
= `device1` 12 + `device2` 12，一台掉一半采样时混读仍然显示"正常"。
所以：

  - 不传 `--device`：结果里会显式提示"未按设备过滤，行数可能是多台之和"；
  - 传 `--device device1`：三条查询都加 `AND device = 'device1'`，才是单设备口径。

用法：
    python src/web/query_tool.py                     # 全表（含提示）
    python src/web/query_tool.py --device device2     # 只看 device2
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Final, Optional

import taosrest

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import COLUMNS   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================
# 连接参数支持环境变量覆盖，默认值与本机直接运行一致。
# 容器里执行：docker compose run --rm web python src/web/query_tool.py

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = os.getenv("TD_URL", "http://localhost:6041")
TD_USER: Final[str] = os.getenv("TD_USER", "root")
TD_PASS: Final[str] = os.getenv("TD_PASS", "taosdata")
TD_DB: Final[str] = os.getenv("TD_DB", "cems")
TD_STABLE: Final[str] = os.getenv("TD_STABLE", "cems_data")

# ---- 测点列：统一来自 src/common/points.py ----
TD_COLUMNS: Final[tuple[str, ...]] = COLUMNS

# ---- 查询参数 ----
LATEST_LIMIT: Final[int] = 5           # 最新明细取几条
INTERVAL_MINUTES: Final[int] = 30      # 聚合查询的时间范围（分钟）
INTERVAL_WINDOW: Final[str] = "1m"     # 窗口大小：1m=分钟均值，1h=小时均值

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("web.query_tool")

# ---- 设备维度（可选过滤）----
# 设备名会被拼进 SQL，所以这里做一次白名单校验（规则与 scripts/_device_scope.py 一致：
# 只允许字母/数字/下划线/中划线）。这条规则在 src/ 里就地实现，不从 scripts/ 引 —— 镜像只
# 拷贝 src/（见 Dockerfile），src 依赖 scripts/ 在容器里会 ImportError。
DEVICE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9_-]+$")

#: 不传 `--device` 时的显式提示。多设备下它是**必须说出来**的一句话：
#: 没有它，"最新 5 条 / 每窗口 24 行"会被读成"这台设备采样很密"。
NO_DEVICE_NOTICE: Final[str] = (
    "⚠️ 未按设备过滤（--device）：本次结果是全表口径，行数/条数可能是多台设备之和，"
    "不能据此判断单台设备的采样是否正常"
)


def device_condition(device: str) -> str:
    """把 `--device` 转成**裸条件** `device = 'device1'`；没传则返回空串（= 全表读）。

    返回裸条件而不是完整 WHERE：三条查询的 WHERE 形态不同（有的没有、有的带时间范围），
    带前导 `AND`/`WHERE` 会逼出 `WHERE 1 = 1 AND ...` 这种掩盖真实条件的写法。
    """
    scope = (device or "").strip()
    if not scope:
        return ""
    if not DEVICE_NAME_RE.match(scope):
        raise SystemExit(f"--device 取值非法（只允许字母/数字/下划线/中划线）: {scope!r}")
    return f"device = '{scope}'"


def scope_note(device: str) -> str:
    """给每条输出的表头配一句口径说明：按设备过滤 vs 全表（多台之和）。"""
    scope = (device or "").strip()
    return f"设备={scope}" if scope else "全表口径：行数/条数可能是多台设备之和"


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


def show_total(conn: Any, device: str = "") -> None:
    """打印入库总条数（给了设备就是该设备的条数，否则是全表条数）。"""
    condition = device_condition(device)
    where = f" WHERE {condition}" if condition else ""
    rows = run_query(conn, f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE}{where}")
    if rows:
        LOGGER.info("入库总条数（%s）: %s", scope_note(device), rows[0][0])


def show_latest(conn: Any, limit: int = LATEST_LIMIT, device: str = "") -> None:
    """打印最新 N 条原始数据（全表口径下是各设备混排的最近 N 条）。"""
    condition = device_condition(device)
    where = f" WHERE {condition}" if condition else ""
    rows = run_query(
        conn,
        f"SELECT ts, {', '.join(TD_COLUMNS)} FROM {TD_DB}.{TD_STABLE}{where} "
        f"ORDER BY ts DESC LIMIT {limit}",
    )
    if rows is None:      # 查询失败（错误已由 run_query 记录），不再误报成"无数据"
        return
    LOGGER.info("最新 %d 条原始数据（%s）:", limit, scope_note(device))
    for row in rows:
        LOGGER.info("  %s | %s", row[0], _format_values(row[1:]))
    if not rows:
        LOGGER.info("  （库中暂无数据，确认 subscriber_to_td.py 在跑）")


def show_interval_avg(
    conn: Any,
    minutes: int = INTERVAL_MINUTES,
    window: str = INTERVAL_WINDOW,
    device: str = "",
) -> None:
    """打印 INTERVAL 时间聚合结果（环保平台"分钟均值"曲线的做法）。

    ⚠️ 多设备下不过滤时，同一个窗口会把两台设备的行一起聚合（每窗口条数 = 两台之和），
    均值也是两台的混合 —— 只能当"全库有没有在写入"的证据，不能当单台设备的曲线。
    """
    condition = device_condition(device)
    averages = ", ".join(f"AVG({column}) AS avg_{column}" for column in TD_COLUMNS)
    device_clause = f" AND {condition}" if condition else ""
    rows = run_query(
        conn,
        f"SELECT _wstart, {averages}, COUNT(*) AS n "
        f"FROM {TD_DB}.{TD_STABLE} WHERE ts >= now - {minutes}m{device_clause} "
        f"INTERVAL({window})",
    )
    if rows is None:      # 查询失败，不再误报成"该时间段无数据"
        return
    LOGGER.info("★ INTERVAL(%s) 各测点均值（最近 %d 分钟，每窗口一条；%s）:",
                window, minutes, scope_note(device))
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

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """命令行参数：只有 `--device` 一个（默认空 = 保留排障用的全表读）。"""
    parser = argparse.ArgumentParser(
        description="TDengine 排障查询（总量 / 最新明细 / 窗口均值）",
    )
    parser.add_argument(
        "--device",
        default="",
        help="可选：按设备 TAG 过滤（例如 device1）。不传 = 全表读，"
        "输出里会显式提示'行数可能是多台设备之和'",
    )
    return parser.parse_args(argv)


def main() -> None:
    """依次执行三类查询（总量 / 最新明细 / 窗口聚合），单项失败不影响其余查询。"""
    setup_logging()
    args = parse_args()
    device = (args.device or "").strip()

    conn = connect_td()
    if conn is None:
        LOGGER.error("TDengine 不可用，请确认容器已启动（docker start tdengine）后重试")
        return

    # ★ 全表读（默认）必须显式提示口径：多设备下"每窗口 24 行 / 最新 5 条"很容易被
    #   读成"单台设备采样正常"，而这正是本工具最容易误导人的地方。
    if not device:
        LOGGER.warning(NO_DEVICE_NOTICE)

    try:
        show_total(conn, device)
        show_latest(conn, device=device)
        show_interval_avg(conn, device=device)
    except Exception:
        LOGGER.exception("查询工具异常退出")
    finally:
        try:
            conn.close()
        except Exception as exc:
            LOGGER.debug("关闭 TDengine 连接时出错（忽略）: %s", exc)
        LOGGER.info("查询完成")


if __name__ == "__main__":
    main()
