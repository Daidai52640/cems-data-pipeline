# -*- coding: utf-8 -*-
"""Modbus TCP 仿真设备：模拟 CEMS 分析仪，用保持寄存器对外提供 8 个烟气测点的实时数据。"""

from __future__ import annotations

import logging
import os
import random
import sys
import threading
import time
from pathlib import Path
from typing import Final

from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.server import StartTcpServer

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS, REG_BASE, REG_COUNT, SCALE   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================
# 监听地址/端口支持环境变量覆盖，默认值与本机直接运行一致；容器部署时由 compose 注入。

# ---- 服务监听 ----
SERVER_HOST: Final[str] = os.getenv("SERVER_HOST", "0.0.0.0")   # 监听所有网卡，同机/跨机都可访问
SERVER_PORT: Final[int] = int(os.getenv("SERVER_PORT", "5020"))  # 用 5020 避开系统保留的 502 端口
SLAVE_ID: Final[int] = int(os.getenv("SLAVE_ID", "1"))           # 从站地址，网关必须按这个地址读

# ---- 寄存器地图 ----
# 测点定义（字段名/地址/量程/单位）与换算系数 SCALE、寄存器数量 REG_COUNT
# 统一来自 src/common/points.py，这里不再重复维护一份。
UPDATE_INTERVAL: Final[float] = 2.0    # 数据刷新周期（秒）

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("device.modbus_server")


# ==================== 2. 工具函数 ====================

def setup_logging() -> None:
    """初始化日志：控制台输出，级别由 LOG_LEVEL 统一控制。"""
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        force=True,
    )


def encode(value: float, scale: int = SCALE) -> int:
    """把真实值按 1 位小数取整后放大 scale 倍，转成寄存器可存的整数。

    ⚠️ scale 是**每个测点自带**的（见 points.py）：Modbus 保持寄存器是 16 位无符号，
    流量这类大数必须配更小的 scale，否则会溢出。
    """
    return int(round(round(value, 1) * scale))


def build_registers() -> list[int]:
    """生成一轮仿真读数对应的寄存器数组（按寄存器地址摆放，未用到的地址补 0）。"""
    values = [0] * REG_COUNT
    for point in POINTS:
        values[point.address] = encode(random.uniform(point.low, point.high), point.scale)
    return values


# ==================== 3. 数据刷新线程 ====================

def update_data(context: ModbusServerContext) -> None:
    """后台线程主循环：周期性刷新寄存器里的仿真数据。"""
    while True:
        try:
            values = build_registers()
            # 写入从站 SLAVE_ID 的保持寄存器（功能码 3 对应 hr 区），起始地址 REG_BASE
            context[SLAVE_ID].setValues(3, REG_BASE, values)
            LOGGER.debug(
                "寄存器已刷新: %s",
                " ".join(
                    f"{point.name}={values[point.address] / SCALE}{point.unit}"
                    for point in POINTS
                ),
            )
        except Exception:
            # 单次刷新失败绝不能让线程退出，否则设备会永远返回旧值
            LOGGER.exception("刷新仿真寄存器失败，%.0f 秒后重试", UPDATE_INTERVAL)
        time.sleep(UPDATE_INTERVAL)


# ==================== 4. 主流程 ====================

def main() -> None:
    """初始化寄存器数据块并启动 Modbus TCP 服务端（阻塞运行）。"""
    setup_logging()

    # 初始化寄存器数据块（地址 0~9，初始全 0）
    block = ModbusSequentialDataBlock(0, [0] * REG_COUNT)
    # 演示项目简化：四个区共用同一个 block，教学够用
    slave = ModbusSlaveContext(di=block, co=block, hr=block, ir=block)
    context = ModbusServerContext(slaves=slave, single=True)

    # 后台线程：持续更新仿真数据
    threading.Thread(
        target=update_data,
        args=(context,),
        name="update-data",
        daemon=True,
    ).start()

    LOGGER.info("Modbus 仿真设备启动: %s:%d 从站地址=%d", SERVER_HOST, SERVER_PORT, SLAVE_ID)
    LOGGER.info(
        "寄存器地图(×%d): %s",
        SCALE,
        ", ".join(f"地址{point.address}={point.name}" for point in POINTS),
    )

    try:
        StartTcpServer(context=context, address=(SERVER_HOST, SERVER_PORT))
    except OSError as exc:
        # 端口被占/无权限属于启动失败：必须以非 0 退出码结束，
        # 否则编排层（compose / 脚本）看到的是"正常退出"，不会告警也不会重启
        LOGGER.critical("Modbus 服务端启动失败（端口 %d 可能被占用）: %s", SERVER_PORT, exc)
        # 先记日志再退出，退出码交给上层判断
        raise SystemExit(1) from exc
    except Exception:
        LOGGER.exception("Modbus 服务端异常退出")
        raise SystemExit(1)
    finally:
        LOGGER.info("Modbus 仿真设备已停止")


if __name__ == "__main__":
    main()
