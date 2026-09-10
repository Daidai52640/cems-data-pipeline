# -*- coding: utf-8 -*-
"""Modbus TCP 仿真设备：模拟 CEMS 分析仪，用保持寄存器对外提供 SO2/NOx/Flow 实时数据。"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Final

from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.server import StartTcpServer

# ==================== 1. 配置区（要改参数只动这里） ====================

# ---- 服务监听 ----
SERVER_HOST: Final[str] = "0.0.0.0"    # 监听所有网卡，网关可在同机或跨机访问
SERVER_PORT: Final[int] = 5020         # 用 5020 避开系统保留的 502 端口
SLAVE_ID: Final[int] = 1               # 从站地址，网关必须按这个地址读

# ---- 寄存器地图（与网关的"内存地图"约定，不可随意改动）----
REG_SO2: Final[int] = 0                # 地址 0：SO2，真实值 ×10 存整数
REG_NOX: Final[int] = 1                # 地址 1：NOx，真实值 ×10 存整数
REG_FLOW: Final[int] = 2               # 地址 2：Flow，真实值 ×10 存整数
REG_COUNT: Final[int] = 10             # 寄存器总数（0~9，后 7 个备用）
SCALE: Final[int] = 10                 # 放大系数：35.2 → 352（Modbus 寄存器只能存整数）

# ---- 仿真数据范围（真实设备读数会小幅波动）----
SO2_RANGE: Final[tuple[float, float]] = (20.0, 50.0)     # mg/m3
NOX_RANGE: Final[tuple[float, float]] = (10.0, 30.0)     # mg/m3
FLOW_RANGE: Final[tuple[float, float]] = (80.0, 120.0)   # m3/s
UPDATE_INTERVAL: Final[float] = 2.0                      # 数据刷新周期（秒）

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


def encode(value: float) -> int:
    """把真实值按 1 位小数取整后放大 SCALE 倍，转成寄存器可存的整数。"""
    return int(round(round(value, 1) * SCALE))


def build_registers() -> list[int]:
    """生成一轮仿真读数对应的寄存器数组（前 3 个为测点，其余补 0）。"""
    return [
        encode(random.uniform(*SO2_RANGE)),
        encode(random.uniform(*NOX_RANGE)),
        encode(random.uniform(*FLOW_RANGE)),
    ] + [0] * (REG_COUNT - 3)


# ==================== 3. 数据刷新线程 ====================

def update_data(context: ModbusServerContext) -> None:
    """后台线程主循环：周期性刷新寄存器里的仿真数据。"""
    while True:
        try:
            values = build_registers()
            # 写入从站 SLAVE_ID 的保持寄存器（功能码 3 对应 hr 区），起始地址 REG_SO2
            context[SLAVE_ID].setValues(3, REG_SO2, values)
            LOGGER.debug(
                "寄存器已刷新: SO2=%.1f NOx=%.1f Flow=%.1f",
                values[REG_SO2] / SCALE,
                values[REG_NOX] / SCALE,
                values[REG_FLOW] / SCALE,
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
        "寄存器地图: 地址%d=SO2(×%d), 地址%d=NOx(×%d), 地址%d=Flow(×%d)",
        REG_SO2, SCALE, REG_NOX, SCALE, REG_FLOW, SCALE,
    )

    try:
        StartTcpServer(context=context, address=(SERVER_HOST, SERVER_PORT))
    except OSError as exc:
        LOGGER.error("Modbus 服务端启动失败（端口 %d 可能被占用）: %s", SERVER_PORT, exc)
    except Exception:
        LOGGER.exception("Modbus 服务端异常退出")
    finally:
        LOGGER.info("Modbus 仿真设备已停止")


if __name__ == "__main__":
    main()
