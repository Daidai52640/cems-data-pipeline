# -*- coding: utf-8 -*-
"""Modbus TCP 仿真设备：模拟 CEMS 分析仪，用保持寄存器对外提供 9 个烟气测点的实时数据。

仿真读数不在这里生成，而是委托给 src/device/simulator.py（有惯性/日周期/可复现）；
本模块只负责"取读数 → 编码 → 写保持寄存器"，以及 Modbus TCP 服务本身。
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Final, Optional

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
from src.device.simulator import (                                 # noqa: E402
    SIM_CLOCK_SKEW_SECONDS,
    SIM_EPOCH,
    SIMULATOR,
    SPIKE_AMPLITUDE_HIGH,
    SPIKE_AMPLITUDE_LOW,
    SPIKE_ENABLED,
    SPIKE_PROBABILITY,
    CemsSimulator,
)

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

# ---- 仿真信号 ----
# 读数不再是 random.uniform(low, high)（每个周期全部重掷、前后无关联），
# 而是由 src/device/simulator.py 按「基线 + 日周期 + 缓慢漂移 + 小毛刺」生成，
# 并且可复现：种子取 SIM_SEED（未设置时用 simulator.DETERMINISTIC_DEFAULT_SEED），
# 时刻由 SIM_CLOCK_SKEW / 本机时钟决定。要调信号特征请改 simulator.py 的配置区。
SIM_SEED_DISPLAY: Final[str] = os.getenv("SIM_SEED", "").strip() or "默认(20261001)"

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
    """把真实值放大 scale 倍并四舍五入，转成寄存器可存的整数。

    ⚠️ scale 是**每个测点自带**的（见 points.py）：Modbus 保持寄存器是 16 位无符号，
    流量这类大数必须配更小的 scale，否则会溢出。

    ⚠️⚠️ 2026-10-01 修正：原来写成 `round(round(value, 1) * scale)`，
    内层先把真实值量化到 **0.1**，scale 只把那个 0.1 的倍数放大 ——
    结果是**任何 scale 的交付分辨率都恒为 0.1**，把 Dust 的 scale 从 10 提到 100 等于没改。
    实测：2880 个采样里，scale=10 与 scale=100 的交付值**一个都不同不了**；
    去掉内层预量化后，Dust 的 24 小时不同取值从 23 个升到 204 个。
    正确做法是**只量化一次**，量化步长由 scale 决定（分辨率 = 1/scale）。
    """
    return int(round(value * scale))


def build_registers(
    simulator: CemsSimulator = SIMULATOR,
    now: Optional[datetime] = None,
) -> list[int]:
    """生成一轮仿真读数对应的寄存器数组（按寄存器地址摆放，未用到的地址补 0）。

    时刻 now 显式可传：默认取当前时钟；验证脚本传"假时钟"就能扫一整天，
    不需要真的等 24 小时（日周期由 now 参数驱动，见 simulator.py）。
    """
    values = [0] * REG_COUNT
    readings = simulator.sample_all(simulator.simulate_clock(now))
    for point in POINTS:
        values[point.address] = encode(readings[point.name], point.scale)
    return values


# ==================== 3. 数据刷新线程 ====================

def update_data(context: ModbusServerContext) -> None:
    """后台线程主循环：周期性刷新寄存器里的仿真数据。

    用"绝对下次唤醒时刻"而不是固定 sleep(UPDATE_INTERVAL)：仿真读数由时钟驱动，
    一轮的耗时波动（编码、写寄存器）会让固定 sleep 越漂越慢，绝对节拍能把周期拉回来。
    """
    next_tick = time.monotonic()
    while True:
        try:
            values = build_registers()
            # 写入从站 SLAVE_ID 的保持寄存器（功能码 3 对应 hr 区），起始地址 REG_BASE
            context[SLAVE_ID].setValues(3, REG_BASE, values)
            if LOGGER.isEnabledFor(logging.DEBUG):
                # 逐点拼接只在开 DEBUG 时做：默认 INFO 下每 2 秒白拼一个字符串是浪费
                LOGGER.debug(
                    "寄存器已刷新: %s",
                    " ".join(
                        f"{point.name}={values[point.address] / point.scale}{point.unit}"
                        for point in POINTS
                    ),
                )
        except Exception:
            # 单次刷新失败绝不能让线程退出，否则设备会永远返回旧值
            LOGGER.exception("刷新仿真寄存器失败，%.0f 秒后重试", UPDATE_INTERVAL)
        next_tick += UPDATE_INTERVAL
        time.sleep(max(0.0, next_tick - time.monotonic()))


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
    # 可复现性靠这三行日志留痕：出问题时按这里的种子/时钟就能原样重放同一段曲线
    LOGGER.info(
        "仿真信号已启用: 种子=%s 刷新=%ss 时间基准=%s 时钟偏移=%.0fs",
        SIM_SEED_DISPLAY, UPDATE_INTERVAL, SIM_EPOCH.isoformat(), SIM_CLOCK_SKEW_SECONDS,
    )
    LOGGER.info(
        "污染物尖峰: 开关=%s 概率=%.4f/周期 幅度=%.2f~%.2f 限值（只作用于 %s）",
        SPIKE_ENABLED, SPIKE_PROBABILITY,
        SPIKE_AMPLITUDE_LOW, SPIKE_AMPLITUDE_HIGH,
        "Dust/SO2/NOx",
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
