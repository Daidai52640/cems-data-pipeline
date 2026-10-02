# -*- coding: utf-8 -*-
"""Modbus TCP 仿真设备：模拟 CEMS 分析仪，用保持寄存器对外提供 9 个烟气测点的实时数据。

仿真读数不在这里生成，而是委托给 src/device/simulator.py（有惯性/日周期/可复现）；
本模块只负责"取读数 → 编码 → 写保持寄存器"，以及 Modbus TCP 服务本身。

★ 多台设备（多从站）：
    一个 TCP 服务端可以同时对外提供**多个从站地址**（`SLAVE_IDS=1,2`），
    每个从站有自己的寄存器数据块**和自己的仿真器实例**（种子/相位不同 ⇒ 曲线不同）。
    这对应现场最常见的形态：一条链路（一个 IP:端口）后面挂着多台仪表，
    数采侧按 unit id 分别读取 —— 所以第二台设备**不需要第二个端口、第二个容器**。
    默认 `SLAVE_IDS=1`：单从站走 `single=True` 的原有代码路径，行为与改造前一致。
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Final, Mapping, Optional, Sequence

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
    seed_from_env,
)

# ==================== 1. 配置区（要改参数只动这里） ====================
# 监听地址/端口支持环境变量覆盖，默认值与本机直接运行一致；容器部署时由 compose 注入。

# ---- 服务监听 ----
SERVER_HOST: Final[str] = os.getenv("SERVER_HOST", "0.0.0.0")   # 监听所有网卡，同机/跨机都可访问
SERVER_PORT: Final[int] = int(os.getenv("SERVER_PORT", "5020"))  # 用 5020 避开系统保留的 502 端口
SLAVE_ID: Final[int] = int(os.getenv("SLAVE_ID", "1"))           # 从站地址，网关必须按这个地址读

#: Modbus TCP 规范里合法的单元标识符范围（0 保留给广播，248~255 保留）
SLAVE_ID_MIN: Final[int] = 1
SLAVE_ID_MAX: Final[int] = 247


def parse_slave_ids(raw: str, fallback: int) -> tuple[int, ...]:
    """把 `SLAVE_IDS`（逗号分隔，如 "1,2"）解析成从站号元组；空值回落到 `SLAVE_ID`。

    - 空/未设置 ⇒ `(SLAVE_ID,)`，即改造前的单从站形态
    - 非法值（非数字、超范围、重复）⇒ 抛 ValueError，在启动阶段就拦下，
      而不是等网关读不到数据才发现自己写错了从站号
    """
    if not raw:
        return (fallback,)
    ids: list[int] = []
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        if not text.isdigit():
            raise ValueError(f"SLAVE_IDS 里的 {text!r} 不是数字（形如 \"1,2\"）")
        slave_id = int(text)
        if not SLAVE_ID_MIN <= slave_id <= SLAVE_ID_MAX:
            raise ValueError(
                f"SLAVE_IDS 里的 {slave_id} 超出合法从站号范围 "
                f"[{SLAVE_ID_MIN}, {SLAVE_ID_MAX}]"
            )
        if slave_id in ids:
            raise ValueError(f"SLAVE_IDS 里的从站号 {slave_id} 重复")
        ids.append(slave_id)
    if not ids:
        return (fallback,)
    return tuple(ids)


SLAVE_IDS: Final[tuple[int, ...]] = parse_slave_ids(os.getenv("SLAVE_IDS", "").strip(), SLAVE_ID)
#: 第 n 台（n≥2）相对第 1 台的**相位偏移**（秒）。默认 10800 = 3 小时：
#: 让两台设备的日周期曲线明显错开，一眼能看出"这是两台不同的设备"（而不是同一条曲线）。
#: 它只改读数所用的仿真时刻，不改网关打的时间戳，所以不会被误读成设备时钟故障。
SLAVE_PHASE_OFFSET: Final[float] = float(os.getenv("SLAVE_PHASE_OFFSET", "10800"))


def device_simulators(
    slave_ids: Sequence[int],
    base_seed: int,
    phase_offset: float,
) -> dict[int, CemsSimulator]:
    """为每个从站建一个**独立**的仿真器（多台设备不能共用同一份读数）。

    - 第 n 台：种子 = `base_seed + (n-1)`，相位 = `(n-1) × phase_offset`
      ⇒ 基线水平、日周期峰值时刻、漂移形状、尖峰幅度全都不同，且与调用顺序无关
    - 第 1 台：种子 = `base_seed`（= SIM_SEED）、相位 = 0
      ⇒ **与改造前的单设备读数逐值一致**（这是"不回归"的硬要求）
    """
    return {
        slave_id: CemsSimulator(
            seed=base_seed + index,
            phase_offset_seconds=index * phase_offset,
        )
        for index, slave_id in enumerate(slave_ids)
    }


#: 仿真信号的基础种子（SIM_SEED）；各从站在它之上按序号派生
BASE_SEED: Final[int] = seed_from_env()

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

def build_slave_context() -> ModbusSlaveContext:
    """建一个从站的寄存器数据块（地址 0~REG_COUNT-1，初始全 0）。

    实现简化：四个寄存器区（di/co/hr/ir）共用同一个 block —— 本仿真只读写保持寄存器，
    不需要区分寄存器区，也就不必让四个区各自持有一份数据。
    ⚠️ 每个从站必须是**独立**的 block：共用一个 block 就等于两台设备读同一份寄存器。
    """
    block = ModbusSequentialDataBlock(0, [0] * REG_COUNT)
    return ModbusSlaveContext(di=block, co=block, hr=block, ir=block)


def build_server_context(
    slave_ids: Sequence[int],
) -> ModbusServerContext:
    """按从站号列表建服务端上下文。

    - 单从站 ⇒ `single=True`：pymodbus 会把唯一的数据块放到内部地址 0 上、
      忽略请求里的 unit id（**与改造前的调用完全一致**，单设备行为不变）
    - 多从站 ⇒ `single=False` + `{unit_id: context}`：按请求的 unit id 分派
      （pymodbus 3.8.6 的 requesthandler 用 `context[dev_id]` 取对应数据块）
    """
    slaves = {slave_id: build_slave_context() for slave_id in slave_ids}
    if len(slaves) == 1:
        return ModbusServerContext(slaves=slaves[slave_ids[0]], single=True)
    return ModbusServerContext(slaves=slaves, single=False)


def refresh_once(context: ModbusServerContext, simulators: Mapping[int, CemsSimulator]) -> None:
    """刷新一轮：逐个从站取自己的仿真读数并写进自己的保持寄存器。

    每个从站单独 try：一台设备的仿真/编码出问题，不该让另一台跟着停更。
    """
    for slave_id, simulator in simulators.items():
        try:
            values = build_registers(simulator)
            # 写入从站的保持寄存器（功能码 3 对应 hr 区），起始地址 REG_BASE
            context[slave_id].setValues(3, REG_BASE, values)
            if LOGGER.isEnabledFor(logging.DEBUG):
                # 逐点拼接只在开 DEBUG 时做：默认 INFO 下每 2 秒白拼一个字符串是浪费
                LOGGER.debug(
                    "[从站%d] 寄存器已刷新: %s",
                    slave_id,
                    " ".join(
                        f"{point.name}={values[point.address] / point.scale}{point.unit}"
                        for point in POINTS
                    ),
                )
        except Exception:
            # 单次刷新失败绝不能让线程退出，否则设备会永远返回旧值
            LOGGER.exception("[从站%d] 刷新仿真寄存器失败，%.0f 秒后重试", slave_id, UPDATE_INTERVAL)


def update_data(context: ModbusServerContext, simulators: Mapping[int, CemsSimulator]) -> None:
    """后台线程主循环：周期性刷新各从站寄存器里的仿真数据。

    用"绝对下次唤醒时刻"而不是固定 sleep(UPDATE_INTERVAL)：仿真读数由时钟驱动，
    一轮的耗时波动（编码、写寄存器）会让固定 sleep 越漂越慢，绝对节拍能把周期拉回来。
    """
    next_tick = time.monotonic()
    while True:
        refresh_once(context, simulators)
        next_tick += UPDATE_INTERVAL
        time.sleep(max(0.0, next_tick - time.monotonic()))


# ==================== 4. 主流程 ====================

def main() -> None:
    """初始化各从站的寄存器数据块并启动 Modbus TCP 服务端（阻塞运行）。"""
    setup_logging()

    simulators = device_simulators(SLAVE_IDS, BASE_SEED, SLAVE_PHASE_OFFSET)
    context = build_server_context(SLAVE_IDS)

    # 后台线程：持续更新仿真数据
    threading.Thread(
        target=update_data,
        args=(context, simulators),
        name="update-data",
        daemon=True,
    ).start()

    LOGGER.info(
        "Modbus 仿真设备启动: %s:%d 从站=%s（%s）",
        SERVER_HOST, SERVER_PORT,
        ",".join(str(slave_id) for slave_id in SLAVE_IDS),
        "单从站 single=True，与单设备形态一致" if len(SLAVE_IDS) == 1
        else "多从站 single=False，按 unit id 分派",
    )
    # 多设备可区分性的证据就在这几行：每台的种子/相位不同 ⇒ 曲线不同
    for slave_id, simulator in simulators.items():
        LOGGER.info(
            "从站%d: 仿真种子=%d 相位偏移=%+.0fs",
            slave_id, simulator.seed, simulator.phase_offset_seconds,
        )
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
