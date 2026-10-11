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

from src.common.logging import setup_logging                       # noqa: E402
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

# ---- 真机接入（2026-10-11）：把 2 个寄存器改由**真实设备**供给 ----
#
# 真机是一台 RS485 温湿度变送器，挂在 **Windows 宿主的 COM3** 上。
# Docker 容器**访问不到宿主的 COM 口**，所以由 `scripts/serial_bridge.py`（跑在宿主）
# 把它转成 Modbus TCP（扮演现场那台"**串口服务器**"），本层再按 TCP 读它。
#
# ⭐ 现场对应形态：CEMS 里的**数采仪**就是把多路仪表聚合成一个寄存器块，上位机只读它。
#    本开关相当于"数采仪接进来一路真仪表" —— **网关/接入层/库表/大屏全都不用改**，
#    因为它们看到的仍然是一个 9 寄存器的 Modbus TCP 从站。
#
# ⚠️⚠️ **语义边界（必须记住）**：真机测的是**环境**温湿度，
#    而本系统的 `Temp`(地址6) / `Humidity`(地址7) 在环保口径上是**烟气**温度/湿度 ——
#    **两个不同的物理量**。所以启用本开关的部署，这两列**只能当"数据链路演示"看，
#    不是环保口径数据**。完整说明见 `docs/runbooks/真机接入.md`。
#
# ⚠️ **读不到真机时绝不回落到仿真值** —— 那正是本项目刚从真机上学到的"静默假数据"
#    （真机读错地址会沉默返回 0，见 `docs/runbooks/真机接入.md` S4 发现 1）。
#    本层的做法：**启动预检**（读不到就拒绝启动）+ **运行期保留上次好值并记 ERROR**。
REAL_DEVICE_ENABLE: Final[bool] = os.getenv("REAL_DEVICE_ENABLE", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
REAL_DEVICE_HOST: Final[str] = os.getenv("REAL_DEVICE_HOST", "host.docker.internal")
REAL_DEVICE_PORT: Final[int] = int(os.getenv("REAL_DEVICE_PORT", "5021"))
REAL_DEVICE_UNIT: Final[int] = int(os.getenv("REAL_DEVICE_UNIT", "1"))
#: 只覆盖**这一个**从站的寄存器（默认 = 第一个从站）。多从站时其余从站仍是纯仿真。
REAL_DEVICE_SLAVE: Final[int] = int(os.getenv("REAL_DEVICE_SLAVE", str(SLAVE_ID)))
#: 覆盖哪两个测点（地址取自 `src/common/points.py`：Temp=6、Humidity=7）
REAL_DEVICE_POINTS: Final[tuple[str, ...]] = ("Temp", "Humidity")
#: 桥的寄存器布局（见 `scripts/serial_bridge.py`）：0=湿度×10、1=温度×10、**2=桥状态**
REAL_DEVICE_REG_HUMIDITY: Final[int] = 0
REAL_DEVICE_REG_TEMPERATURE: Final[int] = 1
#: ⭐ 桥的**状态位**（`0` 新鲜 / 非 0 陈旧）。**必须读它** ——
#: 否则真机在运行中掉线时，桥会一直给"上次好值"，设备层照搬出去就是**静默假数据**
#: （大屏上只表现为一条平线，没人知道真机已经死了）。
REAL_DEVICE_REG_STATUS: Final[int] = 2
REAL_DEVICE_POLL_INTERVAL: Final[float] = float(os.getenv("REAL_DEVICE_POLL_INTERVAL", "2.0"))
#: 连续读失败多少次后，把**保留的旧值标记为陈旧**（≈ 秒数 = 此值 × 轮询周期）。
#: ⭐ 必须有它：**桥自己掉线时设备层连不上桥，也就读不到桥的状态位** ——
#: 若只依赖桥的状态位，旧值会一直带着"新鲜"标记被发出去。
REAL_DEVICE_STALE_AFTER_FAILURES: Final[int] = int(
    os.getenv("REAL_DEVICE_STALE_AFTER_FAILURES", "3")
)


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
# 级别/格式/时间格式统一在 src/common/logging.py（唯一真源）；
# 这里只留本进程的 LOGGER —— 4 个进程的日志靠 getLogger 的名字区分来源。
LOGGER: Final[logging.Logger] = logging.getLogger("device.modbus_server")


# ==================== 2. 工具函数 ====================
# setup_logging() 已提取到 src/common/logging.py，4 个入口共用同一份实现。


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

    ⚠️ **一律 `single=False` + `{unit_id: context}`（2026-10-03 修）**：
    原先单从站走 `single=True`，而 pymodbus 在 `single=True` 下会把**任何**请求的
    unit id 改写成内部地址 0 —— 于是"请求一个不存在的从站"会**成功返回**这份唯一的数据块，
    **既不报错也不是异常码**。实测（pymodbus 3.6.9，临时服务端只建从站 1）：

        read unit=1 → OK   data
        read unit=2 → OK   data   ← 从站 2 不存在，却拿到从站 1 的数据
        read unit=3 → OK   data

    这正是"只开了 `--profile plant2`、忘开 `SLAVE_IDS=1,2`"那个静默错的机制：
    plant2 网关（`MODBUS_UNIT=2`）日志照写"Modbus 已连接: 从站2"，而它读到的
    **是 device1 的数据** → 两台曲线逐值相等、容器全 healthy、一条报错都没有。

    改成 `single=False` 后，`ModbusServerContext.__getitem__` 走原生分支
    （`if slave in self._slaves ... else raise NoSuchSlaveException`），
    请求未登记的从站**会返回 Modbus 异常响应** → 网关 `response.isError()` 为真、
    读不到数据 → **马上暴露成立刻可见的故障**，而不是悄悄给别的设备的数据。

    寄存器访问路径不变：刷新循环用的是 `context[slave_id]`（显式 unit id），
    单从站时 `slaves={1: ctx}`，取 `context[1]` 与原先取同一份数据块。
    """
    slaves = {slave_id: build_slave_context() for slave_id in slave_ids}
    return ModbusServerContext(slaves=slaves, single=False)


# ==================== 3.5 真机接入（可选，默认关闭） ====================


class RealDeviceReader:
    """从"串口服务器"（`scripts/serial_bridge.py` 暴露的 Modbus TCP 从站）读真机温湿度。

    - 后台线程持续轮询；**任何异常都只记不抛**（真机拔线是常态，不能让设备层退出）
    - `snapshot()` 返回最近一次**成功**读到的 `(湿度×10, 温度×10)`；从未成功过返回 `None`
    - ⚠️ 读失败时**保留上次好值**，**绝不回落仿真值**（见配置区注释）
    """

    def __init__(self, host: str, port: int, unit: int, interval: float) -> None:
        self.host = host
        self.port = port
        self.unit = unit
        self.interval = interval
        self._lock = threading.Lock()
        self._values: Optional[tuple[int, int, bool]] = None
        self._failures = 0

    def _note_success(self, value: tuple[int, int, bool]) -> None:
        with self._lock:
            if self._failures:
                LOGGER.warning("真机恢复读取（此前连续失败 %d 次）", self._failures)
            self._failures = 0
            self._values = value

    def _note_failure(self) -> int:
        """记一次失败；连续失败到门限后，**把保留的旧值标记为陈旧**。

        ⭐ 为什么必须这样：**桥自己掉线时，设备层连不上桥，也就读不到桥的状态位** ——
        若只依赖状态位，`self._values` 会一直带着 `stale=False` 的旧值被当成新鲜数据发出去，
        又变回"静默假数据"。所以"**连不上**"本身也必须升级成"**陈旧**"。
        """
        with self._lock:
            self._failures += 1
            if (
                self._failures >= REAL_DEVICE_STALE_AFTER_FAILURES
                and self._values is not None
                and not self._values[2]
            ):
                self._values = (self._values[0], self._values[1], True)
                LOGGER.error(
                    "真机/桥连续 %d 次读不到（≥ 门限 %d）→ 保留值**标记为陈旧**，"
                    "设备层将写越界值让接入层拒收",
                    self._failures,
                    REAL_DEVICE_STALE_AFTER_FAILURES,
                )
            return self._failures

    def read_once(self) -> Optional[tuple[int, int, bool]]:
        """读一次并**更新内部状态**；成功返回 `(湿度×10, 温度×10, 是否陈旧)`，失败返回 `None`。

        ⚠️ **成功时必须写入 `self._values`** —— 启动预检也走这个方法。
        若预检只"读一下"而不落状态，`main()` 通过预检之后、轮询第一次成功之前
        会有一个窗口，期间 `overlay_real_values` 拿不到值 → 设备层会对外**发布仿真温湿度**，
        而那正是我们要避免的"**静默假数据**"。所以状态更新与读取放在同一个方法里。

        ⭐ **第三个元素是"陈旧"标志**，来自桥的状态寄存器（`REAL_DEVICE_REG_STATUS`）：
        桥读不到变送器时会**保留上次好值**并置位。设备层读它，下游才不会把陈旧值当新鲜数据。
        """
        from pymodbus.client import ModbusTcpClient

        client = ModbusTcpClient(self.host, port=self.port, timeout=2)
        try:
            if not client.connect():
                self._note_failure()
                return None
            response = client.read_holding_registers(
                address=REAL_DEVICE_REG_HUMIDITY,
                count=REAL_DEVICE_REG_STATUS + 1,
                slave=self.unit,
            )
            if response.isError():
                self._note_failure()
                return None
            registers = response.registers
            # 状态位缺失（只给 2 个寄存器的老版桥）时按"新鲜"处理，保持向后兼容
            stale = (
                len(registers) > REAL_DEVICE_REG_STATUS
                and int(registers[REAL_DEVICE_REG_STATUS]) != 0
            )
            value = (
                int(registers[REAL_DEVICE_REG_HUMIDITY]),
                int(registers[REAL_DEVICE_REG_TEMPERATURE]),
                stale,
            )
        except Exception:
            self._note_failure()
            return None
        finally:
            try:
                client.close()
            except Exception:
                pass
        self._note_success(value)
        return value

    def snapshot(self) -> Optional[tuple[int, int, bool]]:
        with self._lock:
            return self._values

    def poll_forever(self) -> None:
        while True:
            got = self.read_once()
            if got is None:
                with self._lock:
                    failures = self._failures
                LOGGER.error(
                    "读真机失败（连续第 %d 次）：%s:%d 从站%d —— "
                    "Temp/Humidity 保持**上次好值**，不回落到仿真值",
                    failures, self.host, self.port, self.unit,
                )
            elif got[2]:
                # ⭐ 桥报"陈旧"：读数还在，但**已经不是当前真值**。
                # overlay_real_values 会据此写**越界值**让接入层拒收 —— 故障必须可见。
                LOGGER.error(
                    "真机读数**陈旧**（桥状态位=1）：%s:%d 从站%d —— "
                    "桥仍在保留上次好值，但设备层写越界值让接入层拒收，不冒充新鲜数据",
                    self.host, self.port, self.unit,
                )
            time.sleep(self.interval)


def _stale_raw(point_name: str) -> int:
    """取该测点"**刚好越界一格**"的寄存器原始值（量程上限 +1 个刻度，再 × scale）。

    用途：真机读数**陈旧**时写它 → 接入层按既有的"超量程"规则**拒收整条报文**，
    **故障立刻可见**（日志出现拒收、覆盖率掉、曲线断开）。

    ⚠️ **刻意不用 0**：`0` **落在量程内**，会被当成真实读数一路入库 ——
    那正是真机教我们的坑（读错地址沉默返回 0，见 `docs/runbooks/真机接入.md` S4 发现 1）。
    """
    point = next(p for p in POINTS if p.name == point_name)
    return int((point.high + 1) * point.scale)


#: 真机陈旧时写进寄存器的**越界值**（导入期按契约算一次，不写魔数）
STALE_TEMP_RAW: Final[int] = _stale_raw("Temp")
STALE_HUMIDITY_RAW: Final[int] = _stale_raw("Humidity")

#: 真机读取器（仅 `REAL_DEVICE_ENABLE=1` 时创建，其余情况保持 `None`）
_REAL_READER: Optional[RealDeviceReader] = None


def overlay_real_values(values: list[int], slave_id: int) -> list[int]:
    """把真机的温湿度覆盖进寄存器块（返回新列表，不改入参）。

    ⚠️ 只在 `REAL_DEVICE_ENABLE=1` 且 `slave_id == REAL_DEVICE_SLAVE` 时生效 ——
    **未设开关时逐位不变**。

    单位对齐：CEMS 寄存器存 `物理值 × point.scale`，而 `Temp`/`Humidity` 的 scale 都是 10；
    桥给出的也是「×10」的整数，所以**直接赋值即可，不需要再换算**。

    ⚠️ **负温会被拒收（刻意保留）**：环境温度 <0 degC 时桥给的是补码（如 `0xFF9F`=65439），
    放进无符号寄存器后网关会读到 6543.9 degC，**超出 Temp 量程 300 → 接入层拒收**。
    宁可拒收，也不要一个"看起来合理的错数"。
    """
    if not REAL_DEVICE_ENABLE or slave_id != REAL_DEVICE_SLAVE or _REAL_READER is None:
        return values
    snap = _REAL_READER.snapshot()
    if snap is None:
        # 启动预检保证这里基本不会发生；真到了就叫出来，**不静默**
        LOGGER.error("[从站%d] 真机尚无有效读数，Temp/Humidity 仍是仿真值", slave_id)
        return values
    humidity_raw, temperature_raw, stale = snap
    out = list(values)
    for point in POINTS:
        if point.name == "Humidity":
            # ⭐ 陈旧 ⇒ 写越界值（101 %），让接入层**拒收**，而不是冒充新鲜数据
            out[point.address - REG_BASE] = STALE_HUMIDITY_RAW if stale else humidity_raw
        elif point.name == "Temp":
            out[point.address - REG_BASE] = STALE_TEMP_RAW if stale else temperature_raw
    return out


def refresh_once(context: ModbusServerContext, simulators: Mapping[int, CemsSimulator]) -> None:
    """刷新一轮：逐个从站取自己的仿真读数并写进自己的保持寄存器。

    每个从站单独 try：一台设备的仿真/编码出问题，不该让另一台跟着停更。
    """
    for slave_id, simulator in simulators.items():
        try:
            values = overlay_real_values(build_registers(simulator), slave_id)
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

    # ---- 真机接入（可选）：**启动预检** + 后台轮询 ----
    global _REAL_READER  # noqa: PLW0603 - 模块级单例，供 overlay_real_values 读取
    if REAL_DEVICE_ENABLE:
        reader = RealDeviceReader(
            REAL_DEVICE_HOST, REAL_DEVICE_PORT, REAL_DEVICE_UNIT, REAL_DEVICE_POLL_INTERVAL
        )
        first = reader.read_once()
        if first is None:
            # ⚠️ 刻意**不回落到仿真值**：那样大屏会显示假数据、且没人看得出来。
            # 与网关既有的"读不到本从站就拒绝启动"是同一套纪律。
            LOGGER.critical(
                "REAL_DEVICE_ENABLE=1 但读不到真机（%s:%d 从站%d）—— **拒绝启动**。\n"
                "  排查顺序：① 宿主上 `scripts/serial_bridge.py` 是否在跑；"
                "② REAL_DEVICE_HOST/PORT 是否指对（容器里应是 host.docker.internal）；"
                "③ 变送器供电与 A/B 接线；④ 桥的 5021 端口是否在监听。\n"
                "  ⚠️ 刻意不回落仿真值 —— 那会让大屏显示假数据而无人察觉。",
                REAL_DEVICE_HOST, REAL_DEVICE_PORT, REAL_DEVICE_UNIT,
            )
            raise SystemExit(1)
        _REAL_READER = reader
        threading.Thread(target=reader.poll_forever, name="real-device-poller", daemon=True).start()
        temp_display = first[1] - 65536 if first[1] > 32767 else first[1]
        LOGGER.info(
            "⭐ 真机接入已启用：%s:%d 从站%d → 覆盖从站%d 的 %s（首次读数 湿度%.1f%%RH / 温度%.1f degC）",
            REAL_DEVICE_HOST, REAL_DEVICE_PORT, REAL_DEVICE_UNIT, REAL_DEVICE_SLAVE,
            "/".join(REAL_DEVICE_POINTS), first[0] / SCALE, temp_display / SCALE,
        )
        LOGGER.warning(
            "⚠️ 语义边界：Temp/Humidity 两列装的是**环境**温湿度，"
            "不是烟气参数；本部署只用于**数据链路演示**，不作环保口径依据。"
        )

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
        "按 unit id 严格分派：请求未登记的从站会收到异常响应，"
        "不会拿到本从站的数据（见 build_server_context 的说明）",
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
