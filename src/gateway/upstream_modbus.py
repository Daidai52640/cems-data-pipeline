# -*- coding: utf-8 -*-
"""上游直读服务：把网关的最新采集值，以**只读 Modbus TCP 从站**暴露给 SCADA / DCS。

## 为什么网关要当服务端（本模块存在的理由）

改造前网关是纯客户端：它读设备、发 MQTT，对外不提供任何服务。上游（SCADA / DCS /
就地巡检终端）想拿实时值，只能订阅 MQTT 或去查时序库 —— 前者要求上游懂 MQTT 和报文
格式，后者拿到的是"入库后的历史"，都有延迟或额外依赖。

工业现场的通行做法是让网关**同时扮演两个角色**（Moxa AIG-501：`Modbus master` **加上**
`Modbus TCP server`；MGate：主动轮询并缓存设备数据，**SCADA 直读**）：

    Modbus 设备 ──(本网关是 client)──► 网关 ──(MQTT)──► 接入层 ──► TDengine
                                       │
                                       └──(本网关是 server)──► SCADA / DCS 直读

这两个角色不冲突：**主动上报**（MQTT）保证数据最终入湖、可对账；**被动被读**（本模块）
让现场系统按自己的节奏取当前值，不依赖 MQTT 客户端也不依赖库。数据源都是同一份
"最近一次成功采集的快照"，所以两条路看到的数必然一致。

## 与 OPC UA 的关系（重要，别搞混）

"被上游直读"是一个**角色**，不是某个协议。Modbus TCP server 和 OPC UA server 是同一个
角色的两种协议实现。本项目先做 Modbus，理由：地址与编码与设备层**完全同构**（同一套
`points.py` 契约与 `scale`），上游拿到的数与原设备逐值可比；而 OPC UA 需要维护地址空间、
会话、订阅、证书，是另一个量级的工程（见 `docs/adr/0011`）。

## 只读，而且是真的只读

上游是"取数方"，不该能写回。这里靠**数据块拒绝写入**实现（`_ReadOnlyBlock.setValues`
直接抛错），而不是靠文档约定 —— 免得哪天有人在 SCADA 里点错一下就把快照改了。

## 陈旧语义：宁可标脏，不可装作正常

读设备失败时**不清零、也不回落**，而是保留上一次的成功值并把状态位 `stale` 置起
（与设备层 `RealDeviceReader` 的处置一致）。上游据此能区分"这个值是当前的"和
"这个值是几分钟前的"。用 0 表达故障是本项目明令禁止的做法（0 落在量程内，看不出错）。

## 寄存器地图

    地址        内容
    0..8        9 个测点，**与设备层同布局同编码**（uint16 大端，值 = round(真实值 × scale)）
    9..99       保留（当前恒 0；不要在这里放东西，留给以后扩展而不动 0..8 的兼容性）
    100-101     最近一次成功采集的时间戳（uint32 秒，高字在前；0 = 从未采到）
    102-103     成功采集计数（uint32，高字在前）
    104         状态位 bit0 = 采集链路正常，bit1 = MQTT 在线，bit2 = 快照已陈旧
    105         轮询周期 ×10（5.0 s → 50）
    106         契约指纹（`points.contract_version()` 的低 16 位，见 docs/adr/0011）
    107         陈旧门限 ×10

单点编码越界（设备回了脏数据）时该寄存器写 `REG_INVALID_MARKER`（65535）。本项目所有
测点的合法上限最大值是 Flow 的 65000（scale=1），所以 65535 一定是"非法"而不会误撞真值。
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Final, Optional, Sequence

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

from src.common.points import POINTS, contract_version   # noqa: E402

# ==================== 配置区 ====================
# 默认**关闭**：与 HJ212 北向出口同一条纪律 —— 新增对外接口不随默认部署一起对外开。
# 要开就在 .env 里设 UPSTREAM_MODBUS_ENABLE=1，并显式给端口。
UPSTREAM_MODBUS_ENABLE: Final[bool] = os.getenv(
    "UPSTREAM_MODBUS_ENABLE", "0"
).strip().lower() not in ("0", "false", "no", "off", "")
#: 默认 5022。设备层用 5020、串口桥用 5021，这里避开；
#: 多实例部署时 plant2 必须另给一个端口（同一宿主上不能两个进程听同一端口）。
UPSTREAM_MODBUS_PORT: Final[int] = int(os.getenv("UPSTREAM_MODBUS_PORT", "5022"))
#: 绑 127.0.0.1 与项目其余对外接口口径一致（要跨机读必须显式改，改的人知道自己在做什么）
UPSTREAM_MODBUS_HOST: Final[str] = os.getenv("UPSTREAM_MODBUS_HOST", "127.0.0.1").strip()

#: 快照超过这个时长没更新就置 stale 位。取轮询周期的 3 倍（与"采集周期 3 倍"的全项目
#: 陈旧判据口径一致），下限 6 秒，避免把 POLL_INTERVAL 配得极小时判据失去意义。
STALE_FACTOR: Final[float] = float(os.getenv("UPSTREAM_STALE_FACTOR", "3.0"))
STALE_MIN_SECONDS: Final[float] = 6.0

LOGGER: Final[logging.Logger] = logging.getLogger("gateway.upstream_modbus")

# ==================== 寄存器地图常量 ====================
STATUS_BASE: Final[int] = 100
#: 单点编码越界时写它的哨兵（见模块 docstring：合法上限最大 65000，65535 不会误撞）
REG_INVALID_MARKER: Final[int] = 65535
#: 服务端数据块总长度：覆盖 0..(107)
TOTAL_REGS: Final[int] = STATUS_BASE + 8

FLAG_MODBUS_OK: Final[int] = 0b001
FLAG_MQTT_OK: Final[int] = 0b010
FLAG_STALE: Final[int] = 0b100


def encode_point_value(point_name: str, value: float, scale: int) -> int:
    """真实值 → 寄存器整数，与设备层 `modbus_server.encode()` **同一口径**。

    设备层是 `int(round(value * scale))`；这里保持一致（`round` 在 Python 3 已返回 int，
    写法等价），两台机器读到的数才逐值可比 —— 有测试直接对拍两边。
    多出来的只有越界保护：编码结果落在 uint16 之外时写 `REG_INVALID_MARKER`，
    而不是让它回绕成一个看起来很正常的数（回绕是最难查的一类故障）。
    """
    try:
        raw = round(float(value) * scale)
    except (TypeError, ValueError, OverflowError):
        LOGGER.warning("[上游] 测点 %s 的值无法编码（%r），写非法哨兵", point_name, value)
        return REG_INVALID_MARKER
    if not 0 <= raw <= 0xFFFF:
        LOGGER.warning(
            "[上游] 测点 %s 编码越界（%r × %d = %d），写非法哨兵",
            point_name, value, scale, raw,
        )
        return REG_INVALID_MARKER
    return raw


def build_registers(snapshot: "Snapshot", poll_interval: float, stale_seconds: float) -> list[int]:
    """把一份快照铺成寄存器数组（纯函数，便于单测直接断言）。

    未用到的地址（9..99）与从未采到的字段一律填 0，不做任何"看起来像数据"的填充。
    """
    regs = [0] * TOTAL_REGS
    if snapshot.values:
        for point in POINTS:
            if point.name in snapshot.values:
                regs[point.address] = encode_point_value(
                    point.name, snapshot.values[point.name], point.scale
                )

    epoch = int(snapshot.sampled_at) if snapshot.sampled_at else 0
    regs[STATUS_BASE] = (epoch >> 16) & 0xFFFF
    regs[STATUS_BASE + 1] = epoch & 0xFFFF
    regs[STATUS_BASE + 2] = (snapshot.samples >> 16) & 0xFFFF
    regs[STATUS_BASE + 3] = snapshot.samples & 0xFFFF

    flags = 0
    if snapshot.modbus_ok:
        flags |= FLAG_MODBUS_OK
    if snapshot.mqtt_ok:
        flags |= FLAG_MQTT_OK
    if snapshot.is_stale(stale_seconds):
        flags |= FLAG_STALE
    regs[STATUS_BASE + 4] = flags

    regs[STATUS_BASE + 5] = max(0, min(0xFFFF, round(poll_interval * 10)))
    regs[STATUS_BASE + 6] = int(contract_version(), 16) & 0xFFFF
    regs[STATUS_BASE + 7] = max(0, min(0xFFFF, round(stale_seconds * 10)))
    return regs


class Snapshot:
    """最近一次采集的快照（线程安全）。采集线程写、服务端线程读。"""

    __slots__ = ("_lock", "modbus_ok", "mqtt_ok", "sampled_at", "samples", "values")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.values: dict[str, float] = {}
        self.sampled_at: float = 0.0
        self.samples: int = 0
        self.modbus_ok: bool = False
        self.mqtt_ok: bool = False

    def update(self, values: dict[str, float], *, modbus_ok: bool, mqtt_ok: bool) -> None:
        """记一次成功采集。只有成功采集才更新 `sampled_at` —— 失败不刷新时间戳，
        否则 stale 位永远不亮，等于把"读不到设备"伪装成"数据是新的"。"""
        with self._lock:
            self.values = dict(values)
            self.sampled_at = time.time()
            self.samples += 1
            self.modbus_ok = modbus_ok
            self.mqtt_ok = mqtt_ok

    def touch_flags(self, *, modbus_ok: bool, mqtt_ok: bool) -> None:
        """只更新链路状态位，不动采样时间与计数（用于"读失败"这类事件）。"""
        with self._lock:
            self.modbus_ok = modbus_ok
            self.mqtt_ok = mqtt_ok

    def is_stale(self, stale_seconds: float) -> bool:
        with self._lock:
            if not self.sampled_at:
                return True          # 从未采到 —— 对上游而言就是"不可用"
            return (time.time() - self.sampled_at) > stale_seconds

    def snapshot_for_registers(self) -> "Snapshot":
        """取一份不加锁的浅拷贝，供 `build_registers` 使用（避免持锁做编码）。"""
        with self._lock:
            clone = Snapshot()
            clone.values = dict(self.values)
            clone.sampled_at = self.sampled_at
            clone.samples = self.samples
            clone.modbus_ok = self.modbus_ok
            clone.mqtt_ok = self.mqtt_ok
            return clone


class _ReadOnlyBlock(ModbusSequentialDataBlock):
    """对**服务端请求**只读的数据块。

    pymodbus 在收到写请求时会调 `setValues`，这里直接抛错 → 转成 Modbus 异常响应，
    上游收到的是明确的写失败，而不是"写成功但值没变"那种最难查的假确认。

    本进程自己的刷新线程走 `refresh()`（显式另开一个口子）—— 故意的：
    "谁能写"这件事在代码结构上就写死了，不靠调用方自觉。
    """

    def setValues(self, address: int, values: Sequence[Any]) -> None:   # pymodbus API 名（驼峰）
        LOGGER.warning("[上游] 拒绝写入请求：地址 %s，值 %r（本服务只读）", address, values)
        raise ValueError("上游直读服务是只读的：不接受任何写入")

    def refresh(self, address: int, values: Sequence[int]) -> None:
        """内部刷新用。只有本模块的刷新循环调它。"""
        super().setValues(address, values)


class UpstreamServer:
    """后台线程里跑的只读 Modbus TCP 从站。

    - **从站地址固定 1**（与设备层默认一致）。上游若用别的 unit id 会收到 Modbus
      异常响应 —— 这是故意的：读一个不存在的从站拿到别人的数据，是本项目已经踩过的坑
      （见 `modbus_server.build_server_context` 的长注释）。
    - 生命周期与网关主循环一致：`start()` 起 daemon 线程，进程退出即结束。
    - 启动失败（端口被占等）只记 CRITICAL，**不让网关退出** —— 上游读不到数据是
      可降级的能力，采集主链路不该被它拖死。
    """

    def __init__(
        self,
        snapshot: Snapshot,
        *,
        host: str = UPSTREAM_MODBUS_HOST,
        port: int = UPSTREAM_MODBUS_PORT,
        poll_interval: float = 5.0,
    ) -> None:
        self.snapshot = snapshot
        self.host = host
        self.port = port
        self.poll_interval = poll_interval
        self.stale_seconds = max(STALE_MIN_SECONDS, poll_interval * STALE_FACTOR)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._block: Optional[_ReadOnlyBlock] = None

    # ---- 寄存器取值：每次都按当前快照现算，不在服务端另存一份状态 ----
    def _read_registers(self) -> list[int]:
        return build_registers(
            self.snapshot.snapshot_for_registers(), self.poll_interval, self.stale_seconds
        )

    def _port_available(self) -> bool:
        """先探一次端口：把"端口被占"这种最常见的启动失败在 start() 里就说清楚，
        而不是等上游连不上、去日志里挖。

        两道检查，缺一不可（Windows 语义下的坑）：
        1. **能不能连上** —— 已有服务在听就说明被占。这一步必须在前：仅靠 bind 探测
           在 Windows 上不可靠 —— 若已占用方自己开了 SO_REUSEADDR，第二个 bind 会
           **成功**（Windows 允许"抢端口"，与 Linux 行为不同），于是两个服务都以为
           自己起来了、请求落到谁手上不确定。
        2. **能不能独占 bind** —— 不带 SO_REUSEADDR 试绑，覆盖"端口被持有但没在听"的情况。
        """
        import socket

        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(0.5)
        try:
            if conn.connect_ex((self.host, self.port)) == 0:
                return False           # 有人已在听 → 被占
        finally:
            conn.close()

        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 故意**不设** SO_REUSEADDR：这里问的是"能不能独占"，不是"能不能复用"
        try:
            probe.bind((self.host, self.port))
            return True
        except OSError:
            return False
        finally:
            probe.close()

    def start(self) -> bool:
        """起服务；端口不可用返回 False（不抛，不影响采集主链路）。"""
        if not self._port_available():
            LOGGER.critical(
                "[上游] %s:%d 已被占用，只读 Modbus 服务未启动。"
                "多实例部署时每个网关实例必须给不同的 UPSTREAM_MODBUS_PORT。",
                self.host, self.port,
            )
            return False

        self._block = _ReadOnlyBlock(0, self._read_registers())
        # zero_mode=True 是本模块必须的，理由要说清（设备层的取法不同、但同样正确）：
        #   pymodbus 的 ModbusSlaveContext 默认 zero_mode=False，会把请求地址 +1 再落到
        #   数据块上（getValues/setValues 都加，所以**读写同路时两边抵消**）。
        #   设备层正是靠这个对称性工作的：它用 context[slave].setValues(3, 0, ...) 写，
        #   客户端用地址 0 读，两边都 +1，结果一致。
        #   而本模块的刷新线程直接落到裸数据块（_ReadOnlyBlock.refresh），不经
        #   ModbusSlaveContext，所以只有"读"这一侧被 +1 → 实测客户端读到的是
        #   [第2点, 第3点, ..., 第9点, 0]（整体错位一位，且末位恒 0）。
        #   开 zero_mode=True 后请求地址与数据块下标一一对应，两侧对齐。
        #   （实测矩阵：zero_mode=False + 裸块刷新 → IllegalAddress；True + 裸块 → 正确。）
        context = ModbusServerContext(
            slaves={1: ModbusSlaveContext(di=self._block, co=self._block,
                                          hr=self._block, ir=self._block,
                                          zero_mode=True)},
            single=False,
        )

        def _refresh_loop() -> None:
            # 数据块是静态的，所以周期把最新快照刷进去。
            # 刷新周期取轮询周期的 1/4（下限 1 秒）：上游读到的值最多滞后这一点点，
            # 同时避免"每来一个请求就重建整块寄存器"的开销。
            interval = max(1.0, self.poll_interval / 4.0)
            while not self._stop.is_set():
                try:
                    self._block.refresh(0, self._read_registers())
                except Exception as exc:      # noqa: BLE001 — 刷新失败不能掀翻服务线程
                    LOGGER.debug("[上游] 刷新寄存器失败（忽略）: %s", exc)
                self._stop.wait(interval)

        def _serve() -> None:
            try:
                StartTcpServer(context=context, address=(self.host, self.port))
            except Exception as exc:          # noqa: BLE001 — 服务挂掉不该拖死网关
                LOGGER.critical(
                    "[上游] 只读 Modbus 服务退出（%s:%d）：%s —— "
                    "采集主链路不受影响，但上游读不到数据",
                    self.host, self.port, exc,
                )

        threading.Thread(target=_refresh_loop, name="upstream-refresh", daemon=True).start()
        self._thread = threading.Thread(target=_serve, name="upstream-modbus", daemon=True)
        self._thread.start()
        LOGGER.info(
            "[上游] 只读 Modbus TCP 服务已启动: %s:%d 从站1（寄存器 0..%d，状态区 %d..%d）",
            self.host, self.port, TOTAL_REGS - 1, STATUS_BASE, TOTAL_REGS - 1,
        )
        return True

    def stop(self) -> None:
        self._stop.set()


def enabled() -> bool:
    """是否启用上游直读（默认关）。"""
    return UPSTREAM_MODBUS_ENABLE
