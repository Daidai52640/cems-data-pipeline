# -*- coding: utf-8 -*-
"""设备层直读 RS485：把 Modbus RTU 主站并进 device 进程（FC03，4800/8/N/1，从站 0x01）。

============================ 它替代了什么 ============================
改造前真机走的是「外挂串口服务器」形态：

    [温湿度变送器] --RS485/RTU--> COM3
                                    |
                            scripts/serial_bridge.py（独立进程，跑在宿主）
                                    |
                            Modbus TCP :5021
                                    |
                            device 进程（RealDeviceReader 按 TCP 读）

`serial_bridge.py` **保留**：它也是现场真实存在的形态（数采仪/串口服务器把 RS485 转成
TCP，上位机按 TCP 读），而且 `docs/runbooks/真机接入.md`、`docs/runbooks/物理层取证.md`
与基线冻结文档里的一手证据都指向它。两种形态各自适用：

    | 形态 | 适用场景 | 代价 |
    |---|---|---|
    | serial_bridge.py（外挂） | 一个串口要给多个消费者；或 device 在容器里、串口只在宿主 | 多一个进程、多一跳 TCP、多一份要维护的配置 |
    | 本模块（进程内直读） | device 进程与串口在同一台宿主（现场数采一体机的常见形态） | device 进程要能直接打开串口（容器拿不到宿主 COM 口） |

本模块做的事：自己拼 RTU 帧（FC03 + CRC16）、发请求、按定长读应答、校验 CRC 与从站地址，
把数据段按 `src/common/points_transmitter.py` 的契约解码成两个寄存器原始整数。
**不复制契约**：波特率/数据位/校验/停止位/从站/功能码/帧间隔/寄存器地址/线格式全部取自
`points_transmitter`（那台设备的唯一真源）；字节序解码复用 `points.decode_register`。

============================ 语义（与 serial_bridge.py 一致，不得回退）============================
读失败（串口打不开 / 读超时 / CRC 错 / 坏帧 / 异常响应）时：

1. 状态位 = **1（陈旧）**；连续失败清零后自动回到 0（新鲜）；
2. 对外写的值不是上一次好值，而是**越界值** `101.0 %` / `301.0 degC`
   （同时超出 CEMS `points.py` 与变送器 `points_transmitter.py` 两套契约的量程）；
3. 进程**绝不因为读失败而退出**；
4. 恢复后自动回新鲜（下一次成功读取即 `_failures = 0`）。

⭐ **为什么不用 0 表示无效**：实测这台变送器读不存在的地址会沉默返回全 0 而不报异常
（`docs/runbooks/真机接入.md` S4 发现 1），而 0 落在两套量程内 —— 会被当成真实读数一路入库。
把「无效」做进**值本身**，任何不读状态位的消费者也无法误用。只置状态位不够：那只是把洞留给
下一个人。本模块的哨兵值与 `scripts/serial_bridge.py` 的 `STALE_*_RAW` 必须逐位相等，
`tests/device/test_rtu_master.py` 有一条测试钉住这件事。

============================ 不做什么 ============================
- 不做 Modbus TCP 服务端：对外仍是 `src/device/modbus_server.py` 的 :5020。
- 不做量程拒收 / 入库 / 告警：那是接入层与告警判定的事（本模块只给寄存器原始整数）。
- 不做写寄存器：该设备只读（实测发 FC06 会回显但值不变，见 points_transmitter 顶部）。
- 不 import pyserial 于模块级：容器里没装 pyserial，纯仿真 / TCP 形态的 device 进程不能
  因为「一个可选外设的依赖」起不来。pyserial 只在真正打开串口时延迟导入。

============================ 用法 ============================
    # 设备层启用进程内直读（其余环境变量语义不变，见 modbus_server.py 配置区）
    REAL_DEVICE_ENABLE=1 REAL_DEVICE_SOURCE=serial REAL_DEVICE_SERIAL_PORT=COM3 \
        python -m src.device.modbus_server

    # 单独探针：不占 TCP 端口，只打印原始帧、两个原始寄存器与状态位（取证/排障用）
    python -m src.device.rtu_master --port COM3 --count 5
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Final, Optional, Protocol

# 让 src/common 能被导入：python src/device/rtu_master.py / python -m src.device.rtu_master 都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common import points_transmitter as tx                    # noqa: E402
from src.common.points import POINTS as CEMS_POINTS, WIRE_BYTE_ORDER, decode_register  # noqa: E402

LOGGER: Final[logging.Logger] = logging.getLogger("device.rtu_master")

# ==================== 配置区（设备事实全部取自 points_transmitter，不在这里抄一遍） ====================

#: 从站地址；实测 0x01（发 0x02 完全静默）
SLAVE_ID: Final[int] = tx.SLAVE_ADDRESS
#: 功能码；该设备只支持 03（读保持寄存器）
FUNCTION_CODE: Final[int] = tx.READ_FUNCTION_CODE
#: 串口参数（实测 4800/8/N/1；填 9600 会完全静默）
BAUD_RATE: Final[int] = tx.BAUD_RATE
DATA_BITS: Final[int] = tx.DATA_BITS
PARITY: Final[str] = tx.PARITY
STOP_BITS: Final[int] = tx.STOP_BITS
#: 一次读整块：起始寄存器与数量（本设备就是 0x0000 起的 2 个 = 湿度 + 温度）
REG_START: Final[int] = tx.REG_BASE
REG_COUNT: Final[int] = tx.REG_COUNT
#: 正常应答帧长度 = 从站 1 + 功能码 1 + 字节数 1 + 数据 4 + CRC 2 = 9（实测值）
EXPECTED_RESPONSE_BYTES: Final[int] = tx.EXPECTED_RESPONSE_BYTES
#: 帧间静默下限（字节时间）。手册要求「初始/结束结构 >=4 字节的时间」，比 Modbus 标准的 3.5 字符保守。
FRAME_GAP_MIN_BYTES: Final[int] = tx.FRAME_GAP_MIN_BYTES

#: 状态位取值（与 serial_bridge.py 的 0x0002 寄存器同语义）
STATUS_FRESH: Final[int] = 0
STATUS_STALE: Final[int] = 1

#: 读失败时对外的越界值寄存器原始整数（×10）。
#:
#: 取值方式：CEMS 契约量程上限 + 1 个刻度，再 × scale —— 即 `101.0 %` / `301.0 degC`，
#: 与 `scripts/serial_bridge.py` 的 `STALE_HUMIDITY_RAW=1010` / `STALE_TEMPERATURE_RAW=3010`
#: 逐位相等（测试钉住）。用「量程上限 + 1」推导而不是写字面量：量程改了，哨兵会自己跟着越界。
#: 导入期自检 `validate_stale_sentinels()` 再验一遍「同时超出两套契约」。
def _stale_sentinel_raw(cems_point_name: str) -> int:
    """按 CEMS 契约算某个测点「刚好越界一格」的寄存器原始整数。"""
    point = next(p for p in CEMS_POINTS if p.name == cems_point_name)
    return int((point.high + 1) * point.scale)


STALE_HUMIDITY_RAW: Final[int] = _stale_sentinel_raw("Humidity")    # 1010 = 101.0 %
STALE_TEMPERATURE_RAW: Final[int] = _stale_sentinel_raw("Temp")     # 3010 = 301.0 degC


def validate_stale_sentinels() -> None:
    """导入期自检：越界值必须**同时**超出 CEMS 与变送器两套契约的量程。

    只超出一套不够：`POINT_SET=transmitter` 那条路只读 2 个寄存器、看不到状态位，
    它唯一能察觉「值无效」的途径就是值本身越界。判据与 `scripts/serial_bridge.py`
    的 `_check_stale_sentinels()` 同一条 —— 两处实现共享同一个物理前提，谁放宽了量程都该炸。
    """
    cases = (
        ("Humidity", STALE_HUMIDITY_RAW, "AirHumidity"),
        ("Temp", STALE_TEMPERATURE_RAW, "AirTemp"),
    )
    for cems_name, raw, tx_name in cases:
        value = raw / tx.SCALE
        cems_point = next(p for p in CEMS_POINTS if p.name == cems_name)
        tx_point = next(p for p in tx.POINTS if p.name == tx_name)
        for label, point in (("CEMS", cems_point), ("transmitter", tx_point)):
            if point.low <= value <= point.high:
                raise ValueError(
                    f"越界哨兵失效：{cems_name}={value} 落在 {label} 契约 "
                    f"'{point.name}' 的量程 [{point.low}, {point.high}] 内 —— "
                    "它会被当成合法读数入库。请调大 STALE_*_RAW 的推导基数。"
                )


validate_stale_sentinels()


# ==================== RTU 帧编解码（纯函数，可单独测） ====================


class RtuError(Exception):
    """RTU 链路层错误的基类（读失败的一种，调用方按「陈旧」处理）。"""


class RtuTimeout(RtuError):
    """应答不完整（读超时 / 帧被截断）。"""


class RtuFrameError(RtuError):
    """帧不合法：CRC 错、长度错、从站地址不符、功能码不符、字节数不符。"""


class RtuExceptionResponse(RtuError):
    """从站返回 Modbus 异常响应（功能码最高位置 1）。"""

    def __init__(self, slave: int, function: int, code: int) -> None:
        self.slave = slave
        self.function = function
        self.code = code
        super().__init__(
            f"从站 {slave} 对功能码 {function} 返回异常响应，异常码 {code}"
            f"（1=非法功能 2=非法地址 3=非法数据值 4=从站故障 6=从站忙）"
        )


def crc16_modbus(data: bytes) -> int:
    """标准 Modbus CRC16（多项式 0xA001 反射、初值 0xFFFF、低字节先发）。

    纯函数：同一段字节永远得到同一个值，不需要硬件。
    """
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def append_crc(payload: bytes) -> bytes:
    """给帧体追加 CRC（低字节在前，Modbus RTU 规定）。"""
    crc = crc16_modbus(payload)
    return payload + bytes([crc & 0xFF, (crc >> 8) & 0xFF])


def crc_ok(frame: bytes) -> bool:
    """校验整帧（末两字节是 CRC）。长度不足 3 一律 False，不猜。"""
    if len(frame) < 3:
        return False
    crc = crc16_modbus(frame[:-2])
    return frame[-2] == (crc & 0xFF) and frame[-1] == ((crc >> 8) & 0xFF)


def build_read_request(
    slave: int = SLAVE_ID,
    address: int = REG_START,
    count: int = REG_COUNT,
) -> bytes:
    """拼 FC03 读保持寄存器请求帧。

    真机实测请求帧（`docs/runbooks/物理层取证.md`）：
    `01 03 00 00 00 02 C4 0B` —— 本函数输出必须逐字节相同（测试钉住）。
    """
    if not 0 <= slave <= 247:
        raise ValueError(f"从站地址 {slave} 超出 0~247")
    if not 0 <= address <= 0xFFFF:
        raise ValueError(f"寄存器地址 {address} 超出 16 位范围")
    if not 1 <= count <= 125:  # FC03 单次上限 125 个寄存器（Modbus 规范）
        raise ValueError(f"读取数量 {count} 超出 1~125")
    payload = bytes(
        [
            slave,
            FUNCTION_CODE,
            (address >> 8) & 0xFF,
            address & 0xFF,
            (count >> 8) & 0xFF,
            count & 0xFF,
        ]
    )
    return append_crc(payload)


def parse_read_response(
    frame: bytes,
    slave: int = SLAVE_ID,
    count: int = REG_COUNT,
) -> bytes:
    """解析 FC03 应答帧，返回数据段字节（不含从站/功能码/字节数/CRC）。

    判据从严：长度、CRC、从站地址、功能码、字节数任一不符都抛 `RtuFrameError`，
    异常响应抛 `RtuExceptionResponse`。**不做自动截断/补零** —— 补出来的数看不出错
    （同一个道理见 `points.decode_register` 的边界 2）。

    正常应答（实测）：`01 03 04 02 31 01 11 6B D8`
        → 数据段 `02 31 01 11` → 湿度 0x0231=561、温度 0x0111=273
    异常应答：`01 83 02 C0 F1`（长度 5；这里按异常响应报，不按坏帧报）。
    """
    data = bytes(frame)
    if len(data) < 5:
        raise RtuFrameError(f"应答帧太短（{len(data)} 字节，最少 5 字节）")
    if not crc_ok(data):
        raise RtuFrameError(f"CRC 校验失败：{data.hex(' ')}")
    if data[0] != slave:
        raise RtuFrameError(f"从站地址不符：期望 {slave}，收到 {data[0]}")
    if data[1] & 0x80:
        raise RtuExceptionResponse(slave, data[1] & 0x7F, data[2])
    if data[1] != FUNCTION_CODE:
        raise RtuFrameError(f"功能码不符：期望 {FUNCTION_CODE}，收到 {data[1]}")
    expected = 5 + 2 * count  # 从站1 + 功能码1 + 字节数1 + 数据2n + CRC2
    if len(data) != expected:
        raise RtuFrameError(f"应答帧长度不符：期望 {expected} 字节，收到 {len(data)} 字节")
    byte_count = data[2]
    if byte_count != 2 * count:
        raise RtuFrameError(f"字节数字段不符：期望 {2 * count}，收到 {byte_count}")
    return data[3 : 3 + byte_count]


def decode_data_segment(data: bytes) -> tuple[int, int]:
    """数据段 → (湿度原始整数, 温度原始整数的 16 位补码位型)。

    契约（dtype / endian / 地址 / scale）全部取自 `points_transmitter`，字节序解码复用
    `points.decode_register` —— 不在设备层重写一份字节序逻辑。

    返回的是「寄存器原始整数」（×10），不是工程值：device 层寄存器存的就是这个；
    温度保留 int16 补码位型（如 -97 → 65439），因为对外寄存器是 uint16。
    """
    expected = tx.DATA_BYTES
    if len(data) != expected:
        raise RtuFrameError(f"数据段应为 {expected} 字节，收到 {len(data)} 字节")
    raw_values: dict[str, int] = {}
    offset = 0
    for point in sorted(tx.POINTS, key=lambda item: tx.RAW_ADDRESSES[item.name]):
        width = len(WIRE_BYTE_ORDER[point.endian])
        raw_values[point.name] = int(
            decode_register(data[offset : offset + width], point.dtype, point.endian)
        )
        offset += width
    return raw_values["AirHumidity"], raw_values["AirTemp"] & 0xFFFF


def int16_from_wire(raw: int) -> int:
    """寄存器位型 → 有符号整数（65439 → -97），供日志/探针显示温度用。"""
    return raw - 0x10000 if raw > 0x7FFF else raw


# ==================== 串口传输（可注入，单测不需要硬件） ====================


class SerialTransport(Protocol):
    """RtuMaster 需要的串口能力子集（pyserial.Serial 天然满足；测试可注入假对象）。"""

    def write(self, data: bytes) -> int:
        """写一段字节，返回写入长度。"""

    def read(self, size: int) -> bytes:
        """读最多 size 字节；超时返回已读到的（可能为空/不完整）。"""

    def reset_input_buffer(self) -> None:
        """丢弃接收缓冲里的残留字节（上一次超时留下的半帧）。"""

    def close(self) -> None:
        """关闭串口。"""


TransportFactory = Callable[[], SerialTransport]


# ==================== 轮询 + 陈旧三态（语义同 serial_bridge.Bridge） ====================


class RtuMaster:
    """进程内 RTU 主站：轮询串口，维护「新鲜 / 陈旧 / 从未读到」三态。

    对外接口与 `modbus_server.RealDeviceReader` 一致（`read_once` / `snapshot` /
    `poll_forever`），所以 device 层只换来源、不换混流逻辑。

    transport_factory 注入点：默认每次打开串口时用 pyserial；单测传假对象即可覆盖
    成功 / 超时 / 坏帧 / 恢复，不需要任何硬件。
    """

    def __init__(
        self,
        port: str,
        baud: int = BAUD_RATE,
        slave: int = SLAVE_ID,
        interval: float = 1.0,
        timeout: float = 1.0,
        transport_factory: Optional[TransportFactory] = None,
    ) -> None:
        self.port = port
        self.baud = baud
        self.slave = slave
        self.interval = interval
        self.timeout = timeout
        self._factory: TransportFactory = transport_factory or self._open_serial
        self._lock = threading.Lock()
        self._transport: Optional[SerialTransport] = None
        self._last_humidity = 0
        self._last_temperature = 0
        self._failures = 0
        self._total_failures = 0
        self._have_value = False
        #: 最近一次失败原因（探针/日志用；成功时保留上一次原因，便于排障）
        self.last_error = ""
        #: 最近一次成功读到的原始帧（探针取证用；失败时保留上一帧）
        self.last_request_hex = ""
        self.last_response_hex = ""

    # -------------------------------------------------- 串口侧
    def _open_serial(self) -> SerialTransport:
        """默认传输：pyserial。**延迟导入** —— 见模块顶部「不 import pyserial 于模块级」。"""
        import serial  # noqa: PLC0415 - 延迟导入是刻意的（容器里没装 pyserial 也要能起纯仿真）

        return serial.Serial(
            port=self.port,
            baudrate=self.baud,
            bytesize=DATA_BITS,
            parity=PARITY,
            stopbits=STOP_BITS,
            timeout=self.timeout,
        )

    def _ensure_transport(self) -> SerialTransport:
        if self._transport is None:
            self._transport = self._factory()
        return self._transport

    def _close_transport(self) -> None:
        transport, self._transport = self._transport, None
        if transport is not None:
            try:
                transport.close()
            except Exception:  # noqa: BLE001 - 关不掉也不该影响下一次重连
                pass

    def _read_frame(self, transport: SerialTransport) -> bytes:
        """按「先 3 字节帧头、再按剩余长度读」取一整帧，避免异常响应白等一个超时。"""
        head = bytes(transport.read(3))
        if len(head) < 3:
            raise RtuTimeout(f"应答超时：只收到 {len(head)}/3 字节帧头")
        if head[1] & 0x80:
            remaining = 2  # 异常响应：从站 + 功能码|0x80 + 异常码 + CRC2
        else:
            remaining = EXPECTED_RESPONSE_BYTES - 3
        tail = bytes(transport.read(remaining))
        if len(tail) != remaining:
            raise RtuTimeout(f"应答超时：帧尾只收到 {len(tail)}/{remaining} 字节")
        return head + tail

    def _note_failure(self, exc: BaseException) -> int:
        with self._lock:
            self._failures += 1
            self._total_failures += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            return self._failures

    def read_once(self) -> Optional[tuple[int, int, bool]]:
        """读一次；成功返回 (湿度原始整数, 温度原始整数位型, 是否陈旧)，失败返回 None。

        成功时必然写入内部状态并清零连续失败数 —— 恢复语义就在这里。失败时关闭传输、
        下一次重连（拔插 USB-485 / 端口被别的进程放开后都能自愈，不需要重启进程）。
        """
        try:
            transport = self._ensure_transport()
            try:
                transport.reset_input_buffer()
            except Exception:  # noqa: BLE001 - 老实现可能没有这个方法，不重要
                pass
            request = build_read_request(self.slave, REG_START, REG_COUNT)
            transport.write(request)
            frame = self._read_frame(transport)
            data = parse_read_response(frame, self.slave, REG_COUNT)
            humidity_raw, temperature_raw = decode_data_segment(data)
        except Exception as exc:  # noqa: BLE001 - 读失败是常态（拔线/干扰/端口被占），必须不崩
            self._note_failure(exc)
            self._close_transport()
            return None

        with self._lock:
            if self._failures:
                LOGGER.warning("串口直读恢复（此前连续失败 %d 次）", self._failures)
            self._failures = 0
            self._last_humidity = humidity_raw
            self._last_temperature = temperature_raw
            self._have_value = True
            self.last_request_hex = request.hex(" ")
            self.last_response_hex = frame.hex(" ")
        LOGGER.debug("RTU 请求 %s → 应答 %s", request.hex(" "), frame.hex(" "))
        return humidity_raw, temperature_raw, False

    # -------------------------------------------------- 对外快照
    def snapshot(self) -> Optional[tuple[int, int, bool]]:
        """最近一次成功读数 + 当前陈旧标志，供 device 层 overlay 使用。

        - 从未读到过 ⇒ None（device 层会写越界值，与「陈旧」同样的处置）
        - 有旧值但本轮失败 ⇒ (旧值, 旧值, True)；overlay 据此写越界值，不冒充新鲜数据
        - 新鲜 ⇒ (湿度, 温度, False)
        """
        with self._lock:
            if not self._have_value:
                return None
            stale = self._failures > 0
            return self._last_humidity, self._last_temperature, stale

    def status(self) -> int:
        """状态位：0=新鲜（有值且连续失败为 0），1=陈旧（失败过或从未读到）。"""
        with self._lock:
            return STATUS_STALE if (self._failures > 0 or not self._have_value) else STATUS_FRESH

    def status_registers(self) -> list[int]:
        """与 `scripts/serial_bridge.py` 的 `Bridge.snapshot()` **同布局**的 4 个寄存器。

        [湿度×10, 温度×10(int16 补码位型), 状态位(0 新鲜/1 陈旧), 累计失败次数]

        存在的意义：直读形态没有单独的「桥状态」寄存器可读，但语义必须照旧；
        探针（`python -m src.device.rtu_master`）用这 4 个数取证，
        测试用它钉住「陈旧时不返回旧值、改返回越界值」。
        累计失败数按 uint16 截断（与桥一致）。
        """
        with self._lock:
            stale = self._failures > 0 or not self._have_value
            if stale:
                humidity_raw = STALE_HUMIDITY_RAW
                temperature_raw = STALE_TEMPERATURE_RAW
            else:
                humidity_raw = self._last_humidity
                temperature_raw = self._last_temperature
            return [
                humidity_raw,
                temperature_raw,
                STATUS_STALE if stale else STATUS_FRESH,
                min(self._total_failures, 0xFFFF),
            ]

    @property
    def consecutive_failures(self) -> int:
        with self._lock:
            return self._failures

    @property
    def total_failures(self) -> int:
        with self._lock:
            return self._total_failures

    # -------------------------------------------------- 轮询线程
    def poll_forever(self) -> None:
        """后台轮询循环：任何失败都只记日志、不抛、不退出。"""
        while True:
            got = self.read_once()
            if got is None:
                failures = self.consecutive_failures
                LOGGER.error(
                    "串口直读失败（连续第 %d 次）：%s @ %d %d%s%d，从站 %d —— "
                    "状态位=1(陈旧)、Temp/Humidity 写越界值让接入层拒收，不回落到仿真值；原因：%s",
                    failures,
                    self.port,
                    self.baud,
                    DATA_BITS,
                    PARITY,
                    STOP_BITS,
                    self.slave,
                    self.last_error,
                )
            else:
                LOGGER.info(
                    "串口直读：湿度 %.1f %%RH ｜ 温度 %.1f degC ｜ 状态位=%d",
                    got[0] / tx.SCALE,
                    int16_from_wire(got[1]) / tx.SCALE,
                    STATUS_FRESH,
                )
            time.sleep(self.interval)


# ==================== 探针（不占 TCP 端口；取证 / 排障） ====================


def main(argv: Optional[list[str]] = None) -> int:
    """探针入口：按周期打印原始帧、两个寄存器原始值、状态位与累计失败数。

    ⚠️ 它与 device 进程**抢同一个串口**：取证时先停 device 进程再跑探针，或换一个物理口。
    """
    parser = argparse.ArgumentParser(
        description="串口直读探针：打印 Modbus RTU 原始帧与两个寄存器的原始值（不占 TCP 端口）"
    )
    parser.add_argument("--port", default="COM3", help="串口名，如 COM3（Linux 上是 /dev/ttyUSB0）")
    parser.add_argument("--baud", type=int, default=BAUD_RATE, help=f"波特率（本机默认 {BAUD_RATE}）")
    parser.add_argument("--slave", type=int, default=SLAVE_ID, help=f"从站地址（默认 {SLAVE_ID}）")
    parser.add_argument("--interval", type=float, default=1.0, help="打印/轮询周期秒（默认 1.0）")
    parser.add_argument("--count", type=int, default=5, help="采集次数（默认 5；0 = 一直跑）")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    args = parser.parse_args(argv)

    # Windows 控制台默认 GBK，直接 print 中文会 UnicodeEncodeError 在报错行崩掉；
    # 显式重配 stdout 为 UTF-8（errors=replace 兜底），重定向到文件时也是 UTF-8。
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,
        stream=sys.stdout,  # 显式写 stdout：默认 stderr，重定向时容易"读数全在 .err 里、.log 是空的"
    )

    master = RtuMaster(
        port=args.port,
        baud=args.baud,
        slave=args.slave,
        interval=args.interval,
    )
    print(f"# 探针启动：port={args.port} baud={args.baud} 8N1 从站={args.slave} "
          f"请求帧={build_read_request(args.slave, REG_START, REG_COUNT).hex(' ')}")
    print("# 编号 | 读成功 | 湿度原始/工程值 | 温度原始(int16)/工程值 | 状态位 | 累计失败 | 最近失败原因")
    done = 0
    while args.count <= 0 or done < args.count:
        done += 1
        started = time.monotonic()
        got = master.read_once()
        elapsed_ms = (time.monotonic() - started) * 1000
        regs = master.status_registers()
        if got is None:
            print(
                f"{done:>3} | 失败 | 湿度 {regs[0]}({regs[0] / tx.SCALE:.1f}) "
                f"| 温度 {regs[1]}({int16_from_wire(regs[1]) / tx.SCALE:.1f}) "
                f"| 状态={regs[2]} | 累计失败={regs[3]} | {master.last_error} | {elapsed_ms:.0f} ms"
            )
        else:
            print(
                f"{done:>3} | 成功 | 湿度 {regs[0]}({regs[0] / tx.SCALE:.1f}) "
                f"| 温度 {regs[1]}({int16_from_wire(regs[1]) / tx.SCALE:.1f}) "
                f"| 状态={regs[2]} | 累计失败={regs[3]} | {elapsed_ms:.0f} ms"
            )
            print(f"      请求帧={master.last_request_hex}")
            print(f"      应答帧={master.last_response_hex}")
        if args.count <= 0 or done < args.count:
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
