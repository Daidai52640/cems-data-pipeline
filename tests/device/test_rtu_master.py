# -*- coding: utf-8 -*-
"""`src/device/rtu_master.py`（进程内 Modbus RTU 主站）的离线单元测试。

⭐ 这个文件的立场：**不依赖任何硬件**。串口能力被抽成 `SerialTransport`，测试注入
`FakeSerial`，于是"成功 / 超时 / 坏帧 / 恢复 / 状态位 / 越界值"都能在毫秒级复现，
不需要 COM 口、不需要变送器、不需要拔线。真机与模拟对端的实测证据在
`docs/runbooks/直读串口.md` 与 `docs/evidence/serial-20261018/`。

要钉住的四条语义（与 `scripts/serial_bridge.py` 一致，不得回退）：
1. 读失败 ⇒ 状态位 1（陈旧），对外值是**越界值** 101.0 % / 301.0 degC，**绝不用 0**、
   也不返回上一次好值；
2. 任何失败都不抛给调用方（设备层线程不能被一个坏帧带走）；
3. 恢复后自动回新鲜（连续失败清零，累计失败保留为历史）；
4. 帧编解码与真机实测字节逐位一致（请求 `01 03 00 00 00 02 C4 0B`、
   应答 `01 03 04 02 31 01 11 6B D8`）。
"""

from __future__ import annotations

import pathlib
import sys
from collections import deque
from typing import Optional

import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
_SCRIPTS = PROJECT_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from src.common import points_transmitter as tx          # noqa: E402
from src.device import rtu_master as rtu                # noqa: E402


# ==================== 测试替身：假串口 ====================


class FakeSerial:
    """假串口：按字节流返回预置应答，记录写入的请求。一次实例 = 一次"打开"。"""

    def __init__(
        self,
        *responses: bytes,
        write_error: Optional[BaseException] = None,
    ) -> None:
        self._pending: deque[bytes] = deque(responses)
        self._rx = bytearray()
        self.writes: list[bytes] = []
        self.closed = False
        self.reset_count = 0
        self._write_error = write_error

    def write(self, data: bytes) -> int:
        self.writes.append(bytes(data))
        if self._write_error is not None:
            raise self._write_error
        return len(data)

    def read(self, size: int) -> bytes:
        # 模拟真实串口：给多少拿多少；没有数据就返回空（= 超时）
        if not self._rx and self._pending:
            self._rx += self._pending.popleft()
        take = min(size, len(self._rx))
        chunk = bytes(self._rx[:take])
        del self._rx[:take]
        return chunk

    def reset_input_buffer(self) -> None:
        self.reset_count += 1

    def close(self) -> None:
        self.closed = True


class FakeFactory:
    """每次打开返回列表里的下一个假串口（用完之后返回一个没有应答的口 = 超时）。"""

    def __init__(self, transports: list[FakeSerial]) -> None:
        self._transports = list(transports)
        self.opens = 0

    def __call__(self) -> FakeSerial:
        self.opens += 1
        if self._transports:
            return self._transports.pop(0)
        return FakeSerial()


def response_frame(humidity_raw: int, temperature_raw_wire: int) -> bytes:
    """按真机应答格式拼帧：从站 + 03 + 字节数 4 + 湿度(uint16) + 温度(int16 位型) + CRC。"""
    payload = (
        bytes([rtu.SLAVE_ID, rtu.FUNCTION_CODE, 2 * rtu.REG_COUNT])
        + (humidity_raw & 0xFFFF).to_bytes(2, "big")
        + (temperature_raw_wire & 0xFFFF).to_bytes(2, "big")
    )
    return rtu.append_crc(payload)


#: 真机实测应答（docs/runbooks/物理层取证.md）：湿度 561 → 56.1 %RH，温度 273 → 27.3 degC
REAL_RESPONSE = response_frame(561, 273)


def make_master(
    *responses: bytes,
    write_error: Optional[BaseException] = None,
) -> tuple[rtu.RtuMaster, FakeSerial]:
    transport = FakeSerial(*responses, write_error=write_error)
    master = rtu.RtuMaster(
        port="FAKE0",
        interval=0.0,
        transport_factory=lambda: transport,
    )
    return master, transport


# ==================== 帧编解码（纯函数） ====================


def test_request_frame_matches_physical_layer_capture() -> None:
    """⭐ 请求帧必须与逻辑分析仪抓到的真机帧逐字节相同：`01 03 00 00 00 02 C4 0B`。"""
    assert rtu.build_read_request() == bytes.fromhex("010300000002c40b")


def test_crc16_known_vectors() -> None:
    """CRC 用两条独立已知向量钉住（低字节先发的约定也在这）。"""
    assert rtu.crc16_modbus(bytes.fromhex("010300000002")) == 0x0BC4   # 帧尾 C4 0B
    assert rtu.crc16_modbus(bytes.fromhex("01030402310111")) == 0xD86B  # 帧尾 6B D8


def test_parse_real_response_returns_data_segment() -> None:
    """实测应答 → 数据段，两步解码得到 (561, 273)，与真机 runbook 的样本一致。"""
    data = rtu.parse_read_response(bytes.fromhex("01030402310111 6bd8".replace(" ", "")))
    assert data == bytes.fromhex("02310111")
    assert rtu.decode_data_segment(data) == (561, 273)


def test_negative_temperature_keeps_int16_twos_complement_on_the_wire() -> None:
    """⭐ 温度是 int16：`FF 9F` = -97 → -9.7 degC；对外寄存器保留补码位型 65439。

    如果这里退化成 uint16，-9.7 degC 会变成 6543.9 degC —— 一个"看起来合法"的错数。
    """
    data = rtu.decode_data_segment(bytes.fromhex("01e6ff9f"))
    assert data == (486, 0xFF9F)
    assert rtu.int16_from_wire(0xFF9F) == -97
    assert rtu.int16_from_wire(0x0111) == 273


def test_build_request_rejects_illegal_arguments() -> None:
    with pytest.raises(ValueError, match="从站地址"):
        rtu.build_read_request(slave=248)
    with pytest.raises(ValueError, match="读取数量"):
        rtu.build_read_request(count=0)
    with pytest.raises(ValueError, match="读取数量"):
        rtu.build_read_request(count=126)


def test_parse_rejects_bad_crc() -> None:
    frame = bytearray(REAL_RESPONSE)
    frame[-1] ^= 0xFF
    with pytest.raises(rtu.RtuFrameError, match="CRC"):
        rtu.parse_read_response(bytes(frame))


def test_parse_rejects_wrong_slave() -> None:
    frame = rtu.append_crc(bytes([0x02, 0x03, 0x04]) + bytes.fromhex("02310111"))
    with pytest.raises(rtu.RtuFrameError, match="从站地址不符"):
        rtu.parse_read_response(frame)


def test_parse_rejects_wrong_function_code() -> None:
    frame = rtu.append_crc(bytes([0x01, 0x04, 0x04]) + bytes.fromhex("02310111"))
    with pytest.raises(rtu.RtuFrameError, match="功能码不符"):
        rtu.parse_read_response(frame)


def test_parse_rejects_bad_byte_count() -> None:
    frame = rtu.append_crc(bytes([0x01, 0x03, 0x02]) + bytes.fromhex("02310111"))
    with pytest.raises(rtu.RtuFrameError, match="字节数字段不符"):
        rtu.parse_read_response(frame)


def test_parse_rejects_truncated_frame() -> None:
    with pytest.raises(rtu.RtuFrameError, match="太短"):
        rtu.parse_read_response(b"\x01\x03")


def test_exception_response_is_reported_as_modbus_exception_not_bad_frame() -> None:
    """从站异常响应（功能码最高位置 1）按异常码报，便于排查"地址/功能码不被支持"。"""
    frame = rtu.append_crc(bytes([0x01, 0x83, 0x02]))
    with pytest.raises(rtu.RtuExceptionResponse) as excinfo:
        rtu.parse_read_response(frame)
    assert excinfo.value.code == 2


# ==================== 越界哨兵（与 serial_bridge 逐位一致） ====================


def test_stale_sentinels_are_101_and_301() -> None:
    """⭐ 越界值就是 101.0 % / 301.0 degC（原始整数 1010 / 3010）。"""
    assert rtu.STALE_HUMIDITY_RAW == 1010
    assert rtu.STALE_TEMPERATURE_RAW == 3010
    assert rtu.STALE_HUMIDITY_RAW / tx.SCALE == 101.0
    assert rtu.STALE_TEMPERATURE_RAW / tx.SCALE == 301.0


def test_stale_sentinels_match_serial_bridge_exactly() -> None:
    """⭐ 两条真机路径的哨兵/状态位必须逐位相等 —— 否则同一次故障在两个形态下语义不同。"""
    import serial_bridge as sb

    assert rtu.STALE_HUMIDITY_RAW == sb.STALE_HUMIDITY_RAW
    assert rtu.STALE_TEMPERATURE_RAW == sb.STALE_TEMPERATURE_RAW
    assert (rtu.STATUS_FRESH, rtu.STATUS_STALE) == (sb.STATUS_FRESH, sb.STATUS_STALE)
    assert (rtu.STATUS_FRESH, rtu.STATUS_STALE) == (0, 1)


def test_sentinels_out_of_range_for_both_contracts() -> None:
    """自检必须真能拦：把温度哨兵调到 CEMS 量程内 ⇒ 抛 ValueError。"""
    from src.common import points_transmitter

    for cems_name, raw, tx_name in (
        ("Humidity", rtu.STALE_HUMIDITY_RAW, "AirHumidity"),
        ("Temp", rtu.STALE_TEMPERATURE_RAW, "AirTemp"),
    ):
        value = raw / tx.SCALE
        cems = next(p for p in rtu.CEMS_POINTS if p.name == cems_name)
        device = next(p for p in points_transmitter.POINTS if p.name == tx_name)
        assert not (cems.low <= value <= cems.high), f"{cems_name} 没超出 CEMS 量程"
        assert not (device.low <= value <= device.high), f"{cems_name} 没超出变送器量程"


def test_import_time_self_check_rejects_a_too_small_sentinel(monkeypatch: pytest.MonkeyPatch) -> None:
    """防的是"有人放宽 Temp 量程，而哨兵还是 301.0" —— 那时无效标记会悄悄变成合法读数。"""
    monkeypatch.setattr(rtu, "STALE_TEMPERATURE_RAW", 26 * tx.SCALE)
    with pytest.raises(ValueError, match="越界哨兵失效"):
        rtu.validate_stale_sentinels()


def test_zero_is_never_used_as_a_sentinel() -> None:
    """⭐ 为什么哨兵不能用 0：0 落在两套契约的量程内，会被当成真实读数一路入库。"""
    assert rtu.STALE_HUMIDITY_RAW > 0
    assert rtu.STALE_TEMPERATURE_RAW > 0
    cems_temp = next(p for p in rtu.CEMS_POINTS if p.name == "Temp")
    assert cems_temp.low <= 0.0 <= cems_temp.high, "前提变了：0 现在不在量程内，本测试的论证需重写"


# ==================== 读成功 ====================


def test_read_once_success_updates_state_and_returns_fresh() -> None:
    master, transport = make_master(REAL_RESPONSE)

    got = master.read_once()

    assert got == (561, 273, False)
    assert master.status() == rtu.STATUS_FRESH
    assert master.snapshot() == (561, 273, False)
    assert master.status_registers() == [561, 273, rtu.STATUS_FRESH, 0]
    assert transport.writes == [bytes.fromhex("010300000002c40b")], "写出的请求帧必须对"
    assert master.last_request_hex == "01 03 00 00 00 02 c4 0b"
    assert master.last_response_hex == "01 03 04 02 31 01 11 6b d8"


def test_read_once_uses_reset_input_buffer_before_each_request() -> None:
    """超时后串口里可能留着半帧：请求前必须清接收缓冲，否则下一次读会错位。"""
    master, transport = make_master(REAL_RESPONSE)
    master.read_once()
    assert transport.reset_count >= 1


# ==================== 读失败：状态位 + 越界值，且绝不返回旧值 ====================


def test_timeout_marks_stale_and_writes_sentinels() -> None:
    master, _ = make_master()  # 没有任何应答 = 读超时

    assert master.read_once() is None

    assert master.status() == rtu.STATUS_STALE
    assert master.status_registers() == [
        rtu.STALE_HUMIDITY_RAW,
        rtu.STALE_TEMPERATURE_RAW,
        rtu.STATUS_STALE,
        1,
    ]
    assert master.consecutive_failures == 1
    assert "RtuTimeout" in master.last_error


def test_failure_after_success_does_not_return_the_last_good_value() -> None:
    """⭐⭐ 本模块最关键的一条：陈旧时不返回旧值，改返回越界值。

    旧值保留在 snapshot() 里但打上 stale 标记，device 层 overlay 据此写越界值；
    任何绕过状态位直接读值的人，看到的也是越界值 —— 洞不留给下一个消费者。
    """
    transport = FakeSerial(REAL_RESPONSE)  # 只有一帧：第一次成功，第二次超时
    master = rtu.RtuMaster(port="FAKE0", interval=0.0, transport_factory=lambda: transport)

    assert master.read_once() == (561, 273, False)
    assert master.read_once() is None

    assert master.status() == rtu.STATUS_STALE
    assert master.status_registers()[0] == rtu.STALE_HUMIDITY_RAW
    assert master.status_registers()[1] == rtu.STALE_TEMPERATURE_RAW
    assert master.snapshot() == (561, 273, True)
    assert master.consecutive_failures == 1
    assert master.total_failures == 1


def test_bad_crc_frame_marks_stale_instead_of_using_garbage() -> None:
    corrupted = bytearray(REAL_RESPONSE)
    corrupted[-2] ^= 0xFF
    master, _ = make_master(bytes(corrupted))

    assert master.read_once() is None
    assert master.status() == rtu.STATUS_STALE
    assert master.status_registers()[0] == rtu.STALE_HUMIDITY_RAW
    assert "RtuFrameError" in master.last_error


def test_exception_response_marks_stale() -> None:
    master, _ = make_master(rtu.append_crc(bytes([0x01, 0x83, 0x02])))
    assert master.read_once() is None
    assert master.status() == rtu.STATUS_STALE
    assert "RtuExceptionResponse" in master.last_error


def test_write_error_does_not_raise() -> None:
    """端口被别的进程占着时 write 抛 OSError：read_once 必须吞掉，进程不能崩。"""
    master, _ = make_master(write_error=OSError("Access is denied"))
    assert master.read_once() is None
    assert "OSError" in master.last_error
    assert master.status() == rtu.STATUS_STALE


def test_serial_open_failure_does_not_raise() -> None:
    """串口打不开（拔掉 USB-485 / 端口被占）：不抛、判陈旧、下次重连。"""
    attempts: list[int] = []

    def factory() -> FakeSerial:
        attempts.append(1)
        raise OSError("could not open port COM3")

    master = rtu.RtuMaster(port="COM3", interval=0.0, transport_factory=factory)
    assert master.read_once() is None
    assert len(attempts) == 1
    assert "could not open port COM3" in master.last_error
    assert master.status() == rtu.STATUS_STALE


# ==================== 恢复 ====================


def test_recovers_automatically_after_transient_failure() -> None:
    """⭐ 恢复语义：下一次成功读取即清零连续失败、状态位回 0；累计失败保留为历史。"""
    factory = FakeFactory([FakeSerial(), FakeSerial(REAL_RESPONSE)])
    master = rtu.RtuMaster(port="FAKE0", interval=0.0, transport_factory=factory)

    assert master.read_once() is None
    assert master.consecutive_failures == 1
    assert master.status() == rtu.STATUS_STALE

    assert master.read_once() == (561, 273, False)
    assert master.consecutive_failures == 0
    assert master.status() == rtu.STATUS_FRESH
    assert master.snapshot() == (561, 273, False)
    # 累计失败次数是"出过几次事"的历史，不随恢复清零
    assert master.status_registers()[3] == 1
    assert factory.opens == 2, "失败后必须关闭旧口并重开，而不是继续用坏掉的口"


def test_recovers_after_serial_open_failure_when_port_becomes_free() -> None:
    """端口从"被占用"变成"可用"：同一进程内自愈，不需要重启。"""
    state = {"busy": True}

    def factory() -> FakeSerial:
        if state["busy"]:
            raise OSError("端口被占用")
        return FakeSerial(REAL_RESPONSE)

    master = rtu.RtuMaster(port="COM3", interval=0.0, transport_factory=factory)

    assert master.read_once() is None
    assert master.status() == rtu.STATUS_STALE

    state["busy"] = False
    assert master.read_once() == (561, 273, False)
    assert master.status() == rtu.STATUS_FRESH


def test_never_read_snapshot_is_none_and_registers_are_sentinels() -> None:
    """启动时就读不到：从未读到 ⇒ snapshot None（device 层写越界值），状态位=1。"""
    master = rtu.RtuMaster(port="COM3", interval=0.0, transport_factory=FakeFactory([]))

    assert master.snapshot() is None
    assert master.status() == rtu.STATUS_STALE
    assert master.status_registers()[:3] == [
        rtu.STALE_HUMIDITY_RAW,
        rtu.STALE_TEMPERATURE_RAW,
        rtu.STATUS_STALE,
    ]


def test_status_registers_cap_cumulative_failures_at_uint16() -> None:
    """累计失败数按 uint16 截断（与 serial_bridge 的 0xFFFF 上限一致）。"""
    master = rtu.RtuMaster(port="FAKE0", interval=0.0, transport_factory=FakeFactory([]))
    master._total_failures = 0x1_0000  # noqa: SLF001 - 测试直接摆状态，不靠跑 65536 次
    assert master.status_registers()[3] == 0xFFFF


def test_consecutive_failures_climb_on_repeated_failures() -> None:
    master = rtu.RtuMaster(port="FAKE0", interval=0.0, transport_factory=FakeFactory([]))
    for expected in (1, 2, 3):
        assert master.read_once() is None
        assert master.consecutive_failures == expected
    assert master.total_failures == 3
