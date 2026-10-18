# -*- coding: utf-8 -*-
"""device 层的「真机来源」选择与混流 overlay（`src/device/modbus_server.py` 的相关部分）。

要钉住两件事：

1. **默认行为一字不改**：不设 `REAL_DEVICE_SOURCE` / `REAL_DEVICE_ENABLE` 时仍是纯仿真；
   `tcp` 来源仍是改造前的 `RealDeviceReader`（读宿主上的 `scripts/serial_bridge.py`）。
2. **`serial` 来源接进来的只是"值从哪来"**：混流 overlay、状态位、越界值语义完全不换 ——
   读不到真机时写 301.0 degC / 101.0 %，其余 7 点照旧是仿真；恢复后自动回新鲜。

不依赖硬件：串口主站只在构造时不打开串口；overlay 用假读取器注入。
"""

from __future__ import annotations

import pathlib
import subprocess
import sys
from typing import Optional

import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS, REG_BASE, REG_COUNT   # noqa: E402
from src.device import modbus_server as ms                  # noqa: E402
from src.device import rtu_master as rtu                    # noqa: E402


def _index(point_name: str) -> int:
    """测点在寄存器数组里的下标（寄存器数组按地址摆放，REG_BASE=0）。"""
    return next(p.address - REG_BASE for p in POINTS if p.name == point_name)


class FakeSource:
    """满足 `RealDeviceSource` 的假读取器（只给 overlay 用）。"""

    def __init__(self, snap: Optional[tuple[int, int, bool]]) -> None:
        self._snap = snap

    def read_once(self) -> Optional[tuple[int, int, bool]]:
        return self._snap

    def snapshot(self) -> Optional[tuple[int, int, bool]]:
        return self._snap

    def poll_forever(self) -> None:  # pragma: no cover - overlay 测试不跑轮询
        return None


# ==================== 默认行为不变 ====================


def test_default_source_is_tcp() -> None:
    """⭐ 默认来源必须是 tcp：不设环境变量时行为与改造前完全一致。"""
    assert ms.REAL_DEVICE_SOURCE == "tcp"
    assert ms.REAL_DEVICE_SOURCES == ("tcp", "serial")


def test_parse_real_device_source_accepts_the_two_sources() -> None:
    assert ms.parse_real_device_source("tcp") == "tcp"
    assert ms.parse_real_device_source("serial") == "serial"
    assert ms.parse_real_device_source(" SERIAL ") == "serial"


def test_parse_real_device_source_rejects_unknown_value() -> None:
    """配置错要当场炸，不能静默回落到默认来源（否则会以为直读开了、其实没开）。"""
    with pytest.raises(ValueError, match="REAL_DEVICE_SOURCE"):
        ms.parse_real_device_source("rtu")
    with pytest.raises(ValueError, match="REAL_DEVICE_SOURCE"):
        ms.parse_real_device_source("")


def test_pure_simulation_overlay_is_identity_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """REAL_DEVICE_ENABLE=0（默认）：overlay 原样返回，9 个点全是仿真。"""
    monkeypatch.setattr(ms, "REAL_DEVICE_ENABLE", False)
    monkeypatch.setattr(ms, "_REAL_READER", None)
    values = list(range(REG_COUNT))
    assert ms.overlay_real_values(values, ms.REAL_DEVICE_SLAVE) == values


# ==================== 混流：2 真 + 7 仿真 ====================


def test_fresh_snapshot_overlays_only_temp_and_humidity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ms, "REAL_DEVICE_ENABLE", True)
    monkeypatch.setattr(ms, "_REAL_READER", FakeSource((561, 273, False)))
    values = list(range(REG_COUNT))

    out = ms.overlay_real_values(values, ms.REAL_DEVICE_SLAVE)

    assert out[_index("Humidity")] == 561
    assert out[_index("Temp")] == 273
    # 其余 7 个测点逐位不变（= 仿真值），这就是"2 真 + 7 仿真"
    for point in POINTS:
        if point.name not in ("Temp", "Humidity"):
            assert out[point.address - REG_BASE] == values[point.address - REG_BASE], point.name
    assert out is not values, "overlay 返回新列表，不改入参"


def test_stale_snapshot_writes_out_of_range_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """⭐ 真机陈旧 ⇒ 301.0 degC / 101.0 %（原始 3010 / 1010），不是旧值、不是 0。"""
    monkeypatch.setattr(ms, "REAL_DEVICE_ENABLE", True)
    monkeypatch.setattr(ms, "_REAL_READER", FakeSource((561, 273, True)))
    values = list(range(REG_COUNT))

    out = ms.overlay_real_values(values, ms.REAL_DEVICE_SLAVE)

    assert out[_index("Temp")] == ms.STALE_TEMP_RAW == 3010
    assert out[_index("Humidity")] == ms.STALE_HUMIDITY_RAW == 1010
    assert out[_index("Temp")] / 10 == 301.0
    assert out[_index("Humidity")] / 10 == 101.0


def test_never_read_snapshot_writes_out_of_range_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """启动时就没读到真机（降级启动）：同样写越界值，绝不把仿真温湿度冒充真机值。"""
    monkeypatch.setattr(ms, "REAL_DEVICE_ENABLE", True)
    monkeypatch.setattr(ms, "_REAL_READER", FakeSource(None))
    values = list(range(REG_COUNT))

    out = ms.overlay_real_values(values, ms.REAL_DEVICE_SLAVE)

    assert out[_index("Temp")] == ms.STALE_TEMP_RAW
    assert out[_index("Humidity")] == ms.STALE_HUMIDITY_RAW


def test_overlay_skips_non_target_slave(monkeypatch: pytest.MonkeyPatch) -> None:
    """真机只接 1 号炉：其它从站的温湿度仍是仿真。"""
    monkeypatch.setattr(ms, "REAL_DEVICE_ENABLE", True)
    monkeypatch.setattr(ms, "_REAL_READER", FakeSource((561, 273, False)))
    monkeypatch.setattr(ms, "REAL_DEVICE_SLAVE", 1)
    values = list(range(REG_COUNT))

    assert ms.overlay_real_values(values, 2) == values


# ==================== 来源选择 ====================


def test_build_reader_for_serial_source_returns_rtu_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REAL_DEVICE_SOURCE=serial ⇒ 进程内 RTU 主站，参数取自模块配置。"""
    monkeypatch.setattr(ms, "REAL_DEVICE_SOURCE", "serial")
    monkeypatch.setattr(ms, "REAL_DEVICE_SERIAL_PORT", "COM_TEST")
    monkeypatch.setattr(ms, "REAL_DEVICE_SERIAL_BAUD", 4800)
    monkeypatch.setattr(ms, "REAL_DEVICE_SERIAL_TIMEOUT", 0.8)
    monkeypatch.setattr(ms, "REAL_DEVICE_UNIT", 1)
    monkeypatch.setattr(ms, "REAL_DEVICE_POLL_INTERVAL", 2.0)

    reader, target = ms.build_real_reader()

    assert isinstance(reader, rtu.RtuMaster)
    assert reader.port == "COM_TEST"
    assert reader.baud == 4800
    assert reader.slave == 1
    assert reader.interval == 2.0
    assert reader.timeout == 0.8
    assert "COM_TEST" in target and "RTU" in target
    # 构造不应该打开串口（也不应该 import pyserial）：单测无硬件也能跑
    assert reader._transport is None  # noqa: SLF001 - 断言"没碰硬件"


def test_build_reader_for_tcp_source_keeps_serial_bridge_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """默认 tcp 来源 ⇒ 仍是改造前的 RealDeviceReader（读桥的 Modbus TCP）。"""
    monkeypatch.setattr(ms, "REAL_DEVICE_SOURCE", "tcp")
    monkeypatch.setattr(ms, "REAL_DEVICE_HOST", "host.docker.internal")
    monkeypatch.setattr(ms, "REAL_DEVICE_PORT", 5021)
    monkeypatch.setattr(ms, "REAL_DEVICE_UNIT", 1)
    monkeypatch.setattr(ms, "REAL_DEVICE_POLL_INTERVAL", 2.0)

    reader, target = ms.build_real_reader()

    assert isinstance(reader, ms.RealDeviceReader)
    assert (reader.host, reader.port, reader.unit, reader.interval) == (
        "host.docker.internal",
        5021,
        1,
        2.0,
    )
    assert "serial_bridge" in target


def test_troubleshooting_text_differs_by_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """降级启动的排查提示必须对得上来源（给错提示比不给更耽误事）。"""
    monkeypatch.setattr(ms, "REAL_DEVICE_SOURCE", "serial")
    serial_tip = ms.real_reader_troubleshooting()
    assert "串口名" in serial_tip and "pyserial" in serial_tip

    monkeypatch.setattr(ms, "REAL_DEVICE_SOURCE", "tcp")
    tcp_tip = ms.real_reader_troubleshooting()
    assert "serial_bridge.py" in tcp_tip and "5021" in tcp_tip


def _must_not_open_serial():
    """构造读取器时不应该打开串口；真被调用就说明有人把 I/O 挪进了 __init__。"""
    raise AssertionError("构造 RtuMaster 时不应该打开串口")


def test_rtu_master_satisfies_the_reader_interface() -> None:
    """两种来源接口一致（duck typing）：read_once/snapshot/poll_forever 都在。"""
    master = rtu.RtuMaster(port="COM_TEST", transport_factory=_must_not_open_serial)
    for name in ("read_once", "snapshot", "poll_forever"):
        assert callable(getattr(master, name))


def test_modbus_server_still_imports_and_simulates_without_pyserial() -> None:
    """⭐⭐ 回归钉子：**没有 pyserial** 时 device 层必须仍能导入、纯仿真照常工作。

    容器里只装 `requirements.txt` 的运行期依赖，而 pyserial 是"宿主直读"才需要的。
    本模块把 pyserial 的导入放在"真正打开串口"那一步（延迟导入），这条测试把它钉死：
    用子进程 + `sys.modules['serial'] = None` 构造"serial 不可用"，验证：

    1. `import src.device.modbus_server` 不碰 serial，能成功；
    2. 纯仿真读数路径（`build_registers`）照常工作（9 个寄存器）；
    3. 只有真去打开串口时才失败，且失败被 `read_once` 吞成"陈旧"而不是抛出去。
    """
    code = (
        "import sys\n"
        "sys.modules['serial'] = None\n"
        "import src.device.modbus_server as ms\n"
        "assert ms.REAL_DEVICE_SOURCE == 'tcp', ms.REAL_DEVICE_SOURCE\n"
        "values = ms.build_registers()\n"
        "assert len(values) == 9, len(values)\n"
        "from src.device.rtu_master import RtuMaster\n"
        "master = RtuMaster(port='COM_NOT_EXIST')\n"
        "assert master.read_once() is None\n"
        "assert master.status() == 1\n"
        "print('ok', len(values), master.last_error.split(':')[0])\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"没有 pyserial 时导入/纯仿真失败：\n{result.stderr}"
    # 失败类型是 ImportError 的一个子类（实测 3.12 下为 ModuleNotFoundError），两者都算
    assert "ok 9 " in result.stdout, result.stdout
    assert "ImportError" in result.stdout or "ModuleNotFoundError" in result.stdout, result.stdout
    reader = ms.RealDeviceReader("127.0.0.1", 5021, 1, 2.0)
    for name in ("read_once", "snapshot", "poll_forever"):
        assert callable(getattr(reader, name))
