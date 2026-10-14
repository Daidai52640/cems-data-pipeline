# -*- coding: utf-8 -*-
"""ADR-0011 的三件事：上游只读 Modbus 服务 / 心跳状态快照 / 契约指纹。

分四组：
  1. 契约指纹的性质（稳定、灵敏、不受无关字段影响）
  2. 寄存器编码与地图（必须与设备层逐值一致，否则"上游直读"与"读设备"对不上）
  3. 快照与心跳（陈旧语义、原子写、向后兼容）
  4. 端到端：真起一个只读服务，用 pymodbus 客户端读、并确认写入被拒
"""

from __future__ import annotations

import json
import socket
import time

import pytest
from pymodbus.client import ModbusTcpClient

from src.common import points
from src.device import modbus_server
from src.gateway import upstream_modbus as U
from src.platform import subscriber_to_td as S

# ---------------------------------------------------------------- 测试数据

VALUES = {
    "Flow": 1234.0, "Dust": 12.34, "SO2": 35.2, "NOx": 18.5, "O2": 8.1,
    "Velocity": 9.9, "Temp": 25.1, "Humidity": 64.8, "Pressure": 101.3,
}
#: 与设备层 encode() 同口径的期望寄存器值
EXPECTED_REGS = [1234, 1234, 352, 185, 81, 99, 251, 648, 1013]


def fresh_snapshot(**flags) -> U.Snapshot:
    snap = U.Snapshot()
    snap.update(VALUES, modbus_ok=flags.get("modbus_ok", True),
                mqtt_ok=flags.get("mqtt_ok", True))
    return snap


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ================================================================ 1. 契约指纹


def test_contract_version_is_stable_and_hex():
    first = points.contract_version()
    assert first == points.contract_version(), "纯函数：同进程多次调用必须一致"
    assert len(first) == 8
    int(first, 16)          # 必须是合法十六进制（要能塞进一个寄存器）


def test_contract_version_changes_when_value_semantics_change(monkeypatch):
    """量程 / scale / 地址 / 列名 / 线格式 变了，指纹必须变 —— 否则它白加。"""
    base = points.contract_version()
    for field, new_value in [("scale", 999), ("low", -5.0), ("high", 1e9),
                             ("address", 42), ("column", "so2_x"),
                             ("dtype", "int16"), ("endian", "CDAB")]:
        tampered = list(points.POINTS)
        tampered[2] = tampered[2]._replace(**{field: new_value})
        monkeypatch.setattr(points, "POINTS", tuple(tampered))
        assert points.contract_version() != base, f"{field} 变了指纹却没变"
        monkeypatch.undo()


def test_contract_version_ignores_fields_that_do_not_change_the_number(monkeypatch):
    """unit / code / limit 不决定"这个数字是什么"，改它们不该让历史数据换版。"""
    base = points.contract_version()
    for field, new_value in [("unit", "ppm"), ("code", "z99999"), ("limit", 12345.0)]:
        tampered = list(points.POINTS)
        tampered[2] = tampered[2]._replace(**{field: new_value})
        monkeypatch.setattr(points, "POINTS", tuple(tampered))
        assert points.contract_version() == base, f"{field} 不该影响指纹"
        monkeypatch.undo()


# ================================================================ 2. 寄存器编码


def test_encoding_matches_device_layer_exactly():
    """上游读到的数必须与"直接读设备"逐值相同 —— 这是整个功能的立足点。"""
    for point in points.POINTS:
        mine = U.encode_point_value(point.name, VALUES[point.name], point.scale)
        theirs = modbus_server.encode(VALUES[point.name], point.scale)
        assert mine == theirs, f"{point.name}: 上游编码 {mine} != 设备层 {theirs}"


def test_encoding_overflow_writes_marker_not_wrapped_value():
    """越界绝不能回绕：回绕出来的数看起来完全正常，是最难查的一类故障。"""
    assert U.encode_point_value("Temp", 99999.0, 10) == U.REG_INVALID_MARKER
    assert U.encode_point_value("Temp", -1.0, 10) == U.REG_INVALID_MARKER
    assert U.encode_point_value("Temp", float("nan"), 10) == U.REG_INVALID_MARKER


def test_marker_is_impossible_for_any_legitimate_value():
    """哨兵必须撞不上任何合法值，否则"非法"和"真值"分不开。"""
    worst = max(
        round(point.high * point.scale) for point in points.POINTS
    )
    assert worst < U.REG_INVALID_MARKER


def test_build_registers_layout_matches_expected():
    regs = U.build_registers(fresh_snapshot(), 5.0, 15.0)
    assert regs[:9] == EXPECTED_REGS
    assert len(regs) == U.TOTAL_REGS


def test_build_registers_gap_is_zero():
    """9..99 是保留区，必须是 0 —— 别在这里偷偷放数据，那会破坏 0..8 的兼容性。"""
    regs = U.build_registers(fresh_snapshot(), 5.0, 15.0)
    assert regs[9:U.STATUS_BASE] == [0] * (U.STATUS_BASE - 9)


def test_build_registers_status_block_fields():
    snap = fresh_snapshot(modbus_ok=True, mqtt_ok=False)
    regs = U.build_registers(snap, 5.0, 15.0)
    base = U.STATUS_BASE

    epoch = (regs[base] << 16) | regs[base + 1]
    assert abs(epoch - int(snap.sampled_at)) <= 1, "时间戳要能拼回来"

    assert ((regs[base + 2] << 16) | regs[base + 3]) == 1, "采样计数应为 1"
    assert regs[base + 4] & U.FLAG_MODBUS_OK, "采集正常位"
    assert not regs[base + 4] & U.FLAG_MQTT_OK, "MQTT 断线位不应置起"
    assert not regs[base + 4] & U.FLAG_STALE, "刚采到的快照不该是陈旧"
    assert regs[base + 5] == 50, "轮询周期 5.0s → 50（×10）"
    assert regs[base + 6] == int(points.contract_version(), 16) & 0xFFFF
    assert regs[base + 7] == 150, "陈旧门限 15.0s → 150（×10）"


def test_build_registers_empty_snapshot_is_stale_not_zero_looking():
    """"从未采到"必须体现成 stale 位，而不是让人看到一个全 0 的"合法读数"。"""
    regs = U.build_registers(U.Snapshot(), 5.0, 15.0)
    assert regs[:9] == [0] * 9
    assert regs[U.STATUS_BASE + 4] & U.FLAG_STALE


# ================================================================ 3. 快照与心跳


def test_snapshot_never_sampled_is_stale():
    assert U.Snapshot().is_stale(15.0) is True


def test_snapshot_touch_flags_does_not_refresh_sample_time():
    """读失败只改状态位：一旦刷了时间戳，stale 位就永远不亮 —— 那等于把
    "读不到设备"伪装成"数据是新的"。"""
    snap = fresh_snapshot()
    before = snap.sampled_at
    time.sleep(0.01)
    snap.touch_flags(modbus_ok=False, mqtt_ok=False)
    assert snap.sampled_at == before
    assert snap.samples == 1
    assert snap.modbus_ok is False


def test_snapshot_copy_is_independent():
    snap = fresh_snapshot()
    clone = snap.snapshot_for_registers()
    clone.values["Temp"] = 999.0
    assert snap.values["Temp"] == VALUES["Temp"], "拷贝不能反向污染原快照"


def test_heartbeat_payload_shape():
    from src.gateway import gateway as G

    hb = G.heartbeat_payload(modbus_ok=True, mqtt_ok=False)
    assert hb["cfg"] == G.CFG_VERSION
    assert hb["modbus_ok"] is True and hb["mqtt_ok"] is False
    assert isinstance(hb["backlog_bytes"], int)
    for key in ("samples", "published", "cached", "read_failures", "modbus_reconnects"):
        assert key in hb, f"心跳快照缺计数 {key}"


def test_touch_heartbeat_writes_json_atomically(tmp_path, monkeypatch):
    from src.gateway import gateway as G

    target = tmp_path / ".gateway_alive"
    monkeypatch.setattr(G, "HEARTBEAT_FILE", target)
    G.STATS.update({"samples": 7, "published": 6, "cached": 1})

    G.touch_heartbeat(modbus_ok=True, mqtt_ok=True)

    parsed = json.loads(target.read_text(encoding="utf-8"))
    assert parsed["samples"] == 7 and parsed["cached"] == 1
    assert parsed["modbus_ok"] is True
    # 原子写：不留临时文件，否则读的人可能撞见写了一半的内容
    assert not (tmp_path / (target.name + ".tmp")).exists()


def test_heartbeat_mtime_still_fresh_for_healthcheck(tmp_path, monkeypatch):
    """healthcheck 判的是 mtime，与内容无关 —— 换成 JSON 必须仍然保活。"""
    from src.gateway import gateway as G

    target = tmp_path / ".gateway_alive"
    monkeypatch.setattr(G, "HEARTBEAT_FILE", target)
    G.touch_heartbeat(modbus_ok=True, mqtt_ok=True)
    assert time.time() - target.stat().st_mtime < 1.0


# ================================================================ 4. 报文契约指纹


def test_build_payload_carries_cfg_version():
    from src.gateway import gateway as G

    payload = G.build_payload(VALUES)
    assert payload.endswith(f"Cfg={G.CFG_VERSION}")
    # 前 20 字符仍是时间戳：按位置切分的既有调用方（loadgen）不能被破坏
    assert payload[:19].count("-") == 2 and payload[19] == " "


def test_payload_without_cfg_is_still_parseable():
    """老网关不发 Cfg：接入层必须照常解析，不能因此拒收。"""
    payload = "2026-10-14 12:00:00 " + " ".join(
        f"{p.name}={VALUES[p.name]}" for p in points.POINTS
    ) + " Flag=N"
    _ts, values, invalid = S.parse_payload(payload)
    assert len(values) == 9 and not invalid


def test_check_contract_version_matching_does_not_count():
    from src.gateway import gateway as G

    before = S.STATS["cfg_mismatch"]
    assert S.check_contract_version(G.build_payload(VALUES)) == S.CFG_VERSION
    assert S.STATS["cfg_mismatch"] == before


def test_check_contract_version_mismatch_counts():
    from src.gateway import gateway as G

    payload = G.build_payload(VALUES).replace(f"Cfg={G.CFG_VERSION}", "Cfg=deadbeef")
    before = S.STATS["cfg_mismatch"]
    assert S.check_contract_version(payload) == "deadbeef"
    assert S.STATS["cfg_mismatch"] == before + 1


def test_check_contract_version_missing_returns_none():
    assert S.check_contract_version("2026-10-14 12:00:00 SO2=1.0 Flag=N") is None


# ================================================================ 5. 只读数据块


def test_block_rejects_writes_but_allows_internal_refresh():
    block = U._ReadOnlyBlock(0, [0] * U.TOTAL_REGS)
    with pytest.raises(ValueError):
        block.setValues(0, [7] * 9)          # 服务端收到写请求 → 拒绝
    block.refresh(0, [7] * 9)                # 本进程刷新 → 允许
    assert block.getValues(0, 9) == [7] * 9


# ================================================================ 6. 端到端（真 socket）


def test_upstream_server_end_to_end_read_and_write_rejected():
    """真起服务、真连、真读。这是唯一能证明"上游直读"真的通了的测试。"""
    port = free_port()
    snap = fresh_snapshot()
    server = U.UpstreamServer(snap, host="127.0.0.1", port=port, poll_interval=5.0)
    if not server.start():
        pytest.skip("端口刚被占用，跳过端到端")
    try:
        client = ModbusTcpClient("127.0.0.1", port=port, timeout=3)
        assert client.connect(), "连不上刚起的只读服务"

        # ---- 读：9 个测点必须与设备层同值 ----
        rr = _read(client, address=0, count=9)
        assert not rr.isError(), f"读失败: {rr}"
        assert list(rr.registers) == EXPECTED_REGS

        # ---- 读：状态区 ----
        rs = _read(client, address=U.STATUS_BASE, count=8)
        assert not rs.isError()
        assert ((rs.registers[2] << 16) | rs.registers[3]) == 1
        assert rs.registers[4] & U.FLAG_MODBUS_OK
        assert rs.registers[6] == int(points.contract_version(), 16) & 0xFFFF

        # ---- 写：必须被拒（只读服务）----
        wr = _write(client, address=0, value=4321)
        assert wr.isError(), "写请求竟然成功了 —— 只读服务失效"

        # ---- 写被拒后，值不能被改动 ----
        again = _read(client, address=0, count=1)
        assert again.registers[0] == EXPECTED_REGS[0], "写失败却改动了值"

        client.close()
    finally:
        server.stop()


def _read(client: ModbusTcpClient, *, address: int, count: int):
    """兼容 pymodbus 3.6（slave=）与 3.9+（device_id=）两种参数名。"""
    try:
        return client.read_holding_registers(address=address, count=count, slave=1)
    except TypeError:
        return client.read_holding_registers(address=address, count=count, device_id=1)


def _write(client: ModbusTcpClient, *, address: int, value: int):
    try:
        return client.write_register(address=address, value=value, slave=1)
    except TypeError:
        return client.write_register(address=address, value=value, device_id=1)


def test_upstream_server_refuses_second_instance_on_same_port():
    """端口被占必须返回 False 并只记日志 —— 多实例部署时最容易犯的配置错。"""
    port = free_port()
    first = U.UpstreamServer(fresh_snapshot(), host="127.0.0.1", port=port)
    assert first.start() is True
    try:
        second = U.UpstreamServer(fresh_snapshot(), host="127.0.0.1", port=port)
        assert second.start() is False
    finally:
        first.stop()
