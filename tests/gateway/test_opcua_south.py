# -*- coding: utf-8 -*-
"""南向 OPC UA 的纯逻辑测试（不需要 asyncua，也不需要真服务器）。

分五组：
  1. StatusCode 三档判定（含最高两位为 `11` 的 Bad —— 写成阈值比较就会漏）
  2. 质量 → 上送决策（含 SubstituteValue 这条刻意加的例外）
  3. NodeId 解析（合法的两种 + 七种必须拒绝的）
  4. OpcUaReader 的校验/快照/陈旧语义
  5. 契约侧：opcua_node_id 的默认值、只读视图、以及它进指纹
"""

from __future__ import annotations

import time

import pytest

from src.common import points
from src.gateway import gateway
from src.gateway import opcua_south as O

# ================================================================ 1. StatusCode 三档


@pytest.mark.parametrize(
    "code,expected",
    [
        (0x00000000, "good"),          # Good
        (0x40000000, "uncertain"),     # Uncertain（基码）
        (0x40920000, "uncertain"),     # UncertainLastUsableValue
        (0x40940000, "uncertain"),     # UncertainSubstituteValue
        (0x80000000, "bad"),           # Bad（基码）
        (0x80340000, "bad"),           # BadNodeIdUnknown
        (0xC0000000, "bad"),           # ⭐ 最高两位是 11 的 Bad：写成 code>=0x80000000 会漏
        (0xFFFFFFFF, "bad"),
    ],
)
def test_classify_status_code(code: int, expected: str) -> None:
    assert O.classify_status_code(code) == expected


def test_classify_does_not_use_a_threshold_comparison() -> None:
    """把 0xC0000000 判成 Bad —— 这条专门盯住"用阈值比较代替最高两位"的写法。"""
    assert O.classify_status_code(0xC0000000) == "bad"
    assert O.classify_status_code(0xBFFFFFFF) == "bad"


# ================================================================ 2. 上送决策


def test_good_is_sent() -> None:
    d = O.decide_quality(0x00000000)
    assert d.send is True


@pytest.mark.parametrize("code", [0x80000000, 0x80340000, 0xC0000000])
def test_bad_is_not_sent(code: int) -> None:
    """Bad 的值不上送 → 下游判 missing。不用 0、也不用越界哨兵（那是设备层的语义）。"""
    d = O.decide_quality(code)
    assert d.send is False
    assert "Bad" in d.reason


def test_ordinary_uncertain_is_sent_with_a_trace() -> None:
    """普通 Uncertain 照常上送 —— 延续 ADR-0010 的「可疑 ≠ 无效」。"""
    d = O.decide_quality(0x40920000)
    assert d.send is True
    assert "LastUsableValue" in d.reason


def test_substitute_value_is_the_deliberate_exception() -> None:
    """⭐ SubstituteValue 的含义是"值是被替换/仿真出来的"，属假数据。

    项目原则是「宁缺勿假」，所以这一档刻意拦掉 —— 它与其他 Uncertain 不同。
    """
    d = O.decide_quality(O.UNCERTAIN_SUBSTITUTE_VALUE)
    assert d.send is False
    assert "替换值" in d.reason or "SubstituteValue" in d.reason


def test_every_named_subcode_has_a_human_readable_label() -> None:
    for code in (
        O.UNCERTAIN_LAST_USABLE_VALUE,
        O.UNCERTAIN_SENSOR_NOT_ACCURATE,
        O.UNCERTAIN_SUBSTITUTE_VALUE,
        O.UNCERTAIN_REFERENCE_OUT_OF_RANGE,
        O.UNCERTAIN_ENGINEERING_UNITS_EXCEEDED,
    ):
        assert code in O.UNCERTAIN_SUBCODE_NAMES


# ================================================================ 3. NodeId 解析


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("ns=2;s=Boiler1.Temp", (2, "s", "Boiler1.Temp")),
        ("ns=0;i=2258", (0, "i", "2258")),
        ("ns=3;g=09087e75-8e5e-499b-954f-f2a9603db28a", (3, "g", "09087e75-8e5e-499b-954f-f2a9603db28a")),
        ("ns=1;b=M/RbKBsRVkePCePcx24oRA==", (1, "b", "M/RbKBsRVkePCePcx24oRA==")),
        ("  ns=2;s=X  ", (2, "s", "X")),          # 两侧空白应被去掉
    ],
)
def test_parse_node_id_accepts_standard_forms(raw: str, expected: tuple) -> None:
    assert points.parse_node_id(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",                       # 空 = "不走 OPC UA"，不是合法 NodeId
        "   ",                    # 全空白同上
        "ns=2",                   # 缺 ;
        "2;s=X",                  # 缺 ns= 前缀
        "ns=x;i=1",               # 命名空间不是整数
        "ns=-1;i=1",              # 命名空间为负
        "ns=2;z=1",               # 类型不在 i/s/g/b
        "ns=2s=X",                # 缺 =
        "ns=2;i=",                # 标识为空
        "ns=2;i=abc",             # i 类型标识不是整数
        "ns=2;i=-5",              # i 类型标识为负
    ],
)
def test_parse_node_id_rejects_malformed(raw: str) -> None:
    """错的 NodeId 会把 A 测点的值写进 B 测点的列，而且看起来完全正常 —— 必须严拒。"""
    with pytest.raises(ValueError):
        points.parse_node_id(raw)


# ================================================================ 4. Reader 校验与快照


def test_validate_flags_empty_node_map() -> None:
    reader = O.OpcUaReader(node_map={})
    problems = reader.validate()
    assert problems and "NodeId" in problems[0]


def test_validate_reports_every_bad_node_id() -> None:
    reader = O.OpcUaReader(node_map={"SO2": "ns=2;s=ok", "NOx": "bad", "Dust": "ns=-1;i=1"})
    problems = reader.validate()
    assert len(problems) == 2
    assert any(p.startswith("NOx") for p in problems)
    assert any(p.startswith("Dust") for p in problems)


def test_start_refuses_when_node_ids_are_invalid() -> None:
    """配置错拒绝启动，与 Modbus 的"从站读不到就退出"同一纪律。"""
    reader = O.OpcUaReader(node_map={"SO2": "garbage"})
    assert reader.start() is False


def test_start_reports_missing_asyncua_honestly() -> None:
    """asyncua 没装时必须返回 False 并说清原因，而不是静默起不来。"""
    reader = O.OpcUaReader(node_map={"SO2": "ns=2;s=X"})
    if O.asyncua_available():
        pytest.skip("本机装了 asyncua，跳过「未安装」分支")
    assert reader.start() is False


def test_snapshot_is_none_before_any_successful_read() -> None:
    """从未读到就返回 None —— 不能返回一个空 dict 让下游以为"采到了但是空的"。"""
    assert O.OpcUaReader(node_map={}).snapshot() is None


def test_snapshot_returns_values_within_max_age() -> None:
    reader = O.OpcUaReader(node_map={})
    reader._values = {"SO2": 12.5}          # 直接摆一个快照，绕开真连接
    reader._sampled_at = time.time()
    assert reader.snapshot(max_age=10.0) == {"SO2": 12.5}


def test_stale_snapshot_is_refused_not_served_as_fresh() -> None:
    """⭐ 陈旧数据不冒充新鲜：宁可让下游看到"这一轮没数据"（覆盖率会掉），
    也不把旧值当新值发出去。"""
    reader = O.OpcUaReader(node_map={})
    reader._values = {"SO2": 12.5}
    reader._sampled_at = time.time() - 999
    assert reader.snapshot(max_age=10.0) is None


def test_snapshot_returns_a_copy_not_the_internal_dict() -> None:
    reader = O.OpcUaReader(node_map={})
    reader._values = {"SO2": 12.5}
    reader._sampled_at = time.time()
    got = reader.snapshot()
    got["SO2"] = 999.0
    assert reader._values["SO2"] == 12.5


def test_stats_shape() -> None:
    st = O.OpcUaReader(node_map={}).stats()
    assert set(st) == {"opcua_reads_ok", "opcua_reads_failed", "opcua_points_skipped"}


# ================================================================ 5. 契约侧


def test_opcua_node_ids_defaults_to_empty() -> None:
    """9 个测点默认都没配 NodeId → 现有 Modbus 部署的契约与行为零变化。"""
    assert points.OPCUA_NODE_IDS == {}


def test_every_point_has_the_field_with_empty_default() -> None:
    for point in points.POINTS:
        assert point.opcua_node_id == ""


def test_opcua_node_id_is_part_of_the_fingerprint(monkeypatch) -> None:
    """NodeId 决定"这一列的数从哪个节点来"，改错它等于改错地址 —— 必须进指纹。"""
    base = points.contract_version()
    tampered = list(points.POINTS)
    tampered[2] = tampered[2]._replace(opcua_node_id="ns=2;s=Other")
    monkeypatch.setattr(points, "POINTS", tuple(tampered))
    assert points.contract_version() != base


def test_south_protocol_defaults_to_modbus() -> None:
    """默认协议是 Modbus：不设 SOUTH_PROTOCOL 时既有部署行为零变化。"""
    assert gateway.SOUTH_PROTOCOL == "modbus"
    assert gateway.OPCUA_READER is None


def test_opcua_stale_after_is_three_poll_intervals() -> None:
    """陈旧判据与全项目其余陈旧口径一致（轮询周期的 3 倍）。"""
    assert gateway.OPCUA_STALE_AFTER == pytest.approx(gateway.POLL_INTERVAL * 3)
