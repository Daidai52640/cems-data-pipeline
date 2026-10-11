# -*- coding: utf-8 -*-
"""接入层 `src/platform/subscriber_to_td.py` 的**纯函数**单元测试：离线，不连 TDengine / MQTT / docker。

============================ 为什么现在补（改语义前先立桩） ============================
当前语义是「**任一测点不合格 → 整条报文拒收**」：`parse_payload()` 逐点校验，遇到第一个
不合格的测点就抛 `ValueError`，`on_message()` 捕获后只把 `STATS["rejected"]` 加 1 并返回，
**本来合格的其余测点一起丢**（一个点都不写库）。

下一步要改成「**逐点处理**」（坏点只丢自己，其余照常入库）：
依据 `docs/架构与可维护性说明.md` §四 第 2 档 **2-1**「真机接入会改动网关/接入层，先有测试再改更安全」。

⭐ 所以本文件里凡是名字带 `current_behavior` 的用例，钉的都是**即将被修改的行为**：
   改语义时它们**必须显式失败、被有意识地改写**，不允许"悄悄变了没人发现"。
   钉"整条拒收"的有两处，一内一外：
     1. `test_current_behavior_one_bad_point_rejects_whole_payload` —— `parse_payload` 层（抛异常）
     2. `test_on_message_current_behavior_one_bad_point_...`   —— 回调边界层（不写库 + 拒收计数）
   另有一处同源的现状钉桩：`test_insert_current_behavior_missing_point_raises_key_error`
   （`_insert` 假设整行齐全；逐点化必须先改它，否则第一个坏点就 KeyError）。

覆盖范围（纯函数为主，末尾两节是"离线可测的接线点"，都用假对象/假游标，不碰网络与磁盘）：
    parse_timestamp / parse_payload / reference_values / transmit_reference_values /
    record_reference + latest_reference_values / topic_plant_of /
    check_topic_matches_plant / enforce_topic_scope / resolve_spool_dir / validate_config /
    on_message（拒收计数与落盘）/ TdWriter._insert（列序与整行前提）
====================================================================================
"""
from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import (   # noqa: E402
    NAMES,
    POINTS,
    RANGES,
    ZS_TARGETS,
    ZS_UNAVAILABLE_SENTINEL,
)
from src.platform import subscriber_to_td as sub   # noqa: E402

#: 本文件统一用的报文时间戳（契约格式 `%Y-%m-%d %H:%M:%S`）
TS = "2026-09-10 12:00:00"

#: 9 个测点的合法定点值，**键序与契约 `points.NAMES` 一致**
GOOD_VALUES: dict[str, float] = {
    "Flow": 1000.0,
    "Dust": 2.5,
    "SO2": 10.0,
    "NOx": 20.0,
    "O2": 6.0,          # = 基准氧 6%，折算因子恰为 1，便于断言折算
    "Velocity": 10.0,
    "Temp": 120.0,
    "Humidity": 50.0,
    "Pressure": 101.3,
}


def build_payload(
    values: Optional[dict[str, Any]] = None,
    ts: str = TS,
    tail: str = "Flag=N",
) -> str:
    """组装一条报文：`"<ts> 测点=值 … <tail>"`。

    `values` 传原样的 `str` 也能用（f-string 会原样写出去），
    方便造出 `SO2=NaN` / `SO2=abc` 这类"数值非法"的用例。
    """
    items = GOOD_VALUES if values is None else values
    body = " ".join(f"{name}={value}" for name, value in items.items())
    return " ".join(part for part in (ts, body, tail) if part)


def without(*dropped: str) -> dict[str, float]:
    """GOOD_VALUES 去掉若干测点（用来造"缺测点"的报文）。"""
    return {name: value for name, value in GOOD_VALUES.items() if name not in dropped}


# ==================== 一、parse_timestamp ====================

def test_parse_timestamp_returns_input_unchanged():
    assert sub.parse_timestamp("2026-09-10 12:00:00") == "2026-09-10 12:00:00"


@pytest.mark.parametrize("bad", [
    "2026-09-10",                    # 只有日期
    "12:00:00",                      # 只有时间
    "2026/09/10 12:00:00",           # 分隔符不合契约
    "2026-09-10T12:00:00",           # 缺空格
    "2026-09-10 12:00",              # 缺秒
    "2026-09-10 12:00:00.000",       # 多了毫秒
    "2026-13-01 12:00:00",           # 月份越界
    "2026-02-30 12:00:00",           # 该日期不存在
    "2026-09-10 25:00:00",           # 小时越界
    "1970-01-01 00:00:00+08:00",     # 带时区（契约格式不含时区）
    "",                              # 空串
])
def test_parse_timestamp_rejects_invalid_format(bad):
    with pytest.raises(ValueError) as excinfo:
        sub.parse_timestamp(bad)
    assert "时间戳格式非法" in str(excinfo.value)


def test_parse_timestamp_rejects_none_as_value_error():
    """非字符串（None）在 `strptime` 里抛 TypeError，本函数统一成 ValueError（契约一致）。"""
    with pytest.raises(ValueError):
        sub.parse_timestamp(None)   # type: ignore[arg-type]


def test_parse_timestamp_accepts_non_zero_padded_current_behavior():
    """现状（与直觉不符，钉住）：`strptime` 不要求补零，`2026-9-1 1:00:00` 照样通过。

    ⚠️ 这属于**描述现状**：契约格式写的是 `%Y-%m-%d %H:%M:%S`，但实际接受非补零写法。
    若将来收紧成严格正则，本用例应被有意识地改写。
    """
    assert sub.parse_timestamp("2026-9-1 1:00:00") == "2026-9-1 1:00:00"


# ==================== 二、parse_payload：正常路径 ====================

def test_parse_payload_accepts_all_nine_points():
    ts, data = sub.parse_payload(build_payload())
    assert ts == TS
    assert data == GOOD_VALUES                     # 键 = MQTT 字段名，值 = 原样浮点
    assert len(NAMES) == 9 and set(data) == set(NAMES)


def test_parse_payload_skips_non_point_fields():
    """`Flag=N` / 未知字段 / 没有 `=` 的碎片都被跳过：不参与校验，也不进结果。"""
    payload = build_payload(tail="Flag=N Unknown=abc Note=hello XYZ")
    ts, data = sub.parse_payload(payload)
    assert ts == TS
    assert data == GOOD_VALUES
    assert "Flag" not in data and "Unknown" not in data


def test_parse_payload_ignores_illegal_value_in_non_point_field():
    """非测点字段的值**再离谱也不校验**（只看键在不在契约里）—— `Unknown=???` 不报错。"""
    ts, data = sub.parse_payload(build_payload(tail="Flag=N Unknown=???"))
    assert data == GOOD_VALUES


def test_parse_payload_duplicate_point_last_one_wins():
    """同一测点出现两次时**后者覆盖前者**（现状：不做重键检查，静默取值）。"""
    payload = build_payload() + " SO2=12.0"
    ts, data = sub.parse_payload(payload)
    assert data["SO2"] == 12.0
    assert set(data) == set(NAMES)                 # 重复不会让字典多出键


# ==================== 三、parse_payload：各类整条拒收 ====================

def test_current_behavior_one_bad_point_rejects_whole_payload():
    """⭐⭐ **本测试描述的是即将被修改的行为**。

    现状：9 个测点里**只有 1 个**超量程（Dust=150.0 > 契约量程上限 100.0），
    `parse_payload()` 直接抛 `ValueError` —— **其余 8 个合格测点一起丢**（整条拒收）。
    调用方拿不到任何部分结果：不是"返回 8 个点"，而是彻底没有返回值。

    下一步要改成「**逐点处理**」（坏点只丢自己，另外 8 个点照常入库）。
    届时**本测试应当 FAIL**：`parse_payload` 不再抛异常，返回值会变成
    "8 个合格测点的字典（+ 坏点被丢弃/标记）"。
    ⇒ 改语义时请**有意识地改写本用例**（并同步检查回调层的
      `test_on_message_current_behavior_one_bad_point_is_rejected_and_not_written`），
      不要顺手删掉、也不要只把它改成"仍然抛异常"。
    """
    payload = build_payload({**GOOD_VALUES, "Dust": 150.0})
    # 先自证前提成立：150.0 确实在量程之外（不写死 100.0，跟随契约）
    assert not (RANGES["Dust"][0] <= 150.0 <= RANGES["Dust"][1])

    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(payload)
    message = str(excinfo.value)
    assert "Dust" in message and "超出量程" in message
    assert "150.0" in message


def test_current_behavior_one_bad_point_hides_the_other_eight():
    """同一现象的另一种表述：整条拒收 = 那 8 个合格点的值**无处可取**。

    用一个"只有 1 个点坏、且坏点是最后一个字段"的报文本证明因果：
    报文前 8 个字段都已逐个通过校验，第 9 个字段一律不合格时结果完全相同
    （即失败点出现在哪里都不影响"整条丢弃"）。
    """
    good_prefix = {name: GOOD_VALUES[name] for name in NAMES[:-1]}   # 8 个合格点
    payload = build_payload({**good_prefix, NAMES[-1]: -1.0})        # Pressure 低于下限
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(payload)
    assert NAMES[-1] in str(excinfo.value)


def test_below_range_value_rejects_whole_payload():
    """量程**下限**也是一道门：Pressure=-1.0（低于 85 kPa）同样整条拒收。"""
    payload = build_payload({**GOOD_VALUES, "Pressure": -1.0})
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(payload)
    message = str(excinfo.value)
    assert "Pressure" in message and "超出量程" in message


@pytest.mark.parametrize("name", list(NAMES))
def test_range_boundaries_are_inclusive_for_every_point(name):
    """量程判定是**闭区间** `low <= v <= high`：上下限本身合法（9 个测点各测两端）。"""
    low, high = RANGES[name]
    for bound in (low, high):
        ts, data = sub.parse_payload(build_payload({**GOOD_VALUES, name: bound}))
        assert ts == TS
        assert data[name] == bound


@pytest.mark.parametrize("raw", ["NaN", "nan", "NAN", "-NaN"])
def test_nan_rejects_whole_payload(raw):
    """NaN 整条拒收：NaN 进 TDengine 会报 syntax error，整行连好点一起丢。"""
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(build_payload({**GOOD_VALUES, "SO2": raw}))
    message = str(excinfo.value)
    assert "SO2" in message and "不是有限数值" in message


@pytest.mark.parametrize("raw", ["inf", "Inf", "+inf", "Infinity"])
def test_positive_inf_rejects_whole_payload(raw):
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(build_payload({**GOOD_VALUES, "SO2": raw}))
    message = str(excinfo.value)
    assert "SO2" in message and "不是有限数值" in message


@pytest.mark.parametrize("raw", ["-inf", "-Infinity", "-Inf"])
def test_negative_inf_rejects_whole_payload(raw):
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(build_payload({**GOOD_VALUES, "SO2": raw}))
    message = str(excinfo.value)
    assert "SO2" in message and "不是有限数值" in message


@pytest.mark.parametrize("raw", ["abc", "", "35.2mg", "1,5", "1.2.3"])
def test_non_numeric_value_rejects_whole_payload(raw):
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(build_payload({**GOOD_VALUES, "SO2": raw}))
    message = str(excinfo.value)
    assert "SO2" in message and "数值非法" in message


def test_missing_point_rejects_whole_payload_current_behavior():
    """缺测点（只报 8 个）→ 现状同样是**整条拒收**（最后一个检查点 `missing` 抛 ValueError）。

    ⚠️ 描述现状：逐点化之后"少了某个点"大概率仍然是拒收（契约要求 9 个点齐全），
    但判定位置与措辞可能变，届时请核对本用例。
    """
    payload = build_payload(without("Humidity"))
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(payload)
    message = str(excinfo.value)
    assert "缺少测点" in message and "Humidity" in message


def test_only_non_point_fields_rejects_as_missing_all_nine():
    """`"<ts> Flag=N"` 能过长度检查（3 段），但一个测点都没有 → 报 9 个点全缺。

    同时证明 `Flag` 被**跳过**、没有被当成测点（否则不会报"缺少"）。
    """
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(f"{TS} Flag=N")
    message = str(excinfo.value)
    assert "缺少测点" in message
    for name in NAMES:
        assert name in message


def test_bad_point_error_takes_precedence_over_missing_point():
    """两种不合格同时存在时只报**第一个**原因：逐点校验在前、缺测点检查在后 —— 现状。

    坏点 Dust=150.0 与缺点 Humidity 同时存在 → 报的是"超出量程"，不是"缺少测点"。
    """
    values = {**without("Humidity"), "Dust": 150.0}
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(build_payload(values))
    message = str(excinfo.value)
    assert "Dust" in message and "超出量程" in message
    assert "缺少测点" not in message


@pytest.mark.parametrize("payload", [
    "",                              # 0 段
    "   ",                           # 全空白
    "2026-09-10",                    # 1 段
    "2026-09-10 12:00:00",           # 2 段（只有时间戳，没有测点）
])
def test_parse_payload_too_short_rejects(payload):
    """`len(parts) < 3` 先于时间戳解析与测点校验（最短也要"日期 时间 一个字段"）。"""
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(payload)
    assert "报文长度不足" in str(excinfo.value)


@pytest.mark.parametrize("bad_ts", [
    "2026-09-10",                    # 日期之后直接跟测点（时间戳缺一段）
    "2026/09/10 12:00:00",           # 分隔符不合契约
    "2026-09-10T12:00:00",           # 缺空格
    "2026-13-01 12:00:00",           # 月份越界
])
def test_parse_payload_bad_timestamp_rejects_whole_payload(bad_ts):
    payload = f"{bad_ts} " + " ".join(f"{n}={v}" for n, v in GOOD_VALUES.items())
    with pytest.raises(ValueError) as excinfo:
        sub.parse_payload(payload)
    assert "时间戳格式非法" in str(excinfo.value)


# ==================== 四、折算值：reference_values（数学层） ====================

def test_reference_values_keys_are_tdengine_columns():
    """出参键 = TDengine 列名（`points.ZS_TARGETS`），不是报文里的 MQTT 字段名。"""
    refs = sub.reference_values(GOOD_VALUES)
    assert set(refs) == set(ZS_TARGETS) == {"dust", "so2", "nox"}


def test_reference_values_at_baseline_o2_equals_dry_value():
    """O2 = 基准氧 6% → 折算因子 = 1：(21-6)/(21-6) = 1，折算值 == 标干值。"""
    refs = sub.reference_values({**GOOD_VALUES, "O2": 6.0})
    assert refs["dust"] == pytest.approx(GOOD_VALUES["Dust"])
    assert refs["so2"] == pytest.approx(GOOD_VALUES["SO2"])
    assert refs["nox"] == pytest.approx(GOOD_VALUES["NOx"])


def test_reference_values_scales_up_when_o2_above_baseline():
    """O2=9% → 因子 (21-6)/(21-9) = 1.25（氧含量偏高 → 折算值变大）。"""
    refs = sub.reference_values({**GOOD_VALUES, "O2": 9.0})
    assert refs["so2"] == pytest.approx(12.5)                       # 10.0 × 1.25
    assert refs["dust"] == pytest.approx(2.5 * 1.25)
    assert refs["nox"] == pytest.approx(20.0 * 1.25)


@pytest.mark.parametrize("o2", [21.0, 21.5, 25.0])
def test_reference_values_nan_when_o2_denominator_non_positive(o2):
    """O2 >= 21% → 分母 <= 0，折算无物理意义 → **数学层返回 nan**（不当 0、不瞎除）。"""
    refs = sub.reference_values({**GOOD_VALUES, "O2": o2})
    assert all(math.isnan(value) for value in refs.values())


# ==================== 五、折算值：transmit_reference_values（协议层） ====================

def test_transmit_reference_values_passes_finite_values_through():
    """算得出来的有限折算值**原样透出**（不缩放、不舍入）。"""
    tx = sub.transmit_reference_values({**GOOD_VALUES, "O2": 9.0})
    assert tx["so2"] == pytest.approx(12.5)
    assert set(tx) == set(ZS_TARGETS)


@pytest.mark.parametrize("o2", [21.0, 25.0])
def test_transmit_reference_values_nan_becomes_sentinel(o2):
    """算不出来（nan）→ 哨兵值 +9999.99（HJ 212-2025 §8.1.1 d)：不是 0、不是实测值。"""
    tx = sub.transmit_reference_values({**GOOD_VALUES, "O2": o2})
    assert all(value == ZS_UNAVAILABLE_SENTINEL for value in tx.values())
    assert ZS_UNAVAILABLE_SENTINEL == pytest.approx(9999.99)


@pytest.mark.parametrize("bad", [float("inf"), float("-inf"), float("nan")])
def test_transmit_reference_values_non_finite_dry_value_becomes_sentinel(bad):
    """标干值本身是 ±inf/nan（接入层本不该放它进来）时，协议层**也绝不把 inf 传出去**。"""
    tx = sub.transmit_reference_values({**GOOD_VALUES, "Dust": bad, "O2": 6.0})
    assert tx["dust"] == ZS_UNAVAILABLE_SENTINEL
    assert tx["so2"] == pytest.approx(GOOD_VALUES["SO2"])    # 其余列不受影响


def test_math_layer_keeps_nan_where_protocol_layer_uses_sentinel():
    """★ 两层语义必须分开（判定吃 nan、出站吃哨兵值）：**同一输入、两种输出**，别合并。

    哨兵值大于任何限值，一旦混进判定就会被判成"严重超标"。
    """
    values = {**GOOD_VALUES, "O2": 25.0}
    refs = sub.reference_values(values)
    tx = sub.transmit_reference_values(values)
    assert all(math.isnan(value) for value in refs.values())
    assert set(tx.values()) == {ZS_UNAVAILABLE_SENTINEL}


# ==================== 六、折算值快照：record_reference / latest_* ====================

def test_record_reference_snapshot_holds_transmit_values_not_nan():
    """快照存的是**传输编码后**的值（下游/验证脚本看到的应与上报出去的一致）。"""
    ts = "2026-09-10 13:00:00"
    snapshot = sub.record_reference(ts, {**GOOD_VALUES, "O2": 25.0})
    assert set(snapshot.values()) == {ZS_UNAVAILABLE_SENTINEL}
    assert sub.latest_reference_values() == snapshot
    assert sub.latest_reference_timestamp() == ts
    assert sub.latest_reference_values() is not snapshot        # 返回的是副本


def test_record_reference_accepts_precomputed_math_references():
    """传入已算好的数学层结果时不再重算公式（"折算值全链路只算一次"）。"""
    math_refs = {"dust": 1.0, "so2": 2.0, "nox": float("nan")}
    snapshot = sub.record_reference(TS, GOOD_VALUES, math_refs)
    assert snapshot == {"dust": 1.0, "so2": 2.0, "nox": ZS_UNAVAILABLE_SENTINEL}


def test_latest_reference_values_returns_a_copy():
    """调用方改返回值不能污染进程内快照。"""
    sub.record_reference(TS, GOOD_VALUES)
    first = sub.latest_reference_values()
    first["so2"] = -12345.0
    assert sub.latest_reference_values()["so2"] == pytest.approx(GOOD_VALUES["SO2"])


# ==================== 七、主题 ↔ 厂区 ====================

@pytest.mark.parametrize("topic,expected", [
    ("cems/plant1/data", "plant1"),
    ("cems/Plant2/data", "plant2"),          # 厂区段统一小写
    ("cems/PLANT_3/data", "plant_3"),
    ("cems/p/data", "p"),
    ("cems/plant1/data/extra", "plant1"),    # 只看前两段
    ("  cems/plant1/data  ", "plant1"),      # 两侧空白先 strip
])
def test_topic_plant_of_parses_and_lowercases(topic, expected):
    assert sub.topic_plant_of(topic) == expected


@pytest.mark.parametrize("topic", [
    "plant1/data",            # 缺 cems 前缀
    "cems/plant1",            # 缺第三段（正则要求厂区段后面还有 `/`）
    "cems//data",             # 厂区段为空
    "cems/1plant/data",       # 厂区段不能以数字开头
    "cems/-p/data",           # 厂区段不能以连字符开头
    "cems/plant-1/data",      # 厂区段不允许连字符
    "/cems/plant1/data",      # 前导斜杠
    "cems plant1/data",       # 用空格代替斜杠
    "",
])
def test_topic_plant_of_none_for_unparsable(topic):
    """不是 `cems/<厂区>/...` 形态 → None（"无法判断"，不是"不一致"）。"""
    assert sub.topic_plant_of(topic) is None


@pytest.mark.parametrize("topic,plant", [
    ("cems/plant1/data", "plant1"),
    ("cems/PLANT1/data", "plant1"),         # 主题侧小写化
    ("cems/plant1/data", " Plant1 "),       # 配置侧 strip + 小写
    ("cems/plant1/data/extra", "plant1"),
])
def test_check_topic_matches_plant_true_when_consistent(topic, plant):
    assert sub.check_topic_matches_plant(topic, plant, "device1") is True


@pytest.mark.parametrize("topic", ["custom/topic", "plant1/data", "cems/plant1", ""])
def test_check_topic_matches_plant_true_when_unknowable(topic):
    """无法判断（自定义主题）→ True：护栏只拦"能判断且不一致"的误配。"""
    assert sub.check_topic_matches_plant(topic, "plant1") is True


def test_check_topic_matches_plant_false_on_mismatch():
    """主题是 plant2、配置是 plant1 → False（数据会静默挂到错厂区标签下）。"""
    assert sub.check_topic_matches_plant("cems/plant2/data", "plant1", "device1") is False


def test_enforce_topic_scope_passes_when_consistent(monkeypatch):
    monkeypatch.setattr(sub, "ALLOW_TOPIC_TAG_MISMATCH", False)
    assert sub.enforce_topic_scope("cems/plant1/data", "plant1", "device1") is True


def test_enforce_topic_scope_exits_on_mismatch_by_default(monkeypatch):
    """默认（未显式放行）不一致 → SystemExit：非零退出，由 compose 的 restart 放大成可见故障。"""
    monkeypatch.setattr(sub, "ALLOW_TOPIC_TAG_MISMATCH", False)
    with pytest.raises(SystemExit) as excinfo:
        sub.enforce_topic_scope("cems/plant2/data", "plant1", "device1")
    assert "拒绝启动" in str(excinfo.value)


def test_enforce_topic_scope_allows_when_explicitly_opted_out(monkeypatch):
    """`TD_ALLOW_TOPIC_TAG_MISMATCH=1` 只记 CRITICAL 并放行（给自定义前缀的部署留口子）。"""
    monkeypatch.setattr(sub, "ALLOW_TOPIC_TAG_MISMATCH", True)
    assert sub.enforce_topic_scope("cems/plant2/data", "plant1", "device1") is True


# ==================== 八、配置与路径解析 ====================

def test_resolve_spool_dir_empty_uses_project_root_default():
    assert sub.resolve_spool_dir("") == sub.PROJECT_ROOT / "data" / "subscriber"


def test_resolve_spool_dir_relative_is_relative_to_project_root_not_cwd(tmp_path, monkeypatch):
    """相对路径按**项目根**解析（不是 CWD）：本机运行与容器运行不分叉。"""
    monkeypatch.chdir(tmp_path)
    resolved = sub.resolve_spool_dir("data-plant2/subscriber")
    assert resolved == sub.PROJECT_ROOT / "data-plant2" / "subscriber"
    assert resolved.is_absolute()
    assert tmp_path not in resolved.parents


def test_resolve_spool_dir_absolute_is_used_as_is(tmp_path):
    assert sub.resolve_spool_dir(str(tmp_path / "spool")) == tmp_path / "spool"


def test_resolve_spool_dir_expands_user_home():
    assert sub.resolve_spool_dir("~/cems-spool") == Path.home() / "cems-spool"


GOOD_SQL_NAMES = {"TD_DB": "cems", "TD_STABLE": "cems_data", "TD_PLANT": "plant1", "TD_DEVICE": "device1"}


def test_validate_config_accepts_known_good_names(monkeypatch):
    for attr, value in GOOD_SQL_NAMES.items():
        monkeypatch.setattr(sub, attr, value)
    assert sub.validate_config() is None


@pytest.mark.parametrize("attr,bad", [
    ("TD_DB", "cems;drop"),
    ("TD_STABLE", "cems-data"),
    ("TD_PLANT", "plant 1"),
    ("TD_DEVICE", "device1'"),
    ("TD_DEVICE", "1device"),
    ("TD_DB", ""),
])
def test_validate_config_rejects_non_sql_name(attr, bad, monkeypatch):
    """会被拼进 SQL 的库名/表名/标签值必须过白名单，不合法就抛 ValueError（启动阶段拦下）。"""
    monkeypatch.setattr(sub, attr, bad)
    with pytest.raises(ValueError) as excinfo:
        sub.validate_config()
    message = str(excinfo.value)
    assert attr in message and repr(bad) in message


# ==================== 九、回调边界：on_message（离线假对象） ====================

class FakeWriter:
    """假写入器：记录调用、按构造时的 `ok` 返回成败（不连 TDengine）。"""

    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.calls: list[tuple[str, dict[str, float]]] = []

    def write(self, ts: str, values: dict[str, float]) -> bool:
        self.calls.append((ts, values))
        return self.ok


class FakeSpool:
    """假落盘队列：只记 append 的报文。"""

    def __init__(self) -> None:
        self.appended: list[str] = []

    def append(self, line: str) -> bool:
        self.appended.append(line)
        return True


class FakeJudge:
    """假判据：只记调用、永不产生事件（告警链路不是本文件的对象，但回调会用到它）。"""

    def __init__(self) -> None:
        self.samples: list[str] = []

    def on_sample(self, ts: str, values: dict[str, float], math_references: Any) -> list:
        self.samples.append(ts)
        return []

    @property
    def stats(self) -> dict:
        return {}


class FakeMessage:
    """假 MQTT 报文：`on_message` 只用到 `.payload`。"""

    def __init__(self, payload: str) -> None:
        self.payload = payload.encode("utf-8")


def userdata(writer: Optional[FakeWriter] = None, spool: Optional[FakeSpool] = None,
             judge: Optional[FakeJudge] = None) -> dict:
    return {
        "writer": writer if writer is not None else FakeWriter(),
        "spool": spool if spool is not None else FakeSpool(),
        "judge": judge if judge is not None else FakeJudge(),
    }


@pytest.fixture(autouse=True)
def restore_module_state():
    """`STATS` / `LAST_REFERENCE` 是模块级可变状态：每个用例后复原，避免互相污染。"""
    stats_before = dict(sub.STATS)
    reference_before = dict(sub.LAST_REFERENCE["values"])
    ts_before = sub.LAST_REFERENCE["ts"]
    yield
    sub.STATS.clear()
    sub.STATS.update(stats_before)
    sub.LAST_REFERENCE["values"] = reference_before
    sub.LAST_REFERENCE["ts"] = ts_before


def test_on_message_valid_payload_writes_all_nine_values():
    writer = FakeWriter()
    rejected_before = sub.STATS["rejected"]
    sub.on_message(None, userdata(writer), FakeMessage(build_payload()))
    assert len(writer.calls) == 1
    ts, values = writer.calls[0]
    assert ts == TS and values == GOOD_VALUES
    assert sub.STATS["rejected"] == rejected_before      # 合格报文不动拒收计数


def test_on_message_current_behavior_one_bad_point_is_rejected_and_not_written():
    """⭐ 与 `test_current_behavior_one_bad_point_rejects_whole_payload` 同一条现状的**回调边界**版本。

    现状：Dust 超量程 → `parse_payload` 抛 ValueError → `on_message` 只把
    `STATS["rejected"]` 加 1 然后 return，**`writer.write` 一次都不调**（其余 8 个点全丢）。

    下一步改成逐点处理后**本测试应当 FAIL**：那时 writer 至少应被调用一次
    （把 8 个合格点写进去），拒收计数也会变成"按点计"或不再增加。
    ⇒ 改语义时请**有意识地改写本用例**。
    """
    writer = FakeWriter()
    rejected_before = sub.STATS["rejected"]
    sub.on_message(
        None, userdata(writer), FakeMessage(build_payload({**GOOD_VALUES, "Dust": 150.0})),
    )
    assert writer.calls == []                             # ★ 整条拒收：一个点都不写
    assert sub.STATS["rejected"] == rejected_before + 1


def test_on_message_spools_raw_payload_when_write_fails():
    """入库失败 → 先把**原始报文**落本地队列（返回即 PUBACK，落盘是这条数据唯一的副本）。"""
    writer = FakeWriter(ok=False)
    spool = FakeSpool()
    spooled_before = sub.STATS["spooled"]
    payload = build_payload()
    sub.on_message(None, userdata(writer, spool), FakeMessage(payload))
    assert spool.appended == [payload]
    assert sub.STATS["spooled"] == spooled_before + 1


# ==================== 十、入库 SQL：TdWriter._insert（列序 + 整行前提） ====================

class FakeCursor:
    """假游标：只记录执行过的 SQL。"""

    def __init__(self) -> None:
        self.sqls: list[str] = []

    def execute(self, sql: str) -> None:
        self.sqls.append(sql)


def test_insert_sql_column_and_value_order_follow_points_contract():
    """列名与数值都按 `points.POINTS` 的同序展开：报文顺序/建表顺序漂移不会写错列。"""
    writer = sub.TdWriter()
    cursor = FakeCursor()
    writer._cur = cursor
    assert writer._insert(TS, GOOD_VALUES) is True

    columns = ", ".join(("ts", *sub.TD_COLUMNS))
    numbers = ", ".join(f"{GOOD_VALUES[point.name]}" for point in POINTS)
    sql = cursor.sqls[0]
    assert f"({columns}) VALUES ('{TS}', {numbers})" in sql
    assert f"INSERT INTO {sub.TD_DB}.{sub.CHILD_TABLE}" in sql
    assert f"USING {sub.TD_DB}.{sub.TD_STABLE}" in sql
    assert f"TAGS ('{sub.TD_PLANT}', '{sub.TD_DEVICE}')" in sql


def test_insert_current_behavior_missing_point_raises_key_error():
    """⭐ 现状：`_insert` 假设**整行齐全**（`values[point.name]` 直接取键，缺一个就 KeyError）。

    ⚠️ 这是逐点化必须一起改的地方：只写"合格的点"时这里必须先有落点
    （例如缺列写 NULL，或按实际列拼 SQL），否则第一个坏点就会把入库打挂。
    届时本测试应被**有意识地改写**（改成"缺列 → NULL"或"按传入的键拼列"）。
    """
    writer = sub.TdWriter()
    writer._cur = FakeCursor()
    with pytest.raises(KeyError):
        writer._insert(TS, without("Dust"))
