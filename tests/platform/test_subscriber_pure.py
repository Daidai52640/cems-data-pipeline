# -*- coding: utf-8 -*-
"""接入层 `src/platform/subscriber_to_td.py` 的**纯函数**单元测试：离线，不连 TDengine / MQTT / docker。

============================ 本文件已经过一次"有意的改写" ============================
**旧语义**（本文件最初立桩时钉的）：`任一测点不合格 → 整条报文拒收`。
**新语义**（`docs/adr/0009-接入层逐点质量位.md` 已实施）：**语法坏整条拒收 / 语义坏逐点处理**。

⭐ 原来名字带 `current_behavior` 的用例，钉的正是被改掉的行为，**已逐个有意识地改写**，
   映射如下（docstring 里都写清了"逐点化后现在应该是什么行为"）：

| 旧用例（钉"整条拒收"） | 现在 |
|---|---|
| `test_current_behavior_one_bad_point_rejects_whole_payload` | `test_pointwise_one_bad_point_keeps_the_other_eight` |
| `test_current_behavior_one_bad_point_hides_the_other_eight` | `test_pointwise_bad_point_position_does_not_matter` |
| `test_below_range_value_rejects_whole_payload` | `test_below_range_value_is_invalid_point_only` |
| `test_nan_rejects_whole_payload` / `test_positive_inf_...` / `test_negative_inf_...` | `test_nan_is_invalid_point_only` / `test_inf_is_invalid_point_only` |
| `test_missing_point_rejects_whole_payload_current_behavior` | `test_missing_point_is_kind_missing_not_whole_payload` |
| `test_only_non_point_fields_rejects_as_missing_all_nine` | `test_pointless_payload_is_payload_format_error` |
| `test_bad_point_error_takes_precedence_over_missing_point` | `test_bad_and_missing_points_are_both_collected` |
| `test_parse_payload_duplicate_point_last_one_wins` | `test_duplicate_point_is_payload_format_error` |
| `test_on_message_current_behavior_one_bad_point_is_rejected_and_not_written` | `test_on_message_pointwise_one_bad_point_writes_the_other_eight` |
| `test_insert_current_behavior_missing_point_raises_key_error` | `test_insert_writes_only_the_columns_passed` |

⚠️ **没有为了变绿而绕过任何一条**：`test_parse_timestamp_accepts_non_zero_padded_current_behavior`
   钉的是"非补零时间戳被接受"，**与本改造无关，原样保留、继续通过**。
⚠️ 另有两条只改了**异常类型**（`ValueError` → `PayloadFormatError`，语法坏仍是整条拒收，
   分类依据是 `src/common/errors.py`）：`test_parse_payload_too_short_rejects`、
   `test_parse_payload_bad_timestamp_rejects_whole_payload`。

============================ 第二次"有意的改写"（ADR-0010） ============================
`docs/adr/0010-数据合理性判据.md` 给事件表**加了 `stored` 列**（`0`=丢了 / `1`=可疑但留着），
所以**只有 3 条**钉"列清单/行尾"的断言被**有意改写**（行为语义未变，只是列多了）：

| 用例 | 改动 |
|---|---|
| `test_point_invalid_insert_sql_carries_kind_reason_and_raw` | 列清单 `(ts, point, kind, reason, raw)` → `…, stored)`；行尾 `60.0)` → `60.0, 0)` |
| `test_point_invalid_missing_row_has_null_raw` | 行尾 `NULL)` → `NULL, 0)` |
| `test_point_invalid_non_finite_raw_becomes_null_but_reason_keeps_evidence` | 行尾 `, NULL)` → `, NULL, 0)` |

⭐ 其余 499 条**一条未改**；`test_point_invalid_ddl_shape_and_defaults` 只是**加了**一个
   `stored TINYINT` 片段断言（既有片段断言全保留）。
⚠️ 新用例里 `test_jump_*` / `test_frozen_*` / `test_stored_column_*` / `test_quality_*`
   钉的是 ADR-0010 的机制断言（**可疑照常入库 + 留痕**），与上表无关。
====================================================================================

覆盖范围（纯函数为主，末尾几节是"离线可测的接线点"，都用假对象/假游标，不碰网络与磁盘）：
    parse_timestamp / parse_payload（逐点化后三返回值）/ supports_reference /
    reference_values / transmit_reference_values / record_reference + latest_reference_values /
    topic_plant_of / check_topic_matches_plant / enforce_topic_scope / resolve_spool_dir /
    validate_config / on_message（语法坏拒收、语义坏留痕、可疑留痕、落盘）/
    TdWriter._insert（部分列）/ QualityConfig + QualityChecker（跳变/冻结）/
    PointInvalidTables（DDL/DML 文本 + stored 列平滑升级）+ PointInvalidWriter
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

from src.common.errors import (   # noqa: E402
    PayloadFormatError,
    PointInvalid,
    PointInvalidKind,
)
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
    ⚠️ 与"逐点化"无关，本改造**不动它**（ADR-0009 §10 表 2：无害的宽容，保持）。
    """
    assert sub.parse_timestamp("2026-9-1 1:00:00") == "2026-9-1 1:00:00"


# ==================== 二、parse_payload：正常路径 ====================

def test_parse_payload_accepts_all_nine_points():
    """逐点化后返回值是**三元组**：`(时间戳, 有效测点, 无效测点)`。

    9 个点全合法时：有效 = 9 个、无效 = **空字典**（V6 的纯函数等价物：不产生噪声）。
    """
    ts, values, invalid = sub.parse_payload(build_payload())
    assert ts == TS
    assert values == GOOD_VALUES                   # 键 = MQTT 字段名，值 = 原样浮点
    assert invalid == {}
    assert len(NAMES) == 9 and set(values) == set(NAMES)


def test_parse_payload_skips_non_point_fields():
    """`Flag=N` / 未知字段 / 没有 `=` 的碎片都被跳过：不参与校验，也不进结果。"""
    payload = build_payload(tail="Flag=N Unknown=abc Note=hello XYZ")
    ts, values, invalid = sub.parse_payload(payload)
    assert ts == TS
    assert values == GOOD_VALUES
    assert invalid == {}
    assert "Flag" not in values and "Unknown" not in values


def test_parse_payload_ignores_illegal_value_in_non_point_field():
    """非测点字段的值**再离谱也不校验**（只看键在不在契约里）—— `Unknown=???` 不报错。"""
    ts, values, invalid = sub.parse_payload(build_payload(tail="Flag=N Unknown=???"))
    assert values == GOOD_VALUES


# ==================== 三、parse_payload：语义坏（逐点丢，不抛） ====================

def test_pointwise_one_bad_point_keeps_the_other_eight():
    """⭐⭐ 逐点化后现在应该是什么行为（**旧的整条拒收钉桩已改为本条**）。

    9 个测点里只有 1 个超量程（Dust=150.0 > 契约量程上限 100.0）：
      - **不再抛异常**；
      - 返回值里有效测点 = 其余 **8 个**，值逐一对得上（V2 的纯函数等价物）；
      - 坏点进第三个返回值，`kind=invalid`，**原因 + 原始值都保留**（V1 的证据要求）。
    """
    payload = build_payload({**GOOD_VALUES, "Dust": 150.0})
    # 先自证前提成立：150.0 确实在量程之外（不写死 100.0，跟随契约）
    assert not (RANGES["Dust"][0] <= 150.0 <= RANGES["Dust"][1])

    ts, values, invalid = sub.parse_payload(payload)
    assert ts == TS
    assert values == without("Dust")               # 8 个点逐值可对上
    assert set(invalid) == {"Dust"}                # 只有它一个无效点
    item = invalid["Dust"]
    assert item.point == "Dust"
    assert item.kind is PointInvalidKind.INVALID
    assert item.raw == 150.0                       # 原始值保留（证据）
    assert "超出量程" in item.reason and "150.0" in item.reason


def test_pointwise_bad_point_position_does_not_matter():
    """坏点出现在报文哪个位置都一样：只丢它自己（旧用例证明的是"整条丢弃"）。

    报文前 8 个字段都已逐个通过校验，**最后一个**字段一律不合格 ——
    逐点化后结果与"坏点在中间"完全相同：8 个有效 + 1 个 invalid。
    """
    good_prefix = {name: GOOD_VALUES[name] for name in NAMES[:-1]}   # 8 个合格点
    payload = build_payload({**good_prefix, NAMES[-1]: -1.0})        # Pressure 低于下限
    ts, values, invalid = sub.parse_payload(payload)
    assert set(values) == set(NAMES) - {NAMES[-1]}
    assert set(invalid) == {NAMES[-1]}
    assert "超出量程" in invalid[NAMES[-1]].reason


def test_below_range_value_is_invalid_point_only():
    """量程**下限**也是一道门：Pressure=-1.0（低于 85 kPa）→ 该点 invalid，其余 8 点照常。"""
    ts, values, invalid = sub.parse_payload(build_payload({**GOOD_VALUES, "Pressure": -1.0}))
    assert set(values) == set(NAMES) - {"Pressure"}
    item = invalid["Pressure"]
    assert item.kind is PointInvalidKind.INVALID
    assert item.raw == -1.0
    assert "超出量程" in item.reason


@pytest.mark.parametrize("name", list(NAMES))
def test_range_boundaries_are_inclusive_for_every_point(name):
    """量程判定是**闭区间** `low <= v <= high`：上下限本身合法（9 个测点各测两端）。"""
    low, high = RANGES[name]
    for bound in (low, high):
        ts, values, invalid = sub.parse_payload(build_payload({**GOOD_VALUES, name: bound}))
        assert ts == TS
        assert values[name] == bound
        assert invalid == {}


@pytest.mark.parametrize("raw", ["NaN", "nan", "NAN", "-NaN"])
def test_nan_is_invalid_point_only(raw):
    """逐点化后：NaN **不再整条拒收**，而是"该点无效、其余照常"（V4 的纯函数等价物）。

    ⚠️ 它**照样不进主表**（TDengine 写 NaN 会报 syntax error，`sql_float` 也把非有限值写成
    NULL），区别只在**归类**：这是"单点坏"，不是"整条坏"。原始值（nan）保留在 `raw` 里留证。
    """
    ts, values, invalid = sub.parse_payload(build_payload({**GOOD_VALUES, "SO2": raw}))
    assert set(values) == set(NAMES) - {"SO2"}
    item = invalid["SO2"]
    assert item.kind is PointInvalidKind.INVALID
    assert item.raw is not None and math.isnan(item.raw)
    assert "不是有限数值" in item.reason and repr(raw) in item.reason


@pytest.mark.parametrize("raw,expected", [
    ("inf", float("inf")),
    ("Inf", float("inf")),
    ("+inf", float("inf")),
    ("Infinity", float("inf")),
    ("-inf", float("-inf")),
    ("-Infinity", float("-inf")),
])
def test_inf_is_invalid_point_only(raw, expected):
    """±inf 与 NaN 同一条路：该点 invalid，其余 8 点照常入库。"""
    ts, values, invalid = sub.parse_payload(build_payload({**GOOD_VALUES, "SO2": raw}))
    assert set(values) == set(NAMES) - {"SO2"}
    assert math.isinf(invalid["SO2"].raw)
    assert invalid["SO2"].kind is PointInvalidKind.INVALID


def test_missing_point_is_kind_missing_not_whole_payload():
    """⭐ 逐点化后现在应该是什么行为（旧用例钉的是"缺测点 → 整条拒收"）。

    ADR-0009 §10.1：**缺其中几个测点属语义坏**（与超量程一样是单点问题），
    该点记 `kind=missing`（`reason=报文未提供`、`raw=None`），**其余照常入库**。
    ⚠️ `missing` 与 `invalid` 必须分开：前者是"网关没给"（根因常在采集侧），
       后者是"给了但不可信"（根因在数据侧）。
    """
    ts, values, invalid = sub.parse_payload(build_payload(without("Humidity")))
    assert set(values) == set(NAMES) - {"Humidity"}
    assert set(invalid) == {"Humidity"}
    item = invalid["Humidity"]
    assert item.kind is PointInvalidKind.MISSING
    assert item.raw is None                        # 本来就没值
    assert item.reason == "报文未提供"


def test_bad_and_missing_points_are_both_collected():
    """坏点与缺测点**同时存在**时两个都收（旧用例钉的是"只报第一个原因"）。

    逐点化后一条报文里可以有多个无效点，而且 `invalid/invalid` 与 `missing` 分得清。
    """
    values_in = {**without("Humidity"), "Dust": 150.0}
    ts, values, invalid = sub.parse_payload(build_payload(values_in))
    assert set(values) == set(NAMES) - {"Dust", "Humidity"}
    assert invalid["Dust"].kind is PointInvalidKind.INVALID
    assert invalid["Humidity"].kind is PointInvalidKind.MISSING
    assert len(invalid) == 2


def test_all_points_invalid_returns_empty_values_and_nine_invalid():
    """9 个点全坏 → 有效测点空、无效测点 9 个（**不抛异常**）。

    ⚠️ "没有可写的列"由**调用方**（`on_message`）处置：只留痕、不写主表、不落盘
    （落盘会永远补不进去，只会把队列堵死）。见 `test_on_message_all_invalid_...`。
    """
    values_in = {name: "NaN" for name in NAMES}
    ts, values, invalid = sub.parse_payload(build_payload(values_in))
    assert values == {}
    assert len(invalid) == 9
    assert all(item.kind is PointInvalidKind.INVALID for item in invalid.values())


# ==================== 四、parse_payload：语法坏（整条拒收） ====================

@pytest.mark.parametrize("payload", [
    "",                              # 0 段
    "   ",                           # 全空白
    "2026-09-10",                    # 1 段
    "2026-09-10 12:00:00",           # 2 段（只有时间戳，没有测点）
])
def test_parse_payload_too_short_rejects(payload):
    """`len(parts) < 3` 先于时间戳解析与测点校验（最短也要"日期 时间 一个字段"）。

    ⚠️ 异常类型从 `ValueError` 升格成 `PayloadFormatError`（整条报文语法坏，
    分类真源见 `src/common/errors.py`）；"整条拒收"这个**处置**没变。
    """
    with pytest.raises(PayloadFormatError) as excinfo:
        sub.parse_payload(payload)
    assert "报文长度不足" in str(excinfo.value)


@pytest.mark.parametrize("bad_ts", [
    "2026-09-10",                    # 日期之后直接跟测点（时间戳缺一段）
    "2026/09/10 12:00:00",           # 分隔符不合契约
    "2026-09-10T12:00:00",           # 缺空格
    "2026-13-01 12:00:00",           # 月份越界
])
def test_parse_payload_bad_timestamp_rejects_whole_payload(bad_ts):
    """时间戳不可解析 = **语法坏** → 整条拒收（主表与事件表都不该新增，V3）。

    ⚠️ 异常类型同为 `PayloadFormatError`（同上是升格，处置不变）。
    """
    payload = f"{bad_ts} " + " ".join(f"{n}={v}" for n, v in GOOD_VALUES.items())
    with pytest.raises(PayloadFormatError) as excinfo:
        sub.parse_payload(payload)
    assert "时间戳格式非法" in str(excinfo.value)


def test_pointless_payload_is_payload_format_error():
    """`"<ts> Flag=N"` 能过长度检查（3 段），但**一个测点都没有** → 语法坏 → 整条拒收。

    ⭐ 逐点化后现在应该是什么行为：ADR-0009 §3 明确"一个测点都没有 = 语法坏"
    （没有可写的列，报文无意义）—— 不是"缺 9 个点"那种语义坏，
    所以这里**必须抛** `PayloadFormatError`，且消息要说清"一个测点都没有"。
    同时证明 `Flag` 被**跳过**、没有被当成测点。
    """
    with pytest.raises(PayloadFormatError) as excinfo:
        sub.parse_payload(f"{TS} Flag=N")
    assert "一个测点都没有" in str(excinfo.value)


def test_duplicate_point_is_payload_format_error():
    """⭐ 逐点化后现在应该是什么行为（旧用例钉的是"后者覆盖前者"，静默取值）。

    ADR-0009 §10.1：**同一个点出现两次 = 报文自相矛盾** —— 采信哪个都是猜，
    这不是数据质量问题而是**格式问题**，所以整条拒收（抛 `PayloadFormatError`）。
    """
    payload = build_payload() + " SO2=12.0"
    with pytest.raises(PayloadFormatError) as excinfo:
        sub.parse_payload(payload)
    message = str(excinfo.value)
    assert "SO2" in message and "出现两次" in message


def test_duplicate_missing_detection_covers_invalid_first_occurrence():
    """第一个出现的是**无效值**（超量程）、第二个才是合法值：仍然算重复 → 语法坏。

    ⚠️ 这条专治一个实现细节：重复检测不能只看"有效测点字典"，
    否则 `Dust=150.0 … Dust=2.5` 会被当成"合法覆盖"，把自相矛盾的报文放进来。
    """
    payload = build_payload({**GOOD_VALUES, "Dust": 150.0}) + " Dust=2.5"
    with pytest.raises(PayloadFormatError) as excinfo:
        sub.parse_payload(payload)
    assert "Dust" in str(excinfo.value) and "出现两次" in str(excinfo.value)


@pytest.mark.parametrize("raw", ["abc", "", "35.2mg", "1,5", "1.2.3"])
def test_non_numeric_value_is_payload_format_error(raw):
    """⭐ 判断取舍（ADR-0009 未逐字规定，本次显式定下并钉住）：测点值**不是数** → 语法坏。

    理由：① 它连"一个合法的数"都不是，值对象 `PointInvalid.raw`（`float | None`）表达不了；
    ② 这类报文通常是**编码/配置错**（网关把文本当数发），不是单点仪表故障 ——
    宁可整条拦住，也好过把这种报文悄悄写进"看起来正常"的库里。
    ⚠️ 与"超量程/NaN"（语义坏、只丢该点）的区别正在于此：那些值是**数**，只是不可信。
    """
    with pytest.raises(PayloadFormatError) as excinfo:
        sub.parse_payload(build_payload({**GOOD_VALUES, "SO2": raw}))
    message = str(excinfo.value)
    assert "SO2" in message and "数值非法" in message


# ==================== 五、折算/判定的"同帧齐全"前提 ====================

def test_supports_reference_true_only_when_o2_and_pollutants_present():
    """折算与告警判定要 O2 + 三个污染物**同帧齐全**（ADR-0002 §3.1）。

    逐点化后某一帧可能不齐（某点被判无效/缺失）—— 那时必须**跳过折算与判定**，
    而不是猜一个值。这里把"哪些帧够用"钉死。
    """
    assert sub.supports_reference(GOOD_VALUES) is True
    assert set(sub.REFERENCE_FRAME_POINTS) == {"O2", "Dust", "SO2", "NOx"}
    for name in sub.REFERENCE_FRAME_POINTS:
        assert sub.supports_reference(without(name)) is False    # 少任何一个都不够
    # 少一个"不参与折算"的点（Temp）不影响折算/判定
    assert sub.supports_reference(without("Temp")) is True


# ==================== 六、折算值：reference_values（数学层） ====================

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


# ==================== 七、折算值：transmit_reference_values（协议层） ====================

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


# ==================== 八、折算值快照：record_reference / latest_* ====================

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


# ==================== 九、主题 ↔ 厂区 ====================

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


# ==================== 十、配置与路径解析 ====================

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


@pytest.mark.parametrize("bad", ["cems-point-invalid", "cems point", "1invalid", "invalid;drop"])
def test_validate_config_rejects_bad_point_invalid_stable(bad, monkeypatch):
    """新增的无效点事件表名同样要过白名单（它也会被拼进 SQL）。"""
    monkeypatch.setattr(sub, "POINT_INVALID_STABLE", bad)
    with pytest.raises(ValueError) as excinfo:
        sub.validate_config()
    message = str(excinfo.value)
    assert "POINT_INVALID_STABLE" in message and repr(bad) in message


def test_validate_config_accepts_default_point_invalid_stable():
    assert sub.POINT_INVALID_STABLE == "cems_point_invalid"
    assert sub.validate_config() is None


# ==================== 十一、回调边界：on_message（离线假对象） ====================

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


class FakePointInvalidWriter:
    """假无效点事件表写入器：记录 `(ts, items)`，可让它整批写失败。"""

    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.calls: list[tuple[str, list[PointInvalid]]] = []

    def write(self, ts: str, items: list[PointInvalid]) -> int:
        self.calls.append((ts, list(items)))
        return len(items) if self.ok else 0


class FakeMessage:
    """假 MQTT 报文：`on_message` 只用到 `.payload`。"""

    def __init__(self, payload: str) -> None:
        self.payload = payload.encode("utf-8")


def userdata(writer: Optional[FakeWriter] = None, spool: Optional[FakeSpool] = None,
             judge: Optional[FakeJudge] = None,
             point_invalid_writer: Optional[FakePointInvalidWriter] = None,
             quality_checker: Optional[sub.QualityChecker] = None) -> dict:
    """组装 `on_message` 的 userdata。

    ⚠️ `quality_checker` **默认每次都给一个全新的判据实例**（ADR-0010）——
    否则用例之间会共用同一个**模块级**判据状态，前一个用例的"上一次值/冻结游程"
    会污染后一个用例，测试就变成了顺序相关的（病态地难查）。
    """
    return {
        "writer": writer if writer is not None else FakeWriter(),
        "spool": spool if spool is not None else FakeSpool(),
        "judge": judge if judge is not None else FakeJudge(),
        "point_invalid_writer": (
            point_invalid_writer if point_invalid_writer is not None
            else FakePointInvalidWriter()
        ),
        "quality_checker": (
            quality_checker if quality_checker is not None
            else sub.QualityChecker(sub.QualityConfig())
        ),
    }


@pytest.fixture(autouse=True)
def restore_module_state():
    """`STATS` / `LAST_REFERENCE` / 模块级判据是可变状态：每个用例后复原，避免互相污染。"""
    stats_before = dict(sub.STATS)
    reference_before = dict(sub.LAST_REFERENCE["values"])
    ts_before = sub.LAST_REFERENCE["ts"]
    yield
    sub.STATS.clear()
    sub.STATS.update(stats_before)
    sub.LAST_REFERENCE["values"] = reference_before
    sub.LAST_REFERENCE["ts"] = ts_before
    # ADR-0010：`on_message` 在 userdata 缺判据时回落到模块级 `QUALITY_CHECKER`，
    # 所以它也必须在用例之间复位（换一个新实例 = 清空全部进程内状态）。
    sub.QUALITY_CHECKER = sub.QualityChecker(sub.QUALITY_CONFIG)


def test_stats_keys_are_the_split_counters():
    """计数按 ADR-0009 §10.3 拆分后**不再有** `rejected`（全仓库不留悬空引用）。"""
    assert "rejected" not in sub.STATS
    assert sub.STATS["rejected_messages"] == 0        # 语法坏丢的**报文条数**
    assert sub.STATS["invalid_points"] == 0           # 语义坏丢的**测点个数**
    # ADR-0010：可疑点单独计数 —— ⚠️ 它与 invalid_points 不是一回事（可疑点**没丢**）
    assert sub.STATS["suspect_points"] == 0


def test_on_message_valid_payload_writes_all_nine_values():
    writer = FakeWriter()
    judge = FakeJudge()
    invalid_writer = FakePointInvalidWriter()
    rejected_before = sub.STATS["rejected_messages"]
    invalid_before = sub.STATS["invalid_points"]
    sub.on_message(
        None, userdata(writer, judge=judge, point_invalid_writer=invalid_writer),
        FakeMessage(build_payload()),
    )
    assert len(writer.calls) == 1
    ts, values = writer.calls[0]
    assert ts == TS and values == GOOD_VALUES
    assert sub.STATS["rejected_messages"] == rejected_before   # 合格报文不动拒收计数
    assert sub.STATS["invalid_points"] == invalid_before
    assert invalid_writer.calls == []                          # V6：无无效点 → 事件表零新增
    assert judge.samples == [TS]                               # 帧齐全 → 判据照常收到样本


def test_on_message_pointwise_one_bad_point_writes_the_other_eight():
    """⭐ 逐点化后现在应该是什么行为（旧用例钉的是"一个坏点 → 一个点都不写"）。

    现状：Dust 超量程 → `parse_payload` **不抛异常** →
      - `writer.write` 被调用**一次**，且只带 8 个合格测点（Dust 缺列 → 库里是 NULL）；
      - 无效点事件表收到**正好一条**证据（`kind=invalid` + 原因 + 原始值）；
      - `rejected_messages` **不增加**（它不是语法坏），`invalid_points` +1。
    """
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    rejected_before = sub.STATS["rejected_messages"]
    invalid_before = sub.STATS["invalid_points"]
    sub.on_message(
        None, userdata(writer, point_invalid_writer=invalid_writer),
        FakeMessage(build_payload({**GOOD_VALUES, "Dust": 150.0})),
    )
    assert len(writer.calls) == 1
    ts, values = writer.calls[0]
    assert ts == TS
    assert values == without("Dust")                  # 8 个点照常入库（V2）
    assert "Dust" not in values
    assert sub.STATS["rejected_messages"] == rejected_before
    assert sub.STATS["invalid_points"] == invalid_before + 1
    assert len(invalid_writer.calls) == 1
    ts_event, items = invalid_writer.calls[0]
    assert ts_event == TS and len(items) == 1         # V1：有且仅有一条
    assert items[0].point == "Dust"
    assert items[0].kind is PointInvalidKind.INVALID
    assert items[0].raw == 150.0


def test_on_message_missing_point_is_recorded_and_other_points_still_written():
    """报文里少一个点（Humidity）→ 该点记 `kind=missing`，其余 8 点照常入库。"""
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    sub.on_message(
        None, userdata(writer, point_invalid_writer=invalid_writer),
        FakeMessage(build_payload(without("Humidity"))),
    )
    assert len(writer.calls) == 1
    assert writer.calls[0][1] == without("Humidity")
    _ts, items = invalid_writer.calls[0]
    assert [item.point for item in items] == ["Humidity"]
    assert items[0].kind is PointInvalidKind.MISSING


def test_on_message_incomplete_frame_skips_reference_and_alarm():
    """⭐ 帧不齐时**跳过折算与告警判定**（但该条仍照常入库）。

    折算要 O2、判据要 O2+dust/so2/nox 同帧（`AlarmJudge.on_sample` 直接按名字取键）。
    O2 被判无效时若硬跑判据就是 KeyError；这里钉死"跳过"而不是"猜值/丢整条"。
    """
    writer = FakeWriter()
    judge = FakeJudge()
    invalid_writer = FakePointInvalidWriter()
    before = sub.latest_reference_timestamp()
    sub.on_message(
        None, userdata(writer, judge=judge, point_invalid_writer=invalid_writer),
        FakeMessage(build_payload({**GOOD_VALUES, "O2": 99.0})),   # O2 超量程 → invalid
    )
    assert values_has_other_points(writer.calls)                  # 其余 8 点入库了
    assert judge.samples == []                                    # 判据**没**收到样本
    assert sub.latest_reference_timestamp() == before             # 折算快照也没被这次污染


def values_has_other_points(calls: list) -> bool:
    """辅助断言：库写调用恰好一次、且缺的点只有 O2。"""
    return len(calls) == 1 and set(calls[0][1]) == set(NAMES) - {"O2"}


def test_on_message_all_points_invalid_writes_nothing_and_spools_nothing():
    """9 个点全坏 → 没有可写的列：**只留痕**，不写主表、**不落盘**（别把队列堵死）。"""
    writer = FakeWriter()
    spool = FakeSpool()
    invalid_writer = FakePointInvalidWriter()
    payload = build_payload({name: "NaN" for name in NAMES})
    sub.on_message(
        None, userdata(writer, spool, point_invalid_writer=invalid_writer),
        FakeMessage(payload),
    )
    assert writer.calls == []
    assert spool.appended == []
    assert len(invalid_writer.calls[0][1]) == 9


def test_on_message_syntax_bad_payload_rejects_whole_message():
    """V3 的纯函数等价物：时间戳不可解析 → 整条拒收，主表与事件表**都不新增**。"""
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    rejected_before = sub.STATS["rejected_messages"]
    sub.on_message(
        None, userdata(writer, point_invalid_writer=invalid_writer),
        FakeMessage("2026/09/10 12:00:00 " + " ".join(
            f"{n}={v}" for n, v in GOOD_VALUES.items()
        )),
    )
    assert writer.calls == []
    assert invalid_writer.calls == []
    assert sub.STATS["rejected_messages"] == rejected_before + 1
    assert sub.STATS["invalid_points"] == 0


def test_on_message_duplicate_point_rejects_whole_message():
    """重复测点同样是语法坏 → 整条拒收（不写库、不留痕、只加 `rejected_messages`）。"""
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    rejected_before = sub.STATS["rejected_messages"]
    sub.on_message(
        None, userdata(writer, point_invalid_writer=invalid_writer),
        FakeMessage(build_payload() + " Dust=1.0"),
    )
    assert writer.calls == []
    assert invalid_writer.calls == []
    assert sub.STATS["rejected_messages"] == rejected_before + 1
    assert sub.STATS["invalid_points"] == 0


def test_on_message_event_write_failure_does_not_block_ingest():
    """证据表写失败**绝不打断入库**（反过来丢 8 个正确点才是最糟的）。

    `record_invalid_points` 内部已记 CRITICAL；`on_message` 也再兜一层异常。
    """
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter(ok=False)
    sub.on_message(
        None, userdata(writer, point_invalid_writer=invalid_writer),
        FakeMessage(build_payload({**GOOD_VALUES, "Dust": 150.0})),
    )
    assert len(writer.calls) == 1                     # 数据照样入库
    assert invalid_writer.calls[0][1][0].point == "Dust"


def test_on_message_spools_raw_payload_when_write_fails():
    """入库失败 → 先把**原始报文**落本地队列（返回即 PUBACK，落盘是这条数据唯一的副本）。"""
    writer = FakeWriter(ok=False)
    spool = FakeSpool()
    spooled_before = sub.STATS["spooled"]
    payload = build_payload()
    sub.on_message(None, userdata(writer, spool), FakeMessage(payload))
    assert spool.appended == [payload]
    assert sub.STATS["spooled"] == spooled_before + 1


def test_record_invalid_points_returns_success_count_and_tolerates_zero():
    """`record_invalid_points` 的返回口径：成功条数；空输入不动写入器。"""
    invalid_writer = FakePointInvalidWriter()
    assert sub.record_invalid_points(invalid_writer, TS, {}) == 0
    assert invalid_writer.calls == []

    items = {
        "Dust": PointInvalid("Dust", PointInvalidKind.INVALID, "超出量程", 150.0),
        "Humidity": PointInvalid("Humidity", PointInvalidKind.MISSING, "报文未提供", None),
    }
    assert sub.record_invalid_points(invalid_writer, TS, items) == 2
    assert len(invalid_writer.calls[0][1]) == 2


# ==================== 十二、入库 SQL：TdWriter._insert（部分列） ====================

class FakeCursor:
    """假游标：只记录执行过的 SQL。"""

    def __init__(self) -> None:
        self.sqls: list[str] = []

    def execute(self, sql: str) -> None:
        self.sqls.append(sql)


class FailingCursor:
    """假游标：执行必失败（用来钉"失败返回 False"）。"""

    def execute(self, sql: str) -> None:
        raise RuntimeError("boom")


def test_insert_sql_column_and_value_order_follow_points_contract():
    """列名与数值都按 `points.POINTS` 的同序展开：报文顺序/建表顺序漂移不会写错列。

    ⭐ 这条同时是**回归护栏**：9 列齐全时拼出来的 SQL 与逐点化之前**逐字节相同**
    （既有 9 点路径不许动）。
    """
    writer = sub.TdWriter()
    cursor = FakeCursor()
    writer._cur = cursor
    assert writer._insert(TS, GOOD_VALUES) is True

    columns = ", ".join(("ts", *sub.TD_COLUMNS))
    numbers = ", ".join(f"{GOOD_VALUES[point.name]}" for point in POINTS)
    expected = (
        f"INSERT INTO {sub.TD_DB}.{sub.CHILD_TABLE} USING {sub.TD_DB}.{sub.TD_STABLE} "
        f"TAGS ('{sub.TD_PLANT}', '{sub.TD_DEVICE}') "
        f"({columns}) VALUES ('{TS}', {numbers})"
    )
    assert cursor.sqls == [expected]          # 逐字节相同，不是"包含即可"


def test_insert_writes_only_the_columns_passed():
    """⭐ 逐点化后现在应该是什么行为（旧用例钉的是"缺列 → KeyError"）。

    `_insert` 必须支持"**传入哪几列就写哪几列**"（ADR-0009 §10.2 第 1 步，
    否则逐点化后第一个坏点就会把入库打挂）：缺的点**整列不出现**，
    由 TDengine 按"省略的列就是 NULL"处理。
    列序仍严格跟随 `POINTS` 契约，而不是 `values` 的键序。
    """
    writer = sub.TdWriter()
    cursor = FakeCursor()
    writer._cur = cursor
    assert writer._insert(TS, without("Dust")) is True

    kept = [point for point in POINTS if point.name != "Dust"]
    columns = ", ".join(("ts", *(point.column for point in kept)))
    numbers = ", ".join(f"{GOOD_VALUES[point.name]}" for point in kept)
    sql = cursor.sqls[0]
    assert f"({columns}) VALUES ('{TS}', {numbers})" in sql
    assert "dust" not in columns and "dust" not in numbers    # 缺的列整列不出现


def test_insert_partial_columns_keep_contract_order_even_if_values_reordered():
    """传入的字典键序被打乱也不影响结果：列序只认契约 `POINTS`。"""
    writer = sub.TdWriter()
    cursor = FakeCursor()
    writer._cur = cursor
    shuffled = {name: GOOD_VALUES[name] for name in reversed(list(NAMES))}
    assert writer._insert(TS, shuffled) is True
    columns = ", ".join(("ts", *sub.TD_COLUMNS))
    numbers = ", ".join(f"{GOOD_VALUES[point.name]}" for point in POINTS)
    assert f"({columns}) VALUES ('{TS}', {numbers})" in cursor.sqls[0]


def test_insert_without_any_point_does_not_execute_sql():
    """一个可写的列都没有 → 返回 False 且**不执行 SQL**（调用方负责提前拦住）。"""
    writer = sub.TdWriter()
    cursor = FakeCursor()
    writer._cur = cursor
    assert writer._insert(TS, {}) is False
    assert cursor.sqls == []


def test_insert_returns_false_when_cursor_missing_or_execute_fails():
    writer = sub.TdWriter()
    assert writer._insert(TS, GOOD_VALUES) is False        # 没有游标
    writer._cur = FailingCursor()
    assert writer._insert(TS, GOOD_VALUES) is False        # 执行抛异常 → 吞掉并返回 False


# ==================== 十三、无效点事件表（DDL/DML 文本 + 写入器） ====================

def test_point_invalid_ddl_shape_and_defaults():
    """建表语句：`CREATE STABLE IF NOT EXISTS`，列与 §4.1 + ADR-0010 §4 一致，TAGS 只有 plant/device。

    ⚠️ ADR-0010 之后**多了 `stored TINYINT`**（下面单独钉死），本用例的既有断言**一条未改**。
    """
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    assert tables.stable == "cems_point_invalid"
    sql = tables.create_sql()
    assert sql.startswith("CREATE STABLE IF NOT EXISTS cems.cems_point_invalid (")
    for fragment in ("ts TIMESTAMP", "point NCHAR(16)", "kind NCHAR(10)",
                     "reason NCHAR(200)", "raw FLOAT", "stored TINYINT",
                     "TAGS (plant NCHAR(20), device NCHAR(20))"):
        assert fragment in sql
    assert sub.POINT_INVALID_TABLES.stable == sub.POINT_INVALID_STABLE


def test_main_stable_column_layout_is_untouched():
    """⚠️ ADR-0009 §4.1：**不加列到主表** —— 主表列布局保持不动（既有报表/导出不受影响）。"""
    assert sub.TD_COLUMNS == tuple(point.column for point in POINTS)
    assert "kind" not in sub.TD_COLUMNS and "reason" not in sub.TD_COLUMNS
    assert len(sub.TD_COLUMNS) == 9


def test_point_invalid_child_is_per_point_so_one_row_per_point_per_ts():
    """幂等键 = `(子表, ts)`：**每个测点一张子表**，同一条报文里多点无效不会互相覆盖。"""
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    assert tables.child("Dust") == "plant1_device1_invalid_dust"
    assert tables.child("SO2") == "plant1_device1_invalid_so2"
    assert tables.child("Dust") != tables.child("SO2")

    dust = tables.insert_sql(TS, PointInvalid("Dust", PointInvalidKind.INVALID, "超出量程", 150.0))
    so2 = tables.insert_sql(TS, PointInvalid("SO2", PointInvalidKind.MISSING, "报文未提供", None))
    assert "INSERT INTO cems.plant1_device1_invalid_dust " in dust
    assert "INSERT INTO cems.plant1_device1_invalid_so2 " in so2
    assert f"USING cems.{tables.stable} TAGS ('plant1', 'device1')" in dust


def test_point_invalid_insert_sql_carries_kind_reason_and_raw():
    """一条证据行必须含 `kind` / `reason` / `raw`（含原始值）—— 否则"逐点留痕"是空话。

    ⚠️ **本用例的断言在 ADR-0010 实现时被有意改写**（列的**清单**多了 `stored`）：
    行尾从 `…, 60.0)` 变成 `…, 60.0, 0)`。这不是为了让测试变绿 ——
    `stored` 是 ADR-0010 §4 的硬要求，**每一行都必须表态"这个值进没进主表"**。
    """
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    sql = tables.insert_sql(
        TS, PointInvalid("Pressure", PointInvalidKind.INVALID, "测点 Pressure 超出量程 [85.0, 105.0]: 60.0", 60.0),
    )
    assert "(ts, point, kind, reason, raw, stored) VALUES ('2026-09-10 12:00:00', 'Pressure', 'invalid', " in sql
    assert "'测点 Pressure 超出量程 [85.0, 105.0]: 60.0', 60.0, 0)" in sql      # stored=0：丢了


def test_point_invalid_missing_row_has_null_raw():
    """`kind=missing` 的 `raw` 必须是 `NULL`（本来就没值，别编一个）。

    ⚠️ ADR-0010：行尾由 `NULL)` 变为 `NULL, 0)`（`missing` 同样 `stored=0`）。
    """
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    sql = tables.insert_sql(TS, PointInvalid("Humidity", PointInvalidKind.MISSING, "报文未提供", None))
    assert "'missing', '报文未提供', NULL, 0)" in sql


def test_point_invalid_non_finite_raw_becomes_null_but_reason_keeps_evidence():
    """NaN/inf 的 `raw` 写 `NULL`：**非有限值不能拼进 SQL**（TDengine 会报 syntax error），
    原始文本已经写在 `reason` 里 —— 证据不丢，行也不会写坏。

    ⚠️ ADR-0010：行尾由 `, NULL)` 变为 `, NULL, 0)`。
    """
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    sql = tables.insert_sql(
        TS,
        PointInvalid("SO2", PointInvalidKind.INVALID, "测点 SO2 不是有限数值（NaN/inf）: 'NaN'",
                     float("nan")),
    )
    assert "'测点 SO2 不是有限数值（NaN/inf）: ''NaN'''" in sql   # 单引号被转义
    assert sql.endswith(", NULL, 0)")


def test_point_invalid_insert_sql_is_deterministic_for_repeat_delivery():
    """V5 的**机制断言**：同一条报文重复投递 → 生成的 SQL **一字不差**
    ⇒ `(子表, ts)` 覆盖语义保证事件表不重复。"""
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    item = PointInvalid("Dust", PointInvalidKind.INVALID, "超出量程", 150.0)
    assert tables.insert_sql(TS, item) == tables.insert_sql(TS, item)


def test_point_invalid_reason_is_truncated_to_column_width():
    """`reason` 列是 NCHAR(200)：超长原因按列宽截断，宁可少几个字也别让整行写不进去。"""
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    sql = tables.insert_sql(
        TS, PointInvalid("Dust", PointInvalidKind.INVALID, "x" * 500, 1.0),
    )
    assert "'" + "x" * 200 + "'" in sql and "x" * 201 not in sql


def test_point_invalid_writer_writes_every_item_and_counts_success():
    """`PointInvalidWriter.write` 逐条执行并返回成功条数（用假游标，不连库）。"""
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    writer = sub.PointInvalidWriter(tables)
    cursor = FakeCursor()
    writer._cur = cursor
    items = [
        PointInvalid("Dust", PointInvalidKind.INVALID, "超出量程", 150.0),
        PointInvalid("Humidity", PointInvalidKind.MISSING, "报文未提供", None),
    ]
    assert writer.write(TS, items) == 2
    assert len(cursor.sqls) == 2
    assert all(sql.startswith("INSERT INTO cems.plant1_device1_invalid_") for sql in cursor.sqls)


def test_point_invalid_writer_returns_zero_when_connection_unavailable(monkeypatch):
    """连不上库时 `execute` 返回 False、`write` 返回 0（**不抛异常**，调用方按"没写成"处理）。"""
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    writer = sub.PointInvalidWriter(tables)
    monkeypatch.setattr(writer, "connect", lambda: False)
    assert writer.write(TS, [PointInvalid("Dust", PointInvalidKind.INVALID, "x", 1.0)]) == 0


def test_point_invalid_writer_ready_flag():
    writer = sub.PointInvalidWriter(sub.PointInvalidTables(db="cems", plant="p", device="d"))
    assert writer.ready is False
    writer._cur = FakeCursor()
    assert writer.ready is True


# ==================== 十四、数据合理性判据（跳变/冻结，ADR-0010） ====================
#
# ⭐⭐ 本节钉的是 ADR-0010 的**核心决策**：**「可疑」≠「无效」** ——
#     跳变/冻结（`kind=jump/frozen`，`stored=1`）**照常入库**，只是额外留一笔痕。
#     所以下面每一条"可疑"用例都同时断言两件事：
#       ① 值**在写主表的调用里**（没被丢掉）；② 事件表有一条 `stored=1` 的证据。

def quality_config(**overrides: Any) -> sub.QualityConfig:
    """测试用判据配置：默认取 ADR-0010 §5 那一组，可按用例覆盖。"""
    return sub.QualityConfig(**overrides)


def test_kind_stored_splits_dropped_from_kept():
    """⭐ ADR-0010 §4 的**核心可区分性**：`stored=0` = 丢了 / `stored=1` = 标了但留着。"""
    assert PointInvalidKind.INVALID.stored == 0
    assert PointInvalidKind.MISSING.stored == 0
    assert PointInvalidKind.JUMP.stored == 1
    assert PointInvalidKind.FROZEN.stored == 1


def test_kind_values_fit_nchar10_column():
    """⚠️ ADR-0010 §6②：`kind` 列是 `NCHAR(10)`，取值超 10 字符会 `DB error: Value too long`。

    实测踩过：`kind='suspect_jump'` 直接写不进去。所以取值只能是短名，
    人可读解释放 `reason`（`NCHAR(200)`）。
    """
    for kind in PointInvalidKind:
        assert len(kind.value) <= 10, kind
        assert kind.value.isascii() and kind.value.islower()


def test_quality_config_defaults_match_adr_0010():
    """⚠️ 默认值 = ADR-0010 §5 那一组（**"拍的"、未标定**，标定手段是 §8 的 W4）。"""
    config = sub.QualityConfig()
    assert config.enable is True          # §7：默认开（只留痕不丢数，开着代价小）
    assert config.jump_ratio == 0.25
    assert config.freeze_samples == 12
    assert config.freeze_tolerance_ratio == 0.005
    assert config.resolve_ranges() is RANGES      # 量程唯一真源是契约 points.RANGES


def test_quality_config_from_env_reads_all_four(monkeypatch):
    monkeypatch.setenv("QUALITY_CHECK_ENABLE", "0")
    monkeypatch.setenv("QUALITY_JUMP_RATIO", "0.5")
    monkeypatch.setenv("QUALITY_FREEZE_SAMPLES", "7")
    monkeypatch.setenv("QUALITY_FREEZE_TOLERANCE_RATIO", "0.02")
    config = sub.QualityConfig.from_env()
    assert config.enable is False
    assert config.jump_ratio == 0.5
    assert config.freeze_samples == 7
    assert config.freeze_tolerance_ratio == 0.02


def test_quality_config_from_env_defaults_to_adr_values(monkeypatch):
    for name in ("QUALITY_CHECK_ENABLE", "QUALITY_JUMP_RATIO",
                 "QUALITY_FREEZE_SAMPLES", "QUALITY_FREEZE_TOLERANCE_RATIO"):
        monkeypatch.delenv(name, raising=False)
    assert sub.QualityConfig.from_env() == sub.QualityConfig()


@pytest.mark.parametrize("overrides", [
    {"jump_ratio": 0.0},
    {"jump_ratio": -0.1},
    {"freeze_tolerance_ratio": -0.001},
    {"freeze_samples": 1},          # 1 个样本"不变"是恒真的 —— 会把每条报文都标成冻结
    {"freeze_samples": 0},
])
def test_quality_config_validate_rejects_bad_values(overrides):
    with pytest.raises(ValueError):
        quality_config(**overrides).validate()


def test_quality_config_validate_accepts_adr_defaults():
    sub.QualityConfig().validate()          # 不抛即通过


# ---- 14.1 跳变 ----

def test_jump_fires_on_step_over_ratio():
    """注入单点阶跃：`Dust` 2.5 → 60（量程 [0,100]，跳了 57.5% > 25%）→ 一条 `jump`。"""
    checker = sub.QualityChecker(quality_config())
    assert checker.check({"Dust": 2.5}) == []          # 冷启动：没有"上一次值"
    suspects = checker.check({"Dust": 60.0})
    assert len(suspects) == 1
    item = suspects[0]
    assert item.point == "Dust"
    assert item.kind is PointInvalidKind.JUMP
    assert item.kind.stored == 1                       # ⭐ 值**已入库**（不是丢了）
    assert item.raw == 60.0
    assert "跳变可疑" in item.reason and "Dust" in item.reason


def test_jump_does_not_fire_on_first_sample_cold_start():
    """冷启动窗口（ADR-0010 §9）：第一次见到该点没有参照，**不判**跳变。"""
    assert sub.QualityChecker(quality_config()).check({"Dust": 99.0}) == []


def test_jump_threshold_is_strict_greater_than():
    """边界：`|Δ| == 阈值` **不算**跳变（定义写的是 `>`），略超才算。"""
    checker = sub.QualityChecker(quality_config())
    checker.check({"Dust": 2.5})
    assert checker.check({"Dust": 27.5}) == []          # Δ=25.0 == 量程宽度 100 × 0.25
    assert checker.check({"Dust": 2.5}) == []           # 反向也恰好等于阈值 → 同样不判
    assert [item.kind for item in checker.check({"Dust": 27.51})] == [PointInvalidKind.JUMP]


def test_jump_uses_the_points_own_range_width():
    """免逐点配置的关键：阈值按**该点自己**的量程宽度算（Pressure [85,105] 宽 20 ⇒ 阈值 5）。"""
    checker = sub.QualityChecker(quality_config())
    checker.check({"Pressure": 90.0})
    assert checker.check({"Pressure": 94.9}) == []      # Δ=4.9 < 5
    assert checker.check({"Pressure": 90.0}) == []      # 反向 4.9 < 5
    assert [item.kind for item in checker.check({"Pressure": 95.1})] == [PointInvalidKind.JUMP]


def test_quality_config_accepts_injected_ranges():
    """测试可注入自定义量程（生产走契约 `points.RANGES`，不重复抄一遍量程）。"""
    config = quality_config(ranges={"Dust": (0.0, 10.0)})
    checker = sub.QualityChecker(config)
    checker.check({"Dust": 1.0})
    suspects = checker.check({"Dust": 5.0})             # Δ=4.0 > 10×0.25=2.5
    assert [item.kind for item in suspects] == [PointInvalidKind.JUMP]


def test_check_only_looks_at_points_present_in_values():
    """判据只吃**过了量程检查的点**：报文里没有的点不参与，也不推进状态。"""
    checker = sub.QualityChecker(quality_config())
    assert checker.check({}) == []
    assert checker.check({"O2": 6.0}) == []


# ---- 14.2 冻结 ----

def test_frozen_fires_after_n_samples_inside_tolerance():
    """注入持续不变（同一 `Dust` 值）→ 第 N 个样本报一条 `frozen`（照常入库）。"""
    checker = sub.QualityChecker(quality_config(freeze_samples=4))
    assert checker.check({"Dust": 2.5}) == []
    assert checker.check({"Dust": 2.5}) == []
    assert checker.check({"Dust": 2.5}) == []
    suspects = checker.check({"Dust": 2.5})            # 第 4 个样本
    assert len(suspects) == 1
    assert suspects[0].kind is PointInvalidKind.FROZEN
    assert suspects[0].kind.stored == 1
    assert suspects[0].raw == 2.5
    assert "冻结可疑" in suspects[0].reason


def test_frozen_reports_only_once_per_run():
    """一个游程**只报一次** —— 否则一个冻结的测点会每 5 秒刷一条事件（写入放大）。"""
    checker = sub.QualityChecker(quality_config(freeze_samples=3))
    fired = sum(len(checker.check({"Dust": 2.5})) for _ in range(10))
    assert fired == 1


def test_frozen_tolerance_band_keeps_the_run_alive():
    """⚠️ ADR-0010 §2：判据是「**容差 + 持续**」，不是「相等」——

    `Temp` 量程宽 300 ⇒ 容差 1.5；26.0/26.1/26.2 这种**稳态抖动**都在带内，
    所以它们**算同一个游程**（这正是"用相等会误报"的那条设计取舍的落点）。
    """
    checker = sub.QualityChecker(quality_config(freeze_samples=3))
    assert checker.check({"Temp": 26.0}) == []
    assert checker.check({"Temp": 26.1}) == []
    suspects = checker.check({"Temp": 26.2})
    assert [item.kind for item in suspects] == [PointInvalidKind.FROZEN]


def test_frozen_run_restarts_when_value_leaves_tolerance_band():
    """走出容差带 ⇒ 游程重起算（基准 `v0` 固定为游程第一个值，照 ADR-0010 §2 的字面）。"""
    checker = sub.QualityChecker(quality_config(freeze_samples=3))
    assert checker.check({"Temp": 26.0}) == []
    assert checker.check({"Temp": 26.0}) == []
    assert checker.check({"Temp": 30.0}) == []          # 出了 ±1.5 ⇒ 重起算（此时游程=1）
    assert checker.check({"Temp": 30.0}) == []          # 游程=2
    suspects = checker.check({"Temp": 30.0})            # 游程=3 ⇒ 报
    assert [item.kind for item in suspects] == [PointInvalidKind.FROZEN]
    assert suspects[0].raw == 30.0


def test_jump_starts_a_new_freeze_run():
    """跳变之后值换了 ⇒ 冻结游程从**新值**重新起算（不会把跳变前的游程续上）。"""
    checker = sub.QualityChecker(quality_config(freeze_samples=3))
    checker.check({"Dust": 2.5})
    checker.check({"Dust": 2.5})
    jumped = checker.check({"Dust": 60.0})              # 跳变 + 游程重起算（=1）
    assert [item.kind for item in jumped] == [PointInvalidKind.JUMP]
    assert checker.check({"Dust": 60.0}) == []          # 游程=2
    frozen = checker.check({"Dust": 60.0})              # 游程=3
    assert [item.kind for item in frozen] == [PointInvalidKind.FROZEN]


def test_freeze_state_is_per_point():
    """每个测点一份游程：其它点在动，不影响本点的冻结判定。"""
    checker = sub.QualityChecker(quality_config(freeze_samples=3))
    checker.check({"Dust": 2.5, "O2": 6.0})
    checker.check({"Dust": 2.5, "O2": 6.5})             # O2 出了容差 ⇒ O2 重起算
    suspects = checker.check({"Dust": 2.5, "O2": 6.0})  # Dust 游程=3、O2 游程=2
    assert [(item.point, item.kind) for item in suspects] == [("Dust", PointInvalidKind.FROZEN)]


def test_suspects_are_ordered_by_contract_names():
    """同一条报文里多个点可疑时，事件顺序按契约 `NAMES` 顺序（不用报文键序）→ 确定性。"""
    checker = sub.QualityChecker(quality_config(freeze_samples=2))
    checker.check({"O2": 6.0, "Dust": 2.5})
    suspects = checker.check({"O2": 6.0, "Dust": 2.5})
    assert [item.point for item in suspects] == [
        name for name in NAMES if name in ("Dust", "O2")
    ]


# ---- 14.3 总开关（W5） ----

def test_check_disabled_produces_nothing_while_enabled_produces_evidence():
    """W5 的机制断言：`QUALITY_CHECK_ENABLE=0` ⇒ 不产证据；同样输入开着时**必须**产证据。"""
    sequence = [{"Dust": 2.5}] + [{"Dust": 2.5}] * 20 + [{"Dust": 90.0}]
    off = sub.QualityChecker(quality_config(enable=False))
    assert [item for values in sequence for item in off.check(values)] == []
    on = sub.QualityChecker(quality_config(enable=True, freeze_samples=3))
    kinds = [item.kind for values in sequence for item in on.check(values)]
    assert PointInvalidKind.FROZEN in kinds and PointInvalidKind.JUMP in kinds


def test_disabled_checker_does_not_advance_any_state():
    """关掉时**连状态都不推进**（否则"开着"的那一刻会突然按旧状态报一堆）。"""
    checker = sub.QualityChecker(quality_config(enable=False))
    for _ in range(5):
        checker.check({"Dust": 2.5})
    assert checker._last == {} and checker._run_len == {}


# ---- 14.4 on_message 接线：⭐ 可疑照常入库 + 留痕（W1/W2/W3/W6） ----

def test_on_message_jump_point_is_still_written_and_evidence_marked_stored():
    """⭐⭐ 本 ADR 最重要的一条：跳变点**照常入库**（W1/W6），事件表 `kind=jump / stored=1`。"""
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    checker = sub.QualityChecker(quality_config())
    data = userdata(writer, point_invalid_writer=invalid_writer, quality_checker=checker)
    invalid_before = sub.STATS["invalid_points"]

    sub.on_message(None, data, FakeMessage(build_payload()))                    # 打底
    sub.on_message(None, data, FakeMessage(build_payload({**GOOD_VALUES, "Dust": 60.0})))

    assert len(writer.calls) == 2
    _ts, values = writer.calls[1]
    assert values["Dust"] == 60.0                       # ⭐ 值**在写库调用里**（没被丢掉）
    assert set(values) == set(GOOD_VALUES)              # 一个点都没少
    assert sub.STATS["invalid_points"] == invalid_before     # 可疑 ≠ 无效（不计 invalid）
    assert sub.STATS["suspect_points"] == 1
    ts_event, items = invalid_writer.calls[-1]
    assert ts_event == TS
    assert [(item.point, item.kind, item.kind.stored) for item in items] == [
        ("Dust", PointInvalidKind.JUMP, 1)
    ]


def test_on_message_frozen_point_is_still_written_and_evidence_marked_stored():
    """W2：持续不变 → 每个样本都**照常入库**，事件表出现一条 `kind=frozen / stored=1`。

    ⚠️ 注入的报文体 9 个测点**全都**是不变值，所以第 3 条时 9 个点**同时**判冻结
    （这正是"冻结"在稳态数据上的真实形态）；这里钉的是"照常入库 + 每个点只报一次"。
    """
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    checker = sub.QualityChecker(quality_config(freeze_samples=3))
    data = userdata(writer, point_invalid_writer=invalid_writer, quality_checker=checker)
    for _ in range(4):
        sub.on_message(None, data, FakeMessage(build_payload()))
    assert len(writer.calls) == 4                       # 4 条都进主表了（含被标冻结的那条）
    assert all(call[1]["Dust"] == 2.5 for call in writer.calls)
    # 第 3 条（达到门限）报一次；第 4 条仍在同一游程里 → **不再重复报**
    assert len(invalid_writer.calls) == 1
    _ts, items = invalid_writer.calls[0]
    assert [item.point for item in items] == list(NAMES)      # 按契约 NAMES 顺序
    assert all(item.kind is PointInvalidKind.FROZEN for item in items)
    dust = next(item for item in items if item.point == "Dust")
    assert dust.kind.stored == 1 and dust.raw == 2.5


def test_on_message_invalid_point_still_stored_zero_regression():
    """W3 回归：超量程仍然是"丢"（`stored=0`）—— ADR-0010 **没有**放宽 ADR-0009 的判据。"""
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    sub.on_message(
        None, userdata(writer, point_invalid_writer=invalid_writer),
        FakeMessage(build_payload({**GOOD_VALUES, "Dust": 150.0})),
    )
    assert "Dust" not in writer.calls[0][1]             # 不给主表
    _ts, items = invalid_writer.calls[0]
    assert [(item.kind, item.kind.stored) for item in items] == [(PointInvalidKind.INVALID, 0)]


def test_on_message_uses_module_checker_when_userdata_lacks_one(monkeypatch):
    """接线兜底：userdata 里没给判据时用**模块级**那份（"忘了接线"只少判可疑，不丢数）。"""
    monkeypatch.setattr(sub, "QUALITY_CHECKER", sub.QualityChecker(quality_config()))
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    data = userdata(writer, point_invalid_writer=invalid_writer)
    data.pop("quality_checker")
    sub.on_message(None, data, FakeMessage(build_payload()))
    sub.on_message(None, data, FakeMessage(build_payload({**GOOD_VALUES, "Dust": 60.0})))
    assert writer.calls[1][1]["Dust"] == 60.0
    assert [item.kind for _ts, items in invalid_writer.calls for item in items] == [
        PointInvalidKind.JUMP
    ]


def test_on_message_quality_check_failure_does_not_block_ingest():
    """判据抛异常**绝不能**影响入库（它只是加一笔痕）。"""

    class BrokenChecker:
        def check(self, values):                       # noqa: D401 - 故意抛
            raise RuntimeError("boom")

    writer = FakeWriter()
    sub.on_message(
        None, userdata(writer, quality_checker=BrokenChecker()),
        FakeMessage(build_payload()),
    )
    assert len(writer.calls) == 1 and writer.calls[0][1] == GOOD_VALUES


def test_on_message_disabled_checker_produces_no_jump_or_frozen_rows():
    """W5 的接线断言：判据关闭 + 跳变报文 ⇒ 事件表**没有** jump/frozen，主表照常有值。"""
    writer = FakeWriter()
    invalid_writer = FakePointInvalidWriter()
    checker = sub.QualityChecker(quality_config(enable=False))
    data = userdata(writer, point_invalid_writer=invalid_writer, quality_checker=checker)
    sub.on_message(None, data, FakeMessage(build_payload()))
    sub.on_message(None, data, FakeMessage(build_payload({**GOOD_VALUES, "Dust": 60.0})))
    kinds = [item.kind for _ts, items in invalid_writer.calls for item in items]
    assert PointInvalidKind.JUMP not in kinds and PointInvalidKind.FROZEN not in kinds
    assert writer.calls[1][1]["Dust"] == 60.0


# ---- 14.5 事件表 stored 列（建表 + 老库 ALTER 升级） ----

def test_point_invalid_insert_sql_marks_suspect_rows_stored_1():
    """可疑证据行必须是 `stored=1`（人可读解释在 `reason`，`kind` 保持短名）。"""
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    for kind in (PointInvalidKind.JUMP, PointInvalidKind.FROZEN):
        sql = tables.insert_sql(TS, PointInvalid("Dust", kind, "可疑（值已入库）", 60.0))
        assert f"'{kind.value}', '可疑（值已入库）', 60.0, 1)" in sql


def test_create_sql_and_alter_sql_both_carry_stored_column():
    """⚠️ ADR-0010 §6①：**两条都要有** —— 建表带列（新建），ALTER 补列（老库升级）。"""
    tables = sub.PointInvalidTables(db="cems", plant="plant1", device="device1")
    assert "stored TINYINT" in tables.create_sql()
    assert tables.alter_stored_column_sql() == (
        "ALTER STABLE cems.cems_point_invalid ADD COLUMN stored TINYINT"
    )


class DescribeCursor:
    """假游标：`DESCRIBE` 返回预设列清单，其余 SQL 记进 `sqls`；可让 ALTER 抛错。

    `alter_error`：ALTER 时抛出的异常文本。
    `alter_adds_column`：ALTER 抛错后列是否"其实已经有了"（模拟并发 DDL / 重复 ALTER）。
    """

    def __init__(self, columns: list[str], alter_error: Optional[str] = None,
                 alter_adds_column: bool = False) -> None:
        self.columns = list(columns)
        self.alter_error = alter_error
        self.alter_adds_column = alter_adds_column
        self.sqls: list[str] = []

    def execute(self, sql: str) -> None:
        self.sqls.append(sql)
        head = sql.strip().lower()
        if head.startswith("alter"):
            if self.alter_error is not None:
                if self.alter_adds_column and "stored" not in self.columns:
                    self.columns.append("stored")
                raise RuntimeError(self.alter_error)
            if "stored" not in self.columns:
                self.columns.append("stored")

    def fetchall(self) -> list[list[Any]]:
        return [[name, "TINYINT" if name == "stored" else "NCHAR(16)"] for name in self.columns]


def test_ensure_stored_column_alters_when_missing():
    cursor = DescribeCursor(["ts", "point", "kind", "reason", "raw"])
    assert sub.ensure_stored_column(cursor, sub.POINT_INVALID_TABLES) is True
    assert cursor.sqls[-1] == sub.POINT_INVALID_TABLES.alter_stored_column_sql()


def test_ensure_stored_column_is_idempotent_when_present():
    """列已在 ⇒ 不执行 ALTER（幂等，可重复 connect）。"""
    cursor = DescribeCursor(["ts", "point", "kind", "reason", "raw", "stored"])
    assert sub.ensure_stored_column(cursor, sub.POINT_INVALID_TABLES) is False
    assert not any(sql.lower().startswith("alter") for sql in cursor.sqls)


def test_ensure_stored_column_tolerates_already_exists_error():
    """⚠️ 硬约束：ALTER 报"列已存在"**不能崩** —— 复核后确实有该列就算成功。"""
    cursor = DescribeCursor(
        ["ts", "point", "kind", "reason", "raw"],
        alter_error="Column already exists", alter_adds_column=True,
    )
    assert sub.ensure_stored_column(cursor, sub.POINT_INVALID_TABLES) is False


def test_ensure_stored_column_reraises_when_column_really_missing():
    """ALTER 失败且复核后**确实没有**该列 ⇒ 真失败，必须抛（别静默假装升级好了）。"""
    cursor = DescribeCursor(
        ["ts", "point", "kind", "reason", "raw"],
        alter_error="permission denied", alter_adds_column=False,
    )
    with pytest.raises(RuntimeError):
        sub.ensure_stored_column(cursor, sub.POINT_INVALID_TABLES)


class FakeConnection:
    """假 taosrest 连接：只提供 cursor()/close()。"""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    def cursor(self) -> Any:
        return self._cursor

    def close(self) -> None:
        pass


def test_point_invalid_writer_connect_creates_table_then_alters(monkeypatch):
    """`connect()` 必须**两条都做**：建表（带 stored）+ 补列 —— 否则老库永远缺 `stored`。"""
    cursor = DescribeCursor(["ts", "point", "kind", "reason", "raw"])
    monkeypatch.setattr(
        sub.taosrest, "connect", lambda **kwargs: FakeConnection(cursor)
    )
    writer = sub.PointInvalidWriter(sub.PointInvalidTables(db="cems", plant="p", device="d"))
    assert writer.connect() is True
    assert "stored TINYINT" in cursor.sqls[0]                       # 新建路径带列
    assert [sql for sql in cursor.sqls if sql.lower().startswith("alter")] == [
        sub.POINT_INVALID_TABLES.alter_stored_column_sql()
    ]

