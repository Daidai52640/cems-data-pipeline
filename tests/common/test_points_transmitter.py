# -*- coding: utf-8 -*-
"""`src/common/points_transmitter.py` 的离线单元测试 —— RS485 温湿度变送器点位契约。

============================ 这一组测试在证什么 ============================
被测对象：**一台真实设备**的点位契约（2 个测点）+ 它的导入期自检 + 两个纯解码函数。

真机依据（`docs/runbooks/真机接入.md` S2/S3，均贴原始收发字节）：
    01 03 04 02 31 01 11 6B D8  → 湿 561 → 56.1 %RH；温 273 → 27.3 degC
    01 03 04 00 00 00 00 FA 33  → 读不存在的地址返回**全 0**（不报异常）
    手册原例：01 03 04 01 E6 FF 9F 1B A0 → 48.6 %RH / **-9.7 degC**（温度有符号）

⚠️ 本文件刻意**不追求"跑得通"**，而是**发现偏差**，所以重点钉在五处：
    1. **实测样本逐字节复现**：`02 31`→561→56.1、`01 11`→273→27.3（不是自造的往返）。
    2. **符号性**：`FF 9F`→**-97**→**-9.7**；若温度被写成 `uint16`，同一串字节会变成
       6543.9 degC —— 契约里 dtype 写错**不报错**，只给另一个合法数，故必须钉死。
    3. **手册一致性**：地址 / dtype / endian / scale / 量程 / 单位 逐项对着手册查。
    4. **导入期自检真的会炸**：把源码里温度的 `int16` 改成 `uint16`、把 limit 改成阈值、
       把地址捅出空洞，三种改法都必须在**导入期**抛 ValueError（不是运行期才发现）。
    5. **命名不许与 9 点烟气契约重叠**：两个契约的点名/列名若同名，会被下游按
       `values[point.name]` 静默串线（见 points_transmitter.py 顶部"命名"一节）。

⚠️ 本文件**不测**：RTU 切帧 / CRC / 串口读写 / 量程拒收 / 告警判定（不在本文件职责内）。
===========================================================================
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

import src.common.points as cems
import src.common.points_transmitter as transmitter
from src.common.points import Point, decode_register, encode_register
from src.common.points_transmitter import (
    ADDR_NOTATIONS,
    BAUD_RATE,
    CODES,
    COLUMNS,
    DATA_BITS,
    DATA_BYTES,
    DEVICE_ID,
    DTYPES,
    EMISSION_LIMIT_APPLICABLE,
    ENDIANS,
    EXPECTED_RESPONSE_BYTES,
    FRAME_GAP_MIN_BYTES,
    HJ212_OUTLET_APPLICABLE,
    LIMIT_APPLICABLE,
    LIMITS,
    NAMES,
    PARITY,
    PLC_ADDRESSES,
    POINTS,
    POINTS_BY_NAME,
    RANGES,
    RAW_ADDRESSES,
    READ_ONLY,
    REG_BASE,
    REG_COUNT,
    REGISTER_MAP,
    SCALE,
    SCALES,
    SERIAL_SETTINGS,
    SLAVE_ADDRESS,
    STOP_BITS,
    SUPPORTED_FUNCTION_CODES,
    UNITS,
    decode_point,
    decode_points,
    validate_contract,
)

# ---- 真机原始帧（原样照抄 runbook，不做任何换算）----
#: S2 实测正样本：湿 561 / 温 273
MEASURED_FRAME = bytes.fromhex("01 03 04 02 31 01 11 6B D8")
#: S4 EXP2 实测：读不存在的地址 → 数据段全 0（**不报异常**）
ZERO_FRAME = bytes.fromhex("01 03 04 00 00 00 00 FA 33")
#: 手册原例：湿 486（48.6）/ 温 -97（-9.7，负温）
MANUAL_FRAME = bytes.fromhex("01 03 04 01 E6 FF 9F 1B A0")


def _payload(frame: bytes) -> bytes:
    """从完整的 RTU 应答帧里取出**数据段**（跳过 地址/功能码/字节数，丢掉 CRC）。"""
    return frame[3 : 3 + DATA_BYTES]


# ===========================================================================
# 1. 设备标识 / 通讯参数：必须与 S2 实测一致
# ===========================================================================

def test_device_id_and_serial_params_match_measurement():
    """4800/8/N/1 + 从站 0x01 —— 实测参数，填错任何一个都完全静默（不报错）。"""
    assert DEVICE_ID == "transmitter"
    assert BAUD_RATE == 4800          # ⚠️ 不是 9600（S2 最大的坑）
    assert (DATA_BITS, PARITY, STOP_BITS) == (8, "N", 1)
    assert SERIAL_SETTINGS == "4800/8/N/1"
    assert SLAVE_ADDRESS == 0x01


def test_device_is_read_only_and_only_function_code_03():
    """实测：只支持 03；发 06 被原样回显但值不变 → 契约里不能有写路径。"""
    assert READ_ONLY is True
    assert SUPPORTED_FUNCTION_CODES == (3,)
    assert FRAME_GAP_MIN_BYTES == 4   # 手册"≥4 字节时间"，比标准的 3.5 字符更保守


def test_module_docstring_records_the_evidence():
    """依据必须写在文件里（字节序依据是手册原文，不是我们的推测）。"""
    doc = transmitter.__doc__
    assert doc is not None
    assert "高字节在前" in doc        # 手册原文 → endian="AB" 的唯一依据
    assert "points.py" in doc         # 说清与既有契约的关系
    assert "4800" in doc              # 通讯参数
    assert "limit" in doc             # 语义不适用字段的取舍必须写明


# ===========================================================================
# 2. 两个点位与手册逐项一致（地址 / dtype / endian / scale / 量程 / 单位）
# ===========================================================================

def test_exactly_two_points_in_register_order():
    """本设备只有 2 个寄存器；顺序 = 地址升序（湿度在前，实测应答也是如此）。"""
    assert NAMES == ("AirHumidity", "AirTemp")
    assert len(POINTS) == 2
    assert REG_COUNT == 2
    assert REG_BASE == 0
    assert REGISTER_MAP == (("AirHumidity", 0), ("AirTemp", 1))


def test_humidity_point_matches_manual():
    """湿度：0x0000 / uint16 / AB / ÷10 / % / 0~100。"""
    point = POINTS_BY_NAME["AirHumidity"]
    assert (point.address, point.addr_notation) == (0, "0-based")
    assert point.dtype == "uint16"
    assert point.endian == "AB"
    assert point.scale == 10
    assert (point.low, point.high) == (0.0, 100.0)
    assert point.unit == "%"
    assert point.column == "air_humidity"


def test_temperature_point_matches_manual():
    """温度：0x0001 / **int16** / AB / ÷10 / degC / -40~125。"""
    point = POINTS_BY_NAME["AirTemp"]
    assert (point.address, point.addr_notation) == (1, "0-based")
    assert point.dtype == "int16"      # ⭐ 有符号，见下一条
    assert point.endian == "AB"
    assert point.scale == 10
    assert (point.low, point.high) == (-40.0, 125.0)
    assert point.unit == "degC"
    assert point.column == "air_temp"


def test_plc_address_notation_matches_manual_table():
    """手册"PLC/组态记法"列：40001 / 40002（由 0 基地址推导，不是第二份真源）。"""
    assert RAW_ADDRESSES == {"AirHumidity": 0, "AirTemp": 1}
    assert PLC_ADDRESSES == {"AirHumidity": 40001, "AirTemp": 40002}
    assert PLC_ADDRESSES["AirTemp"] - PLC_ADDRESSES["AirHumidity"] == 1


def test_temperature_must_be_signed_int16():
    """⭐ 鉴别力核心：温度写成 uint16，同一串字节就会变成一个"合法但错"的正数。"""
    assert POINTS_BY_NAME["AirTemp"].dtype == "int16"
    assert decode_point("AirTemp", bytes.fromhex("FF9F")) == -9.7
    # 错配（uint16）下同一串字节：6543.9 degC —— 不报错、不越界、纯错
    assert decode_register(bytes.fromhex("FF9F"), "uint16", "AB") / 10 == 6543.9
    # 湿度反过来必须是 uint16（它不会出现负值）
    assert POINTS_BY_NAME["AirHumidity"].dtype == "uint16"


def test_derived_views_are_consistent_with_points():
    """派生只读视图必须与 POINTS 逐项自洽（各层取用时不再自己推导）。"""
    assert COLUMNS == ("air_humidity", "air_temp")
    assert SCALES == {"AirHumidity": 10, "AirTemp": 10}
    assert UNITS == {"AirHumidity": "%", "AirTemp": "degC"}
    assert DTYPES == {"AirHumidity": "uint16", "AirTemp": "int16"}
    assert ENDIANS == {"AirHumidity": "AB", "AirTemp": "AB"}
    assert ADDR_NOTATIONS == {"AirHumidity": "0-based", "AirTemp": "0-based"}
    assert RANGES["AirTemp"] == (-40.0, 125.0)
    assert LIMITS == {name: RANGES[name][1] for name in NAMES}
    assert CODES == {"AirHumidity": "a01014", "AirTemp": "a01012"}
    assert tuple(POINTS_BY_NAME) == NAMES
    assert REG_COUNT == len(POINTS)
    assert DATA_BYTES == 2 * REG_COUNT == 4


def test_global_scale_is_a_convenience_value_consistent_with_every_point():
    """`SCALE` 与 points.py 同名（设备层按它编码寄存器块），但真源是逐点 scale。"""
    assert SCALE == 10
    assert set(SCALES.values()) == {SCALE}


def test_names_and_columns_do_not_collide_with_cems_contract():
    """⭐ 两台设备的点名/列名不得重叠：下游按 `values[point.name]` 取值，
    同名就是**静默串线**（环境温度被当成烟温写进烟温列）。"""
    assert set(NAMES).isdisjoint(cems.NAMES)
    assert set(COLUMNS).isdisjoint(cems.COLUMNS)
    # 重叠的后果不是"报错"，而是两个契约对同一个键给出不同的量程/限值：
    assert cems.LIMITS["Temp"] == 300.0 != LIMITS["AirTemp"] == 125.0
    assert cems.RANGES["Humidity"] == (0.0, 100.0) == RANGES["AirHumidity"]


# ===========================================================================
# 3. ⭐ 实测样本解码：02 31 → 561 → 56.1；01 11 → 273 → 27.3
# ===========================================================================

def test_measured_sample_humidity_561_is_56_1():
    """S2 实测：数据段首 2 字节 `02 31` = 561 → ÷10 = **56.1 %RH**。"""
    assert decode_register(bytes.fromhex("0231"), "uint16", "AB") == 561
    assert decode_point("AirHumidity", bytes.fromhex("0231")) == 56.1
    assert encode_register(561, "uint16", "AB") == bytes.fromhex("0231")


def test_measured_sample_temperature_273_is_27_3():
    """S2 实测：数据段后 2 字节 `01 11` = 273 → ÷10 = **27.3 degC**。"""
    assert decode_register(bytes.fromhex("0111"), "int16", "AB") == 273
    assert decode_point("AirTemp", bytes.fromhex("0111")) == 27.3
    assert encode_register(273, "int16", "AB") == bytes.fromhex("0111")


def test_measured_full_frame_decodes_end_to_end():
    """整帧端到端：实测 9 字节 → 数据段 → {56.1, 27.3}。"""
    assert len(MEASURED_FRAME) == EXPECTED_RESPONSE_BYTES == 9
    assert _payload(MEASURED_FRAME) == bytes.fromhex("02310111")
    assert decode_points(_payload(MEASURED_FRAME)) == {
        "AirHumidity": 56.1,
        "AirTemp": 27.3,
    }


def test_manual_frame_negative_temperature_end_to_end():
    """手册原例端到端（含**负温**）：48.6 %RH / -9.7 degC。"""
    assert decode_points(_payload(MANUAL_FRAME)) == {
        "AirHumidity": 48.6,
        "AirTemp": -9.7,
    }
    assert decode_register(bytes.fromhex("FF9F"), "int16", "AB") == -97.0
    assert encode_register(-97, "int16", "AB") == bytes.fromhex("FF9F")


def test_measured_zero_frame_is_zeros_and_inside_range():
    """⭐ S4 EXP2 实测：读不存在的地址返回**全 0**（不报异常）——
    而 0 落在两个测点的量程内 → **量程白名单拦不住"地址配错"的假数据**。
    本用例把这个已知边界固化成可执行事实（接入层必须另加跳变/合理性判断）。"""
    values = decode_points(_payload(ZERO_FRAME))
    assert values == {"AirHumidity": 0.0, "AirTemp": 0.0}
    for name, value in values.items():
        low, high = RANGES[name]
        assert low <= value <= high, f"{name} 的 0 竟然越界，那说明量程本身就写错了"


def test_int16_edges_decode_and_are_out_of_range():
    """int16 两端：0x8000=-32768、0x7FFF=32767 —— 都能解出来，但**都超量程**
    （超量程由接入层拒收；本契约只描述，不实现拒收）。"""
    assert decode_point("AirTemp", bytes.fromhex("8000")) == -3276.8
    assert decode_point("AirTemp", bytes.fromhex("7FFF")) == 3276.7
    low, high = RANGES["AirTemp"]
    assert decode_point("AirTemp", bytes.fromhex("8000")) < low
    assert decode_point("AirTemp", bytes.fromhex("7FFF")) > high


def test_uint16_humidity_never_decodes_negative():
    """湿度是无符号：0xFFFF 只能是 6553.5（超量程），不可能出现负值。"""
    assert decode_point("AirHumidity", bytes.fromhex("FFFF")) == 6553.5
    assert decode_point("AirHumidity", bytes.fromhex("0000")) == 0.0


# ===========================================================================
# 4. 纯解码函数的边界：长度不符 / 未知名一律抛错，不静默兜底
# ===========================================================================

@pytest.mark.parametrize("raw", [b"", b"\x02\x31", b"\x02\x31\x01", b"\x02\x31\x01\x11\x6b"])
def test_decode_points_rejects_wrong_length(raw):
    """数据段必须**恰好** 4 字节：截断/补零都会解出"看起来合法"的数。"""
    with pytest.raises(ValueError, match="数据段"):
        decode_points(raw)


def test_decode_point_rejects_unknown_name():
    """点名不在契约里就报 KeyError：不猜、不返回默认值。"""
    with pytest.raises(KeyError):
        decode_point("Humidity", bytes.fromhex("0231"))   # 这是**烟气**契约的名字


def test_decode_point_rejects_wrong_byte_length():
    """宽度不符时由 points.decode_register 兜住（不自动截断/补零）。"""
    with pytest.raises(ValueError):
        decode_point("AirTemp", bytes.fromhex("0111AA"))


# ===========================================================================
# 5. 语义不适用字段：limit（排放限值）与 code（HJ 212 出口）
# ===========================================================================

def test_limit_is_a_placeholder_equal_to_range_upper_bound():
    """本设备无排放限值语义 → limit 按 points.py 的既有约定填量程上限（占位）。"""
    assert LIMITS["AirHumidity"] == 100.0 == RANGES["AirHumidity"][1]
    assert LIMITS["AirTemp"] == 125.0 == RANGES["AirTemp"][1]


def test_limit_is_declared_not_applicable_for_every_point():
    """⭐ "不参与判定"必须是**可查询的事实**，不能只写在注释里。"""
    assert EMISSION_LIMIT_APPLICABLE is False
    assert LIMIT_APPLICABLE == {"AirHumidity": False, "AirTemp": False}
    assert all(value is False for value in LIMIT_APPLICABLE.values())


def test_placeholder_limit_would_misjudge_if_used_for_compliance():
    """把占位 limit 当阈值用的后果：**"超量程"会被判成"超标"**。
    本用例把危害钉死，提醒调用方先查 LIMIT_APPLICABLE。"""
    assert LIMIT_APPLICABLE["AirTemp"] is False
    assert 130.0 > LIMITS["AirTemp"]        # 占位值让 130 degC 看起来"超标"
    assert 130.0 > RANGES["AirTemp"][1]     # 而 130 degC 其实是**超量程**（传感器/接线故障）


def test_no_hj212_outlet_but_codes_are_filled_as_placeholders():
    """本设备是辅助环境量、无环保平台出口；code 只填最接近的既有编码以免空串。"""
    assert HJ212_OUTLET_APPLICABLE is False
    assert set(CODES.values()) == {"a01012", "a01014"}
    assert all(code for code in CODES.values())


# ===========================================================================
# 6. 契约自检：合法配置放行；非法配置在**导入期**就炸
# ===========================================================================

def test_shipped_contract_passes_its_own_selfcheck():
    """随文件发布的契约必须自洽（模块导入本身就会跑一次，这里再显式查一遍）。"""
    validate_contract(POINTS)
    assert transmitter._check_points(POINTS) == ()


@pytest.mark.parametrize(
    ("label", "broken", "expected"),
    [
        (
            "temperature overflow: -40~4000 degC x10 装不进 int16",
            Point("AirTemp", 0, "air_temp", -40.0, 4000.0, "degC", 4000.0, "a01012", 10, "int16", "AB"),
            "32767",
        ),
        (
            "⭐ temperature declared uint16 (low=-40 装不进无符号)",
            Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 125.0, "a01012", 10, "uint16", "AB"),
            "uint16",
        ),
        (
            "limit used as a threshold instead of the range upper bound",
            Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 90.0, "a01012", 10, "int16", "AB"),
            "limit",
        ),
        (
            "scale=0 would divide by zero at decode time",
            Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 125.0, "a01012", 0, "int16", "AB"),
            "必须为正",
        ),
        (
            "per-point scale diverging from the global SCALE convenience value",
            Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 125.0, "a01012", 100, "int16", "AB"),
            "SCALE",
        ),
        (
            "empty platform code",
            Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 125.0, "", 10, "int16", "AB"),
            "code",
        ),
        (
            "illegal wire format: 16-bit dtype with a 32-bit word order",
            Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 125.0, "a01012", 10, "int16", "ABCD"),
            "不匹配",
        ),
        (
            "unknown addr_notation",
            Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 125.0, "a01012", 10, "int16", "AB", "plc-style"),
            "addr_notation",
        ),
    ],
)
def test_selfcheck_rejects_illegal_single_point(label, broken, expected):
    """自检必须拦住每一类非法配置 —— 报错信息里点明是哪一条。"""
    with pytest.raises(ValueError, match=expected):
        validate_contract((broken,))


@pytest.mark.parametrize(
    ("label", "points", "expected"),
    [
        (
            "register hole: 地址不连续（实测空洞返回全 0，不报异常）",
            (
                Point("AirHumidity", 0, "air_humidity", 0.0, 100.0, "%", 100.0, "a01014", 10, "uint16", "AB"),
                Point("AirTemp", 2, "air_temp", -40.0, 125.0, "degC", 125.0, "a01012", 10, "int16", "AB"),
            ),
            "连续",
        ),
        (
            "duplicate point name",
            (
                Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 125.0, "a01012", 10, "int16", "AB"),
                Point("AirTemp", 1, "air_temp2", -40.0, 125.0, "degC", 125.0, "a01012", 10, "int16", "AB"),
            ),
            "name 重复",
        ),
        (
            "duplicate column",
            (
                Point("AirTemp", 0, "air_temp", -40.0, 125.0, "degC", 125.0, "a01012", 10, "int16", "AB"),
                Point("AirHumidity", 1, "air_temp", 0.0, 100.0, "%", 100.0, "a01014", 10, "uint16", "AB"),
            ),
            "column 重复",
        ),
    ],
)
def test_selfcheck_rejects_illegal_point_set(label, points, expected):
    """跨测点的约束（地址连续、点名/列名唯一）同样必须拦住。"""
    with pytest.raises(ValueError, match=expected):
        validate_contract(points)


#: 三种"最像会犯的错"的源码改法：每种都必须在**导入期**就炸，不能等到运行期。
IMPORT_TIME_BREAKS = (
    ("uint16_temperature", r'(Point\("AirTemp".*?)"int16"', r'\1"uint16"', "uint16"),
    ("limit_as_threshold", r'(Point\("AirTemp".*?)125\.0, "a01012"', r'\g<1>90.0, "a01012"', "limit"),
    ("register_hole", r'(Point\("AirTemp",\s*)1', r"\g<1>2", "连续"),
)


@pytest.mark.parametrize(
    ("label", "pattern", "replacement", "expected"),
    IMPORT_TIME_BREAKS,
    ids=[case[0] for case in IMPORT_TIME_BREAKS],
)
def test_import_time_selfcheck_fires_on_broken_source(tmp_path, label, pattern, replacement, expected):
    """⭐ 自检确实挂在**导入期**：改坏源码后 import 就炸（不依赖谁记得去跑测试）。

    ⚠️ 为什么改**源码文本**而不是改内存里的对象：导入期自检的调用点在模块级
       （`validate_contract(POINTS)`），只有真的重新导入一次才算验证到它。
    """
    source = Path(transmitter.__file__).read_text(encoding="utf-8")
    broken, count = re.subn(pattern, replacement, source, count=1, flags=re.DOTALL)
    assert count == 1, f"锚点未命中，本用例会假绿：{label}"
    assert broken != source

    broken_path = tmp_path / "points_transmitter_broken.py"
    broken_path.write_text(broken, encoding="utf-8")
    spec = importlib.util.spec_from_file_location("points_transmitter_broken", broken_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)

    with pytest.raises(ValueError, match=expected):
        spec.loader.exec_module(module)
