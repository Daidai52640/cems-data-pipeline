# -*- coding: utf-8 -*-
"""`src/common/points.py` **线格式**（dtype / endian / addr_notation）的离线单元测试。

============================ 这一组测试在证什么 ============================
被测对象：2026-10-07 新增的 3 个契约字段 + 2 个纯函数 `decode_register` / `encode_register`。

真机依据（RS485 温湿度变送器手册）：**"16bits 数据高字节在前"**；手册两个例子：
    湿度：地址 0、uint16、AB、÷10     01 E6 →  486 →  48.6
    温度：地址 1、**int16**、AB、÷10  FF 9F →  -97 →  -9.7   ← 负值，旧契约根本表达不了

⚠️ 本文件刻意**不追求"跑得通"**，而是**发现偏差**，所以重点钉在三处：
    1. **符号性**：int16 的 -97 / -32768 必须原样出来，且 32768 必须**抛错而不是回绕**
       （回绕出的数是"看起来合法"的数，最难查）。
    2. **字节序**：同一串字节按 AB 与 BA 必须给出**不同的数**（否则等于没测到字节序）；
       32 位的 ABCD/CDAB/BADC/DCBA 四种排列全部单独立项。
    3. **零行为变化**：9 个既有测点仍是 uint16 + AB + "0-based"，字段默认值 = 现状。

边界（长度不符 / 非法 dtype / 非法 endian / 宽度与 dtype 不匹配 / 整数 dtype 收小数）
全部**要求抛 ValueError**，不留静默兜底 —— 猜错的字节序不会报错，只会读成另一个数。
===========================================================================
"""

from __future__ import annotations

import pytest

from src.common.points import (
    ADDR_NOTATION_BASE,
    ADDR_NOTATIONS,
    DTYPES,
    ENDIANS,
    FLOAT32_EXACT_INT_MAX,
    POINTS,
    REG_BASE,
    REG_INT_MAX,
    REG_INT_MAX_16_SIGNED,
    REG_INT_MIN_16_SIGNED,
    REG_INT_RANGES,
    SCALES,
    WIRE_BYTE_ORDER,
    Point,
    _check_point_register_bounds,
    decode_register,
    encode_register,
)

# 手册两个例子的字节（原样照抄，不做任何换算）
HUMIDITY_MANUAL_BYTES = bytes.fromhex("01E6")   # → 486 → 48.6
TEMPERATURE_MANUAL_BYTES = bytes.fromhex("FF9F")  # → -97 → -9.7

#: 32 位四种字节序下，1.5 的 IEEE-754（ABCD = 3F C0 00 00）应该是这些字节
FLOAT32_1_5_WIRE = {
    "ABCD": bytes.fromhex("3FC00000"),
    "CDAB": bytes.fromhex("00003FC0"),
    "BADC": bytes.fromhex("C03F0000"),
    "DCBA": bytes.fromhex("0000C03F"),
}

FOUR_BYTE_ENDIANS = ("ABCD", "CDAB", "BADC", "DCBA")


# ===========================================================================
# 1. 零行为变化：既有 9 个测点的线格式必须是"默认值 = 现状"
# ===========================================================================

def test_existing_nine_points_keep_default_wire_format():
    """9 个测点全部 uint16 + AB + "0-based"：加了字段，但一个读数路径都没动。"""
    assert len(POINTS) == 9
    assert set(DTYPES.values()) == {"uint16"}
    assert set(ENDIANS.values()) == {"AB"}
    assert set(ADDR_NOTATIONS.values()) == {"0-based"}
    for point in POINTS:
        assert point.dtype == "uint16"
        assert point.endian == "AB"
        assert point.addr_notation == "0-based"


def test_existing_address_scale_and_names_unchanged():
    """地址、列名、scale 这三样是老契约的命根子，本次改动不许碰。"""
    assert [point.address for point in POINTS] == list(range(9))
    assert [point.name for point in POINTS] == [
        "Flow", "Dust", "SO2", "NOx", "O2", "Velocity", "Temp", "Humidity", "Pressure",
    ]
    assert SCALES["Flow"] == 1 and SCALES["Dust"] == 100 and SCALES["SO2"] == 10


def test_point_default_field_values_are_wire_format_defaults():
    """只给 8 个必填参数时，线格式字段取默认值（老代码的构造方式必须照旧可用）。"""
    point = Point("Temp", 1, "temp", -50.0, 150.0, "degC", 150.0, "a01012")
    assert point.scale == 10
    assert point.dtype == "uint16"
    assert point.endian == "AB"
    assert point.addr_notation == "0-based"


def test_uint16_register_max_constant_unchanged():
    """REG_INT_MAX 仍是 16 位无符号上限 65535（有消费者，不许改值）。"""
    assert REG_INT_MAX == 65535
    assert REG_INT_MAX == REG_INT_RANGES["uint16"][1]


# ===========================================================================
# 2. 手册两个例子：uint16 正数 + int16 负数
# ===========================================================================

def test_manual_humidity_uint16_ab_01e6_is_48_6():
    """湿度：01 E6 → 486 → ÷10 = 48.6（手册例子，uint16 + AB）。"""
    raw = decode_register(HUMIDITY_MANUAL_BYTES, "uint16", "AB")
    assert raw == 486
    assert raw / 10 == 48.6
    assert encode_register(486, "uint16", "AB") == HUMIDITY_MANUAL_BYTES


def test_manual_temperature_int16_ab_ff9f_is_minus_9_7():
    """温度：FF 9F → **-97** → ÷10 = **-9.7**（手册例子，int16 + AB，负值）。"""
    raw = decode_register(TEMPERATURE_MANUAL_BYTES, "int16", "AB")
    assert raw == -97.0
    assert raw / 10 == -9.7
    assert encode_register(-97, "int16", "AB") == TEMPERATURE_MANUAL_BYTES


def test_same_bytes_uint16_versus_int16_differ():
    """同两字节：uint16 读 65439，int16 读 -97 —— 符号性必须由 dtype 决定，不是猜出来的。"""
    assert decode_register(TEMPERATURE_MANUAL_BYTES, "uint16", "AB") == 65439
    assert decode_register(TEMPERATURE_MANUAL_BYTES, "int16", "AB") == -97.0


def test_decode_returns_float_for_every_dtype():
    """整数 dtype 也返回 float：四种 dtype 返回类型一致，调用方不必分支。"""
    values = [
        decode_register(HUMIDITY_MANUAL_BYTES, "uint16", "AB"),
        decode_register(TEMPERATURE_MANUAL_BYTES, "int16", "AB"),
        decode_register(b"\x00\x00\x00\x01", "int32", "ABCD"),
        decode_register(b"\x3f\xc0\x00\x00", "float32", "ABCD"),
    ]
    assert all(isinstance(value, float) for value in values)


# ===========================================================================
# 3. 字节序：AB ↔ BA 互转，且必须"能区分"
# ===========================================================================

def test_ab_and_ba_are_byte_reversed():
    """AB 与 BA 就是彼此的反序（同值、不同线上形态）。"""
    assert encode_register(486, "uint16", "AB") == b"\x01\xe6"
    assert encode_register(486, "uint16", "BA") == b"\xe6\x01"
    assert encode_register(486, "uint16", "BA") == encode_register(486, "uint16", "AB")[::-1]


def test_endian_choice_actually_changes_the_reading():
    """鉴别力：若把 endian 当摆设（两种都返回同一个数），本用例必须失败。"""
    assert decode_register(HUMIDITY_MANUAL_BYTES, "uint16", "AB") == 486
    assert decode_register(HUMIDITY_MANUAL_BYTES, "uint16", "BA") == 0xE601 == 58881
    assert decode_register(TEMPERATURE_MANUAL_BYTES, "int16", "BA") == -24577


@pytest.mark.parametrize(
    ("dtype", "endian", "value"),
    [
        # 16 位：AB / BA × uint16 / int16（含两种符号的边界）
        ("uint16", "AB", 486),
        ("uint16", "BA", 486),
        ("uint16", "AB", 0),
        ("uint16", "BA", 65535),
        ("int16", "AB", -97),
        ("int16", "BA", -97),
        ("int16", "AB", -32768),
        ("int16", "BA", 32767),
        # 32 位：四种字节序 × 有/无符号
        ("int32", "ABCD", -2147483648),
        ("int32", "CDAB", 123456789),
        ("int32", "BADC", -1),
        ("uint32", "DCBA", 0),
        ("uint32", "BADC", 4294967295),
        ("uint32", "ABCD", 4000000000),
        ("uint32", "CDAB", 1),
    ],
)
def test_encode_decode_roundtrip_all_layouts(dtype, endian, value):
    """所有 (dtype, endian) 组合往返一致，且字节数恰好等于 dtype 宽度。"""
    wire = encode_register(value, dtype, endian)
    assert len(wire) == (2 if dtype.endswith("16") else 4)
    assert decode_register(wire, dtype, endian) == value


def test_word_order_layout_relates_16bit_and_32bit():
    """CDAB 的 32 位里，低字在前：低字（01 E6）应与 16 位 AB 的字节完全一致。

    这条把"16 位字节序"和"32 位字序"钉在一起 —— 两套映射各写一份就必然漂。
    """
    assert encode_register(486, "uint32", "ABCD") == bytes.fromhex("000001E6")
    assert encode_register(486, "uint32", "CDAB") == encode_register(486, "uint16", "AB") + b"\x00\x00"
    assert encode_register(486, "uint32", "DCBA") == bytes.fromhex("E6010000")


def test_wire_byte_order_table_is_self_inverse():
    """表中每个排列作用于自身回到原位 —— 编解码共用一张表的前提（见 points.py 注释）。"""
    for order in WIRE_BYTE_ORDER.values():
        twice = tuple(order[index] for index in order)
        assert twice == tuple(range(len(order)))


# ===========================================================================
# 4. 范围：signed / unsigned 必须分开，且**超范围抛错不回绕**
# ===========================================================================

def test_register_int_ranges_distinguish_signed_from_unsigned():
    """自检的核心：signed 16 位是 -32768~32767，**不是** 0~65535。"""
    assert REG_INT_RANGES["uint16"] == (0, 65535)
    assert REG_INT_RANGES["int16"] == (-32768, 32767)
    assert REG_INT_MIN_16_SIGNED == -32768
    assert REG_INT_MAX_16_SIGNED == 32767
    # 拿 REG_INT_MAX 当 int16 的上限就是漏检：32768~65535 编不进 int16
    assert REG_INT_MAX_16_SIGNED < REG_INT_MAX
    assert REG_INT_RANGES["int32"] == (-2147483648, 2147483647)
    assert REG_INT_RANGES["uint32"] == (0, 4294967295)
    assert FLOAT32_EXACT_INT_MAX == 1 << 24


@pytest.mark.parametrize(
    ("dtype", "value"),
    [
        ("uint16", -1),        # 无符号不收负值
        ("uint16", 65536),     # 上溢 1
        ("int16", 32768),      # ⭐ signed 上限之外（旧自检会漏检的那一段）
        ("int16", 40000),      # 手册设备常见误配：温度按 uint16 编到 400.0 就会落在这里
        ("int16", -32769),
        ("int32", 2147483648),
        ("int32", -2147483649),
        ("uint32", 4294967296),
        ("uint32", -1),
    ],
)
def test_encode_rejects_out_of_range_without_wraparound(dtype, value):
    """超范围**抛 ValueError**，绝不回绕（回绕出的数看着完全合法，最难查）。"""
    with pytest.raises(ValueError, match="超出"):
        encode_register(value, dtype, "AB" if dtype.endswith("16") else "ABCD")


@pytest.mark.parametrize(
    ("dtype", "value"),
    [("uint16", 0), ("uint16", 65535), ("int16", -32768), ("int16", 32767),
     ("int32", -2147483648), ("uint32", 4294967295)],
)
def test_encode_accepts_both_range_edges(dtype, value):
    """边界值本身必须收得下（上下界都是闭区间）——否则等于把量程边界切掉。"""
    endian = "AB" if dtype.endswith("16") else "ABCD"
    assert decode_register(encode_register(value, dtype, endian), dtype, endian) == value


@pytest.mark.parametrize("value", [486.5, -9.7, float("nan")])
def test_integer_dtype_rejects_fractional_value(value):
    """整数 dtype 收小数就抛错：取整是调用方的事（先乘 scale），本函数不替它决定舍还是入。"""
    with pytest.raises(ValueError):
        encode_register(value, "int16", "AB")


def test_contract_points_all_fit_their_dtype_bounds():
    """独立复算一遍契约不变式：9 个测点的 量程 × scale 都装得进各自的 dtype。"""
    for point in POINTS:
        low_bound, high_bound = REG_INT_RANGES[point.dtype]
        assert point.low * point.scale >= low_bound, point.name
        assert point.high * point.scale <= high_bound, point.name
        assert _check_point_register_bounds(point) is None


def test_selfcheck_flags_int16_overflow_and_negative_underflow():
    """自检引擎在有符号字段上真的会炸：这正是"区分 signed/unsigned"的可执行证据。

    若自检仍拿 65535 当上限（旧写法），第一条 int16 用例就会**静默通过** → 本用例失败。
    """
    overflow = Point("T", 1, "t", 0.0, 4000.0, "degC", 4000.0, "a01012", 10, "int16", "AB")
    message = _check_point_register_bounds(overflow)
    assert message is not None and "32767" in message

    # -5000 Pa × scale 10 = -50000，低于 int16 下限 -32768 → 必须报；换 uint16 同样要报
    underflow = Point("P", 2, "p", -5000.0, 0.0, "Pa", 0.0, "a01013", 10, "int16", "AB")
    message = _check_point_register_bounds(underflow)
    assert message is not None and "-32768" in message

    # 同样量程换成 float32 也不再受限（float 没有整数溢出，只受精度上限约束）
    as_float = Point("T", 1, "t", 0.0, 4000.0, "degC", 4000.0, "a01012", 10, "float32", "ABCD")
    assert _check_point_register_bounds(as_float) is None
    too_precise = Point("F", 1, "f", 0.0, 1e7, "m3/h", 1e7, "a00000", 10, "float32", "ABCD")
    message = _check_point_register_bounds(too_precise)
    assert message is not None and str(FLOAT32_EXACT_INT_MAX) in message


# ===========================================================================
# 5. float32：ABCD / CDAB / BADC / DCBA 四种字节序
# ===========================================================================

@pytest.mark.parametrize("endian", FOUR_BYTE_ENDIANS)
def test_float32_decodes_all_four_layouts(endian):
    """同一台设备的 1.5，四种字序都要能读出来（字序是厂家自由的，猜不得）。"""
    assert decode_register(FLOAT32_1_5_WIRE[endian], "float32", endian) == 1.5


@pytest.mark.parametrize("endian", FOUR_BYTE_ENDIANS)
def test_float32_encodes_all_four_layouts(endian):
    """反方向也要对：1.5 编出来的字节就是该字序下的确定位模式。"""
    assert encode_register(1.5, "float32", endian) == FLOAT32_1_5_WIRE[endian]


@pytest.mark.parametrize("endian", FOUR_BYTE_ENDIANS)
def test_float32_negative_roundtrip_all_layouts(endian):
    """温度那类**负的浮点**（-9.7）四种字序往返一致（float32 存不下精确的 9.7，故用 approx）。"""
    wire = encode_register(-9.7, "float32", endian)
    assert len(wire) == 4
    assert decode_register(wire, "float32", endian) == pytest.approx(-9.7, rel=1e-6)


def test_float32_four_layouts_are_four_different_byte_strings():
    """鉴别力：四种字序若被实现成同一个映射，本用例失败。"""
    wire_forms = set(FLOAT32_1_5_WIRE.values())
    assert len(wire_forms) == 4
    assert len({bytes(wire) for wire in wire_forms}) == 4


def test_float32_overflow_raises_value_error():
    """float32 溢出抛 ValueError，不让 struct 的 OverflowError 漏到调用方。"""
    with pytest.raises(ValueError):
        encode_register(1e39, "float32", "ABCD")


# ===========================================================================
# 6. 非法输入：一律抛 ValueError，不静默兜底
# ===========================================================================

@pytest.mark.parametrize(
    ("raw", "dtype", "endian"),
    [
        (b"\x01", "uint16", "AB"),        # 短 1 字节
        (b"\x01\xe6\x00", "uint16", "AB"),  # 长 1 字节（不做截断）
        (b"", "uint16", "AB"),
        (b"\x01\xe6", "float32", "ABCD"),   # 宽度不足
        (b"\x01\xe6\x00\x00\x00", "float32", "ABCD"),
    ],
)
def test_decode_rejects_wrong_byte_length(raw, dtype, endian):
    """长度必须**恰好**等于 dtype 宽度：不补零、不截断（补出来的数看不出错）。"""
    with pytest.raises(ValueError):
        decode_register(raw, dtype, endian)


@pytest.mark.parametrize(
    ("dtype", "endian"),
    [
        ("uint8", "AB"),        # 不支持的 dtype
        ("float64", "ABCD"),
        ("uint16", "ABCD"),     # 16 位 dtype 配 32 位字序
        ("int32", "AB"),        # ⭐ 32 位 dtype 用了 16 位的默认 AB → 必须炸，不许静默当 ABCD
        ("int32", "BA"),
        ("float32", "XYZW"),    # 不认识的 endian
        ("uint16", "A"),
    ],
)
def test_illegal_dtype_endian_combination_raises(dtype, endian):
    """非法组合当场抛错：32 位 dtype 忘了选 4 字母字序是最容易犯的错。"""
    with pytest.raises(ValueError):
        decode_register(b"\x00\x00\x00\x00", dtype, endian)
    with pytest.raises(ValueError):
        encode_register(1, dtype, endian)


# ===========================================================================
# 7. 地址记法：addr_notation 与 REG_BASE 的关系（两者不是一回事）
# ===========================================================================

def test_addr_notation_values_and_bases():
    """三种记法各有基准：0-based→0、40001-style→40001、1-based→1。"""
    assert set(ADDR_NOTATION_BASE) == {"0-based", "40001-style", "1-based"}
    assert ADDR_NOTATION_BASE["0-based"] == 0
    assert ADDR_NOTATION_BASE["40001-style"] == 40001
    assert ADDR_NOTATION_BASE["1-based"] == 1
    assert set(ADDR_NOTATIONS.values()) == {"0-based"}


def test_40001_style_address_derives_same_raw_address_as_0_based():
    """记法只改"字段怎么读"，不改硬件：40001-style 的 40001 与 0-based 的 0 是同一个寄存器。

    同时印证 REG_BASE：本项目 9 个测点都是 0-based，故 REG_BASE(=0) 就是第一个测点的线上地址。
    """
    humidity = Point("Humidity", 0, "humidity", 0.0, 100.0, "%", 100.0, "a01014", 10)
    manual_style = Point(
        "Humidity", 40001, "humidity", 0.0, 100.0, "%", 100.0, "a01014", 10,
        "uint16", "AB", "40001-style",
    )

    def raw_address(point: Point) -> int:
        return point.address - ADDR_NOTATION_BASE[point.addr_notation]

    assert raw_address(humidity) == raw_address(manual_style) == REG_BASE == 0


# ===========================================================================
# 8. 变送器样例：湿度 + 温度两个测点，端到端"手册字节 → 工程值"
# ===========================================================================

def build_transmitter_points() -> tuple[Point, ...]:
    """按 RS485 温湿度变送器手册建两个测点：湿度（uint16）/ 温度（**int16**）。"""
    return (
        Point("Humidity", 0, "humidity", 0.0, 100.0, "%", 100.0, "a01014", 10,
              "uint16", "AB", "0-based"),
        Point("Temp", 1, "temp", -40.0, 125.0, "degC", 125.0, "a01012", 10,
              "int16", "AB", "0-based"),
    )


def test_transmitter_sample_humidity_and_temperature_end_to_end():
    """真机样例端到端：FF 9F 这个**负温度**在旧契约下无法表达，现在可以。"""
    humidity, temperature = build_transmitter_points()

    humidity_raw = decode_register(HUMIDITY_MANUAL_BYTES, humidity.dtype, humidity.endian)
    assert humidity_raw / humidity.scale == 48.6

    temp_raw = decode_register(TEMPERATURE_MANUAL_BYTES, temperature.dtype, temperature.endian)
    assert temp_raw / temperature.scale == -9.7
    assert temperature.dtype == "int16"        # 符号性是显式写在契约里的，不是靠约定
    assert temperature.low < 0 <= temperature.high


def test_transmitter_sample_points_pass_contract_selfcheck():
    """样例测点本身要过契约自检：-40~125 degC ×10 = -400~1250，int16 装得下。"""
    for point in build_transmitter_points():
        assert _check_point_register_bounds(point) is None


def test_transmitter_sample_simulated_write_read_roundtrip():
    """模拟一次"写寄存器→读寄存器"：-9.7 → -97 → 字节 → 读回 -97 → -9.7。"""
    _, temperature = build_transmitter_points()
    register_value = int(round(-9.7 * temperature.scale))
    wire = encode_register(register_value, temperature.dtype, temperature.endian)
    assert wire == TEMPERATURE_MANUAL_BYTES
    assert decode_register(wire, temperature.dtype, temperature.endian) / temperature.scale == -9.7
