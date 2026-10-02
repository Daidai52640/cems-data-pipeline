"""HJ 212—2025 编解码 + SM4 加解密测试。

**官方向量优先**：``test_vectors.json`` 里的每一条都从标准 PDF 逐字提取，
并标注了页码出处。测试只用 ``tmp_path``（pytest 自带），
**不在宿主机写大批文件**（见 ``docs/runbooks/故障台账.md`` 故障 4）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.protocol.hj212 import (
    CRCError,
    FACTORS,
    LOOPBACK_ENCRYPTED,
    OFFICIAL_TEST_KEY,
    PLAIN,
    DecodedPacket,
    Hj212Profile,
    NeedsDecryption,
    Packet,
    ProfileError,
    StructuralError,
    build_flag,
    crc16,
    crc16_hex,
    crc16_verify,
    decode_packet,
    encode_packet,
    encode_with_profile,
    factor_of,
    make_qn,
    parse_flag,
    segment_length_of,
)
from src.protocol.hj212.codec import MAX_DATA_SEGMENT_LENGTH
from src.protocol.hj212.crypto import (
    CryptoError,
    Sm4EcbNoPadding,
    decrypt_region,
    encrypt_region,
    format_hex,
    looks_encrypted,
    parse_hex,
    split_data_segment,
)
from src.protocol.hj212.flags import spec_example_consistency

VECTORS = json.loads(
    (Path(__file__).parent / "test_vectors.json").read_text(encoding="utf-8")
)


# ===========================================================================
# 1. 官方向量：ANSI CRC16（附录 A.1，PDF 第 32 页）
# ===========================================================================


def test_crc16_official_example_a1():
    """附录 A.1 的官方 CRC 示例：声明长度 == 数据段长度 == 87，CRC == 2200。"""
    vec = VECTORS["crc_example_a1"]
    expected = vec["expected"]

    decoded = decode_packet(vec["packet"])

    assert decoded.crc_hex == expected["crc_hex"] == "2200"
    assert decoded.crc_computed == expected["crc_computed"] == "2200"
    assert decoded.crc_ok is True
    assert decoded.declared_length == expected["declared_length"] == 87
    assert decoded.length_matches is True

    # 报文总长 97 = 2(包头) + 4(长度) + 87(数据段) + 4(CRC)
    assert len(vec["packet"]) == 2 + 4 + 87 + 4 == 97
    assert expected["data_segment_length"] == 87


def test_crc16_official_example_a1_header_fields():
    """附录 A.1 示例的头部字段逐项一致（表 3 的字段名与长度）。"""
    vec = VECTORS["crc_example_a1"]
    packet = decode_packet(vec["packet"]).packet

    assert packet.qn == "20240601085857223"
    assert packet.st == "32"
    assert packet.cn == "1011"
    assert packet.pw == "123456"
    assert packet.mn == "010000A8900016F000169DC0"
    assert packet.flag == 9
    assert packet.region == ""
    # 数据区是占位符 &&2200，故 CP=&& 之后紧跟 &&，再跟真实 CRC
    assert vec["packet"].endswith("CP=&&&&2200")


def test_length_field_excludes_crc():
    """长度字段口径：**数据段的字符数，不含末尾 4 位 CRC**。

    两个官方示例交叉印证：
    * 附录 A.1：97 = 2 + 4 + 87 + 4，长度字段写 ``0087``；
    * 附录 A.2 加密示例：长度字段 ``0295`` = 80(头部) + 5 + 208(密文) + 2。
    """
    vec = VECTORS["table3_flag9_reference_packet"]
    packet = vec["packet"]
    declared = int(packet[2:6])

    assert declared == vec["expected"]["declared_length"]
    assert declared == len(packet) - 2 - 4 - 4          # 去包头、长度字段、CRC
    assert declared == len(vec["expected"]["data_segment"])
    # 数据段 + CRC 才是包头之后的全部字符
    assert packet == "##" + f"{declared:04d}" + vec["expected"]["segment_with_crc"]


def test_crc16_official_example_a1_segment_length_matches_declared():
    """附录 A.1 示例的物理长度与 CRC 自洽（长度字段口径见专门测试）。

    ``packet[6:6+87]`` 是被校验的数据段，``packet[6+87:6+91]`` 是 CRC 字段。
    """
    vec = VECTORS["crc_example_a1"]
    packet = vec["packet"]
    assert len(packet) == 97                              # 2 包头 + 4 长度 + 91
    checked, crc_field = packet[6:93], packet[93:97]
    assert len(checked) == 87
    assert crc_field == "2200"
    assert crc16_hex(checked.encode()) == "2200"


def test_crc16_known_reference_values():
    """CRC 基础性质 + 官方示例数据段的完整值（回归锚点）。

    ⚠️ 注意：本标准的 "ANSI CRC16" 是"初值 0xFFFF、多项式 0xA001、字节先与
    寄存器**高字节**异或（C 代码里的 ``crc_reg = (crc_reg >> 8) ^ puchMsg[i]``）
    的变体，**不是**常见的 CRC-16/ARC（后者把字节异或到整个寄存器，check 值
    为 0xBB3D）。判定依据是附录 A.1 的官方示例必须复现出 ``2200``。
    """
    assert crc16(b"") == 0xFFFF
    assert crc16(b"\x00") == 0x4040

    # 官方示例数据段（附录 A.1）→ 必须等于原文给出的 2200
    a1 = VECTORS["crc_example_a1"]
    assert crc16_hex(a1["expected"]["data_segment"].encode()) == "2200"
    assert crc16(a1["expected"]["data_segment"].encode()) == 0x2200
    assert crc16_verify(a1["expected"]["data_segment"].encode(), "2200") is True
    assert crc16_verify(a1["expected"]["data_segment"].encode(), "2201") is False
    assert crc16_verify(a1["expected"]["data_segment"].encode(), "22") is False


# ===========================================================================
# 2. 官方向量：Flag=9 位分解（§6.3.3 表 3，PDF 第 9 页）
# ===========================================================================


def test_flag9_matches_standard_example():
    """Flag=9 → 00001001，版本号为本次修订、需应答、不拆包。"""
    vec = VECTORS["flag9_bit_layout"]

    assert vec["flag"] == 9
    assert vec["binary"] == "00001001"

    bits = parse_flag(9)
    assert bits.to_binary() == "00001001"
    assert bits.version_value == vec["expected"]["version_value"] == 2
    assert f"{bits.version_value:04b}" == vec["expected"]["version_bits"] == "0010"
    assert bits.has_pagination is False
    assert bits.needs_response is True
    assert bits.version_name == "HJ 212—2025（本次修订版）"


def test_flag9_layout_is_self_consistent_with_standard_text():
    """原文位序描述与它自己的示例：本模块布局下两者一致（可断言的事实）。"""
    ok, message = spec_example_consistency()
    assert ok, message


def test_flag_bit_layout_backed_by_appendix_c_histogram():
    """附录 C 全部报文示例只出现 8/9/10/11，且 A 位与 PNUM/PNO 一一对应。"""
    hist = VECTORS["flag9_bit_layout"]["appendix_c_histogram"]
    assert set(hist) == {"8", "9", "10", "11"}

    for value_text, info in hist.items():
        value = int(value_text)
        bits = parse_flag(value)
        assert bits.to_binary() == info["binary"]
        assert bits.version_value == 2, "所有示例的版本号都应是本次修订版"
        assert bits.has_pagination is (info["with_pnum_pno"] > 0)
        # 8 = 版本+不拆包+不应答；9 = 版本+不拆包+需应答
        assert bits.needs_response is (value % 2 == 1)


def test_flag_pagination_pair_from_appendix_c_page77():
    """附录 C（PDF 第 77 页）的分包示例：Flag=10/11 与 PNUM/PNO 同时出现。"""
    assert parse_flag(10).has_pagination is True
    assert parse_flag(11).has_pagination is True
    assert parse_flag(8).has_pagination is False
    assert parse_flag(9).has_pagination is False
    assert VECTORS["flag9_bit_layout"]["pagination_example_page"] == 77


# ===========================================================================
# 3. 编解码往返（表 3 结构 + 本项目 9 测点）
# ===========================================================================


def test_table3_reference_packet_roundtrip():
    """按表 3 结构组包 → 解码 → 再编码，逐字符一致，CRC 通过。"""
    vec = VECTORS["table3_flag9_reference_packet"]
    decoded = decode_packet(vec["packet"])

    assert decoded.crc_ok is True
    assert decoded.length_matches is True
    assert decoded.declared_length == vec["expected"]["declared_length"]
    # 长度字段只数数据段（不含 CRC）；数据段 + CRC 才是包头之后的全部字符
    assert decoded.declared_length + 4 == len(vec["packet"]) - 6
    assert encode_packet(decoded.packet) == vec["packet"] + "\r\n"


def test_decode_data_region_fields():
    """数据区按 6.3.4.1 解析：`=` 连接、`,` 分项、`;` 分组。"""
    vec = VECTORS["table3_flag9_reference_packet"]
    packet = decode_packet(vec["packet"]).packet

    assert packet.region == vec["expected"]["cp_region"]
    assert packet.data.get("DataTime") == "20240601085857"
    # 9 个测点各一个 Rtd 字段 + DataTime = 10 项
    assert len(packet.data.fields) == 10
    assert packet.data.get("a34013-Rtd") == "12.34"
    assert packet.data.get("a21026-Rtd") == "18.5"


def test_encode_rejects_oversized_data_segment():
    """§8.1.2：数据段超 1024 字符应分包；本阶段不分包，故直接报错。

    数据区本身要先满足 CP ≤ 950（表 3），所以这里把 region 控制在 950 以内，
    但让整条数据段超过 1024，以单独验证分包阈值。
    """
    packet = Packet(
        qn="20240601085857223", st="31", cn="2011", pw="123456",
        mn="010000A8900016F000169DC0", flag=9, region="a" * 950,
    )
    assert segment_length_of(packet) > MAX_DATA_SEGMENT_LENGTH
    with pytest.raises(StructuralError, match="分包"):
        encode_packet(packet)


def test_encode_rejects_region_over_cp_limit():
    """表 3：CP 上限 950 字符。"""
    with pytest.raises(StructuralError, match="950"):
        Packet(
            qn="20240601085857223", st="31", cn="2011", pw="123456",
            mn="010000A8900016F000169DC0", flag=9, region="x" * 951,
        )


# ===========================================================================
# 4. RF 补传标志（表 3："非补传时，无本字段"）
# ===========================================================================


def test_realtime_packet_omits_rf_field():
    """实时报文不得带 RF —— 标准里没有 RF=0。"""
    vec = VECTORS["table3_flag9_reference_packet"]
    packet = decode_packet(vec["packet"]).packet

    assert packet.resend is False
    assert "RF=" not in packet.to_data_segment()
    assert "RF=" not in encode_packet(packet)


def test_resend_packet_writes_rf_1_only():
    """补传报文写 RF=1。"""
    packet = Packet(
        qn="20240601085857223", st="31", cn="2011", pw="123456",
        mn="010000A8900016F000169DC0", flag=9, region="DataTime=20240601085857",
        resend=True,
    )
    assert "RF=1;" in packet.to_data_segment()
    assert decode_packet(encode_packet(packet)).packet.resend is True


@pytest.mark.parametrize("bad", ["0", "2", "01", ""])
def test_decode_rejects_rf_other_than_1(bad):
    """表 3 只定义 RF=1；RF=0 等一律报错，不静默容忍。"""
    segment = (
        "QN=20240601085857223;ST=31;CN=2011;PW=123456;"
        "MN=010000A8900016F000169DC0;Flag=9;"
        f"RF={bad};CP=&&DataTime=20240601085857&&"
    )
    crc = crc16_hex(segment.encode())
    raw = f"##{len(segment):04d}{segment}{crc}\r\n"
    with pytest.raises(StructuralError, match="RF"):
        decode_packet(raw)


# ===========================================================================
# 5. 分包结构（PNUM/PNO）——只校验结构，不实现分包闭环
# ===========================================================================


def test_pagination_fields_roundtrip():
    """A 位（Flag=10/11）为 1 时才允许 PNUM/PNO，且二者必须成对。"""
    packet = Packet(
        qn="20240601085857223", st="31", cn="2051", pw="123456",
        mn="010000A8900016F000169DC0",
        flag=build_flag(has_pagination=True, needs_response=False),
        region="DataTime=20240601080000;a00000-Avg=17.5",
        pnum="2", pno="1",
    )
    # build_flag(has_pagination=True, needs_response=False) = 0b00001010 = 10
    # 与附录 C 第 77 页的"分包响应"示例一致
    assert packet.flag == 10
    decoded = decode_packet(encode_packet(packet)).packet
    assert (decoded.pnum, decoded.pno) == ("2", "1")
    assert decoded.flags.has_pagination is True
    assert decoded.flags.needs_response is False


def test_pagination_requires_a_bit_set():
    """带了 PNUM/PNO 却不置 A 位 → 报错。"""
    with pytest.raises(StructuralError, match="A 位"):
        Packet(
            qn="20240601085857223", st="31", cn="2051", pw="123456",
            mn="010000A8900016F000169DC0", flag=9, region="DataTime=1",
            pnum="2", pno="1",
        )


def test_pagination_wants_a_bit_but_no_fields():
    """A 位为 1 却没带 PNUM/PNO → 报错。"""
    with pytest.raises(StructuralError, match="PNUM/PNO"):
        Packet(
            qn="20240601085857223", st="31", cn="2051", pw="123456",
            mn="010000A8900016F000169DC0", flag=build_flag(has_pagination=True),
            region="DataTime=1",
        )


def test_pnum_and_pno_must_pair():
    """PNUM 与 PNO 必须同时出现。"""
    with pytest.raises(StructuralError, match="同时"):
        Packet(
            qn="20240601085857223", st="31", cn="2051", pw="123456",
            mn="010000A8900016F000169DC0", flag=build_flag(has_pagination=True),
            region="DataTime=1", pnum="2",
        )


# ===========================================================================
# 6. 官方向量：SM4 加解密（§6.4.2 / 附录 A.2，PDF 第 32–33 页）
# ===========================================================================


def test_official_test_key_is_16_times_0x30():
    """原文："密钥使用16个0x30"。"""
    assert OFFICIAL_TEST_KEY == bytes([0x30] * 16)
    assert len(OFFICIAL_TEST_KEY) == 16


def test_sm4_matches_official_standard_vector():
    """SM4 算法本体：GB/T 32907 的标准测试向量。"""
    key = bytes.fromhex("0123456789abcdeffedcba9876543210")
    plain = bytes.fromhex("0123456789abcdeffedcba9876543210")
    expected = "681edf34d206965e86b3e94f536e4246"
    assert Sm4EcbNoPadding(key).encrypt(plain).hex() == expected


def test_gmssl_crypt_ecb_padding_trap_is_documented():
    """⚠️ 回归锚点：gmssl 的 ``crypt_ecb`` 会补齐分组，不能直接用来做 Nopadding。

    本模块绕开它、只调用单分组原语。这个测试保证"坑"仍然存在且我们没走回去。
    """
    from gmssl.sm4 import SM4_ENCRYPT, CryptSM4

    cipher = CryptSM4()
    cipher.set_key(OFFICIAL_TEST_KEY, SM4_ENCRYPT)
    assert len(cipher.crypt_ecb(b"A" * 16)) == 32, (
        "gmssl.crypt_ecb 行为发生变化：若它已支持 Nopadding，应重新评估本模块的实现方式"
    )
    assert len(Sm4EcbNoPadding(OFFICIAL_TEST_KEY).encrypt(b"A" * 16)) == 16


def test_nopadding_leaves_short_tail_as_plaintext():
    """附录 A.2：不足 16 字符的部分使用明文。"""
    sm4 = Sm4EcbNoPadding(OFFICIAL_TEST_KEY)
    plain = b"X" * 20
    cipher = sm4.encrypt(plain)
    assert len(cipher) == 20                      # 不是 32，没有补齐
    assert cipher[:16] != plain[:16]              # 第一组确实被加密
    assert cipher[16:] == plain[16:]              # 尾部 4 字符保持明文
    assert sm4.decrypt(cipher) == plain


@pytest.mark.parametrize(
    "vector,expected_cipher_bytes,expected_plain_len",
    [("a2_example_2", 208, 208), ("a2_example_4", 144, 147)],
)
def test_official_a2_encryption(vector, expected_cipher_bytes, expected_plain_len):
    """附录 A.2 的四组官方向量：加密输出与原文密文逐字节一致。"""
    data = VECTORS[vector]
    expected = data["expected"]
    key = bytes.fromhex(data["key_hex"])

    assert key == OFFICIAL_TEST_KEY
    assert expected["plaintext_region_length"] == expected_plain_len

    packet = Packet.from_data_segment(data["plaintext_packet"][6:-4])
    assert packet.region == expected["plaintext_region"]

    cipher = Sm4EcbNoPadding(key).encrypt(packet.region.encode("ascii"))
    official = bytes.fromhex(data["ciphertext_hex"])
    assert len(official) == expected_cipher_bytes

    # 示例 4 的官方密文只印出整分组的 144 字节（余下 3 字符是明文），
    # 故只比较前 len(official) 字节。
    assert cipher[: len(official)] == official


@pytest.mark.parametrize("vector", ["a2_example_2", "a2_example_4"])
def test_official_a2_decryption(vector):
    """附录 A.2 密文用公开密钥解密，得到原文给出的明文数据区。"""
    data = VECTORS[vector]
    expected = data["expected"]
    key = bytes.fromhex(data["key_hex"])

    plain = Sm4EcbNoPadding(key).decrypt(bytes.fromhex(data["ciphertext_hex"]))
    assert plain.decode("latin-1") == expected["decrypted_prefix"]
    assert expected["plaintext_region"].startswith(expected["decrypted_prefix"])


@pytest.mark.parametrize("vector", ["a2_example_2", "a2_example_4"])
def test_official_a2_packet_roundtrip(vector):
    """端到端：按官方明文组包加密 → 与官方密文报文**除长度字段外逐字符一致** → 解码还原。

    官方向量取自 PDF，包尾的 ``\\r\\n`` 在文本层里是字面 4 个字符、提取时已裁掉，
    故比较时把本模块编码结果的包尾去掉（只比包头到 CRC）。

    ⚠️ 唯一的差异是那 4 位长度数字：原文填**明文**长度（``0295`` / ``0234``），
    本模块填**实际传输**长度（``1128`` / ``0811``）。差异见下一个测试。
    """
    data = VECTORS[vector]
    expected = data["expected"]
    key = bytes.fromhex(data["key_hex"])
    official = data["encrypted_packet_trimmed"]

    packet = Packet.from_data_segment(data["plaintext_packet"][6:-4])
    encoded = encode_packet(packet, key=key, max_segment_length=10_000)
    ours = encoded.rstrip("\r\n")

    assert len(ours) == len(official)
    # 包头之后的一切（头部字段 / CP=&& / 密文块 / 明文尾巴 / &&CRC）逐字符一致
    assert ours[6:] == official[6:], "密文与 CRC 必须与标准附录 A.2 印出的完全一致"
    # 差异只允许出现在那 4 位长度数字上（下标 2..5）
    assert ours[2:6] != official[2:6]
    differing = [i for i, (a, b) in enumerate(zip(official, ours)) if a != b]
    assert differing, "长度字段应当与原文不同"
    assert set(differing) <= {2, 3, 4, 5}, f"差异越出长度字段：{differing}"

    decoded = decode_packet(official, key=key)
    assert decoded.was_encrypted is True
    assert decoded.decrypted is True
    assert decoded.region_plaintext == expected["plaintext_region"]
    assert decoded.crc_ok is True, "CRC 必须对明文数据段计算"
    assert decoded.crc_hex == expected["crc_hex"]


def test_official_a2_example_4_plaintext_tail_is_not_hex_encoded():
    """⚠️ 附录 A.2 示例 4：不足 16 字符的尾巴是**明文**，不能一起转成十六进制。

    数据区 147 字符 = 9 个整块（144） + 3 字符余数。密文块写成 ``{0x..}``，
    余下的 ``g=N`` 原样留在后面——原文第 33 页就是这样印的：
    ``…0x03,0xA4}g=N&&B541``。早期实现把整个 147 字节都做了十六进制展开，
    导致数据区多出 21 个字符、长度字段与原文差 21。
    """
    data = VECTORS["a2_example_4"]
    key = bytes.fromhex(data["key_hex"])
    packet = Packet.from_data_segment(data["plaintext_packet"][6:-4])

    assert len(packet.region) % 16 == 3

    encoded = encode_packet(packet, key=key, max_segment_length=10_000)
    region_text = encoded.split("CP=&&", 1)[1].rsplit("&&", 1)[0]

    assert region_text.endswith("}g=N"), "密文块之后应原样保留明文的不足 16 字符部分"
    assert region_text.count("}") == 1
    # 密文块只包含 9 个整块 = 144 字节 → 720 字符的 0xNN 写法
    hex_block = region_text[: region_text.index("}") + 1]
    assert parse_hex(hex_block) == bytes.fromhex(data["ciphertext_hex"])
    assert len(bytes.fromhex(data["ciphertext_hex"])) == 144


def test_encrypted_crc_equals_plaintext_crc():
    """⚠️ 附录 A.2 的顺序约束：CRC 对明文算，加密不改它。"""
    data = VECTORS["a2_example_2"]
    key = bytes.fromhex(data["key_hex"])
    packet = Packet.from_data_segment(data["plaintext_packet"][6:-4])

    plain_encoded = encode_packet(packet)
    plain_crc = plain_encoded[-6:-2]
    assert plain_crc == data["expected"]["crc_hex"] == "2200"

    encrypted = encode_packet(packet, key=key, max_segment_length=10_000)
    assert encrypted[-6:-2] == plain_crc, "加密报文的 CRC 必须与明文报文完全相同"
    assert encrypted[-6:-2] == data["encrypted_packet_trimmed"][-4:]


def test_decode_encrypted_without_key_is_explicit_error():
    """数据区是密文却没给密钥 → 明确报错，不静默返回密文。"""
    data = VECTORS["a2_example_2"]
    with pytest.raises(NeedsDecryption):
        decode_packet(data["encrypted_packet_trimmed"])


def test_encrypted_packet_length_semantics():
    """加密后长度字段记录**实际传输**的数据段长度（密文按十六进制书写形式计）。

    ⚠️ 这是本模块与原文加密示例**唯一**的差异：附录 A.2 的长度字段填的是
    **明文**长度（``0295`` / ``0234``），而它实际传输的字符数是 ``1128`` / ``811``。
    按表 2 的字面定义（"数据段的 ASCII 字符数"），本模块填实际传输长度，
    否则接收方无法据此定位数据段边界。
    """
    data = VECTORS["a2_example_2"]
    expected = data["expected"]
    key = bytes.fromhex(data["key_hex"])
    packet = Packet.from_data_segment(data["plaintext_packet"][6:-4])

    encoded = encode_packet(packet, key=key, max_segment_length=10_000)
    declared = int(encoded[2:6])
    assert encoded.endswith("\r\n")
    # 长度字段 = 数据段的字符数（含密文写法与明文尾巴，不含 CRC）
    assert declared == len(encoded) - 2 - 4 - 4 - 2
    assert declared == segment_length_of(packet, key=key)

    # 与官方示例对齐：报文除长度字段外逐字符一致
    official = data["encrypted_packet_trimmed"]
    assert declared != expected["declared_length_in_standard"], "原文填的是明文长度"
    assert declared == len(official) - 6 - 4
    assert encoded.rstrip("\r\n")[6:] == official[6:]


# ===========================================================================
# 7. 加密区边界与十六进制书写形式
# ===========================================================================


def test_split_data_segment_boundaries():
    """加密范围：CP=&& 之后、&&CRC 之前。"""
    head, region, suffix = split_data_segment("QN=1;ST=31;CP=&&DataTime=1;a=2&&")
    assert head == "QN=1;ST=31;"
    assert region == "DataTime=1;a=2"
    assert suffix == "&&"


def test_split_data_segment_requires_cp_marker():
    with pytest.raises(CryptoError, match="CP=&&"):
        split_data_segment("QN=1;ST=31;")


def test_region_hex_roundtrip():
    """``{0xE4,0x3E}`` 写法与 bytes 互转。"""
    raw = bytes([0xE4, 0x3E, 0x00, 0xFF])
    text = format_hex(raw)
    assert text == "{0xE4,0x3E,0x00,0xFF}"
    assert parse_hex(text) == raw
    assert looks_encrypted(text) is True
    assert looks_encrypted("DataTime=1") is False


def test_region_encrypt_decrypt_roundtrip():
    region = "DataTime=20240520210600;a34013-Rtd=12.34"
    encrypted = encrypt_region(region, OFFICIAL_TEST_KEY)
    assert looks_encrypted(encrypted) is True
    assert decrypt_region(encrypted, OFFICIAL_TEST_KEY) == region


def test_key_length_is_enforced():
    with pytest.raises(CryptoError, match="16 字节"):
        Sm4EcbNoPadding(b"short")


def test_parse_hex_rejects_garbage():
    with pytest.raises(CryptoError):
        parse_hex("{E4,3E}")
    with pytest.raises(CryptoError):
        parse_hex("0xE4,0x3E")


# ===========================================================================
# 8. 结构校验与 CRC 错误处理
# ===========================================================================


def test_decode_rejects_missing_packet_prefix():
    with pytest.raises(StructuralError, match="包头"):
        decode_packet("QN=1;ST=31;CP=&&&&0000\r\n")


def test_decode_rejects_bad_length_field():
    with pytest.raises(StructuralError, match="4 位十进制"):
        decode_packet("##12abQN=1;ST=31;CP=&&&&0000\r\n")


def test_crc_mismatch_is_reported_not_raised():
    """表 2："如果CRC错误，执行结束" —— 解码器如实标记，由调用方决定处置。"""
    vec = VECTORS["crc_example_a1"]
    packet = vec["packet"]
    broken = packet[:-1] + ("0" if packet[-1] != "0" else "1")

    decoded = decode_packet(broken)
    assert decoded.crc_ok is False
    assert decoded.crc_hex != decoded.crc_computed

    with pytest.raises(CRCError):
        decoded.require_crc_ok()


def test_length_mismatch_is_reported():
    vec = VECTORS["crc_example_a1"]
    broken = vec["packet"][:2] + "0099" + vec["packet"][6:]
    decoded = decode_packet(broken)
    assert decoded.length_matches is False
    assert decoded.declared_length == 99


@pytest.mark.parametrize(
    "field,value,match",
    [
        ("QN", "2024", "QN"),
        ("ST", "3", "ST"),
        ("CN", "201", "CN"),
        ("PW", "", "PW"),
        ("MN", "ZZZZ", "MN"),
        ("Flag", "abc", "Flag"),
    ],
)
def test_packet_field_validation(field, value, match):
    fields = {
        "QN": "20240601085857223", "ST": "31", "CN": "2011", "PW": "123456",
        "MN": "010000A8900016F000169DC0", "Flag": "9",
    }
    fields[field] = value
    segment = ";".join(f"{k}={v}" for k, v in fields.items()) + ";CP=&&DataTime=1&&"
    crc = crc16_hex(segment.encode())
    with pytest.raises((StructuralError, ValueError), match=match):
        decode_packet(f"##{len(segment):04d}{segment}{crc}\r\n")


def test_decode_rejects_flag_out_of_range():
    with pytest.raises(StructuralError, match="0..255"):
        Packet(
            qn="20240601085857223", st="31", cn="2011", pw="123456",
            mn="010000A8900016F000169DC0", flag=256, region="a=1",
        )


def test_decode_rejects_missing_required_field():
    segment = "QN=20240601085857223;ST=31;CP=&&a=1&&"
    crc = crc16_hex(segment.encode())
    with pytest.raises(StructuralError, match="缺少必需字段"):
        decode_packet(f"##{len(segment):04d}{segment}{crc}\r\n")


def test_decode_tolerates_printable_crlf_from_pdf():
    """标准 PDF 的示例把包尾印成字面 ``\\r\\n``（4 个字符），解码器应能直接吃。"""
    reference = VECTORS["table3_flag9_reference_packet"]["packet"]
    decoded = decode_packet(reference + r"\r\n")
    assert decoded.crc_ok is True
    assert decoded.length_matches is True
    assert decoded.packet.region == VECTORS["table3_flag9_reference_packet"]["expected"]["cp_region"]


def test_decode_rejects_raw_pdf_line_with_trailing_prose():
    """⚠️ PDF 文本层会把说明文字并到报文本行上，那种原始串**不是**合法报文。

    本测试固定这个事实：解码器必须拒绝它，而不是把说明文字当成 CRC 接受。
    测试向量因此同时保存 ``packet``（已裁剪）与 ``packet_in_standard_raw``（逐字原样）。
    """
    raw = VECTORS["crc_example_a1"]["packet_in_standard_raw"]
    assert "\\r\\n" in raw and "校验码" in raw
    with pytest.raises(StructuralError, match="CRC 字段"):
        decode_packet(raw)


# ===========================================================================
# 9. Profile（配置驱动 / 加密开关）
# ===========================================================================


def test_plain_profile_does_not_encrypt():
    packet = Packet(
        qn="20240601085857223", st="31", cn="2011", pw="123456",
        mn="010000A8900016F000169DC0", flag=PLAIN.flag, region="DataTime=1;a=2",
    )
    encoded = encode_with_profile(PLAIN, region="DataTime=1;a=2", qn=packet.qn)
    assert "{" not in encoded
    assert decode_packet(encoded).packet.region == "DataTime=1;a=2"


def test_loopback_profile_encrypts():
    encoded = encode_with_profile(
        LOOPBACK_ENCRYPTED, region="DataTime=1;a=2", qn="20240601085857223",
    )
    decoded = decode_packet(encoded, key=LOOPBACK_ENCRYPTED.key)
    assert decoded.was_encrypted is True
    assert decoded.decrypted is True
    assert decoded.region_plaintext == "DataTime=1;a=2"


def test_profile_default_flag_is_9():
    """实时报文默认 Flag=9（版本=本次修订、不拆包、需应答）。"""
    assert PLAIN.flag == 9
    assert LOOPBACK_ENCRYPTED.flag == 9
    assert parse_flag(PLAIN.flag).version_name == "HJ 212—2025（本次修订版）"


def test_profile_defines_st_and_cn_per_standard_tables():
    """ST 取表 7 的"大气环境污染源"=31；CN 取表 12 的 2011/2051。"""
    assert PLAIN.st == "31"
    assert PLAIN.cn == "2011"
    assert Hj212Profile(name="minute", cn="2051").cn == "2051"


def test_profile_encryption_switch_is_configurable():
    """§6.4.1：互联网"应"加密、专网"宜"加密 → 必须是可配置开关。"""
    assert PLAIN.encrypt is False
    assert LOOPBACK_ENCRYPTED.encrypt is True
    switched = Hj212Profile(name="x", encrypt=True, key=OFFICIAL_TEST_KEY)
    assert switched.encrypt is True


def test_profile_cn_needs_encryption_follows_6_4_4():
    """§6.4.4：2000~2999 与 1014/3020 应加密。"""
    assert Hj212Profile(name="a", cn="2011").cn_needs_encryption() is True
    assert Hj212Profile(name="b", cn="2051").cn_needs_encryption() is True
    assert Hj212Profile(name="c", cn="1014").cn_needs_encryption() is True
    assert Hj212Profile(name="d", cn="3020").cn_needs_encryption() is True
    assert Hj212Profile(name="e", cn="1011").cn_needs_encryption() is False


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"st": "3"}, "ST"),
        ({"cn": "20"}, "CN"),
        ({"mn": "ABC"}, "MN"),
        ({"mn": "Z" * 24}, "MN"),
        ({"pw": "1234567"}, "PW"),
        ({"encrypt": True}, "密钥"),
    ],
)
def test_profile_validation(kwargs, match):
    with pytest.raises(ProfileError, match=match):
        Hj212Profile(name="bad", **kwargs)


def test_make_qn_is_millisecond_precision():
    """表 3：QN 精确到毫秒，17 位数字。"""
    import datetime as dt

    qn = make_qn(dt.datetime(2024, 6, 1, 8, 58, 57, 223000))
    assert qn == "20240601085857223"
    assert len(qn) == 17 and qn.isdigit()


# ===========================================================================
# 10. 因子编码（附录 B.2，PDF 第 35–38 页）
# ===========================================================================


def test_all_nine_project_points_have_a_factor():
    assert len(FACTORS) == 9
    assert {f.point for f in FACTORS} == {
        "Flow", "Dust", "SO2", "NOx", "O2", "Velocity", "Temp", "Humidity", "Pressure",
    }


@pytest.mark.parametrize(
    "point,code,name,page",
    [
        ("Flow", "a00000", "废气流量", 35),
        ("Dust", "a34013", "颗粒物（烟尘）", 37),
        ("SO2", "a21026", "二氧化硫", 36),
        ("NOx", "a21002", "氮氧化物", 36),
        ("O2", "a19001", "氧含量", 36),
        ("Velocity", "a01011", "废气流速", 36),
        ("Temp", "a01012", "废气温度", 36),
        ("Humidity", "a01014", "废气含湿量", 36),
        ("Pressure", "a01013", "废气压力", 36),
    ],
)
def test_factor_codes_come_from_appendix_b2(point, code, name, page):
    """每个编码都能在原文附录 B.2 的指定页上查到（含页码出处）。"""
    factor = factor_of(point)
    assert factor.code == code
    assert factor.name == name
    assert factor.pdf_page == page
    assert 35 <= factor.pdf_page <= 38


def test_unknown_factor_does_not_get_invented():
    """查不到的编码一律报错，绝不臆造。"""
    with pytest.raises(KeyError):
        factor_of("NotAPoint")


def test_code_review_records_all_project_code_mismatches():
    """测点契约里的既有 code 与原文不一致 —— 逐条登记，不偷偷改。"""
    from src.protocol.hj212 import CODE_REVIEW

    assert len(CODE_REVIEW) == 9
    assert all(review.verdict == "错误" for review in CODE_REVIEW)
    by_point = {r.point: r for r in CODE_REVIEW}
    # 最严重的一处：Dust 的既有 code 指向"氨（氨气）"
    assert by_point["Dust"].project_code == "a21001"
    assert by_point["Dust"].standard_code == "a34013"
    assert by_point["SO2"].project_code == "a21002"   # 原文该码是氮氧化物
    assert by_point["NOx"].standard_code == "a21002"


def test_appendix_c_page59_uses_our_gas_factor_codes():
    """交叉验证：附录 C（PDF 第 59 页）的废气报文用的正是 a00000/a34013/a21002。"""
    from src.protocol.hj212.factors import BY_CODE

    for code in ("a00000", "a34013", "a21002"):
        assert code in BY_CODE


# ===========================================================================
# 11. 无副作用（不写宿主机文件；见故障台账 故障 4）
# ===========================================================================


def test_module_does_not_touch_filesystem(tmp_path, monkeypatch):
    """编解码 + 加解密全程只操作内存，不产生任何文件。"""
    monkeypatch.chdir(tmp_path)
    data = VECTORS["a2_example_2"]
    key = bytes.fromhex(data["key_hex"])
    packet = Packet.from_data_segment(data["plaintext_packet"][6:-4])
    encoded = encode_packet(packet, key=key, max_segment_length=10_000)
    decode_packet(encoded, key=key)
    assert list(tmp_path.iterdir()) == []
