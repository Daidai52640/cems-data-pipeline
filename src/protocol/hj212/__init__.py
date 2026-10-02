"""HJ 212—2025 报文编解码与 SM4 数据段加解密（P1 + P2）。

**范围**（ADR-0006 §6）：

* P1：包头 / 数据段长度 / 数据段 / ANSI CRC16 / ``Flag`` 位掩码的编解码
* P2：SM4 数据段加解密（ECB / Nopadding / 可开关）

**明确不做**（ADR-0006 §3）：

* 不做 SM4 密钥管理闭环（§6.4.3 要求部里上位机注册 + 90 日轮换）
* 不实现分包（``PNUM``/``PNO``）闭环——只预留结构
* 不做真实网络发送、不做补传优先级重排
* 不引入"通用协议适配框架"（那要等第二个协议实现之后）

**定位**：能生成、能自校验、能本地回环的接口层，**不是"已对接环保平台"**。

外部入口主要走这几个名字::

    from src.protocol.hj212 import (
        Packet, encode_packet, decode_packet,          # P1
        Hj212Profile, encode_with_profile,             # 配置驱动
        Sm4EcbNoPadding, encrypt_region, decrypt_region,  # P2
    )
"""

from __future__ import annotations

from .codec import (
    CRCError,
    CodecError,
    DataRegion,
    DecodedPacket,
    NeedsDecryption,
    Packet,
    StructuralError,
    decode_packet,
    encode_packet,
    encode_with_profile,
    packet_from_profile,
    segment_length_of,
)
from .crc16 import CRC_HEX_WIDTH, crc16, crc16_hex, crc16_verify
from .crypto import (
    OFFICIAL_TEST_KEY,
    CryptoError,
    Sm4EcbNoPadding,
    decrypt_region,
    encrypt_region,
    format_hex,
    looks_encrypted,
    parse_hex,
    split_region,
)
from .factors import (
    BY_CODE,
    BY_POINT,
    CODE_REVIEW,
    FACTORS,
    Factor,
    UnknownFactorError,
    code_of,
    factor_of,
    point_of,
)
from .flags import (
    CURRENT_STANDARD_VERSION,
    KNOWN_VERSIONS,
    FlagBits,
    FlagError,
    build_flag,
    parse_flag,
)
from .profiles import (
    BUILTIN_PROFILES,
    CN_UPLOAD_MINUTE,
    CN_UPLOAD_REALTIME,
    LOOPBACK_ENCRYPTED,
    PLAIN,
    ST_ATMOSPHERIC_SOURCE,
    Hj212Profile,
    ProfileError,
    make_qn,
    with_key,
)

__all__ = [
    # P1 编解码
    "Packet",
    "DataRegion",
    "DecodedPacket",
    "encode_packet",
    "decode_packet",
    "encode_with_profile",
    "packet_from_profile",
    "segment_length_of",
    "CodecError",
    "StructuralError",
    "CRCError",
    "NeedsDecryption",
    # Flag 位掩码
    "FlagBits",
    "FlagError",
    "parse_flag",
    "build_flag",
    "KNOWN_VERSIONS",
    "CURRENT_STANDARD_VERSION",
    # CRC
    "crc16",
    "crc16_hex",
    "crc16_verify",
    "CRC_HEX_WIDTH",
    # P2 加解密
    "Sm4EcbNoPadding",
    "encrypt_region",
    "decrypt_region",
    "format_hex",
    "parse_hex",
    "looks_encrypted",
    "split_region",
    "OFFICIAL_TEST_KEY",
    "CryptoError",
    # Profile
    "Hj212Profile",
    "ProfileError",
    "BUILTIN_PROFILES",
    "PLAIN",
    "LOOPBACK_ENCRYPTED",
    "with_key",
    "make_qn",
    "ST_ATMOSPHERIC_SOURCE",
    "CN_UPLOAD_REALTIME",
    "CN_UPLOAD_MINUTE",
    # 因子编码
    "Factor",
    "FACTORS",
    "BY_POINT",
    "BY_CODE",
    "CODE_REVIEW",
    "factor_of",
    "code_of",
    "point_of",
    "UnknownFactorError",
]
