"""SM4 数据段加解密层（HJ 212—2025 §6.4.2 与附录 A.2）。

标准原文（§6.4.2，PDF 第 13 页）：

    采用SM4加密算法，16字节（128位）密钥，工作模式采用ECB模式，填充模式采用
    Nopadding，对数据段"CP=&&"到"&&CRC校验码"之间的字符加密以保证数据安全。

标准原文（附录 A.2，PDF 第 32 页）：

    数据段加密时，每16个字符为一组加密，填充模式采用Nopadding，不足16个字符的
    部分使用明文，先完成CRC校验并组包，再对数据段加密。

三条由此推出的硬性实现约束：

1. **加密范围**是数据段里 ``CP=&&`` 之后、``&&`` + 4 位 CRC 之前的那些字符；
   ``CP=&&`` 本身、结尾 ``&&`` 和 CRC 都不加密。
2. **Nopadding 的真实含义是"不足 16 字符的部分保持明文"**，不是补零、不是补 PKCS#7。
3. **顺序不可反**：先算 CRC 并组包，再加密。

⚠️ 关于 ``gmssl``：本模块**用 gmssl 做 SM4 分组运算**，但**不用它的 ``crypt_ecb``**。
实测 gmssl 3.2.2 的 ``CryptSM4.crypt_ecb`` 无论 ``padding_mode`` 取 ``PKCS7`` 还是
``ZERO``，在加密方向都会**补齐到下一个分组**（16 字节明文 → 32 字节密文），
与"不足 16 字符的部分使用明文"直接冲突。因此本模块只调用 gmssl 的单分组原语
``CryptSM4.one_round``，自己按 16 字符切块循环——算法仍完全来自 gmssl，
只是把分组模式的控制权拿回来。该结论由附录 A.2 的四组官方向量实测得出。

⚠️ 本模块**不做密钥管理**：§6.4.3 要求初始密钥在生态环境部上位机注册、90 日轮换，
客观不具备条件（ADR-0006 §3 第 2 条）。调用方必须自己把密钥传进来。
"""

from __future__ import annotations

import re
from typing import Final

from gmssl.func import bytes_to_list, list_to_bytes
from gmssl.sm4 import SM4_DECRYPT, SM4_ENCRYPT, CryptSM4

from .crc16 import crc16_hex

#: SM4 分组长度（字节 / 字符）。标准：16 字节（128 位）密钥，分组同为 16 字节。
BLOCK_SIZE: Final[int] = 16

#: 密钥长度，标准 §6.4.2 规定 16 字节。
KEY_SIZE: Final[int] = BLOCK_SIZE

#: 加密区在数据段中的边界。
CP_PREFIX: Final[str] = "CP=&&"
REGION_SUFFIX: Final[str] = "&&"

#: 附录 A.2 使用的公开测试密钥："密钥使用16个0x30"。
OFFICIAL_TEST_KEY: Final[bytes] = bytes([0x30] * BLOCK_SIZE)


class CryptoError(ValueError):
    """密钥长度不对、或数据段不满足可加密的结构要求。"""


class EncryptionNotApplicable(CryptoError):
    """对端/本端不加密时误调用加密，或数据段不是 ``CP=&&...&&`` 形态。"""


def _check_key(key: bytes) -> None:
    if not isinstance(key, (bytes, bytearray)):
        raise CryptoError(f"密钥必须是 bytes，收到 {type(key).__name__}")
    if len(key) != KEY_SIZE:
        raise CryptoError(f"密钥必须是 {KEY_SIZE} 字节（128 位），收到 {len(key)} 字节")


class Sm4EcbNoPadding:
    """SM4 / ECB / Nopadding：整 16 字符分组加密，不足一组的尾部保持明文。

    ⚠️ 该类是**纯算法层**，只处理"长度是 16 的倍数的整数倍 + 尾部明文"这件事。
    数据段边界（``CP=&&`` / ``&&CRC``）与十六进制书写形式由 :mod:`codec` 负责。
    """

    __slots__ = ("_encrypt", "_decrypt")

    def __init__(self, key: bytes) -> None:
        _check_key(key)
        self._encrypt = CryptSM4()
        self._encrypt.set_key(bytes(key), SM4_ENCRYPT)
        self._decrypt = CryptSM4()
        self._decrypt.set_key(bytes(key), SM4_DECRYPT)

    @staticmethod
    def _block(cipher: CryptSM4, block: bytes) -> bytes:
        # 只走 gmssl 的单分组原语，绕开 crypt_ecb 的强制补齐行为。
        return list_to_bytes(cipher.one_round(cipher.sk, bytes_to_list(block)))

    def encrypt(self, plaintext: bytes) -> bytes:
        """加密 ``plaintext``：每 16 字节一组，不足一组的尾部原样保留。"""
        full = len(plaintext) - len(plaintext) % BLOCK_SIZE
        out = bytearray()
        for offset in range(0, full, BLOCK_SIZE):
            out += self._block(self._encrypt, plaintext[offset:offset + BLOCK_SIZE])
        out += plaintext[full:]
        return bytes(out)

    def decrypt(self, ciphertext: bytes) -> bytes:
        """解密：按同样的切块规则还原；尾部不足一组的部分原样保留。"""
        full = len(ciphertext) - len(ciphertext) % BLOCK_SIZE
        out = bytearray()
        for offset in range(0, full, BLOCK_SIZE):
            out += self._block(self._decrypt, ciphertext[offset:offset + BLOCK_SIZE])
        out += ciphertext[full:]
        return bytes(out)


def split_data_segment(segment: str) -> tuple[str, str, str]:
    """把数据段切成 ``(CP 之前的头部, 加密区明文, 结尾 &&)``。

    以附录 A.1 的示例数据段为例::

        QN=...;Flag=9;CP=&&&&2200
        └──── head ────┘└region┘└suffix┘

    加密区取 ``CP=&&`` 与最后两个 ``&&`` 之间的字符（示例中为 ``""`` 或 ``"2200"``）。
    """
    start = segment.find(CP_PREFIX)
    if start < 0:
        raise EncryptionNotApplicable(f"数据段里找不到 {CP_PREFIX!r}：{segment[:64]!r}…")
    head = segment[:start]
    body = segment[start + len(CP_PREFIX):]
    if not body.endswith(REGION_SUFFIX):
        raise EncryptionNotApplicable(
            f"数据段没有以 {REGION_SUFFIX!r} 收尾，无法确定加密区右边界：{segment[-32:]!r}"
        )
    return head, body[:-len(REGION_SUFFIX)], REGION_SUFFIX


def encrypt_region(region: str, key: bytes) -> str:
    """加密数据区明文字符串，返回**可直接写回报文的数据区内容**。

    ⚠️ 返回值不一定是纯 ``{0x..}``：按附录 A.2"不足 16 个字符的部分使用明文"，
    尾部不足一组的字符**原样保留**，因此在密文块之后可能直接跟明文尾巴。
    例如 147 字符的数据区 → 9 个密文块（720 字符的 ``{0x..}`` 写法）+ 明文 ``g=N``。
    标准附录 A.2 的示例 4 就是这样印的。

    数据区短于 16 字符时整段都是明文，按标准仍写成 ``{}`` + 明文的形式
    （``{}`` 表示"没有密文块"），以免与未加密报文混淆。
    """
    sm4 = Sm4EcbNoPadding(key)
    data = region.encode("ascii")
    full = len(data) - len(data) % BLOCK_SIZE
    cipher = sm4.encrypt(data[:full])
    tail = data[full:].decode("ascii")
    return format_hex(cipher) + tail


def decrypt_region(region: str, key: bytes) -> str:
    """把 :func:`encrypt_region` 的产物还原成明文字符串。

    同时接受两种形态：

    * 纯 ``{0x..}``（数据区长度是 16 的整数倍）
    * ``{0x..}`` 之后跟明文尾巴（不足一组的部分，附录 A.2）

    另外容忍整体未加密的明文（此时原样返回），便于"同一份解码代码吃两种报文"。
    """
    if not looks_encrypted(region):
        return region
    sm4 = Sm4EcbNoPadding(key)
    cipher_text, tail = split_region(region)
    plain = sm4.decrypt(parse_hex(cipher_text))
    if not tail:
        return plain.decode("ascii")
    return (plain + tail.encode("ascii")).decode("ascii")


def format_hex(data: bytes) -> str:
    """按标准示例的书写形式格式化：``{0xE4,0x3E,...,0x41}``（大写、逗号分隔、花括号包裹）。"""
    return "{" + ",".join(f"0x{b:02X}" for b in data) + "}"


def parse_hex(text: str) -> bytes:
    """解析 ``{0xE4,0x3E}`` 形式；容忍 ``0x`` 大小写与分组间空白。"""
    body = text.strip()
    if not (body.startswith("{") and body.endswith("}")):
        raise CryptoError(f"加密区不是 {{...}} 形式：{text[:32]!r}…")
    items = [item.strip() for item in body[1:-1].split(",") if item.strip()]
    out = bytearray()
    for item in items:
        if not item.lower().startswith("0x"):
            raise CryptoError(f"加密区元素不是 0xNN 形式：{item!r}")
        try:
            out.append(int(item, 16))
        except ValueError as exc:
            raise CryptoError(f"加密区元素不是合法十六进制：{item!r}") from exc
    return bytes(out)


_HEX_ITEM: Final = re.compile(r"0x[0-9A-Fa-f]{2}\Z")


def _is_hex_block(text: str) -> bool:
    """``text`` 是不是 ``{0xNN,0xNN,…}``（允许空块 ``{}``）。"""
    if not (text.startswith("{") and text.endswith("}")):
        return False
    inner = text[1:-1]
    if inner == "":
        return True
    return all(_HEX_ITEM.match(item) for item in inner.split(","))


def looks_encrypted(region: str) -> bool:
    """判断数据区是不是加密后的书写形式。

    加密区形如 ``{0xNN,…}``，且按附录 A.2"不足 16 字符的部分使用明文"，
    密文块之后**可能直接跟明文尾巴**（如 ``{0xE4,…}g=N``）。
    数据区短于 16 字符时密文块为空，写作 ``{}`` + 明文。

    对应地，**未加密**的 HJ 212 数据区不会以 ``{`` 开头（字段名首字母大写），
    所以"以 ``{`` 开头且第一个 ``}`` 前是合法十六进制块"就是可靠判据。
    """
    stripped = region.strip()
    if not stripped.startswith("{"):
        return False
    end = stripped.find("}")
    if end < 0:
        return False
    return _is_hex_block(stripped[: end + 1])


def split_region(region: str) -> tuple[str | None, str]:
    """把加密区切成 ``({0x..} 密文块文本, 明文尾巴)``；非加密时返回 ``(None, region)``。"""
    if not looks_encrypted(region):
        return None, region
    stripped = region.strip()
    end = stripped.index("}")
    return stripped[: end + 1], stripped[end + 1:]


def build_segment_with_region(head: str, region: str) -> str:
    """按 ``head + CP=&& + region + &&`` 重新拼接数据段（不含 CRC）。"""
    return f"{head}{CP_PREFIX}{region}{REGION_SUFFIX}"


def encode_plaintext_segment(head: str, region: str) -> tuple[str, str]:
    """组包：先算 CRC，返回 ``(含 CRC 的明文数据段, CRC 十六进制)``。

    这就是附录 A.2 所说"先完成 CRC 校验并组包"那一步。
    """
    segment = build_segment_with_region(head, region)
    crc = crc16_hex(segment.encode("ascii"))
    return segment + crc, crc


def encrypt_packet_segment(head: str, region: str, key: bytes) -> tuple[str, str]:
    """加密组包：返回 ``(含 CRC 的密文数据段, CRC 十六进制)``。

    先对**明文**数据段算 CRC 并组包，再只把数据区换成密文——CRC 仍是明文那一份。
    顺序与 §6.4.2 / 附录 A.2 一致，不可交换。
    """
    plain_segment, crc = encode_plaintext_segment(head, region)
    return build_segment_with_region(head, encrypt_region(region, key)) + crc, crc
