"""HJ 212—2025 报文编解码（P1）。

报文结构（§6.3.2 表 2 + §6.3.3 表 3，PDF 第 9 页）::

    ## + 数据段长度(4 位十进制) + 数据段 + CRC(4 位十六进制) + \\r\\n

    数据段 = QN=…;ST=…;CN=…;PW=…;MN=…;Flag=…;[PNUM=…;PNO=…;][RF=1;]CP=&&…&&

两条容易踩错、原文写得最明确的地方：

1. **``RF`` 只在补传报文出现。** 表 3 原文："RF=1，标志数据报文属于补传报文；
   **非补传时，无本字段**。" 标准里**没有 ``RF=0``** —— 实时报文不得带 RF 字段。
   本模块对"实时报文却带 RF"直接报错，不静默容忍。
2. **长度字段的语义以"校验一致"为准。** 表 2 写"数据段的 ASCII 字符数"，
   但附录 A.1 的官方示例 ``##0087…`` 实算数据段为 87 字符，与表 2 一致；
   §8.1.2 又要求"数据段超 1 024 个字符时应分包传输"。故本模块按
   **数据段长度 == 声明的 4 位整数** 编解码，并对 1024 上限做显式检查。

字段长度（表 3："长度包含字段名称、半角'='、字段内容三部分"）：

===========  ======  ==================================================
字段         长度    说明
===========  ======  ==================================================
``QN``       20      ``QN=`` + 17 位 ``YYYYMMDDhhmmss`` + 3 位毫秒
``ST``       5       ``ST=`` + 2 位系统编码
``CN``       7       ``CN=`` + 4 位命令编码
``PW``       9       ``PW=`` + 6 位密码
``MN``       27      ``MN=`` + 24 个 0～9/A～F
``Flag``     6..8    字段总长 6~8（由取值位数决定）
``PNUM``     6       ``PNUM=`` + 总包数；不分包时无本字段
``PNO``      5       ``PNO=`` + 包号；不分包时无本字段
``RF``       4       ``RF=1``；非补传时无本字段
``CP``       [0,950] ``CP=&&数据区&&``
===========  ======  ==================================================
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Final, Iterable, Sequence

from .crc16 import CRC_HEX_WIDTH, crc16_hex, crc16_verify
from .crypto import (
    CP_PREFIX,
    REGION_SUFFIX,
    EncryptionNotApplicable,
    decrypt_region,
    encrypt_region,
    looks_encrypted,
    split_data_segment,
)
from .flags import FlagError, FlagBits, parse_flag

# ---- 结构常量 -------------------------------------------------------------

PACKET_PREFIX: Final[str] = "##"
PACKET_SUFFIX: Final[str] = "\r\n"
LENGTH_WIDTH: Final[int] = 4

#: §8.1.2：数据段超 1024 个字符时应分包传输。
MAX_DATA_SEGMENT_LENGTH: Final[int] = 1024
#: 表 3：指令参数 CP 的长度上限。
MAX_CP_LENGTH: Final[int] = 950

#: 表 3 的字段长度约束（含 "名称="）。
QN_LENGTH: Final[int] = 20
ST_LENGTH: Final[int] = 5
CN_LENGTH: Final[int] = 7
PW_LENGTH: Final[int] = 9
MN_LENGTH: Final[int] = 27
FLAG_MIN_LENGTH: Final[int] = 6
FLAG_MAX_LENGTH: Final[int] = 8
PNUM_LENGTH: Final[int] = 6
PNO_LENGTH: Final[int] = 5
RF_LENGTH: Final[int] = 4

#: 补传标志的唯一合法取值（表 3 只定义了 RF=1）。
RF_RESEND: Final[str] = "1"

_HEX_DIGITS: Final[frozenset[str]] = frozenset("0123456789ABCDEFabcdef")


class CodecError(ValueError):
    """报文不符合 HJ 212 结构约束。"""


class StructuralError(CodecError):
    """包头/包尾/长度字段/字段顺序等结构层面不合法。"""


class CRCError(CodecError):
    """CRC 校验不通过。"""

    def __init__(self, expected: str, computed: str) -> None:
        super().__init__(f"CRC 校验失败：报文中为 {expected}，实算为 {computed}")
        self.expected = expected
        self.computed = computed


class NeedsDecryption(CodecError):
    """数据区是加密形态，但调用方没有提供密钥。"""


# ---- 数据结构 -------------------------------------------------------------


@dataclass(frozen=True)
class DataRegion:
    """数据区 ``CP=&&`` 与 ``&&`` 之间的内容（不含 ``&&``，已在报文层剥离）。

    ``raw`` 是**权威值**：编解码一律原样搬运，不做重排或重格式化，
    以免与对端解析结果不一致。``fields`` 只是给本项目读值用的有序视图。
    """

    raw: str
    #: 解析出的 ``(字段名, 值)`` 序列，**保持报文中的原始顺序**。
    fields: tuple[tuple[str, str], ...] = ()

    @staticmethod
    def from_fields(fields: Iterable[tuple[str, str]]) -> "DataRegion":
        """按 6.3.4.1 组装数据区：字段与值用 ``=``、项之间用 ``,``、项目之间用 ``;``。"""
        items = tuple(fields)
        raw = ",".join(f"{name}={value}" for name, value in items)
        return DataRegion(raw=raw, fields=items)

    @staticmethod
    def parse(text: str) -> "DataRegion":
        """解析数据区文本，保留原始顺序；``key=value`` 之外的分段原样记录。"""
        fields: list[tuple[str, str]] = []
        for chunk in text.split(";"):
            for item in chunk.split(","):
                if not item:
                    continue
                name, sep, value = item.partition("=")
                fields.append((name, value) if sep else (item, ""))
        return DataRegion(raw=text, fields=tuple(fields))

    def get(self, name: str, default: str | None = None) -> str | None:
        for key, value in self.fields:
            if key == name:
                return value
        return default

    def items(self, name: str) -> tuple[str, ...]:
        return tuple(value for key, value in self.fields if key == name)


@dataclass(frozen=True)
class Packet:
    """一条 HJ 212 报文的**数据段**（不含包头/长度/CRC/包尾）。"""

    qn: str
    st: str
    cn: str
    pw: str
    mn: str
    flag: int
    region: str
    #: 总包数 PNUM；``None`` 表示不分包（此时不写本字段）。
    pnum: str | None = None
    #: 包号 PNO；``None`` 表示不分包。
    pno: str | None = None
    #: 补传标志。``True`` → 写 ``RF=1``；``False`` → **不写 RF 字段**。
    resend: bool = False
    #: 数据区的解析视图（由 :meth:`from_data_segment` 填充）。
    data: DataRegion = DataRegion(raw="")
    #: 数据区是否处于加密形态（``{0x..}``）。加密后长度可达明文的 5 倍，
    #: 因此这个标记会让"数据区 ≤ 950 字符"的检查改为对**明文**生效。
    encrypted: bool = False

    def __post_init__(self) -> None:
        _check_length("QN", self.qn, QN_LENGTH - 3, digits_only=True)
        _check_length("ST", self.st, ST_LENGTH - 3, digits_only=True)
        _check_length("CN", self.cn, CN_LENGTH - 3, digits_only=True)
        if not 1 <= len(self.pw) <= PW_LENGTH - 3:
            raise StructuralError(f"PW 内容应为 1..{PW_LENGTH - 3} 位，收到 {self.pw!r}")
        _check_length("MN", self.mn, MN_LENGTH - 3)
        if any(c not in _HEX_DIGITS for c in self.mn):
            raise StructuralError(f"MN 只能是 0～9/A～F（表 3），收到 {self.mn!r}")
        try:
            parse_flag(self.flag)
        except FlagError as exc:
            raise StructuralError(str(exc)) from exc
        if not self.encrypted and len(self.region) > MAX_CP_LENGTH:
            raise StructuralError(
                f"数据区 {len(self.region)} 字符超过 CP 上限 {MAX_CP_LENGTH}（表 3）"
            )

        has_pnum, has_pno = self.pnum is not None, self.pno is not None
        if has_pnum != has_pno:
            raise StructuralError("PNUM 与 PNO 必须同时出现或同时缺省（表 3）")
        if has_pnum:
            _check_length("PNUM", self.pnum or "", PNUM_LENGTH - 5, digits_only=True)
            _check_length("PNO", self.pno or "", PNO_LENGTH - 4, digits_only=True)
            if not parse_flag(self.flag).has_pagination:
                raise StructuralError(
                    "报文含 PNUM/PNO，但 Flag 的 A 位（bit6）为 0；"
                    "表 3：A=1 才表示数据包中包含包号和总包数两部分"
                )
        elif parse_flag(self.flag).has_pagination:
            raise StructuralError("Flag 的 A 位为 1，但报文缺少 PNUM/PNO 字段")

    @property
    def flags(self) -> FlagBits:
        return parse_flag(self.flag)

    def header_fields(self) -> tuple[tuple[str, str], ...]:
        """按表 3 的固定顺序返回数据段的头部字段（不含 ``CP``）。"""
        items: list[tuple[str, str]] = [
            ("QN", self.qn),
            ("ST", self.st),
            ("CN", self.cn),
            ("PW", self.pw),
            ("MN", self.mn),
            ("Flag", str(self.flag)),
        ]
        if self.pnum is not None:
            items.append(("PNUM", self.pnum))
            items.append(("PNO", self.pno or ""))
        # ⚠️ 实时报文不写 RF —— 表 3："非补传时，无本字段"。
        if self.resend:
            items.append(("RF", RF_RESEND))
        return tuple(items)

    def to_data_segment(self) -> str:
        """组装数据段（不含 CRC）。"""
        head = "".join(f"{name}={value};" for name, value in self.header_fields())
        return f"{head}{CP_PREFIX}{self.region}{REGION_SUFFIX}"

    @staticmethod
    def from_data_segment(segment: str) -> "Packet":
        """从数据段（不含 CRC）解析出 :class:`Packet`。"""
        try:
            head, region, _ = split_data_segment(segment)
        except EncryptionNotApplicable as exc:
            raise StructuralError(str(exc)) from exc
        fields: dict[str, str] = {}
        for chunk in head.split(";"):
            if not chunk:
                continue
            name, sep, value = chunk.partition("=")
            if not sep:
                raise StructuralError(f"数据段头部字段缺少 '='：{chunk!r}")
            if name in fields:
                raise StructuralError(f"数据段头部字段重复：{name}")
            fields[name] = value

        for required in ("QN", "ST", "CN", "PW", "MN", "Flag"):
            if required not in fields:
                raise StructuralError(f"数据段缺少必需字段 {required}（表 3）")

        if ("PNUM" in fields) != ("PNO" in fields):
            raise StructuralError("PNUM 与 PNO 必须同时出现或同时缺省（表 3）")

        resend = False
        if "RF" in fields:
            if fields["RF"] != RF_RESEND:
                raise StructuralError(
                    f"RF 只有 RF=1 一种合法取值（表 3），收到 RF={fields['RF']!r}"
                )
            resend = True

        try:
            flag = int(fields["Flag"])
        except ValueError as exc:
            raise StructuralError(f"Flag 必须是十进制整数（表 3）：{fields['Flag']!r}") from exc

        return Packet(
            qn=fields["QN"],
            st=fields["ST"],
            cn=fields["CN"],
            pw=fields["PW"],
            mn=fields["MN"],
            flag=flag,
            region=region,
            pnum=fields.get("PNUM"),
            pno=fields.get("PNO"),
            resend=resend,
            data=DataRegion.parse(region),
            encrypted=looks_encrypted(region),
        )

    def as_plaintext(self, key: bytes) -> "Packet":
        """返回数据区已还原成明文的新 :class:`Packet`（原对象不变）。"""
        if not self.encrypted:
            return self
        plain = decrypt_region(self.region, key)
        if len(plain) > MAX_CP_LENGTH:
            raise StructuralError(
                f"解密后的数据区 {len(plain)} 字符超过 CP 上限 {MAX_CP_LENGTH}（表 3）"
            )
        return replace(
            self, region=plain, data=DataRegion.parse(plain), encrypted=False,
        )


@dataclass(frozen=True)
class DecodedPacket:
    """解码结果：报文对象 + 传输层信息。"""

    packet: Packet
    declared_length: int
    length_matches: bool
    crc_hex: str
    crc_computed: str
    crc_ok: bool
    #: 报文里的数据区是不是加密形态。
    was_encrypted: bool
    #: 调用方是否提供了密钥并成功解密。
    decrypted: bool
    #: 原始完整报文（含包头包尾），便于排查。
    raw: str

    @property
    def region_plaintext(self) -> str:
        """数据区明文（未解密时返回加密形态的原文）。"""
        return self.packet.region

    def require_crc_ok(self) -> Packet:
        """CRC 不通过就抛 :class:`CRCError`——标准要求"如果CRC错误，执行结束"。"""
        if not self.crc_ok:
            raise CRCError(self.crc_hex, self.crc_computed)
        return self.packet


# ---- 编码 ----------------------------------------------------------------


def encode_packet(
    packet: Packet,
    *,
    key: bytes | None = None,
    suffix: str = PACKET_SUFFIX,
    max_segment_length: int = MAX_DATA_SEGMENT_LENGTH,
) -> str:
    """把 :class:`Packet` 编成完整报文。

    顺序严格按附录 A.2：**先算 CRC 并组包，再对数据段的数据区加密**——
    因此 CRC 是对**明文**数据段算出来的那一份，加密后不再重算。

    ``key`` 为 ``None`` 时不加密；给出密钥时按 SM4/ECB/Nopadding 加密数据区。

    ``max_segment_length`` 默认 1024（§8.1.2）。**加密后**的数据段长度按
    十六进制书写形式计算，因此加密会把长度放大约 5 倍——这正是"应分包"
    的触发点，本阶段不实现分包闭环（ADR-0006 §3 第 4 条），超限直接报错。
    """
    segment, _ = _build_segment(packet, key)
    length = len(segment) - CRC_HEX_WIDTH
    if length > max_segment_length:
        raise StructuralError(
            f"数据段 {length} 字符超过 {max_segment_length} 上限（§8.1.2 应分包）；"
            f"本阶段不实现分包闭环（ADR-0006 §3 第 4 条）"
        )
    return f"{PACKET_PREFIX}{length:0{LENGTH_WIDTH}d}{segment}{suffix}"


def segment_length_of(packet: Packet, *, key: bytes | None = None) -> int:
    """预计算长度字段的取值（加密时按密文的十六进制书写形式）。

    返回值与报文里那 4 位十进制数字**完全一致**，可直接用于分包边界判断。
    """
    segment, _ = _build_segment(packet, key)
    return len(segment) - CRC_HEX_WIDTH


def _build_segment(packet: Packet, key: bytes | None) -> tuple[str, str]:
    """返回 ``(含 CRC 的数据段, CRC)``；``key`` 非空时数据区加密。

    顺序严格照附录 A.2：**先对明文数据段算 CRC，再把数据区换成密文**——
    CRC 永远是明文那一份，加密后不重算。
    """
    head = "".join(f"{name}={value};" for name, value in packet.header_fields())
    plain = f"{head}{CP_PREFIX}{packet.region}{REGION_SUFFIX}"
    crc = crc16_hex(plain.encode("ascii"))
    if key is None:
        return plain + crc, crc
    encrypted = f"{head}{CP_PREFIX}{encrypt_region(packet.region, key)}{REGION_SUFFIX}"
    return encrypted + crc, crc


# ---- 解码 ----------------------------------------------------------------


def decode_packet(raw: str, *, key: bytes | None = None) -> DecodedPacket:
    """解码一条完整报文。

    * 数据区是 ``{0x..}`` 加密形态且给了 ``key`` → 解密后填入 ``packet.region``
    * 是加密形态但没给 ``key`` → 抛 :class:`NeedsDecryption`（不静默返回密文）
    * 结构合法但 CRC 不符 → **不抛异常**，在 :class:`DecodedPacket` 里如实标记
      （标准："如果CRC错误，执行结束"；由调用方决定怎么处置）
    """
    if not isinstance(raw, str):
        raise StructuralError(f"报文必须是 str，收到 {type(raw).__name__}")

    text = raw
    suffix = _matched_suffix(text)
    if suffix:
        text = text[: -len(suffix)]
    if not text.startswith(PACKET_PREFIX):
        raise StructuralError(f"报文缺少包头 {PACKET_PREFIX!r}：{raw[:16]!r}")

    length_field = text[len(PACKET_PREFIX): len(PACKET_PREFIX) + LENGTH_WIDTH]
    if len(length_field) != LENGTH_WIDTH or not length_field.isdigit():
        raise StructuralError(f"数据段长度必须是 {LENGTH_WIDTH} 位十进制整数：{length_field!r}")
    declared = int(length_field)

    body = text[len(PACKET_PREFIX) + LENGTH_WIDTH:]
    if len(body) < CRC_HEX_WIDTH:
        raise StructuralError("报文过短：没有 CRC 字段")
    segment, crc_hex = body[:-CRC_HEX_WIDTH], body[-CRC_HEX_WIDTH:]
    if not all(c in _HEX_DIGITS for c in crc_hex):
        raise StructuralError(f"CRC 字段必须是 4 位十六进制：{crc_hex!r}")

    # 报文结构：## + 数据段长度(4) + 数据段 + CRC(4) + \r\n
    #
    # ⚠️ 长度字段的口径经**两个官方示例交叉校验**后定为"数据段字符数，不含 CRC"：
    #   * 附录 A.1（PDF 第 32 页）：报文 97 字符 = 2(包头) + 4(长度) + 87(数据段)
    #     + 4(CRC)，长度字段写的正是 "0087" → **不含 CRC**。
    #   * §6.4.2 / 附录 A.2 的加密示例：长度字段 "0295" 等于
    #     80(头部) + 5("CP=&&") + 208(密文书写形式) + 2("&&")，同样不含 CRC。
    #   → 两个示例一致，故采用"不含 CRC"口径；数据段长度上限 1024（§8.1.2）。
    length_matches = declared == len(segment)

    packet = Packet.from_data_segment(segment)

    was_encrypted = packet.encrypted
    decrypted = False
    if was_encrypted:
        if key is None:
            raise NeedsDecryption(
                "数据区是加密形态（{0x..}），解码需要密钥；"
                "本模块不做密钥管理（ADR-0006 §3 第 2 条），请由调用方提供"
            )
        packet = packet.as_plaintext(key)
        decrypted = True

    # ⚠️ CRC 永远对**明文**数据段计算（附录 A.2："先完成CRC校验并组包，再对数据段
    # 加密"）。所以加密报文必须先解密、还原出明文数据段，再校验 CRC；
    # 若拿密文去算，永远对不上。
    plain_segment = _plaintext_segment(packet) if was_encrypted else segment
    crc_computed = crc16_hex(plain_segment.encode("ascii", errors="replace"))
    crc_ok = crc16_verify(plain_segment.encode("ascii", errors="replace"), crc_hex)

    return DecodedPacket(
        packet=packet,
        declared_length=declared,
        length_matches=length_matches,
        crc_hex=crc_hex,
        crc_computed=crc_computed,
        crc_ok=crc_ok,
        was_encrypted=was_encrypted,
        decrypted=decrypted,
        raw=raw,
    )


def _plaintext_segment(packet: Packet) -> str:
    """按已解密的报文对象还原出**明文**数据段（不含 CRC）。"""
    head = "".join(f"{name}={value};" for name, value in packet.header_fields())
    return f"{head}{CP_PREFIX}{packet.region}{REGION_SUFFIX}"


def _matched_suffix(text: str) -> str | None:
    """识别包尾。

    * 标准形态是 ``<CR><LF>``
    * 同时容忍 ``\\r\\n`` 的**可打印写法**——标准 PDF 的示例就是这样印的
      （附录 A.1/A.2 的报文里 ``\\r\\n`` 是 4 个字面字符），测试向量取自 PDF，
      必须能直接喂进来。
    """
    if text.endswith("\\r\\n"):
        return text[-4:]
    for candidate in (PACKET_SUFFIX, "\n", "\r"):
        if text.endswith(candidate):
            return candidate
    return None


# ---- Profile 便捷入口 -----------------------------------------------------


def packet_from_profile(profile, *, region: str, qn: str, **overrides) -> Packet:
    """用一份 Profile 的头部配置组装 :class:`Packet`。

    ``overrides`` 可覆盖 ``pnum`` / ``pno`` / ``resend`` 等字段。
    """
    fields = dict(
        qn=qn,
        st=profile.st,
        cn=profile.cn,
        pw=profile.pw,
        mn=profile.mn,
        flag=profile.flag,
        region=region,
    )
    fields.update(overrides)
    return Packet(**fields)


def encode_with_profile(profile, *, region: str, qn: str, **overrides) -> str:
    """按 Profile 组包并编码；``profile.encrypt`` 决定是否加密。"""
    packet = packet_from_profile(profile, region=region, qn=qn, **overrides)
    return encode_packet(packet, key=profile.key if profile.encrypt else None)


# ---- 内部工具 -------------------------------------------------------------


def _check_length(name: str, value: str, width: int, *, digits_only: bool = False) -> None:
    if len(value) != width:
        raise StructuralError(f"{name} 内容应为 {width} 位（表 3），收到 {len(value)} 位：{value!r}")
    if digits_only and not value.isdigit():
        raise StructuralError(f"{name} 只能是数字（表 3），收到 {value!r}")
