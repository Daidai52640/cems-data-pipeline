"""``Flag`` 位掩码的组装与解析（HJ 212—2025 §6.3.3 表 3）。

标准原文（表 3，PDF 第 9 页）::

    Flag=标志位，标志位包含标准版本号、是否拆分包、数据是否应答。
    包括V5、V4、V3、V2、V1、V0、A、D。
    V5～V0：标准版本号；Bit：000000表示HJ/T 212—2005，000001表
    示HJ 212—2017，000010表示本次修订版本号。
    A：是否有数据包序号；Bit：1表示数据包中包含包号和总包数两部分，
    0表示数据包中不包含包号和总包数两部分。
    D：命令是否应答；Bit：1表示应答，0表示不应答。
    示例：Flag= 9的二进制编码00001001，表示版本号为本次修订，需要
    应答且数据段不包含拆分包。

⚠️ **原文的位序描述与它自己的示例对不上**，本模块以**标准正文里的报文实测**为准。

一、把"示例 " 这句话逐字读：``Flag=9`` → ``00001001`` → 版本号 = ``000010``
    （原文自己说的"本次修订版本号"）。也就是说 ``000010`` 这 6 位**结束在末位的
    前两位之前**，即它占据 bit5~bit2，最低两位留给别的标志。
二、标准附录 C 的**全部 193 条报文示例**只出现 4 个 ``Flag`` 取值：

    ===========  ==========  ========  ==========================
    Flag 取值    二进制      bits5..2  出现次数（是否带 PNUM/PNO）
    ===========  ==========  ========  ==========================
    8            00001000    0010      119（否）
    9            00001001    0010       70（否）
    10           00001010    0010        2（**是**）
    11           00001011    0010        2（**是**）
    ===========  ==========  ========  ==========================

    ``bits5..2`` 恒为 ``0010``（= 本次修订版）；**A 位（bit1）与"报文是否带
    PNUM/PNO"完全一致（2/2）**；**D 位（bit0）与"请求 / 应答"一致**
    （正文里 Flag=9/11 用于现场机或上位机**发出**的命令，Flag=8/10 用于**应答**）。

因此本模块采用::

    bit:  7  6  5  4  3  2  1  0
          -  -  V5 V4 V3 V2 A  D
          未用       版本号(0b0010)

* ``bits5..2`` = 版本号（4 位）。HJ/T 212—2005 = 0b0000、HJ 212—2017 = 0b0001、
  **HJ 212—2025（本次修订）= 0b0010**。
* ``bit1`` = A（是否含包号/总包数）
* ``bit0`` = D（命令是否应答）

按本布局，``Flag=9`` = ``00001001`` → 版本 ``0010``（本次修订）、A=0（不拆包）、
D=1；``Flag=10`` = ``00001010`` → 版本 ``0010``、A=1（**含 PNUM/PNO**）、D=0。

⚠️ 版本号与 A/D 的**具体宽度仍是推断**（原文没写"每一位是第几位"）。
但"版本号在 A/D 之上、A=1 才带包号"这一条由 193 条原文示例直接支撑；
真实平台联调时应优先按对端行为校准，本模块的布局差异只影响 ``Flag`` 的组装，
不影响任何其他字段。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

FLAG_MIN: Final[int] = 0
FLAG_MAX: Final[int] = 255

#: 版本号占 bit5~bit2（4 位）。
VERSION_LOW_BIT: Final[int] = 2
VERSION_BITS: Final[int] = 4
VERSION_MASK: Final[int] = ((1 << VERSION_BITS) - 1) << VERSION_LOW_BIT   # 0b00111100

#: A：是否含包号/总包数（bit1）。
PAGINATION_BIT: Final[int] = 1
PAGINATION_MASK: Final[int] = 1 << PAGINATION_BIT                          # 0b00000010

#: D：命令是否应答（bit0）。
RESPONSE_BIT: Final[int] = 0
RESPONSE_MASK: Final[int] = 1 << RESPONSE_BIT                              # 0b00000001

#: 表 3 明确给出的三个版本号取值。
#:（原文按 6 位书写，此处按其语义换算成 4 位版本号）
KNOWN_VERSIONS: Final[dict[int, str]] = {
    0b0000: "HJ/T 212—2005",
    0b0001: "HJ 212—2017",
    0b0010: "HJ 212—2025（本次修订版）",
}

#: 本标准（HJ 212—2025）的版本号取值。``Flag=9`` / ``Flag=8`` 的高位即此值。
CURRENT_STANDARD_VERSION: Final[int] = 0b0010

#: 原文示例的原文（用于文档/测试对照，不参与运算）。
SPEC_EXAMPLE_FLAG: Final[int] = 9
SPEC_EXAMPLE_BINARY: Final[str] = "00001001"
SPEC_EXAMPLE_VERSION_TEXT: Final[str] = "000010"


class FlagError(ValueError):
    """``Flag`` 取值不是 0..255 的整数，或版本号位不在已知版本内。"""


@dataclass(frozen=True)
class FlagBits:
    """``Flag`` 的位分解结果。"""

    #: 版本号（4 位）。``None`` 表示标准未列举该取值。
    standard_version: int | None
    #: 原始版本号位（即使标准未列举也保留，便于原样回写）。
    version_value: int
    #: A：报文是否包含 ``PNUM`` / ``PNO``。
    has_pagination: bool
    #: D：命令是否应答。
    needs_response: bool

    @property
    def version_name(self) -> str:
        """版本号对应的标准名称；未列举时返回 ``"未知版本(0bxxxx)"``。"""
        if self.standard_version is None:
            return f"未知版本(0b{self.version_value:04b})"
        return KNOWN_VERSIONS[self.standard_version]

    def to_int(self) -> int:
        """按位组装回 0..255 的整数。"""
        value = (self.version_value << VERSION_LOW_BIT) & VERSION_MASK
        if self.has_pagination:
            value |= PAGINATION_MASK
        if self.needs_response:
            value |= RESPONSE_MASK
        return value

    def to_binary(self) -> str:
        """8 位二进制字符串（高位在左），用于日志与报文核对。"""
        return f"{self.to_int():08b}"


def parse_flag(flag: int, *, strict_version: bool = False) -> FlagBits:
    """把 0..255 的 ``Flag`` 整数分解成 :class:`FlagBits`。

    ``strict_version=True`` 时，版本号不在 :data:`KNOWN_VERSIONS` 里就报错；
    默认不报错（现场存在更早版本的对端，解码器应能原样读出）。
    """
    if isinstance(flag, bool) or not isinstance(flag, int):
        raise FlagError(f"Flag 必须是 0..255 的整数，收到 {flag!r}")
    if not FLAG_MIN <= flag <= FLAG_MAX:
        raise FlagError(f"Flag 超出 0..255：{flag}")

    version_value = (flag & VERSION_MASK) >> VERSION_LOW_BIT
    if strict_version and version_value not in KNOWN_VERSIONS:
        raise FlagError(
            f"版本号 0b{version_value:04b} 不在标准已列举取值 "
            f"{sorted(f'0b{k:04b}' for k in KNOWN_VERSIONS)} 内"
        )
    return FlagBits(
        standard_version=version_value if version_value in KNOWN_VERSIONS else None,
        version_value=version_value,
        has_pagination=bool(flag & PAGINATION_MASK),
        needs_response=bool(flag & RESPONSE_MASK),
    )


def build_flag(
    *,
    standard_version: int = CURRENT_STANDARD_VERSION,
    has_pagination: bool = False,
    needs_response: bool = True,
) -> int:
    """按位组装 ``Flag`` 整数。

    默认值即 HJ 212—2025 的实时报文：``Flag=9``（版本=本次修订、不拆包、需应答），
    与表 3 的示例一致。
    """
    if not 0 <= standard_version <= (1 << VERSION_BITS) - 1:
        raise FlagError(f"版本号必须在 0..{(1 << VERSION_BITS) - 1}：{standard_version}")
    return FlagBits(
        standard_version=standard_version if standard_version in KNOWN_VERSIONS else None,
        version_value=standard_version,
        has_pagination=has_pagination,
        needs_response=needs_response,
    ).to_int()


def spec_example_consistency() -> tuple[bool, str]:
    """自检：原文示例 ``Flag=9`` 在**本模块布局**下是否与原文自述一致。

    返回 ``(是否一致, 说明)``。用于把"原文自相矛盾"这件事变成可断言的代码事实，
    而不是只写在注释里。
    """
    bits = parse_flag(SPEC_EXAMPLE_FLAG)
    stated = int(SPEC_EXAMPLE_VERSION_TEXT, 2)          # 原文自述的 000010 = 2
    actual = bits.version_value                          # 本布局解出的版本号 = 2
    if actual == stated:
        return True, f"Flag={SPEC_EXAMPLE_FLAG} 版本号 = {actual}，与原文自述 {stated} 一致"
    return False, (
        f"Flag={SPEC_EXAMPLE_FLAG} 本布局解出版本号 {actual}，"
        f"原文自述 {stated}；原文位序描述与其示例不能同时成立"
    )
