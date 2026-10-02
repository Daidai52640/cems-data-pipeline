"""平台 Profile：把"报文头字段 + Flag + 是否加密"做成配置驱动。

ADR-0006 §8 第 2 条已明确：各厂商扩展字段与地方补充要求**不在国标里**，
所以本模块只负责"结构上支持配置"，**不声称适配了某个省/某个平台**。

一份 Profile 决定：

* ``ST`` 系统编码（附录 / §6.7.1 表 7）
* ``CN`` 命令编码（§6.7.5 表 12）
* ``PW`` 访问密码、``MN`` 数采仪入网编码
* ``Flag`` 的三个位（版本号 / 是否拆包 / 是否应答）
* **是否加密**——§6.4.1：互联网"应"加密、专网"宜"加密
* ``QN`` 的生成方式（毫秒时间戳，标准要求"精确到毫秒"）
"""

from __future__ import annotations

import datetime as _dt
import os
from dataclasses import dataclass, field, replace
from typing import Final

from .crypto import OFFICIAL_TEST_KEY
from .flags import CURRENT_STANDARD_VERSION, build_flag

#: §6.7.1 表 7（PDF 第 17–18 页）：大气环境污染源。
ST_ATMOSPHERIC_SOURCE: Final[str] = "31"
#: 表 7：地表水体环境污染源（本项目不做水，列出仅供对照）。
ST_SURFACE_WATER_SOURCE: Final[str] = "32"

#: §6.7.5 表 12（PDF 第 21 页）：上传污染物实时数据。
CN_UPLOAD_REALTIME: Final[str] = "2011"
#: 表 12：上传污染物分钟数据。
CN_UPLOAD_MINUTE: Final[str] = "2051"
#: 表 12：取污染物实时数据（请求命令）。
CN_REQUEST_REALTIME: Final[str] = "2011"

#: §6.4.4（PDF 第 13 页）：命令编码 2000～2999 的现场机→上位机数据命令应加密。
ENCRYPTED_CN_RANGE: Final[tuple[int, int]] = (2000, 2999)
#: §6.4.4：获取/设置新密钥命令与现场机信息上报也应加密。
ENCRYPTED_CN_EXTRA: Final[frozenset[str]] = frozenset({"1014", "3020"})


class ProfileError(ValueError):
    """Profile 字段不满足标准的长度/字符集约束。"""


def make_qn(now: _dt.datetime | None = None) -> str:
    """生成 ``QN``：``YYYYMMDDhhmmss`` + 3 位毫秒（共 17 字符）。

    标准表 3 要求"精确到毫秒的时间戳…用来唯一标识一次命令交互"。
    """
    moment = now or _dt.datetime.now()
    return f"{moment:%Y%m%d%H%M%S}{moment.microsecond // 1000:03d}"


def default_mn() -> str:
    """从环境变量取 ``MN``；未配置时用全 0 的合法占位值。

    ⚠️ 标准要求 ``MN`` 联网激活后由上位机按 CPUID/MAC 赋码，真实值只能由平台下发。
    这里的默认值**不是**真实入网编码，仅用于本地回环。
    """
    return os.getenv("HJ212_MN", "0" * 24)


@dataclass(frozen=True)
class Hj212Profile:
    """一个上报目标（平台）的报文头配置。"""

    name: str
    #: 系统编码 ST，2 位数字字符串（标准字段长 5，含 "ST="）。
    st: str = ST_ATMOSPHERIC_SOURCE
    #: 命令编码 CN，4 位数字字符串（标准字段长 7，含 "CN="）。
    cn: str = CN_UPLOAD_REALTIME
    #: 访问密码 PW，6 位（标准字段长 9，含 "PW="）。
    pw: str = "123456"
    #: 数采仪入网编码 MN，24 个 0～9/A～F 字符。
    mn: str = field(default_factory=default_mn)
    #: 标准版本号位（V5~V0），默认本次修订版。
    standard_version: int = CURRENT_STANDARD_VERSION
    #: D 位：是否需要应答。
    needs_response: bool = True
    #: A 位：报文是否含包号/总包数。本项目不做分包闭环，默认 False。
    has_pagination: bool = False
    #: **是否加密**。§6.4.1 区分介质：互联网「应」加密、专网「宜」加密。
    encrypt: bool = False
    #: SM4 密钥（16 字节）。为 ``None`` 时若 ``encrypt=True`` 会在编码时报错。
    key: bytes | None = None
    #: 备注：说明这份 Profile 的来源/限制，避免"看起来适配了某省"。
    note: str = ""

    def __post_init__(self) -> None:
        if len(self.st) != 2 or not self.st.isdigit():
            raise ProfileError(f"ST 应为 2 位数字（表 7），收到 {self.st!r}")
        if len(self.cn) != 4 or not self.cn.isdigit():
            raise ProfileError(f"CN 应为 4 位数字（表 12），收到 {self.cn!r}")
        if not 1 <= len(self.pw) <= 6:
            raise ProfileError(f"PW 内容应为 1..6 位（字段长 9 含 'PW='），收到 {self.pw!r}")
        if len(self.mn) != 24 or any(c not in "0123456789ABCDEFabcdef" for c in self.mn):
            raise ProfileError(f"MN 应为 24 个 0～9/A～F 字符（表 3），收到 {self.mn!r}")
        if self.encrypt and not self.key:
            raise ProfileError(f"Profile {self.name!r} 启用了加密但没有配置密钥")

    @property
    def flag(self) -> int:
        """由三个位组装出的 ``Flag`` 整数。"""
        return build_flag(
            standard_version=self.standard_version,
            has_pagination=self.has_pagination,
            needs_response=self.needs_response,
        )

    def cn_needs_encryption(self) -> bool:
        """按 §6.4.4 判断本 Profile 的命令编码是否**属于应加密**的那几类。

        注意：这只回答"标准怎么说"，不回答"这个平台要不要"。
        实际是否加密仍由 :attr:`encrypt` 决定。
        """
        if self.cn in ENCRYPTED_CN_EXTRA:
            return True
        if self.cn.isdigit():
            low, high = ENCRYPTED_CN_RANGE
            return low <= int(self.cn) <= high
        return False


#: 本地回环上位机用的 Profile：加密，密钥用标准附录 A.2 的公开测试密钥。
#: ⚠️ **这不是可用于生产的密钥**（§6.4.3 f/g 禁止固定值/规律值，且要求部里注册与轮换）。
LOOPBACK_ENCRYPTED: Final[Hj212Profile] = Hj212Profile(
    name="loopback-sm4",
    st=ST_ATMOSPHERIC_SOURCE,
    cn=CN_UPLOAD_REALTIME,
    encrypt=True,
    key=OFFICIAL_TEST_KEY,
    note="本地回环演练用；密钥为标准附录 A.2 公开测试密钥，不可用于生产。",
)

#: 专网/不加密的 Profile（§6.4.1 专网"宜"加密 → 允许关掉）。
PLAIN: Final[Hj212Profile] = Hj212Profile(
    name="plain",
    st=ST_ATMOSPHERIC_SOURCE,
    cn=CN_UPLOAD_REALTIME,
    encrypt=False,
    note="不加密上报；对应 §6.4.1 中专网场景（宜加密，非强制）。",
)

#: 供出口层按名字挑选的内置 Profile。**真实平台的 Profile 必须由部署方提供**，
#: 内置这三份只用于本地回环与测试（ADR-0006 §8 第 2 条）。
BUILTIN_PROFILES: Final[dict[str, Hj212Profile]] = {
    p.name: p for p in (PLAIN, LOOPBACK_ENCRYPTED)
}


def with_key(profile: Hj212Profile, key: bytes) -> Hj212Profile:
    """派生一份换过密钥的 Profile（不改原对象）。"""
    return replace(profile, key=key, encrypt=True)
