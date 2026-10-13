# -*- coding: utf-8 -*-
"""本项目的异常分类 —— "这个系统里什么东西会失败"的唯一真源。

审计（docs/架构与可维护性说明.md §二 债 5）发现全项目 57 处 except Exception 且无分类：
广捕获本身合理（长期运行进程不该崩），但看不出"哪里会失败"就无法按类型区分响应、
无法统计、无法告警分级。参考 EMQX 把错误码做成一等值类型。

三类"坏"分开建模（ADR-0009 / ADR-0010）：

| 坏法 | 建模 | 去向 |
|---|---|---|
| 语法坏（整条报文不可解析） | 异常 PayloadFormatError | 整条拒收 |
| 语义坏（单点数值合法但不可信） | 值对象 PointInvalid，不抛 | 逐点丢，进事件表 |
| 可疑（跳变/冻结） | 同上值对象 PointInvalid，不抛 | 照常入库 + 留痕（stored=1） |

语义坏/可疑不抛异常：它们是正常的数据分类结果，不是控制流意外，一条报文里 3 个点超量程
不该走 3 次异常。可疑与无效共用同一个值对象，因为落点一样（同一张事件表），区别只是
kind 的 stored 属性。

只用于"我们自己识别的失败"；第三方库（pymodbus/paho/taospy）抛出的异常不在这里包装，
包装会丢掉原始 traceback。调用方按需分别 except。
"""

from __future__ import annotations

from enum import Enum
from typing import Final, NamedTuple


class CemsError(Exception):
    """本项目所有自定义异常的基类。只用于我们自己识别的失败，第三方异常不包装。"""


# ==================== 启动期：致命，不重试 ====================


class ConfigError(CemsError):
    """配置缺失/非法/自相矛盾 —— 启动期就该暴露，不重试。

    例：POINT_SET 取值不认识、TD_URL 为空、ALARM_* 解析失败、端口非法。
    宁可起不来，也不要带着错配置跑出"看起来正常"的数据。
    """


# ==================== 报文：影响"一条数据" ====================


class PayloadFormatError(CemsError):
    """报文语法坏 —— 整条不可解析，必须整条拒收（ADR-0009 §3）。

    例：时间戳不可解析、报文长度不足、一个测点都没有、同一个测点出现两次。
    与 PointInvalid 的区别：这里连"部分可用"都谈不上，没有可写的列。
    """


class PointInvalidKind(str, Enum):
    """单点不可用的四种原因 —— 关键是"丢"与"留"必须可区分（ADR-0009 §10.1）。

    两者处置可能不同：MISSING 与 INVALID 这两种"丢"必须分开 —— MISSING 根因通常在采集侧
    （网关读不到寄存器），INVALID 在数据侧。
    ADR-0010 再切一刀：可疑 ≠ 无效 ——

    | 判据 | kind | stored | 处置 |
    |---|---|---|---|
    | 超量程/NaN/inf | invalid | 0 | 物理上不可能 → 不写该列（丢） |
    | 报文里没有该点 | missing | 0 | 没有值 → 不写（丢） |
    | 跳变可疑 | jump | 1 | 照常入库 + 留痕 |
    | 冻结可疑 | frozen | 1 | 照常入库 + 留痕 |

    可疑不能丢：跳变可能是真实工况突变（设备启停），冻结可能是真实停机/检修恒值，
    丢掉等于掩盖真实事件。

    取值必须是短名（≤10 字符）：事件表 kind 列是 NCHAR(10)，长名会 DB error。
    """

    #: 有值但不可信（超量程/NaN/inf）—— root cause 在数据侧
    INVALID = "invalid"
    #: 报文里根本没这个测点 —— root cause 可能在采集侧
    MISSING = "missing"
    #: 相邻样本突变（可疑，值已入库）
    JUMP = "jump"
    #: 长期不变（可疑，值已入库）
    FROZEN = "frozen"

    @property
    def stored(self) -> int:
        """该 kind 的值有没有进主表（ADR-0010 §4）：0=丢了 / 1=标了可疑但留着。

        有了它，"丢了什么"（WHERE stored=0）与"标了什么但留着"（WHERE stored=1）
        各是一句 SQL。
        """
        return 0 if self in DROPPED_KINDS else 1


#: 值不进主表的判据（物理上不可能/根本没值）。其余 kind 都是"可疑但已入库"。
#: 新增 kind 时必须在这里表态，否则默认落到 stored=1（留痕不丢数是安全默认）。
DROPPED_KINDS: Final[frozenset[PointInvalidKind]] = frozenset(
    {PointInvalidKind.INVALID, PointInvalidKind.MISSING}
)


class PointInvalid(NamedTuple):
    """一个测点被判为不可用 —— 这不是异常，是数据分类结果（ADR-0009 §3/§4）。

    不要 raise 它：随"有效测点"一起返回，由调用方写进无效点事件表。
    与 PayloadFormatError 构成两条路：整条坏 → 抛 PayloadFormatError；单点坏 → 收集 PointInvalid。
    """

    #: 测点名（契约里的 Point.name）
    point: str
    #: 不可用的原因分类
    kind: PointInvalidKind
    #: 给人看的原因，同时落进事件表的 reason 列
    reason: str
    #: 原始值；kind=MISSING 时为 None。保留证据
    raw: float | None = None


# ==================== 运行期：可重试，影响"一条链路" ====================


class DeviceCommError(CemsError):
    """设备侧通信失败 —— Modbus 读、CRC 校验、超时、串口打不开。

    与"设备返回了坏数据"不同：这里说的是读不到。读到但超量程属 INVALID，不是本异常。
    """

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        #: 是否值得重试。默认 True —— 现场抖动是常态，一次失败就放弃会丢数
        self.retryable = retryable


class BrokerError(CemsError):
    """MQTT broker 侧失败 —— 连接、订阅、PUBACK 超时。"""


class StorageError(CemsError):
    """时序库侧失败 —— INSERT、建表、查询。

    必须与 DeviceCommError 分开：一个是"数据进不来"，一个是"数据存不下"，
    挂在完全不同的补传路径上（网关缓存 vs 接入层落盘队列），合成一类会看不出哪段断了。
    """


# ==================== 程序缺陷：不是运行环境问题 ====================


class InvariantError(CemsError):
    """不变量被破坏 —— 属代码缺陷，不是环境问题。

    例：契约自检失败、Point 字段组合非法、入库列数与值数不匹配。
    与运行期错误分开：这类错误重试没有意义，重试只会掩盖缺陷。
    """


# ==================== 汇总（给扫描、文档与测试用） ====================

#: 本项目的全部自定义异常类型。新增异常时必须同步登记到这里
#:（tests/common/test_errors.py 会逐一核对，漏登记即测试失败）。
ALL_ERRORS: Final[tuple[type[CemsError], ...]] = (
    ConfigError,
    PayloadFormatError,
    DeviceCommError,
    BrokerError,
    StorageError,
    InvariantError,
)

#: 启动期致命、不重试的类型。→ 响应：退出（宁可起不来）
FATAL_AT_STARTUP: Final[tuple[type[CemsError], ...]] = (ConfigError, InvariantError)

#: 运行期可重试的类型。→ 响应：退避后重试（现场抖动是常态）
RETRYABLE: Final[tuple[type[CemsError], ...]] = (
    DeviceCommError,
    BrokerError,
    StorageError,
)

#: 报文级：只影响单条数据 —— 既不致命，重试同一条也没意义。
#: → 响应：丢掉这一条，继续处理下一条（并留痕）
PAYLOAD_LEVEL: Final[tuple[type[CemsError], ...]] = (PayloadFormatError,)

#: 上面三组必须构成 ALL_ERRORS 的不重不漏划分 ——
#: 即"遇到一种失败该做什么"没有洞（tests/common/test_errors.py 会核对）。
RESPONSE_MODES: Final[tuple[tuple[type[CemsError], ...], ...]] = (
    FATAL_AT_STARTUP,
    RETRYABLE,
    PAYLOAD_LEVEL,
)
