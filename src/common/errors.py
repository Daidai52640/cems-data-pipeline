# -*- coding: utf-8 -*-
"""本项目的**异常分类** —— "这个系统里什么东西会失败"的唯一真源。

============================ 为什么要有这个文件 ============================
审计（`docs/架构与可维护性说明.md` §二 **债 5**）发现：全项目 **57 处 `except Exception`，且无分类**。

问题**不是**"捕获得广" —— 一个"绝不崩、绝不丢数据"的长期运行进程，广捕获是**合理**的。
问题是**从代码里看不出"这个系统里什么东西会失败"**，于是：
无法按类型区分响应（该重试的没重试、该致命的在重试）、无法统计、无法告警分级。

Neuron（EMQX）的做法值得借鉴：**把错误码做成一等值类型** ——
错误可进 `errors/tags[].error`，监控页能筛"仅展示错误点位"。

============================ 两类"坏"，两种建模 ============================
`docs/adr/0009-接入层逐点质量位.md` 的核心是**把两种"坏"分开**。
本文件用**两种不同的建模**来表达这个区分：

| 坏法 | 建模 | 去向 |
|---|---|---|
| **语法坏**（整条报文不可解析） | ⭐ **异常**（`PayloadFormatError`） | **整条拒收** |
| **语义坏**（单点数值合法但不可信） | ⭐ **值对象**（`PointInvalid`），**不抛** | **逐点丢**，进事件表 |

⚠️ **为什么语义坏不用异常**：它是**正常的数据分类结果**，不是控制流意外。
一条报文里 3 个测点超量程，不该走 3 次异常 —— 那既慢又难统计，
而且会让"坏数据"看起来像"程序出错了"。

============================ 边界 ============================
⚠️ **只用于"我们自己识别的失败"**。第三方库（pymodbus / paho / taospy）抛出的异常
**不在这里包装** —— 包装会丢掉原始 traceback 与类型，排障时反而更糟。
调用方按需 `except (CemsError, 第三方异常)` 分别处理即可。
"""

from __future__ import annotations

from enum import Enum
from typing import Final, NamedTuple


class CemsError(Exception):
    """本项目所有自定义异常的基类。

    ⚠️ 只用于"我们自己识别的失败"。第三方库的异常**不包装**（见模块 docstring 的边界说明）。
    """


# ==================== 1. 启动期：致命，**不重试** ====================


class ConfigError(CemsError):
    """配置缺失 / 非法 / 自相矛盾 —— **启动期**就该暴露，**不重试**。

    例：`POINT_SET` 取值不认识、`TD_URL` 为空、`ALARM_*` 解析失败、端口非法。

    ⚠️ **宁可起不来，也不要带着错配置跑出"看起来正常"的数据**
    —— 与项目既有的"网关读不到本从站就拒绝启动"是同一套纪律。
    """


# ==================== 2. 报文：影响"一条数据" ====================


class PayloadFormatError(CemsError):
    """报文**语法坏** —— 整条不可解析，**必须整条拒收**（ADR-0009 §3）。

    例：时间戳不可解析、报文长度不足、**一个测点都没有**、**同一个测点出现两次**。

    ⚠️ 与 `PointInvalid` 的区别：**这里连"部分可用"都谈不上** ——
    没有可写的列（或自相矛盾到无法采信任何一个值）。
    """


class PointInvalidKind(str, Enum):
    """单点不可用的两种**不同**原因。

    ⚠️ **必须分开**（ADR-0009 §10.1）：两者的**处置可能不同** —— `MISSING` 的根因
    通常在**采集侧**（网关读不到该寄存器），`INVALID` 的根因在**数据侧**。
    """

    #: 有值但不可信（超量程 / `NaN` / `inf`）—— root cause 在数据侧
    INVALID = "invalid"
    #: 报文里根本没这个测点 —— root cause 可能在采集侧
    MISSING = "missing"


class PointInvalid(NamedTuple):
    """⭐ **一个测点被判为不可用 —— 这不是异常，是数据分类结果**（ADR-0009 §3/§4）。

    ⚠️ **不要 `raise` 它** —— 它随"有效测点"一起被返回，由调用方写进无效点事件表。

    与 `PayloadFormatError` 构成 ADR-0009 §3 的"两条路"：
    整条坏 → 抛 `PayloadFormatError`；单点坏 → 收集 `PointInvalid`。
    """

    #: 测点名（契约里的 `Point.name`）
    point: str
    #: 不可用的原因分类
    kind: PointInvalidKind
    #: 给人看的原因，**同时落进事件表的 `reason` 列**（例：`超出量程 [0.0, 300.0]: 301.0`）
    reason: str
    #: 原始值；`kind=MISSING` 时为 `None`（本来就没值）。**保留证据**
    raw: float | None = None


# ==================== 3. 运行期：可重试，影响"一条链路" ====================


class DeviceCommError(CemsError):
    """**设备侧**通信失败 —— Modbus 读、CRC 校验、超时、串口打不开。

    ⚠️ 与"设备返回了坏数据"不同：这里说的是**读不到**。
    "读到了但数值超量程"属 `PointInvalidKind.INVALID`，**不是本异常**。
    """

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        #: 是否值得重试。⚠️ 默认 `True` —— 现场抖动是常态，一次失败就放弃会丢数
        self.retryable = retryable


class BrokerError(CemsError):
    """**MQTT broker** 侧失败 —— 连接、订阅、PUBACK 超时。"""


class StorageError(CemsError):
    """**时序库**侧失败 —— INSERT、建表、查询。

    ⚠️ **必须与 `DeviceCommError` 分开**：一个是"数据进不来"，一个是"数据存不下" ——
    两者挂在**完全不同的补传路径**上（网关缓存 vs 接入层落盘队列），
    合成一类会让"到底哪一段断了"看不出来。
    """


# ==================== 4. 程序缺陷：不是运行环境问题 ====================


class InvariantError(CemsError):
    """**不变量被破坏** —— 属**代码缺陷**，不是环境问题。

    例：契约自检失败、`Point` 字段组合非法、入库列数与值数不匹配。

    ⚠️ **刻意与运行期错误分开**：这类错误**重试没有意义**，重试只会掩盖缺陷。
    """


# ==================== 5. 汇总（给扫描、文档与测试用） ====================

#: 本项目的**全部**自定义异常类型。⚠️ 新增异常时**必须同步登记到这里**
#: （`tests/common/test_errors.py` 会逐一核对，漏登记即测试失败）。
ALL_ERRORS: Final[tuple[type[CemsError], ...]] = (
    ConfigError,
    PayloadFormatError,
    DeviceCommError,
    BrokerError,
    StorageError,
    InvariantError,
)

#: 启动期**致命、不重试**的类型。→ 响应：**退出**（宁可起不来）
FATAL_AT_STARTUP: Final[tuple[type[CemsError], ...]] = (ConfigError, InvariantError)

#: 运行期**可重试**的类型。→ 响应：**退避后重试**（现场抖动是常态）
RETRYABLE: Final[tuple[type[CemsError], ...]] = (
    DeviceCommError,
    BrokerError,
    StorageError,
)

#: **报文级**：只影响**单条数据** —— 既不致命，**重试同一条也没意义**。
#: → 响应：**丢掉这一条，继续处理下一条**（并留痕）
PAYLOAD_LEVEL: Final[tuple[type[CemsError], ...]] = (PayloadFormatError,)

#: ⚠️ 上面三组必须构成 `ALL_ERRORS` 的**不重不漏**的划分 ——
#: 即"遇到一种失败该做什么"这个问题**没有洞**（`tests/common/test_errors.py` 会核对）。
RESPONSE_MODES: Final[tuple[tuple[type[CemsError], ...], ...]] = (
    FATAL_AT_STARTUP,
    RETRYABLE,
    PAYLOAD_LEVEL,
)
