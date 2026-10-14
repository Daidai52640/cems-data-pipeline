# -*- coding: utf-8 -*-
"""南向 OPC UA 采集：从 OPC UA 服务器读测点，产出与 Modbus 路径同形的工程量字典。

## 这个模块补的是什么

改造前网关的南向只有 Modbus。真实 CEMS 现场不是所有仪表都讲 Modbus —— OPC UA 是
另一大类（PLC、DCS、第三方数采软件常常只暴露 OPC UA 服务器）。本模块让网关能直接
从 OPC UA 服务器取数，**下游一行不用改**：读出来的仍是 `{测点名: 工程量}`，
和 `gateway.read_device()` 的返回同形，进同一条"打时间戳 → MQTT → 入库"流水线。

## 四个冲突怎么处理（都有外部依据，不是自创）

南向接第二种协议会撞上四个语义冲突。这里逐条给出处置与依据。

### 冲突 1：OPC UA 自带质量位（StatusCode），而本项目的质量位是接入层算的

**处置**：驱动层把 StatusCode 归一成"这个点要不要上送"，**不把 StatusCode 塞进报文**。
依据是 Neuron 的做法 —— 它的 MQTT 北向上行格式里一个点位**要么是 `value` 要么是
`error` 码，二选一**（`tags-format` / `values-format` 两种格式都是这个结构），
原文 TIP：「当点位采集成功时，返回采集到的数据。当点位采集发生错误时，返回错误码，
不再返回数值。」也就是说：**行业做法是在驱动层把协议质量位收敛掉，北向不带质量字段。**

本模块比 Neuron 多留一手：**原始 StatusCode 记进日志与计数**（`quality_report()`），
因为本项目的规矩是"证据要留"（对照 `cems_point_invalid` 事件表的思路）。
Neuron 没有这个位置 —— 这是本项目可以做得更细的地方，也是下面「已知边界」里
"Uncertain 还没进库"那条的由来。

### 冲突 2：时间戳用谁的

**处置**：**主时间戳仍用网关采集时刻**（`gateway.build_payload` 里 `time.strftime`）。
依据：Neuron 上行格式里的 `timestamp` 定义为「**数据采集时的** UNIX 时间戳」——
是采集侧时刻，不是源端 `SourceTimestamp`。本项目更进一步：改主键时间戳会动
`(子表, ts)` 的幂等语义，而且源端时钟未经验证（项目自己记录过容器时钟漂移的坑）。
`SourceTimestamp` 本版**不取**，要取也只能当证据字段，不能当主键。

### 冲突 3：OPC UA 支持订阅推送，本项目是 5 秒轮询

**处置**：**保持轮询**，不用 Subscription/MonitoredItem。
依据：Neuron 对**所有**南向驱动都是按组轮询 —— 文档原文「Started, issuing read commands at
**each group's polling interval**」，且有运行指标 `group_last_timer_ms`（「Duration of one
group timer cycle… **approaching or exceeding the group's polling interval** means the
group holds too many tags」）。连专业网关都不默认上订阅。
对本项目还有一层理由：整套验收体系（覆盖率分母、单网关节拍上限 1.05 条/秒、12.93 h
长跑）都建立在"固定节拍"上，换成事件驱动推送这些数字全部作废、要重测。

### 冲突 4：量纲 —— OPC UA 的值已经是工程量，本项目的契约是"寄存器整数 + scale"

**处置**：OPC UA 路径**不做 `/scale`**，值原样当工程量用；`Point.scale` 是 Modbus
专用的线格式字段（这一点在 `points.py` 里已注明）。依据：Neuron 的"统一数据类型"
思路 —— 原话「Modbus 数据类型需要在服务节点内转换为 Neuron 统一数据类型」，
即**协议私有的表示留在驱动内，跨驱动的统一表示是工程量**。本项目的统一表示就是
报文里的工程量，所以两条南向路径在驱动内收敛到同一形状。

## 与 Modbus 路径的形态差异（要如实说）

| | Modbus 路径 | OPC UA 路径（本模块） |
|---|---|---|
| 谁在主循环里阻塞 | `read_device()` 同步读，失败返回 `None` | 后台线程持续刷新，主循环只取 `snapshot()` |
| 为什么不同 | Modbus 一次读写毫秒级，同步最简单 | OPC UA 建连/握手是秒级，放主循环会拖垮采集节拍 |

后台线程 + 快照的形态**照抄本仓库已有的 `RealDeviceReader`**（设备层读真机温湿度用的
就是这套），所以这不是新发明，是同一栋楼里已有的一种房型。

## 已知边界

- **只能读，不写**。CEMS 现场不需要反向控制；加写能力要另做权限与确认设计。
- **Uncertain 的值照常上送，但"可疑"这件事没进库**：北向报文里没有质量字段，
  所以它只到日志与 `quality_report()` 计数为止。要做成"可在库里查"，
  得给报文加字段并改库表 —— 那是下一步，不是本模块。
- **只支持匿名或用户名/密码**，没做证书登录。证书要走信任列表，现场配置成本另算。
- **不做地址空间浏览**（Neuron 有"浏览 OPC UA 地址空间"，本模块要求你在契约里
  显式写 NodeId）—— 浏览是配置期工具，不该进采集进程。
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Final, NamedTuple, Optional

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import OPCUA_NODE_IDS, parse_node_id   # noqa: E402

LOGGER: Final[logging.Logger] = logging.getLogger("gateway.opcua_south")

# ==================== OPC UA StatusCode ====================
# 规范（OPC UA Part 4 / Part 6）：StatusCode 是 UInt32，**最高两位**表达质量：
#     00 = Good      01 = Uncertain      10 = Bad      11 = Bad
# 所以判定不能写成 "code >= 0x80000000 才是 Bad" —— 那会把 0xC0000000 这类漏掉。
SEVERITY_SHIFT: Final[int] = 30
SEVERITY_GOOD: Final[int] = 0
SEVERITY_UNCERTAIN: Final[int] = 1
SEVERITY_BAD: Final[int] = 2
SEVERITY_BAD_FULL: Final[int] = 3

#: 本项目认得的 Uncertain 子码（不是全集；只用来说明"哪种不确定要特别处理"）
UNCERTAIN_LAST_USABLE_VALUE: Final[int] = 0x40920000
UNCERTAIN_SENSOR_NOT_ACCURATE: Final[int] = 0x40930000
UNCERTAIN_SUBSTITUTE_VALUE: Final[int] = 0x40940000
UNCERTAIN_REFERENCE_OUT_OF_RANGE: Final[int] = 0x40950000
UNCERTAIN_ENGINEERING_UNITS_EXCEEDED: Final[int] = 0x40910000

#: 把人看得懂的名字挂到上面几个子码上（日志与报告用）
UNCERTAIN_SUBCODE_NAMES: Final[dict[int, str]] = {
    UNCERTAIN_LAST_USABLE_VALUE: "LastUsableValue（值是上一次可用的）",
    UNCERTAIN_SENSOR_NOT_ACCURATE: "SensorNotAccurate（精度不可信）",
    UNCERTAIN_SUBSTITUTE_VALUE: "SubstituteValue（值是被替换/仿真出来的）",
    UNCERTAIN_REFERENCE_OUT_OF_RANGE: "ReferenceOutOfRange",
    UNCERTAIN_ENGINEERING_UNITS_EXCEEDED: "EngineeringUnitsExceeded",
}


def classify_status_code(code: int) -> str:
    """把 StatusCode 归成三档之一：`"good"` / `"uncertain"` / `"bad"`。

    纯函数，不依赖 asyncua —— 判据要能被测试逐条盯住（尤其 0xC0000000 这种
    "最高两位是 11"的 Bad，写成阈值比较就会漏）。
    """
    severity = (int(code) >> SEVERITY_SHIFT) & 0b11
    if severity == SEVERITY_GOOD:
        return "good"
    if severity == SEVERITY_UNCERTAIN:
        return "uncertain"
    return "bad"


class QualityDecision(NamedTuple):
    """一个读数该不该上送，以及为什么。"""

    send: bool           # True = 这个点的值进报文；False = 不进（下游会判 missing）
    reason: str          # 给人看的原因（进日志/报告）


def decide_quality(code: int) -> QualityDecision:
    """StatusCode → 上送决策。**这是本模块唯一的扩展点。**

    默认规则（三条，各有理由）：

    | StatusCode | 上送？ | 理由 |
    |---|---|---|
    | Good | 是 | — |
    | Bad（含最高两位为 `10` 与 `11` 两种） | **否** | 值不可用。**不用 0、也不发越界哨兵**：越界哨兵是设备层表达「我读不到设备」的手段，而这里是「服务端明确说这个值坏」——两者混用会让事件表里的原因变得分不清 |
    | Uncertain | **是**（除下面那一类） | 本项目已有「可疑 ≠ 无效」的原则（ADR-0010）：值与"可疑"是两件事，可疑的值照常入库，只是要留痕 |
    | Uncertain **SubstituteValue** | **否** | ⭐ 这一条是刻意加的：`SubstituteValue` 的含义是"这个值是被替换/仿真出来的"，也就是**假数据**。项目原则是「宁缺勿假」——收到一个已知是替换值的数还当真实数据入库，正好违反那条原则。它的语义更接近 Bad，不像其他 Uncertain |

    要改映射（比如把 `SensorNotAccurate` 也拦掉），改这个函数并补测试即可；
    调用方只认 `QualityDecision`，不认 StatusCode。
    """
    kind = classify_status_code(code)
    if kind == "good":
        return QualityDecision(True, "Good")
    if kind == "uncertain":
        name = UNCERTAIN_SUBCODE_NAMES.get(int(code), "Uncertain（其他子码）")
        if int(code) == UNCERTAIN_SUBSTITUTE_VALUE:
            return QualityDecision(
                False, f"{name} —— 替换值属假数据，按「宁缺勿假」拦掉"
            )
        return QualityDecision(True, name)
    return QualityDecision(False, f"Bad（StatusCode=0x{int(code):08X}）")


def asyncua_available() -> bool:
    """`asyncua` 装没装。没装时本模块仍可 import（纯逻辑可测），只是连不上。"""
    import importlib.util

    return importlib.util.find_spec("asyncua") is not None


# ==================== 默认配置 ====================
DEFAULT_ENDPOINT: Final[str] = os.getenv("OPCUA_ENDPOINT", "opc.tcp://localhost:4840")
DEFAULT_USERNAME: Final[str] = os.getenv("OPCUA_USERNAME", "")
DEFAULT_PASSWORD: Final[str] = os.getenv("OPCUA_PASSWORD", "")
#: 建连超时（秒）。OPC UA 握手比 Modbus 慢得多，给宽松一点的默认值。
DEFAULT_CONNECT_TIMEOUT: Final[float] = float(os.getenv("OPCUA_CONNECT_TIMEOUT", "10.0"))


class QualityReport(NamedTuple):
    """最近一轮读取的质量统计（进心跳快照与日志，是"证据"这一半）。"""

    good: int
    uncertain: int
    bad: int
    skipped: int          # 因质量决策被拦掉、没上送的点数
    last_codes: dict[str, int]   # 测点 → 最近一次的原始 StatusCode


class OpcUaReader:
    """后台线程维持 OPC UA 连接并持续刷新快照；主循环只读 `snapshot()`。

    与 `src/device/modbus_server.py` 的 `RealDeviceReader` 同形：
      - 一切异常只记不抛（现场链路抖动是常态，不能让采集进程退出）
      - `snapshot()` 返回最近一次成功读到的工程量；从未成功过返回 None
      - **陈旧的数据不冒充新鲜**：超过 `max_age` 没刷新就返回 None，
        让下游走"没有数据"这条诚实路径（覆盖率下降），而不是把旧值当新值发出去
    """

    def __init__(
        self,
        node_map: Optional[dict[str, str]] = None,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        username: str = DEFAULT_USERNAME,
        password: str = DEFAULT_PASSWORD,
        poll_interval: float = 5.0,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    ) -> None:
        #: 测点 → NodeId。默认取契约里非空的那批（OPCUA_NODE_IDS）。
        self.node_map: dict[str, str] = dict(node_map or OPCUA_NODE_IDS)
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.poll_interval = poll_interval
        self.connect_timeout = connect_timeout

        self._lock = threading.Lock()
        self._values: Optional[dict[str, float]] = None
        self._sampled_at: float = 0.0
        self._reads_ok: int = 0
        self._reads_failed: int = 0
        self._report = QualityReport(0, 0, 0, 0, {})
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # ---- 校验：错的 NodeId 必须在启动时就被拦住，而不是等连上才发现 ----
    def validate(self) -> list[str]:
        """逐条校验 NodeId，返回错误说明列表（空列表 = 全部合法）。"""
        problems: list[str] = []
        if not self.node_map:
            problems.append(
                "没有任何测点配了 OPC UA NodeId（points.py 的 opcua_node_id 全为空）"
            )
        for name, raw in sorted(self.node_map.items()):
            try:
                parse_node_id(raw)
            except ValueError as exc:
                problems.append(f"{name}: {exc}")
        return problems

    # ---- 主循环侧接口 ----
    def snapshot(self, max_age: Optional[float] = None) -> Optional[dict[str, float]]:
        """最近一次成功读取的工程量；从未读到、或已陈旧超过 `max_age` 则返回 None。"""
        with self._lock:
            if self._values is None:
                return None
            age = time.time() - self._sampled_at
            if max_age is not None and age > max_age:
                LOGGER.warning(
                    "[OPCUA] 快照已陈旧 %.1f 秒（超过 %.1f），本轮按「没有数据」处理",
                    age, max_age,
                )
                return None
            return dict(self._values)

    def quality_report(self) -> QualityReport:
        with self._lock:
            return self._report

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "opcua_reads_ok": self._reads_ok,
                "opcua_reads_failed": self._reads_failed,
                "opcua_points_skipped": self._report.skipped,
            }

    # ---- 生命周期 ----
    def start(self) -> bool:
        """起后台采集线程。返回是否启动成功（asyncua 缺失/NodeId 非法 → False）。"""
        problems = self.validate()
        if problems:
            for line in problems:
                LOGGER.error("[OPCUA] 配置不合法：%s", line)
            return False
        if not asyncua_available():
            LOGGER.critical(
                "[OPCUA] 未安装 asyncua，南向 OPC UA 无法启用（pip install asyncua）。"
                "纯逻辑部分不依赖它，但真正的连接需要。"
            )
            return False

        self._thread = threading.Thread(target=self._run, name="opcua-south", daemon=True)
        self._thread.start()
        LOGGER.info(
            "[OPCUA] 南向采集已启动: %s，%d 个测点，轮询 %.1f 秒",
            self.endpoint, len(self.node_map), self.poll_interval,
        )
        return True

    def stop(self) -> None:
        self._stop.set()

    # ---- 后台线程：自己的事件循环，不与主循环共用 ----
    def _run(self) -> None:
        import asyncio

        asyncio.run(self._serve_forever())

    async def _serve_forever(self) -> None:
        """连上 → 循环读 → 断了就退避重连。任何异常只记不抛。"""
        import asyncua

        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with asyncua.Client(
                    url=self.endpoint, timeout=self.connect_timeout
                ) as client:
                    if self.username:
                        await client.set_user(self.username)
                        await client.set_password(self.password)
                    LOGGER.info("[OPCUA] 已连上 %s", self.endpoint)
                    backoff = 1.0
                    await self._poll_loop(client)
            except Exception as exc:  # noqa: BLE001 — 现场链路抖动是常态，不能让线程死掉
                with self._lock:
                    self._reads_failed += 1
                LOGGER.error(
                    "[OPCUA] 连接/读取异常，%.0f 秒后重连: %s", backoff, exc
                )
                self._stop.wait(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _poll_loop(self, client: Any) -> None:
        """按固定节拍逐个节点读取（**不用 Subscription**，理由见模块 docstring 冲突 3）。"""
        nodes = {name: client.get_node(raw) for name, raw in self.node_map.items()}
        while not self._stop.is_set():
            started = time.time()
            values: dict[str, float] = {}
            good = uncertain = bad = skipped = 0
            last_codes: dict[str, int] = {}

            for name, node in nodes.items():
                try:
                    data_value = await node.read_data_value()
                except Exception as exc:  # noqa: BLE001 — 单点失败不该中断整轮
                    bad += 1
                    skipped += 1
                    LOGGER.warning("[OPCUA] 测点 %s 读取失败，本轮不上送: %s", name, exc)
                    continue

                code = int(data_value.StatusCode.value)
                last_codes[name] = code
                decision = decide_quality(code)
                if classify_status_code(code) == "good":
                    good += 1
                elif classify_status_code(code) == "uncertain":
                    uncertain += 1
                    # Uncertain 是本模块最需要"说出来"的一档：它照常上送，
                    # 所以不会体现在下游任何计数里，只有这行日志能证明它发生过。
                    LOGGER.warning(
                        "[OPCUA] 测点 %s 质量不确定（StatusCode=0x%08X）：%s",
                        name, code, decision.reason,
                    )
                else:
                    bad += 1

                if not decision.send:
                    skipped += 1
                    LOGGER.warning(
                        "[OPCUA] 测点 %s 本轮不上送：%s", name, decision.reason
                    )
                    continue

                raw_value = data_value.Value.Value
                if raw_value is None:
                    skipped += 1
                    LOGGER.warning("[OPCUA] 测点 %s 质量为 Good 但值为空，本轮不上送", name)
                    continue
                if isinstance(raw_value, bool) or not isinstance(
                    raw_value, (int, float)
                ):
                    skipped += 1
                    LOGGER.warning(
                        "[OPCUA] 测点 %s 的值不是数值（%r），本轮不上送",
                        name, type(raw_value).__name__,
                    )
                    continue
                values[name] = float(raw_value)

            # 一个点都没读到 = 这轮整体失败：不动快照，让陈旧判据去暴露它
            if not values:
                with self._lock:
                    self._reads_failed += 1
                LOGGER.error(
                    "[OPCUA] 本轮一个测点都没读到（%d 个被拦），保持上一次快照",
                    skipped,
                )
            else:
                with self._lock:
                    self._values = values
                    self._sampled_at = time.time()
                    self._reads_ok += 1
                    self._report = QualityReport(good, uncertain, bad, skipped, last_codes)

            elapsed = time.time() - started
            if elapsed > self.poll_interval:
                # 与 Neuron 的 group_last_timer_ms 同一回事：一轮读得比节拍还久，
                # 说明测点太多或链路太慢，节拍已经被自己撑爆了。
                LOGGER.warning(
                    "[OPCUA] 单轮读取耗时 %.2f 秒 > 轮询周期 %.2f 秒，"
                    "实际节拍已被拉长（考虑拆分组或加大 POLL_INTERVAL）",
                    elapsed, self.poll_interval,
                )
            self._stop.wait(max(0.0, self.poll_interval - elapsed))
