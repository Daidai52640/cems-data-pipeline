# -*- coding: utf-8 -*-
"""排放超标告警判据：事件状态机 + 小时结论 + 幂等落库 SQL 文本（ADR-0002 方案乙）。

============================ 这个模块是什么 ============================
**纯逻辑**：不 import paho / taosrest，不做任何 I/O，不知道 MQTT 与数据库的存在。
它只做三件事（每一件都能离线回放、都能被断言）：

    1. 逐条判**折算值**（判据吃调用方已经算好的折算结果，本模块**不重算公式**）
    2. 事件状态机：滑窗触发 → START / REMIND → 恢复 → END；折算算不出来 → INVALID
    3. 小时结算：覆盖率三态（ok / over / insufficient），**样本不足绝不判达标**
    4. 把上面三类结论翻译成可执行的 SQL 文本（`AlarmTables`），由接入层负责执行

设计依据：`docs/adr/0002-告警判据选型.md`（**已定稿**）。关键口径与出处：
    §3.1 判**折算值**，不是标干值；折算用的 O2 必须与浓度**同帧**（本项目一条报文
         共用时间戳，`reference_values()` 天然满足）
    §3.2 用**滑窗计数**（最近 M 条有效样本里至少 N 条越限），不用严格连续、不用单点
    §3.3 开始 = 滑窗触发；开始时间取**首个越限样本的时间戳**；结束 = 连续 K 条
         折算值 ≤ 限值 × 恢复系数（滞回）；同一 `(point, judge_type)` 同时只允许一个
         OPEN 事件；事件表 append-only，幂等靠 `(子表, ts)` 覆盖
    §3.4 小时三态：覆盖率 ≥ 门限 且 均值 ≤ 限值 → ok；均值 > 限值 → over；
         覆盖率 < 门限 → **insufficient（既不判达标也不判超标）**
    §3.5 O2 ≥ 21% → `to_reference_o2()` 返回 nan → 该样本**不参与任何达标判定**，
         单独产 `phase=invalid` 记录（不是达标、不是超标）

============================ 三条硬纪律 ============================
1. **绝不 import 哨兵值参与判定**：判定吃的是数学层折算结果（O2 ≥ 21% 时是 nan）。
   协议层把它编码成 +9999.99 的那一份（`to_transmit_value()`）**只用于出站**，
   它大于任何限值，混进判定必然把"算不出来"判成"严重超标"。
2. **限值判定统一走 `points.over_limit()`**：`>` 语义，"正好等于限值"算**达标**
   （实测 dust 折算峰值正好 5.00，用 `>=` 会把达标判成超标）。
3. **判定集合显式声明**：只判 `points.ZS_TARGETS`（dust/so2/nox），
   不写成"遍历所有测点、有 limit 就判"—— Flow/O2/Temp 的 limit 是量程上限。
============================================================================
"""

from __future__ import annotations

import json
import math
import os
import re
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Mapping, Optional, Sequence

from src.common.points import (
    LIMITS,
    O2_DRY_BASIS,
    O2_REFERENCE,
    POINTS,
    ZS_TARGETS,
    over_limit,
    to_reference_o2,
)

# ==================== 1. 常量 ====================

# ---- phase（事件行）----
PHASE_START: Final[str] = "start"        # 事件开始（滑窗触发）
PHASE_REMIND: Final[str] = "remind"      # 事件仍 OPEN，距上次通知已超 ALARM_REMIND_SECONDS
PHASE_END: Final[str] = "end"            # 事件结束（连续 K 条恢复）
PHASE_INVALID: Final[str] = "invalid"    # 折算算不出来（O2>=21%）或小时数据不足

# ---- judge_type（判据类型）----
JUDGE_INSTANT: Final[str] = "instant"    # 逐条滑窗判据（§3.2 的 B）
JUDGE_HOURLY: Final[str] = "hourly"      # 小时结算判据（§3.2 的 C）

# ---- 小时结论（§3.4 三态 + §3.5 的 invalid 语义）----
VERDICT_OK: Final[str] = "ok"
VERDICT_OVER: Final[str] = "over"
VERDICT_INSUFFICIENT: Final[str] = "insufficient"
VERDICT_INVALID: Final[str] = "invalid"  # 保留值：当前判据不产出（全部无效样本也判 insufficient）

# ---- 推送记录（一期把"推送"落成可验证的动作）----
PUSH_STATUS_RECORDED: Final[str] = "recorded"
#: 推送记录状态：真正送达到外部通道（例如 Webhook 返回 2xx）
PUSH_STATUS_SENT: Final[str] = "sent"
#: 推送记录状态：尝试送外部通道但失败（**不影响事件落库**，只如实标记）
PUSH_STATUS_FAILED: Final[str] = "failed"

# ---- 时间戳格式（与接入层 parse_timestamp 同一口径）----
TS_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
HOUR_SECONDS: Final[float] = 3600.0

# ---- event_id 的分隔符（ts 自带冒号，不能用冒号做分隔符）----
EVENT_ID_SEP: Final[str] = "|"

# ---- 从契约派生：列名 ↔ 测点名 ↔ O2 字段名（不重复推导，只取派生视图）----
COLUMN_TO_NAME: Final[dict[str, str]] = {point.column: point.name for point in POINTS}
O2_FIELD: Final[str] = COLUMN_TO_NAME["o2"]      # "O2"

# ---- 会被拼进 SQL 的值的白名单（判据版本号/推送渠道可以从环境变量来，必须校验）----
JUDGE_VERSION_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._:/+\-]{1,24}$")
CHANNEL_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._:\-]{1,16}$")


# ==================== 2. 配置（全部来自 ALARM_* 环境变量） ====================

@dataclass(frozen=True)
class AlarmConfig:
    """告警判据参数。默认值即 ADR-0002 §2.2 4) 给出的那一组，可被环境变量覆盖。"""

    enable: bool = True                     # ALARM_ENABLE
    window_samples: int = 6                 # ALARM_WINDOW_SAMPLES（M）
    min_over_samples: int = 3               # ALARM_MIN_OVER_SAMPLES（N）
    recover_ratio: float = 0.95             # ALARM_RECOVER_RATIO（恢复滞回系数）
    recover_samples: int = 6                # ALARM_RECOVER_SAMPLES（K）
    remind_seconds: float = 1800.0          # ALARM_REMIND_SECONDS
    hourly_enable: bool = True              # ALARM_HOURLY_ENABLE
    hourly_backfill_hours: int = 2          # ALARM_HOURLY_BACKFILL_HOURS（启动时补结算几个已闭合小时）
    coverage_min: float = 0.75              # ALARM_COVERAGE_MIN（§3.4 覆盖率门限）
    invalid_ratio_max: float = 0.10         # ALARM_INVALID_RATIO_MAX（§3.5 无效样本占比上限）
    o2_denominator_min: float = 2.0         # ALARM_O2_DENOM_MIN（21-O2 下限，低于它折算视为不可用）
    poll_interval: float = 5.0              # ALARM_POLL_INTERVAL（算"应有样本数"用，须与网关一致）
    judge_version: str = "v1.0.0"           # ALARM_JUDGE_VERSION（写进事件快照，公式/口径变更时改）
    push_channel: str = "log"               # ALARM_PUSH_CHANNEL（一期：log = 只落记录 + 日志）

    @property
    def expected_samples_per_hour(self) -> int:
        """§3.4 的"应有样本数" = 3600 / 轮询周期（5s ⇒ 720 条）。"""
        return int(round(HOUR_SECONDS / self.poll_interval))

    def validate(self) -> None:
        """参数自检；不合法抛 ValueError（由接入层在启动阶段拦下，别带病运行）。"""
        if self.window_samples < 1:
            raise ValueError(f"ALARM_WINDOW_SAMPLES 必须 >= 1: {self.window_samples}")
        if not 1 <= self.min_over_samples <= self.window_samples:
            raise ValueError(
                f"ALARM_MIN_OVER_SAMPLES 必须落在 [1, ALARM_WINDOW_SAMPLES={self.window_samples}]: "
                f"{self.min_over_samples}"
            )
        if self.recover_samples < 1:
            raise ValueError(f"ALARM_RECOVER_SAMPLES 必须 >= 1: {self.recover_samples}")
        if not 0.0 < self.recover_ratio <= 1.0:
            raise ValueError(f"ALARM_RECOVER_RATIO 必须落在 (0, 1]: {self.recover_ratio}")
        if self.remind_seconds < 0:
            raise ValueError(f"ALARM_REMIND_SECONDS 不能为负: {self.remind_seconds}")
        if self.hourly_backfill_hours < 0:
            raise ValueError(f"ALARM_HOURLY_BACKFILL_HOURS 不能为负: {self.hourly_backfill_hours}")
        if not 0.0 <= self.coverage_min <= 1.0:
            raise ValueError(f"ALARM_COVERAGE_MIN 必须落在 [0, 1]: {self.coverage_min}")
        if not 0.0 <= self.invalid_ratio_max <= 1.0:
            raise ValueError(f"ALARM_INVALID_RATIO_MAX 必须落在 [0, 1]: {self.invalid_ratio_max}")
        # 折算分母下限：0 与 O2_DRY_BASIS(=21) 之间是开区间 —— 取 0 等于关掉这条判据
        # （分母 > 0 才是数学可算），取 21 会把 O2=0% 这种正常样本也判无效。
        if not 0.0 < self.o2_denominator_min < O2_DRY_BASIS:
            raise ValueError(
                f"ALARM_O2_DENOM_MIN 必须落在 (0, {O2_DRY_BASIS}): {self.o2_denominator_min}"
            )
        if self.poll_interval <= 0:
            raise ValueError(f"ALARM_POLL_INTERVAL 必须 > 0: {self.poll_interval}")
        if self.poll_interval > HOUR_SECONDS:
            raise ValueError(f"ALARM_POLL_INTERVAL 不能大于 3600s: {self.poll_interval}")
        if not JUDGE_VERSION_RE.match(self.judge_version):
            raise ValueError(
                f"ALARM_JUDGE_VERSION 只允许字母数字与 . _ : / + -（1~24 字符）: "
                f"{self.judge_version!r}"
            )
        if not CHANNEL_RE.match(self.push_channel):
            raise ValueError(
                f"ALARM_PUSH_CHANNEL 只允许字母数字与 . _ : -（1~16 字符）: {self.push_channel!r}"
            )

    @classmethod
    def from_env(cls) -> "AlarmConfig":
        """从 ALARM_* 环境变量构造（未设置时取 ADR 默认值）。"""

        def flag(key: str, default: str) -> bool:
            return os.getenv(key, default).strip().lower() not in ("0", "false", "no", "off", "")

        return cls(
            enable=flag("ALARM_ENABLE", "1"),
            window_samples=int(os.getenv("ALARM_WINDOW_SAMPLES", "6")),
            min_over_samples=int(os.getenv("ALARM_MIN_OVER_SAMPLES", "3")),
            recover_ratio=float(os.getenv("ALARM_RECOVER_RATIO", "0.95")),
            recover_samples=int(os.getenv("ALARM_RECOVER_SAMPLES", "6")),
            remind_seconds=float(os.getenv("ALARM_REMIND_SECONDS", "1800")),
            hourly_enable=flag("ALARM_HOURLY_ENABLE", "1"),
            hourly_backfill_hours=int(os.getenv("ALARM_HOURLY_BACKFILL_HOURS", "2")),
            coverage_min=float(os.getenv("ALARM_COVERAGE_MIN", "0.75")),
            invalid_ratio_max=float(os.getenv("ALARM_INVALID_RATIO_MAX", "0.10")),
            o2_denominator_min=float(os.getenv("ALARM_O2_DENOM_MIN", "2.0")),
            poll_interval=float(os.getenv("ALARM_POLL_INTERVAL", "5.0")),
            judge_version=os.getenv("ALARM_JUDGE_VERSION", "v1.0.0").strip(),
            push_channel=os.getenv("ALARM_PUSH_CHANNEL", "log").strip(),
        )


# ==================== 3. 结论数据结构 ====================

@dataclass(frozen=True)
class AlarmEvent:
    """一条事件行（= 事件表里的**判定快照**，也是"推送记录"的源）。

    ⚠️ 快照为什么合规：项目约定"折算是导出量、不落数据表"针对的是 `cems_data` 的冗余列；
       事件表里的快照是**判定证据**（每事件一行，不是每采样一行），用来事后复现
       "当时按哪个公式、哪个基准氧、哪个限值判的"（ADR-0002 §0 与 §2.2 4)）。
    """

    ts: str                # 该阶段的时间戳（start = 首个越限样本 ts）
    event_id: str          # point:judge_type:start_ts（可复现，无随机数）
    phase: str             # start / remind / end / invalid
    point: str             # 列名 dust/so2/nox
    judge_type: str        # instant / hourly
    converted: float       # 判定当时的**折算值**快照（hourly 时是"该小时折算均值"= 被判定的量）
    raw_value: float       # 判定当时的标干值（hourly 时是该小时标干均值）
    o2: float              # 判定当时的实测氧含量（hourly 时是该小时均值）
    limit_value: float     # 判定用的限值
    o2_reference: float    # 判定用的基准氧含量（换行业后历史行仍可解释）
    over_samples: int      # 该事件累计越限样本数（hourly = 该小时瞬时越限样本数）
    window_seconds: float  # 该事件已持续秒数（start=0；end=时长；hourly=3600）
    judge_version: str     # 判定版本（公式/口径变更后可解释历史结论）
    trigger_ts: str        # 触发时刻（start/first-over 与 ts 的区别就在这里）
    reason: str = ""       # **只进日志、不进库**：本次判定的简要理由

    def log_text(self, channel: str, status: str = PUSH_STATUS_RECORDED) -> str:
        """把一条事件渲染成结构化日志行（一期的"推送动作"落点之一）。"""
        return (
            f"ALARM_PUSH channel={channel} status={status} phase={self.phase} "
            f"point={self.point} judge_type={self.judge_type} event_id={self.event_id} "
            f"ts={self.ts} trigger_ts={self.trigger_ts} "
            f"converted={_fmt(self.converted)} raw_value={_fmt(self.raw_value)} "
            f"limit_value={_fmt(self.limit_value)} o2={_fmt(self.o2)} "
            f"o2_reference={_fmt(self.o2_reference)} over_samples={self.over_samples} "
            f"window_seconds={_fmt(self.window_seconds)} judge_version={self.judge_version} "
            f"reason={self.reason or '-'}"
        )

    def payload_json(self) -> str:
        """推送记录里的载荷（下游据此即可复现判定，不必回查事件表）。

        ⚠️ 必须塞得进 `payload NCHAR(256)`：event_id / ts 这类长字段已经在**列**里，
           不再重复写进载荷（实测精简后约 170 字符）。
        """
        return json.dumps(
            {
                "point": self.point,
                "judge_type": self.judge_type,
                "phase": self.phase,
                "converted": None if not math.isfinite(self.converted) else round(self.converted, 4),
                "raw": None if not math.isfinite(self.raw_value) else round(self.raw_value, 4),
                "o2": None if not math.isfinite(self.o2) else round(self.o2, 4),
                "limit": self.limit_value,
                "o2_ref": self.o2_reference,
                "over_samples": self.over_samples,
                "dur_s": round(self.window_seconds, 3),
                "ver": self.judge_version,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class HourlyVerdict:
    """一个测点、一个小时的结论行（写 `cems_hourly_verdict`，按 (子表, ts) 幂等覆盖）。"""

    ts: str            # 小时起点（北京时间整点）
    point: str
    n_total: int
    n_valid: int
    n_invalid: int
    coverage: float
    conv_mean: float   # 该小时折算均值（只统计有效样本；无有效样本为 nan）
    conv_max: float
    limit_value: float
    verdict: str
    judge_version: str = ""


# ==================== 4. 内部工具（纯函数） ====================

def _fmt(value: float) -> str:
    """日志里打印数值：nan 打印 nan 而不是异常。"""
    return "nan" if not math.isfinite(value) else f"{value:.4f}"


def to_datetime(ts: str) -> Optional[datetime]:
    """把 'YYYY-MM-DD HH:MM:SS' 解析成本地朴素时间；解析不了返回 None（不抛）。"""
    try:
        return datetime.strptime(ts, TS_FORMAT)
    except (ValueError, TypeError):
        return None


def seconds_between(start_ts: str, end_ts: str) -> float:
    """两个时间戳之间的秒数；任一解析失败返回 0.0（不因为一条脏 ts 丢掉整个事件）。"""
    start, end = to_datetime(start_ts), to_datetime(end_ts)
    if start is None or end is None:
        return 0.0
    return (end - start).total_seconds()


def normalize_ts(text: Any) -> str:
    """把任意读回来的时间戳规范化成 `YYYY-MM-DD HH:MM:SS`（容器本地时间）。

    ⚠️ 必须做，而且必须认三种形态。本机实测（2026-10-02）**两条 REST 读路径的表示不同**：
        - `taosrest` 库（接入层用的这条）→ 朴素本地 `datetime(2026,10,2,14,0)`，与 taos CLI 一致
        - 裸 `POST /rest/sql`（taosAdapter 的 JSON）→ UTC ISO `'2026-10-02T06:00:00.000Z'`
      两者是**同一时刻**（14:00 北京 = 06:00Z），但字符串不同（`'T' > ' '`）。
      所以 `Z`/带偏移的那种必须先转成容器本地时间再比较，否则水位判断会差 8 小时。
    """
    if isinstance(text, datetime):
        return text.strftime(TS_FORMAT)
    raw = str(text).strip()
    if not raw:
        return ""
    try:
        return datetime.strptime(raw, TS_FORMAT).strftime(TS_FORMAT)
    except (ValueError, TypeError):
        pass
    # 带时区（Z / +08:00）→ 转成本地时间再格式化
    if raw.endswith("Z") or raw.endswith("z") or re.search(r"[+-]\d{2}:?\d{2}$", raw):
        try:
            moment = datetime.fromisoformat(raw.replace("Z", "+00:00").replace("z", "+00:00"))
            return moment.astimezone().strftime(TS_FORMAT)
        except (ValueError, TypeError):
            pass
    candidate = raw.replace("/", "-").replace("T", " ").replace("Z", "").split(".")[0].strip()
    try:
        return datetime.strptime(candidate, TS_FORMAT).strftime(TS_FORMAT)
    except (ValueError, TypeError):
        return raw


def hour_start_of(moment: datetime) -> datetime:
    """取所在小时的整点（分钟/秒/微秒归零）。"""
    return moment.replace(minute=0, second=0, microsecond=0)


def make_event_id(column: str, judge_type: str, start_ts: str) -> str:
    """事件标识：point + judge_type + start_ts（ADR-0002 §3.3，可复现、无随机数）。

    ⚠️ 分隔符用 `|` 而不是 `:`：ts 自身带冒号，用 `|` 才能把 start_ts 原样解析回来
      （重启恢复要靠它把事件的"首个越限样本时间戳"从库里折回来）。
    """
    return f"{column}{EVENT_ID_SEP}{judge_type}{EVENT_ID_SEP}{start_ts}"


def parse_event_start(event_id: str, fallback: str = "") -> str:
    """从 event_id 里取回该事件的 start_ts；格式不符返回 fallback。"""
    parts = str(event_id).split(EVENT_ID_SEP)
    if len(parts) != 3 or not parts[2]:
        return fallback
    return parts[2]


def sql_text(value: str) -> str:
    """SQL 字符串字面量转义（单引号翻倍）。"""
    return "'" + str(value).replace("'", "''") + "'"


def sql_float(value: float) -> str:
    """SQL 浮点字面量；非有限值（nan/inf）必须写 NULL。

    ⚠️ 不能把 nan 直接拼进 SQL：TDengine 会报 syntax error 并丢掉**整行**
      （接入层 parse_payload 拒收 NaN 就是为了这件事）。
    """
    if value is None or not math.isfinite(float(value)):
        return "NULL"
    return repr(float(value))


def sql_timestamp(ts: str) -> str:
    """SQL 时间戳字面量。"""
    return sql_text(ts)


# ==================== 5. 逐条判据 + 事件状态机 ====================

@dataclass
class _InstantState:
    """某个测点在 `judge_type=instant` 下的状态（§3.3 的状态机实例）。"""

    window: deque = field(default_factory=deque)   # 最近 M 条**有效**样本：(ts, 是否越限, 折算值, 标干值)
    event_id: str = ""                             # 非空 = 该测点当前有 OPEN 事件
    start_ts: str = ""                             # 事件首个越限样本的时间戳
    last_notify_ts: str = ""                       # 上次通知（START/REMIND）的时刻
    over_samples: int = 0                          # 该事件累计越限样本数
    recover_count: int = 0                         # 连续恢复样本计数


class AlarmJudge:
    """逐条判据 + 事件状态机（纯内存、确定性；状态可从事件表折叠恢复）。

    调用方（接入层）每入库成功一条报文调一次 `on_sample()`，传入：
        ts         报文时间戳（字符串，与入库的 ts 同一口径）
        values     标干值，键 = MQTT 字段名（Dust/SO2/NOx/O2…）
        references 折算值，键 = TDengine 列名（dust/so2/nox），来自
                   `points.to_reference_o2()`；O2 >= 21% 时是 **nan**
    返回待落库的事件行列表（可能为空）。
    """

    def __init__(self, config: AlarmConfig) -> None:
        self.config = config
        self._states: dict[str, _InstantState] = {
            column: _InstantState(window=deque(maxlen=config.window_samples))
            for column in ZS_TARGETS
        }
        self._last_ts: str = ""     # 已处理样本的时间水位（QoS1 重投/乱序保护）
        self.stats: dict[str, int] = {
            "samples": 0,           # 已判定样本条数
            "duplicate": 0,         # 被时间水位挡下的重投/乱序样本条数
            "events": 0,            # 累计 START 事件数
            "ends": 0,
            "reminds": 0,
            "invalid_samples": 0,   # 折算为 nan 的样本条数（§3.5）
        }

    # ---- 5.1 状态恢复（§3.3「重启恢复」）----

    def open_points(self) -> list[str]:
        """当前处于 OPEN 状态的测点列表（重启恢复的可见性用）。"""
        return [column for column, state in self._states.items() if state.event_id]

    def restore(self, rows: Sequence[Sequence[Any]]) -> int:
        """从事件表折叠出各测点的最后状态；返回折叠的行数。

        rows 按 ts **升序**，每行 = (ts, event_id, phase, point, judge_type, over_samples,
        trigger_ts)。语义（§3.3「落库形态」）：
            - 最后一条 phase 是 `start`/`remind` → 该事件仍 OPEN，继续累计，**不重发 START**
            - 最后一条 phase 是 `end` → 已闭合
            - `invalid` 行**不改变** OPEN/CLOSED 状态（它不参与任何达标判定，§3.5）
        同时用全部行的最大 ts 作为时间水位，重投同一条报文不会改状态。
        恢复的 start_ts 从 event_id 里解析（`point|judge_type|start_ts`），
        last_notify_ts 用该行的 ts 近似（≤ 一个滑窗的误差，只影响 remind 的最早时刻）。
        """
        folded = 0
        for row in rows:
            ts = normalize_ts(row[0])
            event_id = str(row[1] or "")
            phase = str(row[2] or "")
            point = str(row[3] or "")
            judge_type = str(row[4] or "")
            over_samples = row[5] if len(row) > 5 else 0
            trigger_ts = normalize_ts(row[6]) if len(row) > 6 and row[6] else ts
            if ts > self._last_ts:
                self._last_ts = ts
            folded += 1
            if judge_type != JUDGE_INSTANT or point not in self._states:
                continue                       # hourly 事件不参与 instant 状态机
            if phase not in (PHASE_START, PHASE_REMIND, PHASE_END):
                continue                       # invalid：不改变 OPEN/CLOSED
            state = self._states[point]
            if phase in (PHASE_START, PHASE_REMIND):
                state.event_id = event_id
                state.start_ts = parse_event_start(event_id, fallback=trigger_ts or ts)
                state.last_notify_ts = ts
                state.over_samples = int(over_samples or 0)
                state.recover_count = 0
            else:
                self._close(state)
        return folded

    # ---- 5.2 逐条判定 ----

    def on_sample(
        self,
        ts: str,
        values: Mapping[str, float],
        references: Mapping[str, float],
    ) -> list[AlarmEvent]:
        """判一条样本，返回本次产生的事件行（0 条也正常）。

        `references` 必须是**数学层**折算值（`reference_values()` 的产物）：
        O2 >= 21% 时是 nan，本函数据此产 `phase=invalid`。
        时间水位：ts <= 已处理水位 ⇒ 视为重投/乱序，**不改状态、不产事件**（幂等）。
        """
        config = self.config
        if not config.enable:
            return []

        if self._last_ts and ts <= self._last_ts:
            self.stats["duplicate"] += 1
            return []
        self._last_ts = ts
        self.stats["samples"] += 1

        o2 = float(values[O2_FIELD])
        events: list[AlarmEvent] = []
        for column in ZS_TARGETS:
            name = COLUMN_TO_NAME[column]
            raw = float(values[name])
            limit = float(LIMITS[name])
            converted = float(references[column])

            if not math.isfinite(converted):
                # §3.5：折算算不出来（O2 >= 21%）→ 判"数据无效"
                #   不参与任何达标判定：不计越限、不计达标、不打断恢复计数
                self.stats["invalid_samples"] += 1
                events.append(self._make_event(
                    ts=ts, column=column, judge_type=JUDGE_INSTANT, phase=PHASE_INVALID,
                    converted=converted, raw=raw, o2=o2, limit=limit,
                    over_samples=0, window_seconds=0.0, trigger_ts=ts, event_start_ts=ts,
                    reason="折算值=nan（O2>=21%，分母<=0，折算无物理意义；按 §3.5 判数据无效）",
                ))
                continue

            events.extend(self._judge_instant(ts, column, converted, raw, o2, limit))
        return events

    def _judge_instant(
        self, ts: str, column: str, converted: float, raw: float, o2: float, limit: float,
    ) -> list[AlarmEvent]:
        """单测点的 instant 判据：滑窗计数触发 + 滞回恢复 + 周期提醒。"""
        config = self.config
        name = COLUMN_TO_NAME[column]
        state = self._states[column]

        # `>` 语义：正好等于限值算达标（ADR-0002 §2.2 3) 的边界取值教训）
        over = over_limit(name, converted)
        # 恢复用滞回：≤ 限值 × 恢复系数（默认 0.95）才算"回来了"
        recovered = converted <= limit * config.recover_ratio
        state.window.append((ts, over, converted, raw, o2))

        out: list[AlarmEvent] = []

        if state.event_id:
            # ---- 事件仍 OPEN：只累计 / 提醒 / 判恢复，绝不产生第二个 START ----
            if over:
                state.over_samples += 1
            elapsed = seconds_between(state.start_ts, ts)

            if (
                over
                and config.remind_seconds > 0
                and seconds_between(state.last_notify_ts, ts) >= config.remind_seconds
            ):
                state.last_notify_ts = ts
                self.stats["reminds"] += 1
                out.append(self._make_event(
                    ts=ts, column=column, judge_type=JUDGE_INSTANT, phase=PHASE_REMIND,
                    converted=converted, raw=raw, o2=o2, limit=limit,
                    over_samples=state.over_samples, window_seconds=elapsed,
                    trigger_ts=ts, event_start_ts=state.start_ts,
                    reason=f"事件仍 OPEN，距上次通知 >= {config.remind_seconds:.0f}s",
                ))

            state.recover_count = state.recover_count + 1 if recovered else 0
            if state.recover_count >= config.recover_samples:
                out.append(self._make_event(
                    ts=ts, column=column, judge_type=JUDGE_INSTANT, phase=PHASE_END,
                    converted=converted, raw=raw, o2=o2, limit=limit,
                    over_samples=state.over_samples, window_seconds=elapsed,
                    trigger_ts=ts, event_start_ts=state.start_ts,
                    reason=(
                        f"连续 {config.recover_samples} 条折算值 <= 限值×{config.recover_ratio}"
                        f"（{limit * config.recover_ratio:.4f}），事件结束"
                    ),
                ))
                self.stats["ends"] += 1
                self._close(state)
            return out

        # ---- IDLE → 触发判定：最近 M 条有效样本里至少 N 条越限（§3.2 的 B）----
        over_samples_in_window = [item for item in state.window if item[1]]
        if len(over_samples_in_window) >= config.min_over_samples:
            # ★ 事件开始 = 该事件**首个越限样本**；判定快照取**同一帧**的值，
            #   这样行内 ts / converted / raw_value / o2 相互一致，可拿去和 cems_data 对账
            #   （ADR-0002 §5 验收判据：拿 raw_value/o2/o2_reference 代入公式应与 converted 逐位一致）
            first_ts, _first_over, first_converted, first_raw, first_o2 = over_samples_in_window[0]
            state.event_id = make_event_id(column, JUDGE_INSTANT, first_ts)
            state.start_ts = first_ts
            state.last_notify_ts = ts
            state.over_samples = len(over_samples_in_window)
            state.recover_count = 0
            self.stats["events"] += 1
            out.append(self._make_event(
                ts=first_ts, column=column, judge_type=JUDGE_INSTANT, phase=PHASE_START,
                converted=first_converted, raw=first_raw, o2=first_o2, limit=limit,
                over_samples=state.over_samples, window_seconds=0.0,
                trigger_ts=ts, event_start_ts=first_ts,
                reason=(
                    f"滑窗触发：最近 {len(state.window)}/{config.window_samples} 条有效样本里 "
                    f"{len(over_samples_in_window)} 条越限 >= {config.min_over_samples}"
                    f"（触发时刻 {ts}）"
                ),
            ))
        return out

    @staticmethod
    def _close(state: _InstantState) -> None:
        """闭合事件并清空滑窗。

        ⚠️ 必须清窗：否则已 END 的那几条越限样本会留在窗口里，
          再来一条越限样本就立刻凑够 N 条重新触发（事件风暴）。
        """
        state.event_id = ""
        state.start_ts = ""
        state.last_notify_ts = ""
        state.over_samples = 0
        state.recover_count = 0
        state.window.clear()

    def _make_event(
        self,
        *,
        ts: str,
        column: str,
        judge_type: str,
        phase: str,
        converted: float,
        raw: float,
        o2: float,
        limit: float,
        over_samples: int,
        window_seconds: float,
        trigger_ts: str,
        event_start_ts: str,
        reason: str = "",
    ) -> AlarmEvent:
        """组装一条事件行。

        两个时间戳是**两件事**（ADR-0002 §3.3）：
            event_start_ts = 该事件**首个越限样本**的时间戳（进 event_id，事件的身份）
            trigger_ts     = 触发判定的那一帧（追溯用）
        判定快照（converted/raw_value/o2/limit_value/o2_reference）取**同一帧**的值。
        """
        return AlarmEvent(
            ts=ts,
            event_id=make_event_id(column, judge_type, event_start_ts),
            phase=phase,
            point=column,
            judge_type=judge_type,
            converted=converted,
            raw_value=raw,
            o2=o2,
            limit_value=limit,
            o2_reference=float(O2_REFERENCE),
            over_samples=int(over_samples),
            window_seconds=float(window_seconds),
            judge_version=self.config.judge_version,
            trigger_ts=trigger_ts,
            reason=reason,
        )

    # ---- 5.3 小时结算（§3.4 / §3.5）----

    def judge_hour(
        self,
        hour_start: str,
        samples: Sequence[tuple[str, float, Mapping[str, float]]],
    ) -> tuple[list[HourlyVerdict], list[AlarmEvent]]:
        """算一个小时的结论（纯函数式，输入 = 该小时的原始样本）。

        samples：按 ts 升序的 `(ts, o2, {列名: 标干值})`。
        ⚠️ 折算值在这里**重新调用契约函数** `to_reference_o2()` 算（折算不落库，
           所以只能重算；但全仓库仍然只有 points.py 一份公式）。
        三态判定（§3.4），覆盖率不足时**绝不判达标**：
            覆盖率 < 门限            → insufficient（同时产"数据不足"事件）
            无效样本占比 > 上限      → insufficient
            折算均值 > 限值          → over（产 judge_type=hourly 的超标事件）
            否则                     → ok

        ★ **折算不可用**有两条路径，都归入"无效样本"（§3.5），**不新造第四种状态**：
            ① O2 ≥ 21% ⇒ `to_reference_o2()` 返回 nan（分母 ≤ 0，原有语义）；
            ② 分母 `21 - O2` 小于 `ALARM_O2_DENOM_MIN`（默认 2.0）⇒ 放大倍数过大
               （O2=12% 时 1.67×、18% 时 5×、20% 时 15×、20.99% 时 1500×）。
               后果是**标干完全达标**的读数被折算放大到越过限值 ⇒ 小时判 over ⇒ 假超标。
               这类样本并入无效样本后：覆盖率与无效占比按既有门限走，
               无效占比超上限 ⇒ insufficient（既不判达标也不判超标），与①同一条路径。
        ⚠️ 逐条判定（instant 滑窗）**不改**：那里的语义是"单点折算值越限"，
           M/N、滞回、`_clear`、`>` 边界全部保持原样。
        """
        config = self.config
        expected = max(1, config.expected_samples_per_hour)
        n_total = len(samples)
        verdicts: list[HourlyVerdict] = []
        events: list[AlarmEvent] = []
        # 从库里读回来的 ts 要先规范化（REST 给的是 ISO 带 T/Z 的形态）
        last_ts = normalize_ts(samples[-1][0]) if samples else normalize_ts(hour_start)

        for column in ZS_TARGETS:
            name = COLUMN_TO_NAME[column]
            limit = float(LIMITS[name])
            valid: list[float] = []
            raw_sum = o2_sum = 0.0
            over_count = 0
            # 分母过小被判无效的样本数：只用于把原因写清楚，不参与任何判定
            floor_dropped = 0
            for _ts, o2, raw_values in samples:
                raw = float(raw_values[column])
                o2_value = float(o2)
                converted = to_reference_o2(raw, o2_value)
                raw_sum += raw
                o2_sum += o2_value
                # 折算是否可用于**达标判定**：先看数学可算性，再看分母下限（见 docstring ★）
                if not math.isfinite(converted):
                    continue
                if (O2_DRY_BASIS - o2_value) < config.o2_denominator_min:
                    floor_dropped += 1
                    continue
                valid.append(converted)
                if over_limit(name, converted):
                    over_count += 1

            n_valid = len(valid)
            n_invalid = n_total - n_valid
            # ⚠️ 覆盖率是**比率**，上限 1.0。
            # 实测：本机轮询实际比标称 5 秒略快（3600/722 ≈ 4.99 秒），
            # 一个满小时会有 721~722 行 > 标称 720 → 裸算会得到 1.0028 这种"100.28%"。
            # 那对外没有意义（比率不可能超过全部）。真实条数仍由 n_total/n_valid 如实保留，
            # 需要看"多采样了几条"就查那两列，不要看覆盖率。
            coverage = min(1.0, n_valid / expected)
            invalid_ratio = (n_invalid / n_total) if n_total else 1.0
            conv_mean = (sum(valid) / n_valid) if n_valid else float("nan")
            conv_max = max(valid) if n_valid else float("nan")
            raw_mean = (raw_sum / n_total) if n_total else float("nan")
            o2_mean = (o2_sum / n_total) if n_total else float("nan")

            if coverage < config.coverage_min or invalid_ratio > config.invalid_ratio_max:
                verdict = VERDICT_INSUFFICIENT
                reason = (
                    f"覆盖率 {coverage:.3f} < 门限 {config.coverage_min}"
                    if coverage < config.coverage_min
                    else f"无效样本占比 {invalid_ratio:.3f} > 上限 {config.invalid_ratio_max}"
                )
                # 分母过小是"无效样本"里最容易被误读为"现场数据没问题"的一类，指名道姓写出来
                if floor_dropped:
                    reason += (
                        f"（其中 {floor_dropped} 条因 21-O2 < {config.o2_denominator_min} "
                        "折算放大不可信，按无效样本处理）"
                    )
            elif conv_mean > limit:
                verdict = VERDICT_OVER
                reason = f"折算均值 {conv_mean:.4f} > 限值 {limit}，且覆盖率 {coverage:.3f} 达标"
            else:
                verdict = VERDICT_OK
                reason = f"折算均值 {conv_mean:.4f} <= 限值 {limit}，覆盖率 {coverage:.3f}"

            verdicts.append(HourlyVerdict(
                ts=hour_start, point=column, n_total=n_total, n_valid=n_valid,
                n_invalid=n_invalid, coverage=coverage, conv_mean=conv_mean,
                conv_max=conv_max, limit_value=limit, verdict=verdict,
                judge_version=config.judge_version,
            ))

            if verdict == VERDICT_OVER:
                # 小时超标事件：start = 小时起点，end = 该小时最后一条样本（避开下一个
                # 整点与"下一小时的 start"撞同一个 (子表, ts)）
                common = dict(
                    column=column, judge_type=JUDGE_HOURLY, converted=conv_mean,
                    raw=raw_mean, o2=o2_mean, limit=limit, over_samples=over_count,
                    window_seconds=HOUR_SECONDS,
                )
                events.append(self._make_event(
                    ts=hour_start, phase=PHASE_START, trigger_ts=hour_start,
                    event_start_ts=hour_start,
                    reason=f"小时折算均值超标（{reason}）", **common,
                ))
                events.append(self._make_event(
                    ts=last_ts, phase=PHASE_END, trigger_ts=last_ts,
                    event_start_ts=hour_start,
                    reason="小时结论闭合（end 落在该小时最后一条样本的 ts 上）", **common,
                ))
            elif verdict == VERDICT_INSUFFICIENT:
                events.append(self._make_event(
                    ts=hour_start, column=column, judge_type=JUDGE_HOURLY,
                    phase=PHASE_INVALID, converted=conv_mean, raw=raw_mean, o2=o2_mean,
                    limit=limit, over_samples=n_invalid, window_seconds=HOUR_SECONDS,
                    trigger_ts=hour_start, event_start_ts=hour_start,
                    reason=f"小时数据不足，不判达标也不判超标（{reason}）",
                ))
        return verdicts, events


# ==================== 6. 告警表的 DDL / DML 文本（纯字符串） ====================

@dataclass(frozen=True)
class AlarmTables:
    """告警表在 TDengine 里的位置与命名（子表名 = plant_device_point_judgetype）。

    ⚠️ 子表名规则来自 ADR-0002 §2.2 4)：同一 `(plant, device, point, judge_type)`
       的所有事件行都写进**同一张子表**；再靠 `(子表, ts)` 覆盖语义做幂等
       （实测本机 TDengine 3.3.6.13：同一子表写同一 ts，COUNT(*) 不变、值被覆盖）。
    """

    db: str
    plant: str
    device: str
    event_stable: str = "cems_alarm_event"
    verdict_stable: str = "cems_hourly_verdict"
    push_stable: str = "cems_alarm_push"
    data_stable: str = "cems_data"      # 小时结算回读原始行用的数据超级表

    # ---- 命名 ----
    def event_child(self, column: str, judge_type: str) -> str:
        return f"{self.plant}_{self.device}_{column}_{judge_type}".lower()

    def push_child(self, column: str, judge_type: str) -> str:
        return f"{self.plant}_{self.device}_{column}_{judge_type}_push".lower()

    def verdict_child(self, column: str) -> str:
        return f"{self.plant}_{self.device}_{column}".lower()

    # ---- DDL（幂等；沿用接入层 TdWriter 的"显式带库名前缀"写法）----
    def create_sql(self) -> list[str]:
        """三张超级表的建表语句（CREATE ... IF NOT EXISTS，可重复执行）。"""
        return [
            f"CREATE STABLE IF NOT EXISTS {self.db}.{self.event_stable} ("
            f"ts TIMESTAMP, event_id NCHAR(80), phase NCHAR(10), converted FLOAT, "
            f"raw_value FLOAT, o2 FLOAT, limit_value FLOAT, o2_reference FLOAT, "
            f"over_samples INT, window_seconds FLOAT, judge_version NCHAR(24), "
            f"trigger_ts TIMESTAMP"
            f") TAGS (plant NCHAR(20), device NCHAR(20), point NCHAR(16), judge_type NCHAR(12))",
            f"CREATE STABLE IF NOT EXISTS {self.db}.{self.verdict_stable} ("
            f"ts TIMESTAMP, n_total INT, n_valid INT, n_invalid INT, coverage FLOAT, "
            f"conv_mean FLOAT, conv_max FLOAT, limit_value FLOAT, verdict NCHAR(16)"
            f") TAGS (plant NCHAR(20), device NCHAR(20), point NCHAR(16))",
            f"CREATE STABLE IF NOT EXISTS {self.db}.{self.push_stable} ("
            f"ts TIMESTAMP, event_id NCHAR(80), phase NCHAR(10), channel NCHAR(16), "
            f"status NCHAR(16), payload NCHAR(256)"
            f") TAGS (plant NCHAR(20), device NCHAR(20), point NCHAR(16), judge_type NCHAR(12))",
        ]

    # ---- DML ----
    def event_insert_sql(self, event: AlarmEvent) -> str:
        """事件行 INSERT（幂等键 = (子表, ts)）。"""
        columns = (
            "ts, event_id, phase, converted, raw_value, o2, limit_value, o2_reference, "
            "over_samples, window_seconds, judge_version, trigger_ts"
        )
        values = ", ".join((
            sql_timestamp(event.ts),
            sql_text(event.event_id),
            sql_text(event.phase),
            sql_float(event.converted),
            sql_float(event.raw_value),
            sql_float(event.o2),
            sql_float(event.limit_value),
            sql_float(event.o2_reference),
            str(int(event.over_samples)),
            sql_float(event.window_seconds),
            sql_text(event.judge_version),
            sql_timestamp(event.trigger_ts),
        ))
        return (
            f"INSERT INTO {self.db}.{self.event_child(event.point, event.judge_type)} "
            f"USING {self.db}.{self.event_stable} "
            f"TAGS ({sql_text(self.plant)}, {sql_text(self.device)}, "
            f"{sql_text(event.point)}, {sql_text(event.judge_type)}) "
            f"({columns}) VALUES ({values})"
        )

    def push_insert_sql(
        self, event: AlarmEvent, channel: str, status: str = PUSH_STATUS_RECORDED,
    ) -> str:
        """推送记录 INSERT（幂等键 = (子表, ts)，与事件行一一对应）。"""
        columns = "ts, event_id, phase, channel, status, payload"
        values = ", ".join((
            sql_timestamp(event.ts),
            sql_text(event.event_id),
            sql_text(event.phase),
            sql_text(channel),
            sql_text(status),
            sql_text(event.payload_json()),
        ))
        return (
            f"INSERT INTO {self.db}.{self.push_child(event.point, event.judge_type)} "
            f"USING {self.db}.{self.push_stable} "
            f"TAGS ({sql_text(self.plant)}, {sql_text(self.device)}, "
            f"{sql_text(event.point)}, {sql_text(event.judge_type)}) "
            f"({columns}) VALUES ({values})"
        )

    def verdict_insert_sql(self, verdict: HourlyVerdict) -> str:
        """小时结论 INSERT（幂等键 = (子表, ts)，可安全重算覆盖）。"""
        columns = (
            "ts, n_total, n_valid, n_invalid, coverage, conv_mean, conv_max, "
            "limit_value, verdict"
        )
        values = ", ".join((
            sql_timestamp(verdict.ts),
            str(int(verdict.n_total)),
            str(int(verdict.n_valid)),
            str(int(verdict.n_invalid)),
            sql_float(verdict.coverage),
            sql_float(verdict.conv_mean),
            sql_float(verdict.conv_max),
            sql_float(verdict.limit_value),
            sql_text(verdict.verdict),
        ))
        return (
            f"INSERT INTO {self.db}.{self.verdict_child(verdict.point)} "
            f"USING {self.db}.{self.verdict_stable} "
            f"TAGS ({sql_text(self.plant)}, {sql_text(self.device)}, {sql_text(verdict.point)}) "
            f"({columns}) VALUES ({values})"
        )

    def restore_select_sql(self, limit: int = 400) -> str:
        """启动恢复用的查询：按 ts 升序取**本设备**最近 limit 条事件行（列序与 `AlarmJudge.restore` 一致）。

        ⚠️ **必须按 plant/device 过滤**（与 `hour_rows_sql` 同一理由）：
        `cems_alarm_event` 是多设备共用的超级表（TAG = `(plant, device, point, judge_type)`），
        而 `AlarmJudge.restore()` 是按 **point 名**（`dust`/`so2`/`nox`）折叠状态的 ——
        它没有设备维度。不过滤的话，**另一台设备的事件会被折进本实例的判定状态**：
        例如对方一条 `phase=start` 会让本设备某测点被误判为"仍 OPEN"，
        于是**真正的越限不会补发 START**；对方一条 `phase=end` 又可能把本设备
        正在累计的事件误标为已闭合。`_last_ts` 水位也会被对方的时间戳推高。

        ⚠️ 用子查询取"最近 N 条"再正序排：TDengine 的 ORDER BY ... DESC LIMIT 配
          子查询才拿得到最近的那一批（直接 ASC LIMIT 拿的是最早的一批）。
        """
        return (
            f"SELECT ts, event_id, phase, point, judge_type, over_samples, trigger_ts FROM ("
            f"SELECT ts, event_id, phase, point, judge_type, over_samples, trigger_ts "
            f"FROM {self.db}.{self.event_stable} "
            f"WHERE plant = {sql_text(self.plant)} AND device = {sql_text(self.device)} "
            f"ORDER BY ts DESC LIMIT {int(limit)}"
            f") ORDER BY ts ASC"
        )

    def hour_rows_sql(self, hour_start: str, hour_end: str) -> str:
        """取某小时的原始行（小时结算用；只取折算需要的列）。

        ⚠️ **必须按 plant/device 过滤**：`cems_data` 是多设备共用的超级表
        （每台设备一条子表，TAG 为 `(plant, device)`）。不过滤的话，
        第二台设备上线后**设备 1 的整点结算会把设备 2 的样本一起算进小时均值**，
        小时结论（ok/over/insufficient）直接算错 —— 后果是**告警误报或漏报**。

        plant/device 直接取自本对象的 TAG 字段，所以调用方（接入层 `settle_hour`）
        不需要额外传参：它构造 `AlarmTables` 时已经带上了这台设备的身份。
        """
        columns = ", ".join(("ts", "o2", *ZS_TARGETS))
        return (
            f"SELECT {columns} FROM {self.db}.{self.data_stable} "
            f"WHERE ts >= {sql_timestamp(hour_start)} AND ts < {sql_timestamp(hour_end)} "
            f"AND plant = {sql_text(self.plant)} AND device = {sql_text(self.device)} "
            f"ORDER BY ts ASC"
        )
