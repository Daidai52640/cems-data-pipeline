# -*- coding: utf-8 -*-
"""`src/common/alarm_judge.py` 的离线单元测试：事件状态机 + 小时三态 + SQL 文本。

============================ 这一组测试的边界 ============================
**纯逻辑、完全离线**：不连 TDengine（6041）、不连 EMQX（1883）、不读 `data/`、
不 `docker exec`、不依赖容器在跑。`alarm_judge` 本身不 import paho/taosrest，
所以这里**没有任何 mock/stub**：直接把样本喂给 `AlarmJudge` 断言语义，
SQL 只断言**生成的字符串**（不执行）。

被测口径（ADR-0002，出处见被测模块 docstring）：
    §3.1 判**折算值**（本模块不重算公式，吃 `points.to_reference_o2()` 的结果）
    §3.2 滑窗计数：最近 M 条**有效**样本里 >= N 条越限才触发
    §3.3 开始 = 首个越限样本的 ts；同一 (point, judge_type) 只允许一个 OPEN 事件；
         恢复 = 连续 K 条 <= 限值×恢复系数；事件表 append-only，幂等靠 (子表, ts) 覆盖
    §3.4 小时三态：覆盖率 < 门限 → insufficient（**绝不判达标**）
    §3.5 O2 >= 21% → 折算值 nan → 判"数据无效"（不是达标、不是超标）

⚠️ 断言按上述口径写死，不为"跑得通"放宽：本文件的目标是**发现偏差**，
   所以边界（刚好 N-1/N、刚好 K-1/K、刚好覆盖率门限、刚好等于限值）全部单独立项。
   鉴别力实测：把 `over_limit` 的 `>` 改成 `>=`、或把 `recover_count >= K` 改成 `> K`，
   本文件立刻有测试 FAIL（见交付报告）。
===========================================================================
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Mapping, Optional, Sequence

import pytest

from src.common.alarm_judge import (
    CHANNEL_RE,
    COLUMN_TO_NAME,
    EVENT_ID_SEP,
    JUDGE_HOURLY,
    JUDGE_INSTANT,
    JUDGE_VERSION_RE,
    PHASE_END,
    PHASE_INVALID,
    PHASE_REMIND,
    PHASE_START,
    TS_FORMAT,
    VERDICT_INSUFFICIENT,
    VERDICT_OK,
    VERDICT_OVER,
    AlarmConfig,
    AlarmEvent,
    AlarmJudge,
    AlarmTables,
    hour_start_of,
    make_event_id,
    normalize_ts,
    parse_event_start,
    seconds_between,
    sql_float,
    sql_text,
    sql_timestamp,
    to_datetime,
)
from src.common.points import (
    LIMITS,
    O2_REFERENCE,
    ZS_TARGETS,
    ZS_UNAVAILABLE_SENTINEL,
    over_limit,
    to_reference_o2,
)

# ===========================================================================
# 0. 夹具与工具（全部离线、确定性；不用随机数、不用真实 I/O）
# ===========================================================================

#: 折算基准氧 6% ⇒ O2=6.0 时折算值 == 标干值（比值 (21-6)/(21-6) = 1）。
#: 测试用它与"比值 1.0"性质，让断言可以直接写数学结论。
O2_NEUTRAL: float = O2_REFERENCE

DUST_LIMIT: float = LIMITS["Dust"]
SO2_LIMIT: float = LIMITS["SO2"]
NOX_LIMIT: float = LIMITS["NOx"]

#: 明显越限 / 明显达标 的浓度（按取整值造样本，比值 1.0 时即折算值本身）
DUST_OVER: float = 6.0        # > 5.0
DUST_CLEAN: float = 4.0       # <= 5.0 × 0.95 = 4.75（既达标又满足恢复滞回）
SO2_OVER: float = 40.0        # > 35.0
SO2_CLEAN: float = 30.0       # <= 35.0 × 0.95 = 33.25
NOX_OVER: float = 60.0        # > 50.0
NOX_CLEAN: float = 45.0       # <= 50.0 × 0.95 = 47.5

#: 折算算不出来的氧含量（分母 21 - 21 = 0 ⇒ to_reference_o2 返回 nan）
O2_UNAVAILABLE: float = 21.0

#: 测试里的落库位置；子表名/标签都会被拼进 SQL，断言时按它算期望值。
TABLES: AlarmTables = AlarmTables(db="cems", plant="plant1", device="cems1")

#: 从契约派生（不手抄测点名，契约改了这里跟着改）
O2_KEY: str = COLUMN_TO_NAME["o2"]
DUST_KEY: str = COLUMN_TO_NAME["dust"]
SO2_KEY: str = COLUMN_TO_NAME["so2"]
NOX_KEY: str = COLUMN_TO_NAME["nox"]


def tight_config(**overrides: object) -> AlarmConfig:
    """一份**小参数**配置：滑窗 3 条里 2 条越限触发、连续 2 条恢复结束。

    小参数不是为了"好过"，而是为了把边界（N-1/N、K-1/K）逐条摆在断言里；
    默认参数（6/3/6）另有一条专门的测试守住。
    """
    base = AlarmConfig(
        window_samples=3,
        min_over_samples=2,
        recover_samples=2,
        recover_ratio=0.95,
        remind_seconds=1800.0,
        poll_interval=5.0,
        judge_version="vtest",
        push_channel="log",
    )
    return replace(base, **overrides)  # type: ignore[arg-type]


def sample(
    *,
    clock: Optional[datetime] = None,
    step: int = 5,
    dust: float = DUST_CLEAN,
    so2: float = SO2_CLEAN,
    nox: float = NOX_CLEAN,
    o2: float = O2_NEUTRAL,
) -> tuple[str, dict[str, float], dict[str, float]]:
    """造一条样本：返回 `(ts, values, references)`。

    `values` / `references` 按接入层真实形态构造（键分别是 MQTT 字段名 / TDengine 列名），
    折算值走契约函数 `points.to_reference_o2()` —— 测试**不自己重算公式**。
    `clock=None` 表示 ts 由显式字符串给出时用（本函数此时抛错，用 `at()` 或直接传字符串）。
    """
    if clock is None:
        raise ValueError("sample() 需要显式 clock；只要 ts 字符串时用 at()")
    return at(clock, dust=dust, so2=so2, nox=nox, o2=o2)


def at(clock: datetime, **concentrations: float) -> tuple[str, dict[str, float], dict[str, float]]:
    """按整点时间造一条样本；`concentrations` 只写关心的测点（其余取默认值）。"""
    ts = clock.strftime(TS_FORMAT)
    values: dict[str, float] = {
        "Flow": 10000.0,
        DUST_KEY: concentrations.get("dust", DUST_CLEAN),
        SO2_KEY: concentrations.get("so2", SO2_CLEAN),
        NOX_KEY: concentrations.get("nox", NOX_CLEAN),
        O2_KEY: concentrations.get("o2", O2_NEUTRAL),
        "Velocity": 10.0,
        "Temp": 120.0,
        "Humidity": 8.0,
        "Pressure": 100.0,
    }
    o2 = values[O2_KEY]
    references: dict[str, float] = {column: 0.0 for column in ZS_TARGETS}
    for column in ZS_TARGETS:
        references[column] = to_reference_o2(values[COLUMN_TO_NAME[column]], o2)
    return ts, values, references


def clean_samples(count: int, start: datetime, step: int = 5) -> list[tuple[str, dict, dict]]:
    """连续的达标样本（既用于"不触发"的负例，也用于恢复/水位推进）。"""
    return [at(start + timedelta(seconds=step * index)) for index in range(count)]


def feed(
    judge: AlarmJudge,
    batch: Sequence[tuple[str, Mapping[str, float], Mapping[str, float]]],
) -> list[AlarmEvent]:
    """把一批样本依次喂给判据，收集全部事件行（保持顺序）。"""
    out: list[AlarmEvent] = []
    for ts, values, references in batch:
        out.extend(judge.on_sample(ts, values, references))
    return out


def only(events: Sequence[AlarmEvent], point: str) -> list[AlarmEvent]:
    """取出某个测点的事件行（多测点并行判定时用来去掉噪声）。"""
    return [event for event in events if event.point == point]


def phases(events: Sequence[AlarmEvent]) -> list[str]:
    return [event.phase for event in events]


# 时间轴公共起点：2026-10-02 10:00:00（北京时间，与 docs 里的实测时刻同源）
T0: datetime = datetime(2026, 10, 2, 10, 0, 0)


# ===========================================================================
# 1. 滑窗触发（§3.2）：最近 M 条有效样本里 >= N 条越限
# ===========================================================================

class TestSlidingWindowTrigger:
    def test_n_minus_one_over_samples_does_not_trigger(self) -> None:
        """窗口 M=3、门限 N=2：2 条越限的窗口里只有 N-1 条 → 不触发。"""
        judge = AlarmJudge(tight_config(window_samples=4, min_over_samples=3))
        events = feed(judge, [
            at(T0, dust=DUST_OVER),
            at(T0 + timedelta(seconds=5), dust=DUST_OVER),
            at(T0 + timedelta(seconds=10), dust=DUST_CLEAN),
        ])
        assert only(events, "dust") == []
        assert judge.stats["events"] == 0
        assert judge.open_points() == []

    def test_exactly_n_over_samples_triggers(self) -> None:
        """刚好 N 条越限（第 N 条到达时）→ 触发 START。"""
        judge = AlarmJudge(tight_config())          # M=3, N=2
        events = feed(judge, [
            at(T0, dust=DUST_OVER),
            at(T0 + timedelta(seconds=5), dust=DUST_CLEAN),
            at(T0 + timedelta(seconds=10), dust=DUST_OVER),
        ])
        starts = [event for event in only(events, "dust") if event.phase == PHASE_START]
        assert len(starts) == 1
        assert judge.stats["events"] == 1

    def test_start_ts_is_first_over_limit_sample_not_trigger_frame(self) -> None:
        """START 的 ts / event_id 用**首个越限样本**，trigger_ts 才是触发帧。"""
        judge = AlarmJudge(tight_config())
        first_over = T0
        trigger_frame = T0 + timedelta(seconds=10)
        events = feed(judge, [
            at(first_over, dust=DUST_OVER),
            at(T0 + timedelta(seconds=5), dust=DUST_CLEAN),
            at(trigger_frame, dust=DUST_OVER),
        ])
        start = only(events, "dust")[0]
        assert start.phase == PHASE_START
        assert start.ts == first_over.strftime(TS_FORMAT)
        assert start.trigger_ts == trigger_frame.strftime(TS_FORMAT)
        assert start.ts != start.trigger_ts           # 两个时间戳是两件事（§3.3）
        assert start.event_id == make_event_id("dust", JUDGE_INSTANT, start.ts)
        # 事件身份可逆：能从 event_id 解析回 start_ts（重启恢复靠它）
        assert parse_event_start(start.event_id) == start.ts
        # 快照取的是**首个越限样本那一帧**的值，因此行内自洽可对账
        assert start.converted == pytest.approx(DUST_OVER)
        assert start.raw_value == DUST_OVER
        assert start.o2 == O2_NEUTRAL
        assert start.limit_value == DUST_LIMIT
        assert start.o2_reference == O2_REFERENCE
        assert start.over_samples == 2                 # 窗口里已有 2 条越限
        assert start.window_seconds == 0.0             # START 时事件刚开始
        assert start.judge_version == "vtest"

    def test_window_is_sliding_not_cumulative(self) -> None:
        """越限样本必须落在**最近 M 条有效样本**里；滑出窗口就不算（M=3）。"""
        judge = AlarmJudge(tight_config())            # M=3, N=2
        events = feed(judge, [
            at(T0, dust=DUST_OVER),                                  # 越限①
            at(T0 + timedelta(seconds=5), dust=DUST_CLEAN),
            at(T0 + timedelta(seconds=10), dust=DUST_CLEAN),
            # 此时窗口 = [越限①, 达标, 达标]；再进两条达标 ⇒ 越限① 被挤出窗口
            at(T0 + timedelta(seconds=15), dust=DUST_CLEAN),
            at(T0 + timedelta(seconds=20), dust=DUST_CLEAN),
        ])
        assert only(events, "dust") == []
        assert judge.stats["events"] == 0

    def test_default_contract_six_three(self) -> None:
        """默认参数（6/3）是 ADR-0002 §2.2 4) 的那一组：5 条越限不够、3 条够。"""
        assert (AlarmConfig().window_samples, AlarmConfig().min_over_samples) == (6, 3)
        judge = AlarmJudge(AlarmConfig())
        events = feed(judge, [
            at(T0 + timedelta(seconds=5 * index), dust=DUST_OVER) for index in range(6)
        ])
        starts = [event for event in only(events, "dust") if event.phase == PHASE_START]
        assert len(starts) == 1
        assert starts[0].ts == T0.strftime(TS_FORMAT)          # 首个越限样本（第 1 条）

    def test_invalid_samples_do_not_fill_window(self) -> None:
        """折算为 nan 的样本**不进窗口**：M 条窗口只由有效样本填满（§3.5）。"""
        judge = AlarmJudge(tight_config())            # M=3, N=2
        events = feed(judge, [
            at(T0, dust=DUST_OVER),                                       # 有效·越限①
            at(T0 + timedelta(seconds=5), dust=DUST_OVER, o2=O2_UNAVAILABLE),  # nan
            at(T0 + timedelta(seconds=10), dust=DUST_OVER),               # 有效·越限②
        ])
        starts = [event for event in only(events, "dust") if event.phase == PHASE_START]
        assert len(starts) == 1
        assert starts[0].over_samples == 2                             # 只数了 2 条有效越限
        assert judge.stats["invalid_samples"] == 3                     # 三个测点各一条无效

    def test_all_points_judged_independently(self) -> None:
        """同一条样本上 dust/so2/nox 各自独立判定；Flow/O2 等**不在判定集合**里。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [at(T0, dust=DUST_OVER, so2=SO2_CLEAN, nox=NOX_OVER)])
        assert sorted(event.point for event in events) == ["dust", "nox"]
        # Flow 的量程上限就是它的 limit，绝不参与排放判定
        assert ZS_TARGETS == ("dust", "so2", "nox")
        assert "flow" not in {event.point for event in events}


# ===========================================================================
# 2. 时间水位：重投 / 乱序 / 幂等（§3.3 的 append-only + (子表, ts) 覆盖）
# ===========================================================================

class TestWatermarkAndIdempotency:
    def test_duplicate_ts_ignored_by_watermark(self) -> None:
        """同一个 ts 再来一次（QoS1 重投）→ 不改状态、不产事件、dedup 计数 +1。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        ts = T0.strftime(TS_FORMAT)
        _, values, references = at(T0, dust=DUST_OVER)

        first = judge.on_sample(ts, values, references)
        assert phases(first) == [PHASE_START]
        assert judge.stats["samples"] == 1

        second = judge.on_sample(ts, values, references)
        assert second == []                                   # 第二条 START 绝不允许
        assert judge.stats["duplicate"] == 1
        assert judge.stats["samples"] == 1
        assert judge.stats["events"] == 1

    def test_out_of_order_old_ts_ignored(self) -> None:
        """乱序回退的时间戳被水位挡下（不会把窗口搞乱）。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        feed(judge, [at(T0 + timedelta(seconds=60), dust=DUST_CLEAN)])
        events = judge.on_sample(*at(T0, dust=DUST_OVER))     # 比水位还旧
        assert events == []
        assert judge.stats["duplicate"] == 1

    def test_replaying_same_batch_is_idempotent(self) -> None:
        """幂等（评估点名处）：同一批样本重放 → 事件行数不变、没有第二个 START。

        模拟场景：接入层重连后 broker 补投同一批 QoS1 报文。
        """
        config = tight_config(window_samples=1, min_over_samples=1)
        batch = [
            at(T0, dust=DUST_OVER),
            at(T0 + timedelta(seconds=5), dust=DUST_OVER),
            at(T0 + timedelta(seconds=10), dust=DUST_CLEAN),
            at(T0 + timedelta(seconds=15), dust=DUST_CLEAN),   # 连续 2 条 → END
        ]

        judge = AlarmJudge(config)
        original = feed(judge, batch)
        assert phases(original) == [PHASE_START, PHASE_END]
        assert judge.open_points() == []

        # ---- 同一个判据实例：整批重放 ----
        replay_same = feed(judge, batch)
        assert replay_same == []
        assert judge.stats["duplicate"] == len(batch)
        assert judge.stats["events"] == 1
        assert judge.stats["ends"] == 1

        # ---- 重启后的新实例（水位从零）：水位只挡"ts <= 已处理水位"，
        #      所以重放会重算事件行；但行内容必须逐位一致 —— 这就是 (子表, ts) 覆盖
        #      幂等的全部前提：同 (子表, ts) 写同样的值。
        restarted = AlarmJudge(config)
        replay_new = feed(restarted, batch)
        for first, again in zip(original, replay_new):
            assert first == again                              # frozen dataclass 逐字段相等
        assert [event.event_id for event in replay_new] == [
            event.event_id for event in original
        ]
        assert phases(replay_new).count(PHASE_START) == 1       # 每个事件只一个 START
        assert len({event.event_id for event in replay_new if event.phase == PHASE_START}) == 1

    def test_replay_does_not_resurrect_closed_event(self) -> None:
        """事件已 END 后，重放/继续来越限样本必须重新走 M 条滑窗，不能立刻再 START。"""
        judge = AlarmJudge(tight_config())                    # M=3, N=2
        events = feed(judge, [
            at(T0, dust=DUST_OVER),
            at(T0 + timedelta(seconds=5), dust=DUST_OVER),    # → START
            at(T0 + timedelta(seconds=10), dust=DUST_CLEAN),
            at(T0 + timedelta(seconds=15), dust=DUST_CLEAN),  # → END（连续 2 条恢复）
        ])
        assert phases(only(events, "dust")) == [PHASE_START, PHASE_END]

        # END 之后窗口必须被清空：单条越限样本不足以触发（否则就是事件风暴）
        after = feed(judge, [at(T0 + timedelta(seconds=20), dust=DUST_OVER)])
        assert only(after, "dust") == []
        assert judge.stats["events"] == 1

    def test_disabled_judge_returns_nothing(self) -> None:
        """ALARM_ENABLE=0：判定整条短路（只入库、只算折算值）。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1, enable=False))
        events = feed(judge, [at(T0, dust=DUST_OVER)])
        assert events == []
        assert judge.stats["samples"] == 0
        assert judge.open_points() == []


# ===========================================================================
# 3. 滞回恢复（§3.3）：连续 K 条 <= 限值 × 恢复系数 才结束
# ===========================================================================

class TestHysteresisRecovery:
    def _open_and_get_start(self, judge: AlarmJudge, start: datetime) -> AlarmEvent:
        events = feed(judge, [
            at(start, dust=DUST_OVER),
            at(start + timedelta(seconds=5), dust=DUST_OVER),
        ])
        starts = [event for event in only(events, "dust") if event.phase == PHASE_START]
        assert len(starts) == 1
        return starts[0]

    def test_k_minus_one_recovery_samples_do_not_end(self) -> None:
        """连续 K-1 条恢复不结束（K=2 → 1 条不算）。"""
        judge = AlarmJudge(tight_config())
        start = self._open_and_get_start(judge, T0)
        events = feed(judge, [at(T0 + timedelta(seconds=10), dust=DUST_CLEAN)])
        assert only(events, "dust") == []
        assert judge.open_points() == ["dust"]
        assert judge._states["dust"].event_id == start.event_id

    def test_exactly_k_recovery_samples_end(self) -> None:
        """连续 K 条恢复 → END，且 END 行沿用同一个 event_id。"""
        judge = AlarmJudge(tight_config())
        start = self._open_and_get_start(judge, T0)
        events = feed(judge, [
            at(T0 + timedelta(seconds=10), dust=DUST_CLEAN),
            at(T0 + timedelta(seconds=15), dust=DUST_CLEAN),
        ])
        ends = [event for event in only(events, "dust") if event.phase == PHASE_END]
        assert len(ends) == 1
        assert ends[0].event_id == start.event_id                  # 同一事件的身份
        assert ends[0].window_seconds == pytest.approx(15.0)       # 事件持续 15s
        assert ends[0].over_samples == 2                            # 累计越限样本数
        assert judge.stats["ends"] == 1
        assert judge.open_points() == []

    def test_over_limit_in_the_middle_resets_recovery_counter(self) -> None:
        """恢复序列中间插一条越限 → 计数清零，必须重新连续 K 条。"""
        judge = AlarmJudge(tight_config())                          # K=2
        self._open_and_get_start(judge, T0)
        events = feed(judge, [
            at(T0 + timedelta(seconds=10), dust=DUST_CLEAN),        # 恢复 1
            at(T0 + timedelta(seconds=15), dust=DUST_OVER),         # 打断 → 归零
            at(T0 + timedelta(seconds=20), dust=DUST_CLEAN),        # 恢复 1（重启计数）
        ])
        assert [event.phase for event in only(events, "dust")] == []  # 尚未结束
        assert judge.open_points() == ["dust"]

        events = feed(judge, [at(T0 + timedelta(seconds=25), dust=DUST_CLEAN)])
        assert phases(only(events, "dust")) == [PHASE_END]

    def test_equal_to_limit_does_not_count_as_over(self) -> None:
        """限值比较是 `>`（§2.2 3) 的边界取值教训）：正好等于限值**不能**算越限。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [at(T0, dust=DUST_LIMIT)])            # 5.0 == limit
        assert only(events, "dust") == []
        assert judge.stats["events"] == 0

        # 而略高于限值就必须触发（证明上面不是"永远不触发"）。
        # ⚠️ 不能用 math.nextafter(5.0)：折算比值正好是 1.0，但 (21-6)/(21-6) 在浮点里
        #    不精确等于 1，5.0×比值 会被舍回 5.0 —— 那是**浮点表示**问题，不是判据问题。
        #    这里用明确大于限值的值，避免把浮点噪声写成"判据要求"。
        just_over = DUST_LIMIT * 1.0001
        judge2 = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events2 = feed(judge2, [at(T0, dust=just_over)])
        assert phases(only(events2, "dust")) == [PHASE_START]
        assert events2[0].converted > DUST_LIMIT

    def test_hysteresis_band_between_ratio_and_limit_is_neither(self) -> None:
        """滞回带：限值×系数 < 值 <= 限值 ⇒ 既不越限、也不算"回来了"。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        self._open_and_get_start(judge, T0)
        band_value = DUST_LIMIT * 0.97                             # 4.85 ∈ (4.75, 5.0]
        events = feed(judge, [
            at(T0 + timedelta(seconds=10), dust=band_value),
            at(T0 + timedelta(seconds=15), dust=band_value),
            at(T0 + timedelta(seconds=20), dust=band_value),
        ])
        assert only(events, "dust") == []                          # 永远结束不了
        assert judge.open_points() == ["dust"]

    def test_recover_ratio_boundary_is_inclusive(self) -> None:
        """恢复判据是 `<= 限值×系数`：正好等于门限算恢复。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        self._open_and_get_start(judge, T0)
        threshold = DUST_LIMIT * 0.95                              # 4.75
        events = feed(judge, [
            at(T0 + timedelta(seconds=10), dust=threshold),
            at(T0 + timedelta(seconds=15), dust=threshold),
        ])
        assert phases(only(events, "dust")) == [PHASE_END]

    def test_invalid_sample_does_not_break_recovery_chain(self) -> None:
        """§3.5：无效样本**不打断恢复计数**，但也**不算**一条恢复。"""
        judge = AlarmJudge(tight_config())                          # K=2
        self._open_and_get_start(judge, T0)
        # 1 条恢复 + 1 条 nan（不算恢复也不清零）→ 仍只有 1 条连续恢复
        events = feed(judge, [
            at(T0 + timedelta(seconds=10), dust=DUST_CLEAN),
            at(T0 + timedelta(seconds=15), dust=DUST_CLEAN, o2=O2_UNAVAILABLE),
        ])
        # 无效样本照常产 invalid 行，但它不参与状态机
        assert phases(only(events, "dust")) == [PHASE_INVALID]
        assert judge.open_points() == ["dust"]
        assert judge._states["dust"].recover_count == 1             # 计数没被清零
        # 再来 1 条恢复 ⇒ 凑满连续 2 条
        events = feed(judge, [at(T0 + timedelta(seconds=20), dust=DUST_CLEAN)])
        assert phases(only(events, "dust")) == [PHASE_END]

    def test_remind_after_interval_while_still_open(self) -> None:
        """事件仍 OPEN 且距上次通知 >= remind_seconds → 产 REMIND（不是第二个 START）。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [at(T0, dust=DUST_OVER)])
        assert phases(only(events, "dust")) == [PHASE_START]

        # 30 分钟内（< 1800s）不提醒
        before = feed(judge, [
            at(T0 + timedelta(seconds=300), dust=DUST_OVER),
            at(T0 + timedelta(seconds=1200), dust=DUST_OVER),
        ])
        assert phases(only(before, "dust")) == []

        # 第 1800 秒整 → 提醒
        events = feed(judge, [at(T0 + timedelta(seconds=1800), dust=DUST_OVER)])
        reminds = [event for event in only(events, "dust") if event.phase == PHASE_REMIND]
        assert len(reminds) == 1
        assert reminds[0].event_id == events[0].event_id == only(events, "dust")[0].event_id
        assert reminds[0].trigger_ts == (T0 + timedelta(seconds=1800)).strftime(TS_FORMAT)
        assert reminds[0].window_seconds == pytest.approx(1800.0)
        assert judge.stats["reminds"] == 1
        assert judge.stats["events"] == 1                          # 没有第二个 START

    def test_remind_disabled_when_interval_zero(self) -> None:
        judge = AlarmJudge(tight_config(
            window_samples=1, min_over_samples=1, remind_seconds=0.0,
        ))
        events = feed(judge, [
            at(T0, dust=DUST_OVER),
            at(T0 + timedelta(seconds=7200), dust=DUST_OVER),
        ])
        assert phases(only(events, "dust")) == [PHASE_START]
        assert judge.stats["reminds"] == 0


# ===========================================================================
# 4. O2 >= 21%（§3.5）：判"数据无效"，既不是达标也不是超标
# ===========================================================================

class TestO2Unavailable:
    def test_o2_at_21_percent_yields_invalid_not_start(self) -> None:
        """O2 == 21% ⇒ 折算 nan ⇒ phase=invalid；连续给 5 条也绝不触发 START。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [
            at(T0 + timedelta(seconds=5 * index), dust=DUST_OVER, o2=O2_UNAVAILABLE)
            for index in range(5)
        ])
        assert phases(events) == [PHASE_INVALID] * 15              # 3 个测点 × 5 条
        assert PHASE_START not in phases(events)
        assert PHASE_END not in phases(events)
        assert PHASE_REMIND not in phases(events)
        assert judge.stats["events"] == 0
        assert judge.stats["invalid_samples"] == 15
        assert judge.open_points() == []

    def test_invalid_event_shape_and_reason(self) -> None:
        """invalid 行的契约：converted=nan、event_id 可复现、reason 说明按 §3.5 判无效。"""
        judge = AlarmJudge(tight_config())
        events = feed(judge, [at(T0, dust=DUST_OVER, o2=O2_UNAVAILABLE)])
        invalid = only(events, "dust")[0]
        assert invalid.phase == PHASE_INVALID
        assert invalid.judge_type == JUDGE_INSTANT
        assert math.isnan(invalid.converted)                        # 不是哨兵值
        assert invalid.raw_value == DUST_OVER                       # 标干值如实保留
        assert invalid.o2 == O2_UNAVAILABLE
        assert invalid.ts == T0.strftime(TS_FORMAT)
        assert invalid.event_id == make_event_id("dust", JUDGE_INSTANT, invalid.ts)
        assert "无效" in invalid.reason

    def test_sentinel_never_enters_judgement(self) -> None:
        """哨兵值（+9999.99）**只用于出站**；一旦混进判定就会被判成严重超标。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [at(T0, dust=ZS_UNAVAILABLE_SENTINEL, o2=O2_UNAVAILABLE)])
        # 拿来判的就是 nan（数学层），所以结论是"数据无效"
        assert phases(only(events, "dust")) == [PHASE_INVALID]
        assert math.isnan(only(events, "dust")[0].converted)
        # 反证：若误用哨兵参与比较，它大于任何限值
        assert ZS_UNAVAILABLE_SENTINEL > DUST_LIMIT
        assert over_limit("Dust", ZS_UNAVAILABLE_SENTINEL) is True

    def test_o2_below_21_still_judged(self) -> None:
        """O2 略低于 21%（分母仍 > 0）⇒ 照常折算、照常判定（不能一刀切当无效）。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [at(T0, dust=DUST_OVER, o2=20.9)])
        assert phases(only(events, "dust")) == [PHASE_START]
        expected = DUST_OVER * (21.0 - O2_REFERENCE) / (21.0 - 20.9)
        assert events[0].converted == pytest.approx(expected)
        assert judge.stats["invalid_samples"] == 0

    def test_all_o2_unavailable_hour_is_insufficient_not_over(self) -> None:
        """全小时折算不出来 ⇒ 无效样本占比超上限 ⇒ insufficient（绝不判超标/达标）。"""
        hour = datetime(2026, 10, 2, 9, 0, 0)
        samples = [
            (ts, O2_UNAVAILABLE, {column: DUST_OVER for column in ZS_TARGETS})
            for ts, _values, _refs in clean_samples(720, hour)
        ]
        judge = AlarmJudge(tight_config())
        verdicts, events = judge.judge_hour(hour.strftime(TS_FORMAT), samples)

        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.verdict == VERDICT_INSUFFICIENT
        assert dust.n_valid == 0
        assert dust.n_invalid == 720
        assert math.isnan(dust.conv_mean)
        assert math.isnan(dust.conv_max)
        assert VERDICT_OVER not in {verdict.verdict for verdict in verdicts}
        # 只产"数据不足"的 invalid 行，不产小时超标事件
        assert {event.phase for event in events} == {PHASE_INVALID}


# ===========================================================================
# 5. 小时三态（§3.4）：ok / over / insufficient 的边界
# ===========================================================================

class TestHourlyVerdict:
    HOUR: datetime = datetime(2026, 10, 2, 9, 0, 0)

    @staticmethod
    def make_samples(
        count: int,
        *,
        hour: datetime,
        o2: float = O2_NEUTRAL,
        dust: float = DUST_CLEAN,
        so2: float = SO2_CLEAN,
        nox: float = NOX_CLEAN,
    ) -> list[tuple[str, float, Mapping[str, float]]]:
        """按 ts 升序造 count 条原始行（形态与 `hour_rows_sql` 回读的一致）。"""
        return [
            (
                (hour + timedelta(seconds=5 * index)).strftime(TS_FORMAT),
                o2,
                {column: value for column, value in (
                    ("dust", dust), ("so2", so2), ("nox", nox),
                )},
            )
            for index in range(count)
        ]

    def test_coverage_exactly_at_threshold_is_ok(self) -> None:
        """覆盖率**正好等于**门限 0.75 ⇒ 不算不足（判据是 `< 门限`）。

        720 条/小时 ⇒ 540 条有效正好 0.750000；其中 270 条无效（占比 0.375）
        会触发"无效占比超上限"，所以这里用**满量有效样本**来测覆盖率门限本身。
        """
        samples = self.make_samples(540, hour=self.HOUR)
        judge = AlarmJudge(tight_config())
        verdicts, _events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), samples)
        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.coverage == pytest.approx(0.75)
        assert dust.verdict == VERDICT_OK

    def test_coverage_just_below_threshold_is_insufficient(self) -> None:
        """覆盖率 539/720 = 0.74861 < 0.75 ⇒ insufficient（绝不判达标）。"""
        samples = self.make_samples(539, hour=self.HOUR)
        judge = AlarmJudge(tight_config())
        verdicts, events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), samples)
        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.coverage < 0.75
        assert dust.verdict == VERDICT_INSUFFICIENT
        # insufficient 产 invalid 事件（"数据不足"），绝不产 ok
        assert VERDICT_OK not in {verdict.verdict for verdict in verdicts}
        assert all(event.phase == PHASE_INVALID for event in events)

    def test_empty_samples_is_insufficient(self) -> None:
        """空样本（库里这小时一行没有）⇒ 三态里的 insufficient。"""
        judge = AlarmJudge(tight_config())
        verdicts, events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), [])
        assert {verdict.verdict for verdict in verdicts} == {VERDICT_INSUFFICIENT}
        for verdict in verdicts:
            assert verdict.n_total == 0
            assert verdict.n_valid == 0
            assert verdict.coverage == 0.0
            assert math.isnan(verdict.conv_mean)
        assert len(events) == len(ZS_TARGETS)
        assert all(event.phase == PHASE_INVALID for event in events)

    def test_mean_over_limit_is_over_with_two_hourly_events(self) -> None:
        """折算均值 > 限值 ⇒ over，并产 judge_type=hourly 的 START + END 两行。"""
        samples = self.make_samples(720, hour=self.HOUR, dust=DUST_OVER)
        judge = AlarmJudge(tight_config())
        verdicts, events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), samples)

        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.verdict == VERDICT_OVER
        assert dust.n_total == 720
        assert dust.n_valid == 720
        assert dust.coverage == 1.0                                # 720/720
        assert dust.conv_mean == pytest.approx(DUST_OVER)
        assert dust.conv_max == pytest.approx(DUST_OVER)
        assert dust.limit_value == DUST_LIMIT

        hourly = [
            event for event in events
            if event.point == "dust" and event.judge_type == JUDGE_HOURLY
        ]
        assert phases(hourly) == [PHASE_START, PHASE_END]
        assert hourly[0].ts == self.HOUR.strftime(TS_FORMAT)        # start = 小时起点
        expected_end = (self.HOUR + timedelta(seconds=5 * 719)).strftime(TS_FORMAT)
        assert hourly[1].ts == expected_end                         # end = 最后一条样本
        assert hourly[0].event_id == hourly[1].event_id             # 同一事件
        assert hourly[0].window_seconds == 3600.0
        assert hourly[0].over_samples == 720
        assert hourly[0].converted == pytest.approx(DUST_OVER)      # 快照 = 小时均值

    def test_mean_equal_to_limit_is_ok_not_over(self) -> None:
        """小时判据同样是 `>`：折算均值**正好等于限值** ⇒ ok（§2.2 3) 的边界教训）。"""
        samples = self.make_samples(720, hour=self.HOUR, dust=DUST_LIMIT)
        judge = AlarmJudge(tight_config())
        verdicts, events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), samples)
        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.conv_mean == pytest.approx(DUST_LIMIT)
        assert dust.verdict == VERDICT_OK
        assert [event for event in events if event.point == "dust"] == []

    def test_mean_just_above_limit_is_over(self) -> None:
        samples = self.make_samples(720, hour=self.HOUR, dust=math.nextafter(DUST_LIMIT, math.inf))
        judge = AlarmJudge(tight_config())
        verdicts, _events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), samples)
        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.verdict == VERDICT_OVER

    def test_invalid_ratio_above_max_is_insufficient(self) -> None:
        """覆盖率够但无效样本占比 > 0.10 ⇒ insufficient（第二条兜底判据）。"""
        samples = self.make_samples(720, hour=self.HOUR)
        # 把 100 条改成无效（100/720 = 0.1389 > 0.10）
        mixed = [
            (ts, O2_UNAVAILABLE if index < 100 else o2, values)
            for index, (ts, o2, values) in enumerate(samples)
        ]
        judge = AlarmJudge(tight_config())
        verdicts, _events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), mixed)
        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.n_invalid == 100
        assert dust.coverage == pytest.approx(620 / 720)
        assert dust.verdict == VERDICT_INSUFFICIENT

    def test_invalid_ratio_exactly_at_max_is_not_insufficient(self) -> None:
        """无效占比**正好等于**上限 0.10 ⇒ 不算超限（判据是 `> 上限`）。"""
        samples = self.make_samples(720, hour=self.HOUR)
        mixed = [
            (ts, O2_UNAVAILABLE if index < 72 else o2, values)
            for index, (ts, o2, values) in enumerate(samples)
        ]
        judge = AlarmJudge(tight_config())
        verdicts, _events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), mixed)
        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.n_invalid == 72
        assert dust.n_invalid / dust.n_total == pytest.approx(0.10)
        assert dust.verdict == VERDICT_OK                          # 648/720 = 0.9 覆盖率达标

    def test_coverage_clamped_to_one(self) -> None:
        """多采样（721~722 条/小时）时覆盖率封顶 1.0，不做"100.28%"这种比率。"""
        samples = self.make_samples(722, hour=self.HOUR)
        judge = AlarmJudge(tight_config())
        verdicts, _events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), samples)
        for verdict in verdicts:
            assert verdict.coverage == 1.0
            assert verdict.n_total == 722                          # 真实条数如实保留

    def test_verdict_ts_is_hour_start_and_snapshot_means(self) -> None:
        """结论行的 ts = 小时起点；均值类快照只统计有效样本（标干/O2 按全部样本平均）。"""
        samples = self.make_samples(720, hour=self.HOUR, dust=DUST_CLEAN)
        samples[0] = (samples[0][0], O2_UNAVAILABLE, samples[0][2])   # 一条无效
        judge = AlarmJudge(tight_config())
        verdicts, _events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), samples)
        dust = {verdict.point: verdict for verdict in verdicts}["dust"]
        assert dust.ts == self.HOUR.strftime(TS_FORMAT)
        assert dust.n_valid == 719
        assert dust.conv_mean == pytest.approx(DUST_CLEAN)            # 只算有效样本
        assert dust.judge_version == "vtest"

    def test_each_target_gets_its_own_verdict_row(self) -> None:
        """三个判定测点各一行结论；O2/Flow 等不在判定集合里（它们的 limit 是量程上限）。"""
        samples = self.make_samples(720, hour=self.HOUR, dust=DUST_OVER, nox=NOX_OVER)
        judge = AlarmJudge(tight_config())
        verdicts, _events = judge.judge_hour(self.HOUR.strftime(TS_FORMAT), samples)
        by_point = {verdict.point: verdict.verdict for verdict in verdicts}
        assert by_point == {"dust": VERDICT_OVER, "so2": VERDICT_OK, "nox": VERDICT_OVER}


# ===========================================================================
# 6. SQL 文本生成（不执行；只断言字符串）
# ===========================================================================

class TestSqlLiterals:
    def test_sql_float_nan_writes_null(self) -> None:
        """nan ⇒ NULL（不是 0、不是哨兵）：拼进 SQL 会 syntax error 并丢整行。"""
        assert sql_float(float("nan")) == "NULL"
        assert sql_float(float("inf")) == "NULL"
        assert sql_float(float("-inf")) == "NULL"
        assert sql_float(None) == "NULL"          # type: ignore[arg-type]
        assert "NULL" not in sql_float(0.0)
        assert "9999" not in sql_float(float("nan"))

    def test_sql_float_finite_roundtrip(self) -> None:
        """有限值取 repr（往返精度不丢），不用哨兵值顶替。"""
        for value in (0.0, -1.5, 4.75, DUST_LIMIT, 1e-7, 12345.6789):
            text = sql_float(value)
            assert float(text) == value
            assert text == repr(float(value))

    def test_sql_text_escapes_single_quote(self) -> None:
        assert sql_text("plant1") == "'plant1'"
        assert sql_text("o'brien") == "'o''brien'"
        assert sql_timestamp("2026-10-02 10:00:00") == "'2026-10-02 10:00:00'"

    def test_event_insert_sql_shape(self) -> None:
        """事件 INSERT：显式带库名前缀 + 子表名 = plant_device_point_judgetype（小写）。"""
        event = AlarmEvent(
            ts="2026-10-02 10:00:00", event_id="dust|instant|2026-10-02 10:00:00",
            phase=PHASE_START, point="dust", judge_type=JUDGE_INSTANT,
            converted=6.0, raw_value=6.0, o2=6.0, limit_value=DUST_LIMIT,
            o2_reference=O2_REFERENCE, over_samples=2, window_seconds=0.0,
            judge_version="v1.0.0", trigger_ts="2026-10-02 10:00:10",
        )
        sql = TABLES.event_insert_sql(event)
        assert sql.startswith("INSERT INTO cems.plant1_cems1_dust_instant USING cems.cems_alarm_event")
        assert "TAGS ('plant1', 'cems1', 'dust', 'instant')" in sql
        assert "VALUES ('2026-10-02 10:00:00', 'dust|instant|2026-10-02 10:00:00', 'start'" in sql
        assert "'2026-10-02 10:00:10'" in sql                  # trigger_ts 独立成列
        assert "6.0, 6.0, 6.0, 5.0, 6.0, 2, 0.0" in sql        # converted/raw/o2/limit/o2_ref/over/dur

    def test_nan_converted_event_writes_null_columns(self) -> None:
        """无效样本那行：converted 是 nan ⇒ SQL 里必须是 NULL，整行照样写进去。"""
        judge = AlarmJudge(tight_config())
        events = feed(judge, [at(T0, dust=DUST_OVER, o2=O2_UNAVAILABLE)])
        sql = TABLES.event_insert_sql(only(events, "dust")[0])
        assert "NULL" in sql
        assert "nan" not in sql.lower()
        assert "'invalid'" in sql

    def test_verdict_insert_sql_nan_means_null(self) -> None:
        """空小时的结论行：conv_mean/conv_max 是 nan ⇒ 写 NULL（整行不丢）。"""
        judge = AlarmJudge(tight_config())
        verdicts, _events = judge.judge_hour("2026-10-02 09:00:00", [])
        sql = TABLES.verdict_insert_sql(verdicts[0])
        assert "NULL" in sql
        assert "nan" not in sql.lower()
        assert sql.startswith("INSERT INTO cems.plant1_cems1_dust USING cems.cems_hourly_verdict")

    def test_create_sql_is_idempotent_ddl(self) -> None:
        statements = TABLES.create_sql()
        assert len(statements) == 3
        assert all(statement.startswith("CREATE STABLE IF NOT EXISTS cems.") for statement in statements)
        assert "cems.cems_alarm_event" in statements[0]
        assert "cems.cems_hourly_verdict" in statements[1]
        assert "cems.cems_alarm_push" in statements[2]
        # 判定证据快照的列必须齐（事后可复现"当时按哪个公式/基准氧/限值判的"）
        for column in ("converted", "raw_value", "o2", "limit_value", "o2_reference", "judge_version"):
            assert column in statements[0]

    def test_push_insert_sql_carries_payload(self) -> None:
        judge = AlarmJudge(tight_config())
        events = feed(judge, [at(T0, dust=DUST_OVER, o2=O2_UNAVAILABLE)])
        sql = TABLES.push_insert_sql(only(events, "dust")[0], "log")
        assert "_push USING cems.cems_alarm_push" in sql
        assert "'log', 'recorded'" in sql

    def test_hour_rows_sql_column_order_matches_settle_hour(self) -> None:
        """小时回读的列序必须与 `settle_hour` 解包一致：ts, o2, dust, so2, nox。"""
        sql = TABLES.hour_rows_sql("2026-10-02 09:00:00", "2026-10-02 10:00:00")
        assert "SELECT ts, o2, dust, so2, nox FROM cems.cems_data" in sql
        assert "ts >= '2026-10-02 09:00:00' AND ts < '2026-10-02 10:00:00'" in sql
        assert sql.endswith("ORDER BY ts ASC")

    def test_restore_select_sql_column_order_matches_restore(self) -> None:
        """恢复查询的列序必须与 `AlarmJudge.restore` 的下标解包一致。"""
        sql = TABLES.restore_select_sql(400)
        assert sql.startswith(
            "SELECT ts, event_id, phase, point, judge_type, over_samples, trigger_ts FROM ("
        )
        assert "ORDER BY ts DESC LIMIT 400" in sql       # 先取最近 N 条
        assert sql.endswith(") ORDER BY ts ASC")         # 再正序（拿的是最近那一批）

    def test_child_table_names_are_lowercased(self) -> None:
        tables = AlarmTables(db="cems", plant="Plant1", device="CEMS1")
        assert tables.event_child("dust", "instant") == "plant1_cems1_dust_instant"
        assert tables.push_child("dust", "instant") == "plant1_cems1_dust_instant_push"
        assert tables.verdict_child("nox") == "plant1_cems1_nox"


# ===========================================================================
# 7. 时间戳归一化与工具函数（实测两条 REST 路径表示不同）
# ===========================================================================

class TestNormalizeTimestamp:
    def test_naive_local_is_identity(self) -> None:
        assert normalize_ts("2026-10-02 14:00:00") == "2026-10-02 14:00:00"

    def test_datetime_object_is_formatted(self) -> None:
        assert normalize_ts(datetime(2026, 10, 2, 14, 0, 0)) == "2026-10-02 14:00:00"

    def test_utc_iso_and_naive_local_agree(self) -> None:
        """两条 REST 读路径的两种形态**必须归一到同一个字符串**（否则水位差 8 小时）。

        - taosrest / taos CLI → 朴素本地 `'2026-10-02 14:00:00'`
        - 裸 `POST /rest/sql`  → UTC ISO  `'2026-10-02T06:00:00.000Z'`（= 北京 14:00）
        """
        local_naive = "2026-10-02 14:00:00"
        utc_iso = "2026-10-02T06:00:00.000Z"
        # 前提：本机时区是 UTC+8（ADR 里那条"差 8 小时"的实测就是按它写的）
        assert datetime(2026, 10, 2, 14, 0, 0).astimezone().utcoffset() == timedelta(hours=8)
        assert normalize_ts(utc_iso) == local_naive
        assert normalize_ts(utc_iso) == normalize_ts(local_naive)

    def test_offset_forms_agree_with_utc_z(self) -> None:
        expected = normalize_ts("2026-10-02T06:00:00Z")
        assert normalize_ts("2026-10-02T06:00:00+00:00") == expected
        assert normalize_ts("2026-10-02T14:00:00+08:00") == expected

    def test_other_forms_are_repaired(self) -> None:
        """斜杠分隔 / 带毫秒 / 带 T 无时区：都归一成本地朴素形态。"""
        assert normalize_ts("2026/10/02 14:00:00") == "2026-10-02 14:00:00"
        assert normalize_ts("2026-10-02T14:00:00.500") == "2026-10-02 14:00:00"
        assert normalize_ts("2026-10-02 14:00:00.123456") == "2026-10-02 14:00:00"
        assert normalize_ts("  2026-10-02 14:00:00  ") == "2026-10-02 14:00:00"

    def test_empty_and_garbage_are_not_crashed(self) -> None:
        assert normalize_ts("") == ""
        assert normalize_ts("   ") == ""
        assert normalize_ts(None) == "None"            # 不抛异常；接入层不会再拿它比较大小
        assert normalize_ts("not-a-timestamp") == "not-a-timestamp"

    def test_normalized_strings_compare_lexicographically(self) -> None:
        """归一化之后才能用字符串比大小（水位判断就是 `ts <= last_ts`）。"""
        earlier = normalize_ts("2026-10-02T06:00:00.000Z")     # 北京 14:00
        later = normalize_ts("2026-10-02 15:00:00")
        assert earlier < later
        # 反例（说明为什么必须归一）：未归一时 'T' > ' '
        assert "2026-10-02T06:00:00.000Z" > "2026-10-02 15:00:00"


class TestSmallUtilities:
    def test_to_datetime_and_seconds_between(self) -> None:
        assert to_datetime("2026-10-02 10:00:00") == T0
        assert to_datetime("2026-10-02 10:00") is None          # 形态不符 → None（不抛）
        assert seconds_between("2026-10-02 10:00:00", "2026-10-02 10:30:00") == 1800.0
        assert seconds_between("坏时间", "2026-10-02 10:00:00") == 0.0

    def test_event_id_roundtrip_uses_pipe_separator(self) -> None:
        """ts 自带冒号，所以分隔符必须是 `|`，否则 start_ts 解析不回来。"""
        event_id = make_event_id("dust", JUDGE_INSTANT, "2026-10-02 10:00:00")
        assert event_id == f"dust{EVENT_ID_SEP}instant{EVENT_ID_SEP}2026-10-02 10:00:00"
        assert parse_event_start(event_id) == "2026-10-02 10:00:00"
        assert parse_event_start("dust:instant:2026-10-02 10:00:00") == ""
        assert parse_event_start("garbage", fallback="fallback") == "fallback"

    def test_hour_start_of(self) -> None:
        assert hour_start_of(datetime(2026, 10, 2, 14, 37, 12, 345678)) == \
            datetime(2026, 10, 2, 14, 0, 0)

    def test_expected_samples_per_hour_from_poll_interval(self) -> None:
        assert AlarmConfig(poll_interval=5.0).expected_samples_per_hour == 720
        assert AlarmConfig(poll_interval=10.0).expected_samples_per_hour == 360
        assert AlarmConfig(poll_interval=2.0).expected_samples_per_hour == 1800


class TestEventRendering:
    def test_log_text_renders_nan_without_crashing(self) -> None:
        judge = AlarmJudge(tight_config())
        events = feed(judge, [at(T0, dust=DUST_OVER, o2=O2_UNAVAILABLE)])
        text = only(events, "dust")[0].log_text("log")
        assert text.startswith("ALARM_PUSH channel=log status=recorded phase=invalid")
        assert "converted=nan" in text
        assert "reason=折算值=nan" in text

    def test_payload_json_fits_nchar_256_and_nulls_nan(self) -> None:
        """载荷要塞得进 `payload NCHAR(256)`；nan 序列化成 null 而不是 NaN 字面量。"""
        judge = AlarmJudge(tight_config())
        events = feed(judge, [at(T0, dust=DUST_OVER, o2=O2_UNAVAILABLE)])
        payload = only(events, "dust")[0].payload_json()
        assert len(payload) <= 256
        assert "NaN" not in payload
        body = json.loads(payload)
        assert body["converted"] is None
        assert body["raw"] == DUST_OVER
        assert body["phase"] == PHASE_INVALID
        assert body["limit"] == DUST_LIMIT
        assert body["o2_ref"] == O2_REFERENCE

    def test_payload_json_for_normal_event(self) -> None:
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [at(T0, dust=DUST_OVER)])
        payload = only(events, "dust")[0].payload_json()
        assert len(payload) <= 256
        body = json.loads(payload)
        assert body["point"] == "dust"
        assert body["judge_type"] == "instant"
        assert body["converted"] == DUST_OVER
        assert body["ver"] == "vtest"


# ===========================================================================
# 8. 状态恢复（§3.3「重启恢复」）：从事件行折叠出 OPEN 状态
# ===========================================================================

class TestRestoreFromEventRows:
    @staticmethod
    def row(
        ts: str, event_id: str, phase: str, point: str, judge_type: str = JUDGE_INSTANT,
        over_samples: int = 2, trigger_ts: Optional[str] = None,
    ) -> tuple:
        return (ts, event_id, phase, point, judge_type, over_samples, trigger_ts or ts)

    def test_last_row_start_keeps_event_open(self) -> None:
        judge = AlarmJudge(tight_config())
        start_ts = "2026-10-02 10:00:00"
        event_id = make_event_id("dust", JUDGE_INSTANT, start_ts)
        folded = judge.restore([self.row("2026-10-02 10:00:10", event_id, PHASE_START, "dust")])

        assert folded == 1
        assert judge.open_points() == ["dust"]
        state = judge._states["dust"]
        assert state.event_id == event_id
        assert state.start_ts == start_ts                          # 从 event_id 解析回来
        assert state.last_notify_ts == "2026-10-02 10:00:10"
        assert state.over_samples == 2

    def test_restored_open_event_does_not_resend_start(self) -> None:
        """恢复后继续喂越限样本：**不许**再发一个 START（防重复告警）。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        start_ts = "2026-10-02 10:00:00"
        event_id = make_event_id("dust", JUDGE_INSTANT, start_ts)
        judge.restore([
            self.row("2026-10-02 10:00:00", event_id, PHASE_START, "dust"),
            self.row("2026-10-02 10:05:00", event_id, PHASE_REMIND, "dust"),
        ])
        assert judge.open_points() == ["dust"]

        # 水位已被 restore 推到 10:05:00：更早的样本一律当重投挡下
        assert feed(judge, [at(datetime(2026, 10, 2, 10, 0, 0), dust=DUST_OVER)]) == []
        assert judge.stats["duplicate"] == 1

        events = feed(judge, [
            at(datetime(2026, 10, 2, 10, 10, 0), dust=DUST_OVER),
            at(datetime(2026, 10, 2, 10, 10, 5), dust=DUST_CLEAN),
            at(datetime(2026, 10, 2, 10, 10, 10), dust=DUST_CLEAN),
        ])
        assert PHASE_START not in phases(only(events, "dust"))      # 没有第二个 START
        assert phases(only(events, "dust")) == [PHASE_END]
        ends = [event for event in only(events, "dust") if event.phase == PHASE_END]
        assert ends[0].event_id == event_id                         # 闭合的是恢复的那个事件

    def test_last_row_end_closes_event(self) -> None:
        judge = AlarmJudge(tight_config())
        event_id = make_event_id("dust", JUDGE_INSTANT, "2026-10-02 10:00:00")
        judge.restore([
            self.row("2026-10-02 10:00:00", event_id, PHASE_START, "dust"),
            self.row("2026-10-02 10:10:00", event_id, PHASE_END, "dust"),
        ])
        assert judge.open_points() == []
        assert judge._states["dust"].event_id == ""

    def test_invalid_row_does_not_change_open_state(self) -> None:
        """§3.5：invalid 行不参与达标判定，因此**不改变** OPEN/CLOSED 状态。"""
        judge = AlarmJudge(tight_config())
        event_id = make_event_id("dust", JUDGE_INSTANT, "2026-10-02 10:00:00")
        judge.restore([
            self.row("2026-10-02 10:00:00", event_id, PHASE_START, "dust"),
            self.row("2026-10-02 10:05:00", make_event_id("dust", JUDGE_INSTANT, "2026-10-02 10:05:00"),
                     PHASE_INVALID, "dust"),
        ])
        assert judge.open_points() == ["dust"]
        assert judge._states["dust"].event_id == event_id           # 还是原来那个事件

    def test_hourly_rows_do_not_touch_instant_state_but_advance_watermark(self) -> None:
        """hourly 事件不参与 instant 状态机；但它的 ts 仍要参与水位。"""
        judge = AlarmJudge(tight_config())
        hourly_id = make_event_id("dust", JUDGE_HOURLY, "2026-10-02 09:00:00")
        judge.restore([
            self.row("2026-10-02 09:59:55", hourly_id, PHASE_START, "dust", JUDGE_HOURLY),
            self.row("2026-10-02 09:59:55", hourly_id, PHASE_END, "dust", JUDGE_HOURLY),
        ])
        assert judge.open_points() == []
        assert judge._last_ts == "2026-10-02 09:59:55"
        # 水位挡住了小时结算 end 行那个 ts，避免同一条样本被判两次
        assert feed(judge, [at(datetime(2026, 10, 2, 9, 59, 55), dust=DUST_OVER)]) == []

    def test_hourly_invalid_row_does_not_advance_instant(self) -> None:
        judge = AlarmJudge(tight_config())
        hourly_id = make_event_id("dust", JUDGE_HOURLY, "2026-10-02 09:00:00")
        judge.restore([self.row("2026-10-02 09:00:00", hourly_id, PHASE_INVALID, "dust", JUDGE_HOURLY)])
        assert judge.open_points() == []
        assert judge._last_ts == "2026-10-02 09:00:00"

    def test_restore_iso_timestamps_are_normalized(self) -> None:
        """事件表里读回的 ts 可能是 UTC ISO 形态，恢复也必须归一（否则水位差 8 小时）。"""
        judge = AlarmJudge(tight_config())
        event_id = make_event_id("dust", JUDGE_INSTANT, "2026-10-02 10:00:00")
        judge.restore([
            self.row("2026-10-02T02:00:00.000Z", event_id, PHASE_START, "dust"),
        ])
        assert judge._last_ts == "2026-10-02 10:00:00"
        assert judge._states["dust"].last_notify_ts == "2026-10-02 10:00:00"

    def test_restore_unknown_point_is_skipped(self) -> None:
        judge = AlarmJudge(tight_config())
        judge.restore([self.row("2026-10-02 10:00:00", make_event_id("flow", JUDGE_INSTANT, "2026-10-02 10:00:00"),
                                PHASE_START, "flow")])
        assert judge.open_points() == []

    def test_restore_folder_returns_row_count(self) -> None:
        judge = AlarmJudge(tight_config())
        rows = [
            self.row(f"2026-10-02 10:0{index}:00", make_event_id("dust", JUDGE_INSTANT, "2026-10-02 10:00:00"),
                     PHASE_REMIND, "dust")
            for index in range(5)
        ]
        assert judge.restore(rows) == 5
        assert judge.restore([]) == 0

    def test_missing_event_id_falls_back_to_trigger_ts(self) -> None:
        """event_id 坏掉（老数据/手工写入）时用 trigger_ts 兜底，不丢状态。"""
        judge = AlarmJudge(tight_config())
        judge.restore([self.row("2026-10-02 10:00:10", "", PHASE_START, "dust", trigger_ts="2026-10-02 10:00:10")])
        assert judge._states["dust"].start_ts == "2026-10-02 10:00:10"


# ===========================================================================
# 9. 参数自检与非法/边界输入
# ===========================================================================

class TestConfigValidation:
    def test_defaults_match_adr(self) -> None:
        config = AlarmConfig()
        assert config.enable is True
        assert (config.window_samples, config.min_over_samples) == (6, 3)
        assert config.recover_ratio == pytest.approx(0.95)
        assert config.recover_samples == 6
        assert config.coverage_min == pytest.approx(0.75)
        assert config.invalid_ratio_max == pytest.approx(0.10)
        assert config.hourly_enable is True
        assert config.poll_interval == 5.0
        config.validate()                                   # 默认值必须自检通过

    @pytest.mark.parametrize(
        "overrides",
        [
            {"window_samples": 0},
            {"min_over_samples": 0},
            {"min_over_samples": 7},                        # > window_samples
            {"recover_samples": 0},
            {"recover_ratio": 0.0},
            {"recover_ratio": 1.01},
            {"remind_seconds": -1.0},
            {"hourly_backfill_hours": -1},
            {"coverage_min": -0.01},
            {"coverage_min": 1.01},
            {"invalid_ratio_max": -0.01},
            {"invalid_ratio_max": 1.01},
            {"poll_interval": 0.0},
            {"poll_interval": 3601.0},
            {"judge_version": "v1.0.0; DROP TABLE"},        # 会被拼进 SQL，必须白名单校验
            {"judge_version": ""},
            {"push_channel": "log; DROP"},
            {"push_channel": ""},
        ],
    )
    def test_invalid_config_raises(self, overrides: dict) -> None:
        base = AlarmConfig()
        with pytest.raises(ValueError):
            replace(base, **overrides).validate()

    def test_valid_boundaries_pass(self) -> None:
        AlarmConfig(poll_interval=3600.0).validate()
        AlarmConfig(coverage_min=0.0, invalid_ratio_max=0.0).validate()
        AlarmConfig(coverage_min=1.0, invalid_ratio_max=1.0).validate()
        AlarmConfig(recover_ratio=1.0).validate()
        AlarmConfig(remind_seconds=0.0).validate()

    def test_sql_whitelists_reject_injection(self) -> None:
        assert JUDGE_VERSION_RE.match("v1.0.0")
        assert JUDGE_VERSION_RE.match("hj75/c8+2026-10-02")
        assert not JUDGE_VERSION_RE.match("v1; DROP TABLE x")
        assert not JUDGE_VERSION_RE.match("v" * 25)
        assert CHANNEL_RE.match("log")
        assert CHANNEL_RE.match("webhook:prod-1")
        assert not CHANNEL_RE.match("log'; DELETE")

    def test_from_env_reads_alarm_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALARM_WINDOW_SAMPLES", "10")
        monkeypatch.setenv("ALARM_MIN_OVER_SAMPLES", "5")
        monkeypatch.setenv("ALARM_COVERAGE_MIN", "0.9")
        monkeypatch.setenv("ALARM_ENABLE", "0")
        monkeypatch.setenv("ALARM_PUSH_CHANNEL", "log")
        config = AlarmConfig.from_env()
        assert config.window_samples == 10
        assert config.min_over_samples == 5
        assert config.coverage_min == pytest.approx(0.9)
        assert config.enable is False
        config.validate()

    def test_from_env_rejects_empty_flag_as_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALARM_ENABLE", "")
        assert AlarmConfig.from_env().enable is False


class TestInvalidAndBoundaryInput:
    """接入层负责的校验**不在本模块**；这里锁住 AlarmJudge 侧的边界行为（不假装它做校验）。"""

    def test_missing_point_key_raises_keyerror(self) -> None:
        """values 缺测点键 → KeyError（拒收是**接入层** parse_payload 的职责，本模块不静默兜底）。"""
        judge = AlarmJudge(tight_config())
        ts = T0.strftime(TS_FORMAT)
        _, values, references = at(T0, dust=DUST_OVER)
        incomplete = {key: value for key, value in values.items() if key != DUST_KEY}
        with pytest.raises(KeyError):
            judge.on_sample(ts, incomplete, references)

    def test_missing_o2_key_raises_keyerror(self) -> None:
        judge = AlarmJudge(tight_config())
        ts = T0.strftime(TS_FORMAT)
        _, values, references = at(T0, dust=DUST_OVER)
        incomplete = {key: value for key, value in values.items() if key != O2_KEY}
        with pytest.raises(KeyError):
            judge.on_sample(ts, incomplete, references)

    def test_reference_without_target_key_raises_keyerror(self) -> None:
        judge = AlarmJudge(tight_config())
        ts = T0.strftime(TS_FORMAT)
        _, values, references = at(T0, dust=DUST_OVER)
        with pytest.raises(KeyError):
            judge.on_sample(ts, values, {})

    def test_empty_values_and_references_raise_keyerror(self) -> None:
        judge = AlarmJudge(tight_config())
        with pytest.raises(KeyError):
            judge.on_sample(T0.strftime(TS_FORMAT), {}, {})

    def test_out_of_range_reading_is_not_screened_by_judge(self) -> None:
        """超量程数据由接入层**拒收**（量程 ≠ 限值）；本模块收到就照判，行为显式锁住。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [at(T0, dust=100.0)])        # 正好压在 Dust 量程上限
        assert phases(only(events, "dust")) == [PHASE_START]
        assert events[0].converted == pytest.approx(100.0)

    def test_negative_concentration_is_judged_as_below_limit(self) -> None:
        """负值（不可能但会出现于脏数据）：不越限、也不满足恢复（恢复用的是不等式）。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        events = feed(judge, [at(T0, dust=-1.0)])
        assert only(events, "dust") == []
        assert judge.open_points() == []

    def test_all_nan_references_produce_only_invalid(self) -> None:
        judge = AlarmJudge(tight_config())
        ts = T0.strftime(TS_FORMAT)
        _, values, _ = at(T0, dust=DUST_OVER, o2=O2_UNAVAILABLE)
        nan_references = {column: float("nan") for column in ZS_TARGETS}
        events = judge.on_sample(ts, values, nan_references)
        assert phases(events) == [PHASE_INVALID] * 3       # 每个判定测点一行 invalid
        assert judge.open_points() == []

    def test_stale_or_broken_ts_does_not_crash(self) -> None:
        """脏 ts（解析不了）不能让判定抛异常：秒数按 0 处理、事件照产。"""
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        _, values, references = at(T0, dust=DUST_OVER)
        events = judge.on_sample("2026-10-02 10:00", values, references)   # 形态不符
        assert phases(only(events, "dust")) == [PHASE_START]
        assert events[0].window_seconds == 0.0

    def test_stats_counters_are_consistent(self) -> None:
        judge = AlarmJudge(tight_config(window_samples=1, min_over_samples=1))
        feed(judge, [
            at(T0, dust=DUST_OVER),
            at(T0 + timedelta(seconds=5), dust=DUST_CLEAN),
            at(T0 + timedelta(seconds=10), dust=DUST_CLEAN),      # → END
            at(T0 + timedelta(seconds=15), dust=DUST_OVER, o2=O2_UNAVAILABLE),  # 3 条 invalid
        ])
        stats = judge.stats
        assert stats["samples"] == 4
        assert stats["events"] == 1
        assert stats["ends"] == 1
        assert stats["invalid_samples"] == 3
        assert stats["duplicate"] == 0
