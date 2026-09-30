# -*- coding: utf-8 -*-
"""验收自检 5/5：污染物偶发尖峰与"折算值超标"口径（任务 B，不需要 Docker）。

用法：
    python scripts/verify_sim_spike_exceedance.py

输出五段证据：
    A. 尖峰幅度与"偶发性"：触发概率、命中占比、相邻周期爬升幅度
    B. 尖峰确实把**折算值**顶过限值（平时达标 → 事件式超标）
    C. 判定口径：折算值口径 vs 标干值口径的差异，并复现 points.py 里的反例
       （标干 30 达标，O2=9% 时折算 37.5 超标）
    D. 尖峰受环境变量控制：SIM_SPIKE_PROB 变则事件率变；0 则完全关闭
    E. 结束码：全部通过 0，任一不通过 1

⚠️ 口径纪律（本脚本严格照做）：
    - 折算值只在**一处**算：调用 src/common/points.to_reference_o2()，不自己抄公式
    - 不新建 TDengine 列、不写库：折算值是导出量，见 points.py 顶部说明
    - 判定用**折算值**（环保口径），不是标干值
"""

from __future__ import annotations

import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import LIMITS, O2_REFERENCE, POINTS, to_reference_o2   # noqa: E402
from src.device import simulator as SIM                                       # noqa: E402

# ---- 检查参数 ----
STEP_SECONDS: float = 2.0                     # 设备刷新周期
SAMPLE_HOURS: int = 24                        # 统计窗口
FAKE_START: datetime = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
POLLUTANTS: tuple[str, ...] = ("Dust", "SO2", "NOx")
SPAN_BY_NAME: dict[str, float] = {
    point.name: point.high - point.low for point in POINTS
}
# 尖峰允许的最大"每周期爬升"，按量程百分比计。
#   尖峰是**真实事件**，它的上升沿本来就比常态漂移陡，所以这里给 5% 量程
#   （常态漂移每周期只走约 0.002% 量程，量级差 3 个数量级，两者不会混）。
SPIKE_RAMP_LIMIT: float = 0.05
EXCEED_LIMIT_PCT: float = 50.0                # 折算值越限时间占比上限（有尖峰时）
BASELINE_MEAN_LIMIT: float = 1.0              # 无尖峰时均值必须 < 1.0 倍限值
BASELINE_OVER_LIMIT_PCT: float = 5.0          # 无尖峰时越限时间占比上限（基线必须留余量）
EVENTS_GAP_SECONDS: int = 300                 # 折算越限间断超过这么久就算两次事件

FAILURES: list[str] = []


def _check(condition: bool, description: str) -> None:
    """记录一条验收判定；失败时收进 FAILURES，最后统一汇总。"""
    print(f"  [{'PASS' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def simulator_with_spikes(probability: float) -> SIM.CemsSimulator:
    """构造一个指定尖峰概率的仿真器。

    ⚠️ 必须改**模块级常量**再构造，不能用 dataclasses.replace(p, spike_probability=...)
       逐个替换：那样会把尖峰也发给物理量（Flow/Temp/O2 的 spike_probability 本来被
       _is_limit_anchored 闸到 0），实测会把 O2 的比值从 0.29 顶到 0.80，
       整个折算统计全乱（这正是我踩过的坑，留在这里当护栏）。
       probability=0.0 时等价于"关掉尖峰"，用来取基线。
    """
    original = SIM.SPIKE_PROBABILITY
    try:
        SIM.SPIKE_PROBABILITY = probability      # type: ignore[misc]
        return SIM.CemsSimulator(seed=20261001)
    finally:
        SIM.SPIKE_PROBABILITY = original         # type: ignore[misc]


def series(simulator: SIM.CemsSimulator, name: str) -> list[float]:
    """按假时钟取一整天该测点的读数（物理量，标干口径）。"""
    count = int(SAMPLE_HOURS * 3600 / STEP_SECONDS)
    return [
        simulator.sample(name, FAKE_START + timedelta(seconds=STEP_SECONDS * i))
        for i in range(count)
    ]


def section_a_spike_rarity() -> None:
    """A. 尖峰必须"偶发且可数"：次数可数、上下沿平缓、不夺走常态趋势。"""
    print(f"\n=== A. 尖峰特性（概率={SIM.SPIKE_PROBABILITY}/周期，"
          f"幅度={SIM.SPIKE_AMPLITUDE_LOW}~{SIM.SPIKE_AMPLITUDE_HIGH} 限值，"
          f"半宽={SIM.SPIKE_WIDTH_STEPS}步={SIM.SPIKE_WIDTH_STEPS * SIM.SPIKE_STEP_SECONDS:.0f}s）===")
    spikes_on = simulator_with_spikes(SIM.SPIKE_PROBABILITY)
    spikes_off = simulator_with_spikes(0.0)
    print(f"{'测点':<7}{'窗口命中占比':>13}{'尖峰事件数':>11}{'峰值(限值比)':>13}"
          f"{'每周期最大爬升':>15}{'爬升/量程':>11}")
    all_ok = True
    for name in POLLUTANTS:
        limit = LIMITS[name]
        diffs = [
            a - b for a, b in zip(series(spikes_on, name), series(spikes_off, name))
        ]
        # ⚠️ 命中占比**不是**"超标时间占比"：三角包络的两头很小，落在窗口内 ≠ 越限。
        #    所以这里只做参考打印，真正判"偶发"的是 B 段的折算值越限占比。
        hit_share = 100.0 * sum(1 for d in diffs if d > 1e-9) / len(diffs)
        # 事件数 = 从"无尖峰"跳到"有尖峰"的次数
        events = sum(
            1 for index, diff in enumerate(diffs)
            if diff > 1e-9 and (index == 0 or diffs[index - 1] <= 1e-9)
        )
        peak_ratio = max(diffs) / limit
        ramp = max(abs(b - a) for a, b in zip(diffs, diffs[1:]))
        ramp_share = ramp / SPAN_BY_NAME[name]
        print(f"{name:<7}{hit_share:>12.1f}%{events:>11d}{peak_ratio:>13.2f}"
              f"{ramp:>15.3f}{ramp_share:>11.3%}")
        all_ok = all_ok and events >= 1 and 0.2 <= peak_ratio <= 0.8 and ramp_share < SPIKE_RAMP_LIMIT
    _check(
        all_ok,
        f"24h 内每个污染物至少 1 次尖峰、峰值落在 0.2~0.8 限值、"
        f"每周期爬升 < {SPIKE_RAMP_LIMIT:.0%} 量程（上下沿不是假跳变）",
    )


def section_b_exceedance() -> None:
    """B. 尖峰必须把**折算值**顶过限值：平时达标 → 事件式超标。"""
    print(f"\n=== B. 折算值越限（判定口径 = 折算值，基准氧 {O2_REFERENCE}%）===")
    spikes_on = simulator_with_spikes(SIM.SPIKE_PROBABILITY)
    spikes_off = simulator_with_spikes(0.0)
    o2_values = series(spikes_on, "O2")
    print(f"  O2 实测区间 {min(o2_values):.2f}%~{max(o2_values):.2f}%"
          f"（基准 {O2_REFERENCE}%，折算放大倍数 {(21 - O2_REFERENCE) / (21 - min(o2_values)):.2f}"
          f"~{(21 - O2_REFERENCE) / (21 - max(o2_values)):.2f}）")
    print(f"{'测点':<7}{'限值':>7}{'标干均值':>9}{'折算均值':>9}{'折算峰值':>9}"
          f"{'越限%':>8}{'峰值/限值':>10}")

    all_ok = True
    for name in POLLUTANTS:
        limit = LIMITS[name]
        raw_on = series(spikes_on, name)
        converted_on = [to_reference_o2(v, o) for v, o in zip(raw_on, o2_values)]
        over_pct = 100.0 * sum(1 for c in converted_on if c > limit) / len(converted_on)
        mean_converted = statistics.mean(converted_on)
        peak = max(converted_on)
        print(f"{name:<7}{limit:>7.1f}{statistics.mean(raw_on):>9.2f}{mean_converted:>9.2f}"
              f"{peak:>9.2f}{over_pct:>7.1f}%{peak / limit:>10.2f}")
        all_ok = all_ok and over_pct > 0.0 and over_pct < EXCEED_LIMIT_PCT
    _check(
        all_ok,
        f"开尖峰后每个污染物都有折算值越限，且越限时间占比 < {EXCEED_LIMIT_PCT:.0f}%"
        "（是事件，不是常态）",
    )

    # 基线（无尖峰）必须达标：这才是"超标是事件"的前提
    o2_clean = series(spikes_off, "O2")
    baseline_rows = []
    worst_baseline_over = 0.0
    for name in POLLUTANTS:
        limit = LIMITS[name]
        converted = [to_reference_o2(v, o) for v, o in zip(series(spikes_off, name), o2_clean)]
        over_pct = 100.0 * sum(1 for c in converted if c > limit) / len(converted)
        worst_baseline_over = max(worst_baseline_over, over_pct)
        baseline_rows.append((name, statistics.mean(converted) / limit, max(converted) / limit,
                              over_pct))
    print("  无尖峰基线：")
    for name, mean_ratio, peak_ratio, over_pct in baseline_rows:
        print(f"    {name:<5} 折算均值/限值={mean_ratio:.2f}  折算峰值/限值={peak_ratio:.2f}  "
              f"越限={over_pct:.1f}%")
    _check(
        all(mean_ratio < BASELINE_MEAN_LIMIT and over_pct < EXCEED_LIMIT_PCT
            for _, mean_ratio, _, over_pct in baseline_rows),
        "无尖峰时每个污染物折算均值 < 限值、越限 < 50%",
    )
    # ⚠️ 这一条是新加的（原来是缺口）：只验"均值 < 限值"挡不住"基线+漂移就已经常年越限"。
    #    实测教训：基线锚到限值 75% 时 NOx 有 20% 的时间在越限，超标就不再是事件。
    _check(
        worst_baseline_over < BASELINE_OVER_LIMIT_PCT,
        f"无尖峰时越限时间占比 < {BASELINE_OVER_LIMIT_PCT:.0f}%"
        f"（基线留出余量，越限由尖峰驱动而不是漂移）",
    )

    # 逐次超标事件：证明"超标"是可被下游观测到、可复现的事件，而不是统计上的噪声。
    # ⚠️ 要把"连续越限"合并成一次事件：折算值在限值附近抖动时（O2 与尖峰各自都在动）
    #    会出现多次"上穿/下穿"，不合并会把 1 次尖峰数成十几次，看着像"常年超标"。
    #    这里按 EVENTS_GAP_SECONDS 的间隔判定事件边界，并取事件内的折算峰值。
    print(f"\n  折算值超标事件（24h 窗口；间隔 > {EVENTS_GAP_SECONDS // 60} 分钟算两次事件）：")
    for name in POLLUTANTS:
        limit = LIMITS[name]
        converted = [
            to_reference_o2(v, o)
            for v, o in zip(series(spikes_on, name), o2_values)
        ]
        events: list[tuple[int, int, float]] = []      # (起始下标, 结束下标, 峰值)
        start_index: int | None = None
        for index, value in enumerate(converted):
            if value > limit and start_index is None:
                start_index = index
            elif value <= limit and start_index is not None:
                events.append((start_index, index - 1, 0.0))
                start_index = None
        if start_index is not None:
            events.append((start_index, len(converted) - 1, 0.0))

        grouped: list[tuple[int, int, float]] = []
        for begin, end, _ in events:
            if grouped and (begin - grouped[-1][1]) * STEP_SECONDS <= EVENTS_GAP_SECONDS:
                previous_begin, _, _ = grouped[-1]
                grouped[-1] = (previous_begin, end, 0.0)
            else:
                grouped.append((begin, end, 0.0))
        peaks = [
            (begin, end, max(converted[begin:end + 1])) for begin, end, _ in grouped
        ]

        total_seconds = sum((end - begin + 1) for begin, end, _ in peaks) * STEP_SECONDS
        head = " ".join(
            f"[{FAKE_START + timedelta(seconds=STEP_SECONDS * begin):%H:%M:%S}"
            f"~{FAKE_START + timedelta(seconds=STEP_SECONDS * end):%H:%M:%S} "
            f"峰值={peak:.1f} ({peak / limit:.2f}×)]"
            for begin, end, peak in peaks[:3]
        )
        print(f"    {name:<5} {len(peaks):3d} 次事件，累计 {total_seconds / 60:5.1f} 分钟"
              f"（占一天 {total_seconds / 864:.1f}%）：{head if head else '（无）'}")


def section_c_converted_vs_standard() -> None:
    """C. 判定口径：必须用折算值；折算既会放大也会衰减，别写成"一定更大"。

    ⚠️ 我一开始在这里断言"折算越限% ≥ 标干越限%"，实测**不成立**：
        折算 = 标干 × (21-6)/(21-O2)，O2 > 6% 时放大、O2 < 6% 时**衰减**。
        本模块 O2 实测区间 4.3%~7.3%（跨过基准 6%），所以两种情形都会出现。
        ⇒ 正确断言是"折算与标干不同，且方向由 O2 与基准的大小关系决定"。
    """
    print("\n=== C. 判定口径对比（折算值 vs 标干值）===")
    simulator = simulator_with_spikes(SIM.SPIKE_PROBABILITY)
    o2_values = series(simulator, "O2")
    factors: list[float] = []
    print(f"{'测点':<7}{'标干越限%':>11}{'折算越限%':>11}{'差值':>9}")
    for name in POLLUTANTS:
        limit = LIMITS[name]
        raw = series(simulator, name)
        converted = [to_reference_o2(v, o) for v, o in zip(raw, o2_values)]
        raw_over = 100.0 * sum(1 for v in raw if v > limit) / len(raw)
        converted_over = 100.0 * sum(1 for c in converted if c > limit) / len(converted)
        print(f"{name:<7}{raw_over:>10.1f}%{converted_over:>10.1f}%"
              f"{converted_over - raw_over:>8.1f}%")
        _check(
            abs(converted_over - raw_over) > 0.0,
            f"{name} 折算口径（{converted_over:.1f}%）与标干口径（{raw_over:.1f}%）不同"
            "—— 判定必须选一个口径并说清是哪个",
        )

    # 折算倍数的方向必须跟 O2 与基准的关系一致
    # ⚠️ 必须扫**一整天**：只取开头几百个采样（约十几分钟）时 O2 变化很小，
    #    倍数区间会窄到看不出"跨过 1.0"，会得到一个假的失败。
    factors = [
        to_reference_o2(value, o2) / value
        for value, o2 in zip(series(simulator, "SO2"), o2_values)
    ]
    expected_min = (21 - O2_REFERENCE) / (21 - max(o2_values))
    expected_max = (21 - O2_REFERENCE) / (21 - min(o2_values))
    print(f"  O2 区间 {min(o2_values):.2f}%~{max(o2_values):.2f}%（基准 {O2_REFERENCE}%），"
          f"折算倍数 {min(factors):.3f}~{max(factors):.3f}"
          f"（理论 {expected_min:.3f}~{expected_max:.3f}）")
    _check(
        min(factors) < 1.0 < max(factors),
        "折算倍数跨过 1.0：O2 高于基准放大、低于基准衰减（所以不能用'折算一定更大'做断言）",
    )

    # 复现 points.py 里给出的反例：标干 30 达标，O2=9% 时折算 37.5 超标
    example_raw, example_o2 = 30.0, 9.0
    example_limit = LIMITS["SO2"]
    example_converted = to_reference_o2(example_raw, example_o2)
    print(f"  反例复现：标干 {example_raw}（限值 {example_limit}，"
          f"{'达标' if example_raw <= example_limit else '超标'}） → "
          f"O2={example_o2}% 折算 {example_converted:.1f}"
          f"（{'超标' if example_converted > example_limit else '达标'}）")
    _check(
        example_raw <= example_limit < example_converted,
        "反例成立：标干 30 达标，但 O2=9% 时折算 37.5 超标（判定必须用折算值）",
    )


def section_d_env_control() -> None:
    """D. 尖峰必须受环境变量控制：概率变则事件率变，0 则完全关闭。"""
    print("\n=== D. 环境变量控制（SIM_SPIKE_PROB）===")
    print(f"{'概率':>8}{'Dust 命中%':>12}{'SO2 命中%':>11}{'NOx 命中%':>11}")
    clean_series = {name: series(simulator_with_spikes(0.0), name) for name in POLLUTANTS}
    shares: list[float] = []
    for probability in (0.0, 0.004, 0.008, 0.016):
        simulator = simulator_with_spikes(probability)
        row = []
        for name in POLLUTANTS:
            values = series(simulator, name)
            row.append(100.0 * sum(
                1 for a, b in zip(values, clean_series[name]) if a - b > 1e-9
            ) / len(values))
        shares.append(row[1])
        print(f"{probability:>8.3f}{row[0]:>11.1f}%{row[1]:>10.1f}%{row[2]:>10.1f}%")

    _check(shares[0] == 0.0, "SIM_SPIKE_PROB=0 时完全不产生尖峰（可用于基线标定）")
    _check(
        shares[0] < shares[1] < shares[2] < shares[3],
        "命中占比随 SIM_SPIKE_PROB 单调上升（概率真的在控制事件率）",
    )
    _check(shares[3] < 100.0, "概率翻倍也不会让每个采样都落在尖峰里（避免又变成常年超标）")


def main() -> int:
    """依次跑完四段自检，返回进程退出码。"""
    print("CEMS 污染物尖峰与折算值超标自检（离线，不需要 Docker / MQTT / TDengine）")
    print(f"种子=20261001  起点={FAKE_START.isoformat()}  窗口={SAMPLE_HOURS}h × 每 {STEP_SECONDS:.0f}s")
    print(f"尖峰总开关 SIM_SPIKE_ENABLE={SIM.SPIKE_ENABLED}（1/0）")
    section_a_spike_rarity()
    section_b_exceedance()
    section_c_converted_vs_standard()
    section_d_env_control()

    print("\n================ 结论 ================")
    if FAILURES:
        print(f"不通过 {len(FAILURES)} 项:")
        for item in FAILURES:
            print(f"  - {item}")
        return 1
    print("全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
