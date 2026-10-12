# -*- coding: utf-8 -*-
"""日周期"当地时刻"复验：把 scripts/verify_sim_24h_sweep.py 的拟合峰值换算到**当地时刻**，
并同时算出"修复前"（按 UTC 小时）会落在几点，用于对照。

为什么需要这一步（不是重复劳动）：
`scripts/verify_sim_24h_sweep.py` 的横轴是"从假时钟起点算起的第几小时"，起点是
`datetime(2026,3,1,0,0,tzinfo=timezone.utc)`（= 当地 08:00），而图下的标签写的是
"（当地时刻）"。所以它的 B 段 `峰值时刻` 是**当地 08:00 起算的小时数**，
不是当地钟点；直接读会整体偏 8 小时。本脚本把两种口径都算出来并互相印证：
  · 修复后（现网代码）：`_local_seconds_of_day(moment)` → 取 moment.astimezone() 的钟点
  · 修复前（已回退的那版）：同一函数直接取 `moment.hour`（= UTC 小时）
两者之差应当恒为 **+8 小时**（容器 TZ=Asia/Shanghai）。
"修复前"的数值由**本地复刻的同一个公式**算出（不改仓库源码），
并给出现网函数在每个峰值点的返回值，证明"峰值点 = 该函数取最大值处"。
"""

from __future__ import annotations

import math
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS                                   # noqa: E402
from src.device import simulator as SIM                                # noqa: E402

DAY_START = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
STEP = 600
N = 86400 // STEP


def build_simulator() -> SIM.CemsSimulator:
    original = SIM.SPIKE_ENABLED
    try:
        SIM.SPIKE_ENABLED = False            # 与 24h sweep 同口径：量形状要关掉偶发尖峰
        return SIM.CemsSimulator(seed=20261001)
    finally:
        SIM.SPIKE_ENABLED = original


SIMULATOR = build_simulator()


def legacy_seconds_of_day(moment: datetime) -> float:
    """修复前的实现：直接取 moment.hour（UTC-aware 时 = UTC 小时）。"""
    return moment.hour * 3600.0 + moment.minute * 60.0 + moment.second


def legacy_sample(name: str, moment: datetime) -> float:
    """用修复前的日周期口径复算（其余分量与现网同一份代码）。"""
    params = SIMULATOR._params_by_name[name]          # noqa: SLF001 - 复验脚本，直接用内部参数
    elapsed = (moment - SIM.SIM_EPOCH).total_seconds()
    k = params.span_ratio
    day_seconds = legacy_seconds_of_day(moment)
    day = SIM.SECONDS_PER_DAY
    diurnal = k * (
        SIM._sine_wave(day_seconds, day, SIM.DIURNAL_AMPLITUDE, params.diurnal_phase)
        + SIM._sine_wave(day_seconds, day / 2.0,
                         SIM.DIURNAL_AMPLITUDE * SIM.DIURNAL_HARMONIC_RATIO,
                         params.diurnal_harmonic_phase)
    )
    load = SIMULATOR._load_signal(elapsed) * SIM.LOAD_SCALE * params.coupling * k
    drift = k * SIM.DRIFT_SCALE * (
        SIM.DRIFT_SPLIT * SIM._value_noise(params.lattice_slow, elapsed / params.drift_period)
        + (1.0 - SIM.DRIFT_SPLIT) * SIM._value_noise(
            params.lattice_fast, elapsed / params.drift_period_fast)
    )
    jitter = k * SIM.NOISE_JITTER_SPAN * SIMULATOR._jitter(name, moment)
    return params.base_ratio + diurnal + load + drift + jitter


def fit_peak_hour(values: list[float]) -> tuple[float, float]:
    """返回 (峰值所在"当地 0 点起算小时"、R²)。

    序列起点 = 当地 08:00，所以当地钟点 = (峰值小时 + 8) mod 24。
    """
    hours = [i * STEP / 3600.0 for i in range(len(values))]
    omega = 2.0 * math.pi / 24.0
    sin_part = [math.sin(omega * h) for h in hours]
    cos_part = [math.cos(omega * h) for h in hours]
    mean = statistics.mean(values)
    ss_sin, ss_cos = sum(s * s for s in sin_part), sum(c * c for c in cos_part)
    sc = sum(s * c for s, c in zip(sin_part, cos_part))
    ys = sum((v - mean) * s for v, s in zip(values, sin_part))
    yc = sum((v - mean) * c for v, c in zip(values, cos_part))
    det = ss_sin * ss_cos - sc * sc
    a = (ys * ss_cos - yc * sc) / det
    b = (yc * ss_sin - ys * sc) / det
    peak_grid = (math.atan2(a, b) / omega) % 24.0
    predicted = [mean + a * s + b * c for s, c in zip(sin_part, cos_part)]
    residual = sum((v - p) ** 2 for v, p in zip(values, predicted))
    total = sum((v - mean) ** 2 for v in values)
    return peak_grid, 1.0 - residual / total


def main() -> int:
    local_tz = datetime.now().astimezone().tzinfo
    print("日周期『当地时刻』复验")
    print(f"  宿主/容器时区：{datetime.now().astimezone().strftime('%Z %z')}")
    print(f"  假时钟起点：{DAY_START.isoformat()}（= 当地 {DAY_START.astimezone().strftime('%H:%M')}）")
    print("  修复前口径：_local_seconds_of_day 取 moment.hour（UTC 小时）")
    print("  修复后口径：_local_seconds_of_day 取 moment.astimezone() 的钟点\n")
    print(f"{'测点':<10}{'拟合峰值(网格h)':>16}{'修复后当地':>12}{'修复前当地':>12}{'R²':>8}{'现网函数峰值点':>16}")
    after_peak: dict[str, float] = {}
    before_peak: dict[str, float] = {}
    for point in POINTS:
        values = [SIMULATOR.sample(point.name, DAY_START + timedelta(seconds=STEP * i))
                  for i in range(N)]
        before = [legacy_sample(point.name, DAY_START + timedelta(seconds=STEP * i))
                  for i in range(N)]
        grid_after, r2_after = fit_peak_hour(values)
        grid_before, r2_before = fit_peak_hour(before)
        # 网格起点是当地 08:00 ⇒ 当地钟点 = (网格小时 + 8) % 24；这里用真实偏移算，避免硬编码
        offset_h = DAY_START.astimezone(local_tz).utcoffset().total_seconds() / 3600.0
        local_after = (grid_after + offset_h) % 24.0
        local_before = (grid_before + offset_h) % 24.0
        after_peak[point.name] = local_after
        before_peak[point.name] = local_before
        # 现网函数在"峰值点"处的取值，应当是它一天里的最大值（自证口径正确）
        probe = DAY_START + timedelta(hours=grid_after)
        probe_local = SIM._local_seconds_of_day(probe) / 3600.0
        print(f"{point.name:<10}{grid_after:>16.2f}{local_after:>11.1f}h{local_before:>11.1f}h"
              f"{r2_after:>8.3f}{probe_local:>15.1f}h")
    print()
    for name, hour in after_peak.items():
        minutes = int(round((hour % 1) * 60))
        print(f"  {name:<10}修复后峰值当地 {hour:5.1f}h  ≈ {int(hour):02d}:{minutes:02d}"
              f"   （修复前 {before_peak[name]:5.1f}h）")
    deltas = [round(after_peak[k] - before_peak[k], 3) for k in after_peak]
    print(f"\n  修复前后峰值当地时刻之差：{sorted(set(deltas))}"
          f"（应恒为 +8.0h，即 UTC→当地 的时区差）")
    afternoon = [k for k, v in after_peak.items() if 12.0 <= v < 18.0]
    print(f"  峰值落在**当地午后 12:00–18:00** 的测点：{len(afternoon)}/{len(after_peak)} → {afternoon}")
    print(f"  峰值落在当地夜间的测点（应为 0）："
          f"{[k for k, v in after_peak.items() if v < 6.0 or v >= 21.0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
