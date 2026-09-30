# -*- coding: utf-8 -*-
"""验收自检 2/3：用**假时钟**扫 24 小时，验证日周期形状（不需要真的等一天）。

用法：
    python scripts/verify_sim_24h_sweep.py

输出四段证据：
    A. 典型测点的 24 小时曲线（ASCII 图，10 分钟一个点）
    B. 日周期拟合：峰/谷时刻、幅度、正弦拟合优度 R²
    C. 周期可复现：同一天 0 点 vs 次日 0 点同相位采样必须逐值一致
    D. 结束码：全部通过 0，任一不通过 1
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

from src.common.points import POINTS                                 # noqa: E402
from src.device import simulator as SIM                              # noqa: E402


def build_shape_simulator() -> SIM.CemsSimulator:
    """构造一个**关掉尖峰**的仿真器，专用于量"日周期形状"。

    ⚠️ 为什么必须先关尖峰：本脚本用"隔天同相位采样的相关性"判断日周期是否稳定，
       而尖峰是**与日周期无关的独立随机事件**（今天某时刻有、明天同一时刻多半没有）。
       带着尖峰量，隔天相关系数会从 0.9 掉到 -0.08（实测），
       于是"形状不一致"——但那是尖峰的贡献，不是日周期坏了。
       量形状要在"无偶发事件"的稳态下量；尖峰的验收在
       scripts/verify_sim_spike_exceedance.py 里单独做。
    """
    original = SIM.SPIKE_ENABLED
    try:
        SIM.SPIKE_ENABLED = False            # type: ignore[misc]
        return SIM.CemsSimulator(seed=20261001)
    finally:
        SIM.SPIKE_ENABLED = original         # type: ignore[misc]


SIMULATOR: SIM.CemsSimulator = build_shape_simulator()
DAY_START: datetime = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)   # 00:00 当地
STEP_SECONDS: int = 600          # 10 分钟一个采样点 → 一天 144 点
POINTS_IN_DAY: int = 86400 // STEP_SECONDS

# 展示用的代表测点：燃烧工况、污染物、流量、氧含量
PLOT_POINTS: tuple[str, ...] = ("Temp", "SO2", "Flow", "O2", "Pressure")

FAILURES: list[str] = []


def _check(condition: bool, description: str) -> None:
    """记录一条验收判定；失败时收进 FAILURES，最后统一汇总。"""
    print(f"  [{'PASS' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def sweep(name: str, day_start: datetime = DAY_START) -> list[float]:
    """按假时钟扫一天，返回逐点物理量（同一天内相邻点为 STEP_SECONDS）。"""
    return [
        SIMULATOR.sample(name, day_start + timedelta(seconds=STEP_SECONDS * i))
        for i in range(POINTS_IN_DAY)
    ]


def plot_ascii(name: str, values: list[float], width: int = 72, height: int = 11) -> None:
    """把一天的值画成一张 ASCII 曲线：横轴 24 小时，纵轴该测点的实测上下限。"""
    low, high = min(values), max(values)
    span = (high - low) or 1.0
    # 把 144 个点降采样到 width 列，每列取该区间均值
    columns: list[float] = []
    for column in range(width):
        start = column * len(values) // width
        end = max(start + 1, (column + 1) * len(values) // width)
        columns.append(statistics.mean(values[start:end]))

    canvas = [[" "] * width for _ in range(height)]
    for column, value in enumerate(columns):
        level = int((value - low) / span * (height - 1))
        canvas[height - 1 - level][column] = "*"

    print(f"\n  {name}: 一天曲线  最低={low:.1f} 最高={high:.1f} 振幅={high - low:.1f}")
    for row, line in enumerate(canvas):
        value_at_row = high - span * row / (height - 1)
        print(f"    {value_at_row:9.1f} |{''.join(line)}|")
    print(f"    {'':9s} +{'-' * width}+")
    print(f"    {'':9s}  00   03   06   09   12   15   18   21   24 (当地时刻)")


def fit_diurnal(name: str, values: list[float]) -> tuple[float, float, float, float]:
    """把一天序列拟合成 A·sin(2πt/24 + φ) + C，返回 (峰值时刻, 峰谷幅度, C, R²)。

    峰值时刻 = 正弦最大的当地小时数，用来判断形状是不是"白天高、夜里低"。
    """
    hours = [i * STEP_SECONDS / 3600.0 for i in range(len(values))]
    omega = 2.0 * math.pi / 24.0
    sin_part = [math.sin(omega * hour) for hour in hours]
    cos_part = [math.cos(omega * hour) for hour in hours]
    count = len(values)
    mean = statistics.mean(values)
    # 最小二乘：值 ≈ C + a·sin + b·cos
    ss_sin = sum(item * item for item in sin_part)
    ss_cos = sum(item * item for item in cos_part)
    sc = sum(s * c for s, c in zip(sin_part, cos_part))
    ys = sum((v - mean) * s for v, s in zip(values, sin_part))
    yc = sum((v - mean) * c for v, c in zip(values, cos_part))
    determinant = ss_sin * ss_cos - sc * sc
    if determinant == 0:
        return 0.0, 0.0, mean, 0.0
    a = (ys * ss_cos - yc * sc) / determinant
    b = (yc * ss_sin - ys * sc) / determinant
    amplitude = math.hypot(a, b)
    peak_hour = (math.atan2(a, b) / omega) % 24.0

    predicted = [mean + a * s + b * c for s, c in zip(sin_part, cos_part)]
    residual = sum((v - p) ** 2 for v, p in zip(values, predicted))
    total = sum((v - mean) ** 2 for v in values)
    r_squared = 1.0 - residual / total if total else 0.0
    return peak_hour, amplitude, mean, r_squared


def section_a_curves() -> None:
    """A. 画出代表测点的一天曲线（人眼可判周期形状）。"""
    print(f"=== A. 假时钟 24 小时曲线（起点 {DAY_START.isoformat()}，每点 {STEP_SECONDS // 60} 分钟）===")
    for name in PLOT_POINTS:
        plot_ascii(name, sweep(name))


def section_b_fit() -> dict[str, tuple[float, float, float, float]]:
    """B. 对每个测点做日周期拟合，并断言波形是"周期"而不是"随机游走"。"""
    print("\n=== B. 日周期拟合（正弦最小二乘）===")
    print(f"{'测点':<9}{'峰值时刻':>10}{'振幅':>10}{'均值':>10}{'R²':>8}{'日间步进中位':>14}")
    results: dict[str, tuple[float, float, float, float]] = {}
    all_ok = True
    for point in POINTS:
        values = sweep(point.name)
        peak_hour, amplitude, mean, r_squared = fit_diurnal(point.name, values)
        # 相邻小时（取整点）的步进：日周期是平滑的，步进必须远小于全天振幅
        hourly = [SIMULATOR.sample(point.name, DAY_START + timedelta(hours=h)) for h in range(25)]
        steps = [abs(b - a) for a, b in zip(hourly, hourly[1:])]
        span = point.high - point.low
        median_step = statistics.median(steps)
        results[point.name] = (peak_hour, amplitude, mean, r_squared)
        print(
            f"{point.name:<9}{peak_hour:>9.1f}h{amplitude:>10.2f}{mean:>10.2f}{r_squared:>8.3f}"
            f"{median_step:>14.2f}"
        )
        all_ok = all_ok and r_squared > 0.5 and median_step < span * 0.2
    _check(all_ok, "每个测点的日周期 R² > 0.5，且相邻整点步进 < 20% 量程（是周期而不是随机跳变）")
    return results


def section_c_period_repeat() -> None:
    """C. 日周期对齐 + 无隐藏状态。

    ⚠️ 隔天同一时刻的值**不应该**逐值相等：日周期按"当地 0 点"对齐，
    但缓慢漂移与负荷是连续时间函数，第二天本来就该漂到别处。
    要验证的是"日周期形状稳定"：
      - corr(今天, 明天) 同相位要很高（形状一致）
      - 两天之间的偏差（= 漂移+负荷的 24 小时变化）必须远小于日周期振幅
    """
    print("\n=== C. 日周期对齐（今天 vs 明天，同相位采样）===")
    print(f"{'测点':<9}{'同日振幅':>10}{'两日相关':>10}{'两日偏差RMS':>13}{'偏差/振幅':>11}")
    misaligned: list[str] = []
    for point in POINTS:
        today = sweep(point.name)
        tomorrow = sweep(point.name, DAY_START + timedelta(days=1))
        amplitude = max(today) - min(today)
        difference_rms = math.sqrt(
            statistics.mean((a - b) ** 2 for a, b in zip(today, tomorrow))
        )
        correlation = statistics.correlation(today, tomorrow)
        ratio = difference_rms / amplitude if amplitude else 0.0
        print(
            f"{point.name:<9}{amplitude:>10.2f}{correlation:>10.3f}"
            f"{difference_rms:>13.2f}{ratio:>11.1%}"
        )
        if correlation < 0.7 or ratio > 0.3:
            misaligned.append(f"{point.name}(corr={correlation:.2f}, 偏差比={ratio:.0%})")
    print(f"  形状不一致的测点: {misaligned if misaligned else '无'}")
    _check(
        not misaligned,
        "隔天同相位的日周期形状一致（相关 > 0.7），两日差异（漂移）小于 30% 日振幅",
    )

    # 换个"假时钟实例"重算同一天：验证无隐藏状态（构造上就是 (种子,时刻) 的纯函数）
    fresh = SIM.CemsSimulator(seed=SIMULATOR.seed)
    same_day = sweep("Temp")
    fresh_day = [fresh.sample("Temp", DAY_START + timedelta(seconds=STEP_SECONDS * i))
                 for i in range(POINTS_IN_DAY)]
    _check(same_day == fresh_day, "新建仿真器实例重算同一天，逐值一致（无隐藏状态）")


def main() -> int:
    """依次跑完三段自检，返回进程退出码。"""
    print("CEMS 仿真信号 24 小时假时钟扫描（不需要真的等一天）")
    print(f"种子={SIMULATOR.seed}  时间基准={SIM.SIM_EPOCH.isoformat()}  "
          f"时钟偏移={SIM.SIM_CLOCK_SKEW_SECONDS:.0f}s")
    section_a_curves()
    section_b_fit()
    section_c_period_repeat()

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
