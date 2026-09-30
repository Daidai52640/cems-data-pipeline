# -*- coding: utf-8 -*-
"""验收自检 1/3：仿真信号的"工业特征"与前后对比（不需要 Docker，纯离线可跑）。

用法：
    python scripts/verify_sim_characteristics.py

输出四段证据：
    A. 20 个刷新周期内，相邻两个周期同一测点的跳变（新信号 vs 改造前的 random.uniform）
    B. 量程合规：连续 24 小时采样是否始终落在 points.py 的 [low, high] 内
    C. 层次分离：日周期 / 缓慢漂移 / 毛刺三者幅度的量级差
    D. 结束码：全部通过 0，任一不通过 1（可直接进 CI）
"""

from __future__ import annotations

import random
import statistics
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import LIMITS, POINTS                         # noqa: E402
from src.device import simulator as SIM                              # noqa: E402
from src.device.modbus_server import build_registers, encode         # noqa: E402

# ---- 检查参数 ----
CYCLES: int = 20                       # 验收要求：连续 20 个刷新周期
REFRESH_SECONDS: float = 2.0           # 设备层真实刷新周期
FAKE_START: datetime = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
OLD_RANDOM_SEEDS: tuple[int, ...] = (1, 2, 3, 4, 5)   # 改造前信号取多个种子看最坏情况
RANGE_CHECK_SAMPLES: int = 2880        # 24 小时 × 每 30 秒一个点

SIMULATOR: SIM.CemsSimulator = SIM.CemsSimulator(seed=20261001)

FAILURES: list[str] = []


def _check(condition: bool, description: str) -> None:
    """记录一条验收判定；失败时收进 FAILURES，最后统一汇总。"""
    print(f"  [{'PASS' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def new_readings(name: str, cycles: int = CYCLES) -> list[float]:
    """用假时钟取某测点连续 cycles 个刷新周期的物理量。"""
    return [
        SIMULATOR.sample(name, FAKE_START + timedelta(seconds=REFRESH_SECONDS * i))
        for i in range(cycles)
    ]


def old_readings(name: str, seed: int, cycles: int = CYCLES) -> list[float]:
    """复现改造前的算法：每个周期在量程内独立均匀随机（random.uniform）。

    这里刻意**照抄**改造前 modbus_server.build_registers() 的那一行，
    作为"前后对比"的基线，不是新实现的回退路径。
    """
    point = next(item for item in POINTS if item.name == name)
    rng = random.Random(seed)
    return [rng.uniform(point.low, point.high) for _ in range(cycles)]


def encoded_deltas(values: list[float], scale: int) -> list[int]:
    """把物理量按设备层口径编码成寄存器整数，返回相邻差值绝对值。

    ⚠️ 必须比较**编码后**的整数：网关拿到的就是它，
    而且能自动消掉量程差异（Flow 量程 65000、O2 只有 25，直接比物理值没意义）。
    """
    encoded = [encode(value, scale) for value in values]
    return [abs(b - a) for a, b in zip(encoded, encoded[1:])]


def section_a_adjacent_delta() -> None:
    """A. 相邻周期跳变：新信号必须明显小于改造前的纯随机。"""
    print(f"\n=== A. 连续 {CYCLES} 个周期（每 {REFRESH_SECONDS:.0f}s 一次）的相邻跳变 ===")
    print("   基线 = 改造前的 random.uniform(low, high)（取 5 个种子里的最大跳变）")
    print(f"{'测点':<9}{'量程':>9}{'旧-最大':>10}{'旧-占比':>10}{'新-最大':>10}{'新-占比':>10}{'缩小':>8}")

    all_shrunk = True
    for point in POINTS:
        old_max = max(
            max(encoded_deltas(old_readings(point.name, seed), point.scale))
            for seed in OLD_RANDOM_SEEDS
        )
        new_max = max(encoded_deltas(new_readings(point.name), point.scale))
        span = point.high - point.low
        ratio = new_max / old_max
        all_shrunk = all_shrunk and ratio < 0.5
        print(
            f"{point.name:<9}{span:>9.1f}{old_max:>10d}{old_max / span:>10.1%}"
            f"{new_max:>10d}{new_max / span:>10.2%}{ratio:>8.1%}"
        )

    _check(all_shrunk, f"每个测点的最大相邻跳变都缩小到改造前的 50% 以下（{CYCLES} 周期）")


def section_b_range_compliance() -> None:
    """B. 量程合规：任何时刻的值都必须落在 points.py 的 [low, high] 内。"""
    print(f"\n=== B. 量程合规（24 小时 × 每 30 秒，共 {RANGE_CHECK_SAMPLES} 点/测点）===")
    out_of_range: list[str] = []
    for point in POINTS:
        for i in range(RANGE_CHECK_SAMPLES):
            value = SIMULATOR.sample(point.name, FAKE_START + timedelta(seconds=30 * i))
            if not point.low <= value <= point.high:
                out_of_range.append(f"{point.name}={value}")
                break
    print(f"  超出量程的测点: {out_of_range if out_of_range else '无'}")
    _check(not out_of_range, "所有测点在 24 小时内的采样都落在量程内")

    # 端到端口径：build_registers() 产出的寄存器整数换算回物理量也要在量程内
    decode_failures = []
    for i in range(240):
        registers = build_registers(now=FAKE_START + timedelta(seconds=300 * i))
        for point in POINTS:
            value = registers[point.address] / point.scale
            if not point.low <= value <= point.high:
                decode_failures.append(f"{point.name}={value}")
    print(f"  寄存器编码后越界的测点: {decode_failures if decode_failures else '无'}")
    _check(not decode_failures, "build_registers() 编码后的寄存器值换算回物理量仍在量程内")


def section_c_layering() -> None:
    """C. 层次分离：日周期 / 漂移 / 毛刺的幅度必须是"大的大、小的小"。"""
    print("\n=== C. 三层信号的幅度（以量程百分比计）===")
    print(f"  日周期幅度      {SIM.DIURNAL_AMPLITUDE:.4f}（±，含 12 小时谐波）")
    print(f"  独立漂移幅度    {SIM.DRIFT_SCALE:.4f}（±，两个尺度叠加）")
    print(f"  传感器毛刺幅度  {SIM.NOISE_JITTER_SPAN:.4f}（±，白噪声半幅）")
    print(f"  毛刺/日周期 = {SIM.NOISE_JITTER_SPAN / SIM.DIURNAL_AMPLITUDE:.2%}；"
          f"毛刺/漂移 = {SIM.NOISE_JITTER_SPAN / SIM.DRIFT_SCALE:.2%}")
    _check(
        SIM.NOISE_JITTER_SPAN * 10 < SIM.DRIFT_SCALE < SIM.DIURNAL_AMPLITUDE,
        "毛刺比漂移小一个数量级以上，且漂移小于日周期",
    )


def section_d_load_coupling() -> None:
    """D. 负荷耦合：把"负荷分量"单独剥出来，验证相关性符号符合 COUPLING_BY_NAME。

    ⚠️ 不能直接对采样序列求相关：所有测点共享同一个 24 小时日周期，
    默认参数下 corr(Flow, O2) 实测是 **正的**（+0.85，被日周期主导）——
    这本身不是缺陷（白天负荷与氧含量一起涨落是正常工况），
    但它会掩盖负荷耦合项。所以这里按"开耦合 − 关耦合"差分出负荷分量再判符号。
    """
    print("\n=== D. 负荷耦合分量（180s 间隔 200 点；剥离日周期与漂移后再看相关方向）===")
    step = 180
    moments = [FAKE_START + timedelta(seconds=step * i) for i in range(200)]
    load_off = SIM.CemsSimulator(seed=SIMULATOR.seed)
    # 逐点把负荷耦合系数清零：其余分量（基线/日周期/漂移/毛刺）逐值完全相同
    load_off.params = tuple(
        replace(item, coupling=0.0) for item in load_off.params
    )
    load_off._params_by_name = {item.name: item for item in load_off.params}

    def load_component(name: str) -> list[float]:
        """剥离负荷后的残差 = 该测点的负荷分量（正比于 coupling）。"""
        return [
            SIMULATOR.sample(name, moment) - load_off.sample(name, moment)
            for moment in moments
        ]

    def correlation(left: list[float], right: list[float]) -> float:
        left_mean, right_mean = statistics.mean(left), statistics.mean(right)
        numerator = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right))
        denominator = statistics.pstdev(left) * statistics.pstdev(right) * len(left)
        return numerator / denominator if denominator else 0.0

    flow = load_component("Flow")
    velocity = load_component("Velocity")
    o2 = load_component("O2")
    flow_velocity = correlation(flow, velocity)
    flow_o2 = correlation(flow, o2)
    print(f"  耦合系数: Flow={SIM.COUPLING_BY_NAME['Flow']:+.2f} "
          f"Velocity={SIM.COUPLING_BY_NAME['Velocity']:+.2f} "
          f"O2={SIM.COUPLING_BY_NAME['O2']:+.2f}")
    print(f"  corr(负荷分量 Flow, Velocity) = {flow_velocity:+.3f}（期望 > 0）")
    print(f"  corr(负荷分量 Flow, O2)       = {flow_o2:+.3f}（期望 < 0）")
    _check(flow_velocity > 0.0, "Flow 与 Velocity 的负荷分量正相关")
    _check(flow_o2 < 0.0, "Flow 与 O2 的负荷分量负相关（负荷大时氧含量低）")

    # 顺带留个观测：默认参数下两个测点的整条曲线因共享日周期而正相关
    flow_all = [SIMULATOR.sample("Flow", moment) for moment in moments]
    o2_all = [SIMULATOR.sample("O2", moment) for moment in moments]
    print(f"  参考：含日周期的整段采样 corr(Flow, O2) = {correlation(flow_all, o2_all):+.3f}"
          "（日周期主导，故为正）")


def section_e_baseline_calibration() -> None:
    """E. 基线标定：污染物必须**平时达标**（2026-10-01 补）。

    ⚠️ 这一节是补上一个真实事故的护栏：
       第一版把基线取成"量程的 50%~90%"，而限值只占量程的 5%~17.5%，
       结果污染物常年顶在限值 3~10 倍（实测 SO2 均值 105 vs 限值 35），
       "超标告警"永远亮着 = 没有告警。
       根因是**锚错参照物**：污染物该锚"限值"，物理量才锚"量程"。
    """
    print("\n=== E. 基线标定（污染物应锚在限值上，物理量锚在量程上）===")
    print("%-10s %8s %10s %10s %9s %8s" % ("测点", "限值", "均值", "最大", "均值/限值", "越限%"))
    moments = [
        FAKE_START + timedelta(seconds=30 * i) for i in range(24 * 60 * 2)   # 24 小时
    ]
    for point in POINTS:
        values = [SIMULATOR.sample(point.name, m) for m in moments]
        mean = sum(values) / len(values)
        peak = max(values)
        limit = LIMITS.get(point.name)
        anchored = limit is not None and limit < point.high * 0.5
        if not anchored:
            print("%-10s %8s %10.2f %10.2f %9s %8s"
                  % (point.name, "-", mean, peak, "-", "-"))
            continue
        over_pct = 100.0 * sum(1 for v in values if v > limit) / len(values)
        print("%-10s %8.1f %10.2f %10.2f %9.2f %7.2f%%"
              % (point.name, limit, mean, peak, mean / limit, over_pct))
        _check(mean < limit, f"{point.name} 均值 {mean:.2f} 低于限值 {limit:.1f}（平时达标）")
        _check(over_pct < 50.0, f"{point.name} 越限占比 {over_pct:.1f}% 低于 50%（超标是事件不是常态）")


def main() -> int:
    """依次跑完五段自检，返回进程退出码。"""
    print("CEMS 仿真信号自检（离线，不需要 Docker / MQTT / TDengine）")
    print(f"种子={SIMULATOR.seed}  假时钟起点={FAKE_START.isoformat()}  刷新={REFRESH_SECONDS:.0f}s")
    section_a_adjacent_delta()
    section_b_range_compliance()
    section_c_layering()
    section_d_load_coupling()
    section_e_baseline_calibration()

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
