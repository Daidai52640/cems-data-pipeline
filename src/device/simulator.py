# -*- coding: utf-8 -*-
"""CEMS 仿真信号发生器：给设备层提供「有工业特征 + 可复现」的测点读数。

============================ 为什么要单独一个模块 ============================
改造前 modbus_server.build_registers() 是 `random.uniform(low, high)`：
每个刷新周期全部测点重新随机、彼此独立、前后无关联。
实测连续三条 Temp = 52.6 -> 225.1 -> 9.7，一眼假，没法做趋势/报表/故障演练。

本模块把「数值怎么造」与「怎么放进 Modbus 寄存器」拆开：
    - 本模块只负责"某一时刻各测点的物理量是多少"，不依赖 pymodbus
    - modbus_server 只负责 encode() 后写寄存器
好处：验证脚本可以不启服务、不连端口，直接用假时钟驱动本模块跑验收。

============================ 信号模型（五项叠加） ============================
每个测点的值 = 量程百分比，各项相加后再裁回 [low, high]：

    1. base      基线        每个测点一个固定水平（基荷工况），种子决定
    2. diurnal   日周期      用实测时钟的"当天 0 点起的秒数"驱动正弦 + 12 小时谐波，
                            白天高、夜里低，峰值时刻按测点错开（烟气温度固定在 14 点）
    3. load * g  负荷耦合    全网共用一个缓慢的日负荷波形（流量/流速/温度正相关、
                            氧含量负相关），模拟"生产负荷一起涨落"
    4. drift     独立缓慢漂移 每测点自己的 value noise（多尺度平滑噪声），
                            相邻周期只挪一点点 → 这就是"惯性"
    5. jitter    传感器毛刺   白噪声，幅度只有日周期/漂移的**几十分之一**
                            （见 NOISE_JITTER_SPAN）

============================ 可复现性的关键设计 ============================
本模块**没有随机游走状态**：每一点的读数都是 (种子, 时刻) 的纯函数。
    - 种子：SIM_SEED，未设置时用 DETERMINISTIC_DEFAULT_SEED（不是随机默认值）；
            每测点用 sha256(种子:测点名) 派生子种子 → 增删测点不影响其它测点序列
    - 时刻：`now` 参数显式传入（默认取本机时钟），测试时传假时钟即可
所以「同种子 + 同时刻参数 => 逐值一致」是构造上成立的，不需要额外对齐状态。

============================ 参数标定（不是拍脑袋） ============================
幅度一律用**量程百分比**表示，量程来自 points.py（不得改）。以 Temp(0~300, scale=10) 为例：

    日周期幅度  3.5% 量程 = ±10.5 degC     （烟气温度昼夜波动量级）
    独立漂移    2.5% 量程 = ±7.5  degC     （数小时级工况漂移）
    毛刺        0.05% 量程 = ±0.15 degC    （仪表末位跳动，见 NOISE_JITTER_SPAN）

日周期与漂移的幅度是毛刺的 **50 倍以上**，时间尺度也分得开：
24 小时（日周期）/ 数小时（漂移）/ 单次刷新（毛刺），
所以曲线是"缓慢走势 + 末位小毛刺"，而不是随机数。
============================================================================
"""

from __future__ import annotations

import hashlib
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Final, Mapping, Optional

from src.common.points import LIMITS, POINTS, Point

# ==================== 1. 配置区（要改参数只动这里） ====================

# ---- 随机种子（可复现的根）----
# 不设置 SIM_SEED 时用下面这个固定默认值，**不是**随机生成的种子：
# 否则每次跑出来的"基准曲线"都不一样，故障演练无法复现。
DETERMINISTIC_DEFAULT_SEED: Final[int] = 20261001

# ---- 时间基准与时钟偏移 ----
# SIM_EPOCH：漂移/负荷波形的 t=0 时刻（ISO 8601，必须带时区偏移，默认 UTC）。
#            改它只整体平移漂移波形；日周期不受影响（日周期按当地 0 点算）。
# SIM_CLOCK_SKEW：给仿真时钟加的固定偏移（秒）。故障演练用：
#            例如设 43200 让设备认为现在是 12 小时之后，制造"时间错位"故障。
SIM_EPOCH: Final[datetime] = datetime.fromisoformat(
    os.getenv("SIM_EPOCH", "2026-01-01T00:00:00+00:00")
)
SIM_CLOCK_SKEW_SECONDS: Final[float] = float(os.getenv("SIM_CLOCK_SKEW", "0"))
SECONDS_PER_DAY: Final[float] = 86400.0

# ---- 各层幅度（单位：量程百分比；1.0 = 满量程）----
# ⚠️⚠️ 基线锚在哪儿，是**业务口径**，不是代码风格问题（2026-10-01 修正）⚠️⚠️
#
# 第一版把基线取成"量程的 50%~90%"，结果污染物常年顶在限值 3~10 倍：
#     实测 SO2 均值 105（限值 35）、NOx 227（限值 50）、Dust 52（限值 5）
#     → 告警会**一直亮着**，等于没有告警；"超标"也就不再是事件。
# 根因：**限值只占量程的一小截**，而基线锚错了参照物。
#     SO2  限值 35 / 量程 200 = 量程的 17.5%
#     NOx  限值 50 / 量程 400 = 量程的 12.5%
#     Dust  限值  5 / 量程 100 = 量程的  5%
#     基线却取量程的 50%~90% ⇒ 必然远高于限值。
#
# 修正后的规则（按测点的**判据**锚定，不是按量程）：
#   - 污染物（有环保限值，见 LIMITS[name] < high）：基线 = **限值 × 60%~85%**
#       ⇒ 平时达标；日周期+漂移+毛刺+偶发尖峰才偶尔越过限值 ⇒ **超标成为事件**
#   - 物理量（无限值判据）：基线 = 量程 × 40%~70%
#       ⇒ 按工况合理区间取，没有法规参照
# ⚠️ 换行业/换限值（points.py）时，这两个比例要跟着现场实际调整。
BASELINE_OF_LIMIT_LOW: Final[float] = 0.60       # 污染物：占限值的比例区间下限
BASELINE_OF_LIMIT_HIGH: Final[float] = 0.85
BASELINE_OF_RANGE_LOW: Final[float] = 0.40       # 物理量：占量程的比例区间下限
BASELINE_OF_RANGE_HIGH: Final[float] = 0.70
# DIURNAL_*：日周期幅度与峰值时刻
DIURNAL_AMPLITUDE: Final[float] = 0.035
DIURNAL_HARMONIC_RATIO: Final[float] = 0.30      # 12 小时谐波占比（让白天/夜里不完全对称）
DIURNAL_PEAK_HOUR_LOW: Final[float] = 9.0
DIURNAL_PEAK_HOUR_HIGH: Final[float] = 15.0      # 未固定峰值的测点落在 9~15 点之间
# DRIFT_*：独立缓慢漂移（两个尺度的 value noise 叠加）
#   ⚠️ 2026-10-01：引入参照跨度缩放后漂移被压小，我先把它抬到 0.12 —— **这是错的**：
#      它违反了本模块的层次约束「毛刺×10 < 漂移 < 日周期」，导致漂移盖过日周期，
#      24h 周期性自检直接不过。正确做法是**保持层次**，用下面的 scale 去解决分辨率问题。
#   取值 0.030：物理量漂移 ±2.2% 量程；污染物按参照跨度缩放后约 ±0.5% 限值。
DRIFT_SCALE: Final[float] = 0.030
DRIFT_MIN_PERIOD: Final[float] = 900.0           # 最快漂移分量约 15 分钟一峰
DRIFT_PERIOD_FACTOR: Final[float] = 4.0          # 慢分量周期 = 快分量 × 4
DRIFT_SPLIT: Final[float] = 0.65                 # 慢分量占漂移幅度的比例
# NOISE_JITTER_SPAN：传感器毛刺（白噪声）的半幅，单位是**量程百分比**。
#   ⚠️ 不能用"LSB（寄存器量化步长）"当单位：各测点 scale 差 10 倍（Flow=1、其余=10），
#      同一个 LSB 幅度落到 Flow 上相当于 1/1=1.0 个物理单位、
#      落到 Temp 上只有 1/10=0.1 个物理单位，再乘量程后前者会大到 ±0.25 倍量程。
#      按量程百分比取，各测点的相对幅度才一致。
#   取值 0.0025 → 物理量毛刺 ±0.125% 量程；与漂移(3.0%) 的比值 = 1/12，
#      满足"毛刺比漂移小一个数量级以上"的自检约束（这是硬约束，别为了曲线好看去破它）。
NOISE_JITTER_SPAN: Final[float] = 0.0025
# LOAD_*：公共负荷波形（全网共享，"各测点随生产负荷同步涨落"）
LOAD_SCALE: Final[float] = 0.012
LOAD_PERIOD: Final[float] = 1800.0
LOAD_HARMONIC_RATIO: Final[float] = 0.40

# ---- 物理相关性系数（谁跟着负荷走）----
# 正相关：负荷涨 → 流量/流速/烟温涨；负相关：负荷涨 → 氧含量降（燃烧更充分）
COUPLING_BY_NAME: Final[Mapping[str, float]] = {
    "Flow": 1.00,
    "Velocity": 0.95,
    "Pressure": -0.35,      # 引风机后静压：负荷大时负压更深（本表演示为表压，故取弱负相关）
    "Temp": 0.70,
    "Humidity": 0.35,
    "O2": -0.45,
    "Dust": 0.25,
    "SO2": 0.20,
    "NOx": 0.15,
}
# 未登记的测点按 0 处理（不跟负荷走，只走自己的日周期与漂移）

# ---- 日周期峰值时刻（当地时刻，小时）----
# 不在表里的测点由种子在 [DIURNAL_PEAK_HOUR_LOW, DIURNAL_PEAK_HOUR_HIGH] 内随机决定；
# 写死的表示按工艺常识固定（烟气温度峰值在 14 点前后），方便用假时钟验证相位。
PEAK_HOUR_BY_NAME: Final[Mapping[str, float]] = {"Temp": 14.0}

# ---- value noise 的格点数 ----
# 8 个格点：够平滑（不会出现折角），也够"不重复"（周期内形状不单调）
LATTICE_SIZE: Final[int] = 8


# ==================== 2. 工具函数 ====================

def _is_limit_anchored(point: Point) -> bool:
    """该测点的基线是否按**环保限值**锚定。

    判定：契约里登记了 limit，且 limit 明显小于量程上限 ⇒ 这是有法规判据的污染物。
    （物理量如温度、压力没有排放限值，按量程锚定。）
    """
    limit = LIMITS.get(point.name)
    return limit is not None and limit < point.high * 0.5


def _baseline_span(point: Point) -> float:
    """基线可浮动的参照跨度：污染物 = 限值，物理量 = 量程。"""
    if _is_limit_anchored(point):
        return LIMITS[point.name]
    return point.high - point.low


def _baseline_band(point: Point) -> tuple[float, float]:
    """返回该测点的基线比例区间（占"参照跨度"的比例）。"""
    if _is_limit_anchored(point):
        return (BASELINE_OF_LIMIT_LOW, BASELINE_OF_LIMIT_HIGH)
    return (BASELINE_OF_RANGE_LOW, BASELINE_OF_RANGE_HIGH)


def _derive_rng(salt: int | str, stream: str) -> random.Random:
    """按 (盐, 派生名) 派生子随机源。

    ⚠️ 不用 hash()：CPython 对 str 的 hash 每个进程带随机盐（PYTHONHASHSEED），
    用它派生子种子会导致"同一个 SIM_SEED 两次运行不同结果"。
    改用 sha256，跨进程/跨机器稳定。
    """
    digest = hashlib.sha256(f"{salt}:{stream}".encode("utf-8")).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _jit(stream: str, moment: datetime) -> float:
    """按 (派生名, 时刻) 确定性地取一个毛刺值 ∈ [-1, 1]。

    用**有界均匀分布**而不是正态：正态抽样偶尔会给 4~6σ 的离群值，
    在曲线上就是一次"仪表跳一大格"的假故障，会污染后续故障演练的判断；
    均匀分布本身有界，乘上 NOISE_JITTER_SPAN 后幅度完全可控。

    ⚠️ 两个坑，别踩：
      1. 不能用模块级 random：它的状态随调用次数漂移，同一时刻重算会得到不同的值，
         "同种子两次输出一致"就不成立了。
      2. random.uniform(a, b) 的签名里**没有** seed 参数，写成 uniform(seed, -1.0, 1.0)
         不会报错，但会把 seed 当成下界用（结果变成 [seed, -1.0] 之间的随机数）。
         正确姿势是先构造 Random 再 uniform。
    """
    return _derive_rng(int(moment.timestamp() * 1_000_000), stream).uniform(-1.0, 1.0)


def _smoothstep(ratio: float) -> float:
    """三次平滑插值 3t²-2t³：让 value noise 在格点处一阶连续，不会出现折角。"""
    return ratio * ratio * (3.0 - 2.0 * ratio)


def _value_noise(lattice: tuple[float, ...], x: float) -> float:
    """在预生成格点值之间平滑插值，得到 [-1, 1] 的连续噪声。

    格点值是固定序列（由派生子种子一次生成），所以同一 x 永远得到同一个值。
    """
    span = len(lattice) - 1
    position = x % span
    index = int(position)
    ratio = _smoothstep(position - index)
    left, right = lattice[index], lattice[(index + 1) % len(lattice)]
    return left + (right - left) * ratio


def _sine_wave(t: float, period: float, amplitude: float, phase: float) -> float:
    """一个正弦分量；t 单位秒，phase 单位弧度。"""
    return amplitude * math.sin(2.0 * math.pi * t / period + phase)


def _local_seconds_of_day(moment: datetime) -> float:
    """取"当地当天 0 点起过了多少秒"，用于驱动日周期。

    用本地时间而不是 UTC：厂区的白天就该是曲线的高位段
    （用 UTC 会让峰值整体偏 8 小时，报表上"中午最高"会变成"凌晨最高"）。
    """
    return (
        moment.hour * 3600.0 + moment.minute * 60.0 + moment.second + moment.microsecond / 1e6
    )


# ==================== 3. 测点参数与发生器 ====================

@dataclass(frozen=True)
class PointSimParams:
    """单个测点的仿真参数（全部由种子一次性决定，之后只读）。"""

    name: str
    low: float
    high: float
    scale: int
    base_ratio: float               # 基线：占"参照跨度"的比例（污染物=限值，物理量=量程）
    span_ratio: float               # 参照跨度 / 量程 —— 把"量程百分比"幅度换算到同一参照
                                    #   污染物：limit/(high-low)（很小，如 SO2=0.175）
                                    #   物理量：1.0
                                    #   ⚠️ 没有它就会把"量程 3.5% 的日周期"直接加到
                                    #      "限值 75% 的基线"上 → 污染物周期性超标 = 又变成常年超标
    diurnal_phase: float            # 日周期相位（弧度）
    diurnal_harmonic_phase: float
    drift_period: float             # 慢漂移分量周期（秒）
    drift_period_fast: float
    coupling: float
    lattice_slow: tuple[float, ...]
    lattice_fast: tuple[float, ...]

    @property
    def span(self) -> float:
        """量程宽度（high - low）。"""
        return self.high - self.low

    @property
    def ref_span(self) -> float:
        """参照跨度：污染物=环保限值，物理量=量程宽。

        ⚠️ base_ratio / _ratio_at 都是以它为 1.0，**不是**以量程为 1.0。
           量程单位只用于最后裁剪 [low, high]。
        """
        return self.span_ratio * self.span


def _build_lattice(rng: random.Random) -> tuple[float, ...]:
    """生成 value noise 的格点值序列（[-1, 1]）。"""
    return tuple(rng.uniform(-1.0, 1.0) for _ in range(LATTICE_SIZE))


def _build_params(point: Point, seed: int) -> PointSimParams:
    """按测点契约 + 种子，派生该测点的全部仿真参数。

    ⚠️ 派生名用测点名：这样增删某个测点只影响它自己的序列，其它测点的曲线不变。
    """
    rng = _derive_rng(seed, point.name)
    peak_hour = PEAK_HOUR_BY_NAME.get(
        point.name, rng.uniform(DIURNAL_PEAK_HOUR_LOW, DIURNAL_PEAK_HOUR_HIGH),
    )
    drift_period = DRIFT_MIN_PERIOD * rng.uniform(1.0, 1.8)

    return PointSimParams(
        name=point.name,
        low=point.low,
        high=point.high,
        scale=point.scale,
        base_ratio=rng.uniform(*_baseline_band(point)),   # ⚠️ 占"限值"或"量程"的比例，见 _baseline_band
        span_ratio=(
            _baseline_span(point) / (point.high - point.low)
            if point.high > point.low else 1.0
        ),
        # 相位换算：sin 在 π/2 处取峰 → 想让峰值落在 peak_hour，相位要减掉(2π × 小时/24)
        diurnal_phase=math.pi / 2.0 - 2.0 * math.pi * peak_hour / 24.0,
        diurnal_harmonic_phase=rng.uniform(0.0, 2.0 * math.pi),
        drift_period=drift_period,
        drift_period_fast=drift_period / DRIFT_PERIOD_FACTOR,
        coupling=COUPLING_BY_NAME.get(point.name, 0.0),
        lattice_slow=_build_lattice(rng),
        lattice_fast=_build_lattice(rng),
    )


def _seed_from_env() -> int:
    """读 SIM_SEED；非法值抛 ValueError（启动阶段就炸，别静默换成别的种子）。"""
    raw = os.getenv("SIM_SEED", "").strip()
    if not raw:
        return DETERMINISTIC_DEFAULT_SEED
    return int(raw)


class CemsSimulator:
    """CEMS 测点仿真器：给定时刻，产出该时刻各测点的物理量（可复现）。

    seed=None → 读 SIM_SEED（未设置则用 DETERMINISTIC_DEFAULT_SEED）。
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        self.seed: int = _seed_from_env() if seed is None else seed
        self.params: tuple[PointSimParams, ...] = tuple(
            _build_params(point, self.seed) for point in POINTS
        )
        self._load_lattice: tuple[float, ...] = _build_lattice(
            _derive_rng(self.seed, "LOAD"),
        )
        self._params_by_name: dict[str, PointSimParams] = {
            item.name: item for item in self.params
        }

    # ---- 内部分量 ----

    def _load_signal(self, elapsed: float) -> float:
        """公共负荷波形 ∈ 约 [-1.4, 1.4]：所有测点共用同一个 t，保证彼此同步涨落。"""
        return _value_noise(self._load_lattice, elapsed / LOAD_PERIOD) + _sine_wave(
            elapsed, LOAD_PERIOD, LOAD_HARMONIC_RATIO, 0.0,
        )

    def _jitter(self, name: str, moment: datetime) -> float:
        """传感器毛刺：由 (种子, 测点, 时刻) 确定性生成，与调用顺序无关。"""
        return _jit(f"jitter:{self.seed}:{name}", moment)

    def _ratio_at(self, params: PointSimParams, moment: datetime) -> float:
        """算出某测点该时刻的值，单位是"参照跨度比例"（污染物=限值比，物理量=量程比）。"""
        elapsed = (moment - SIM_EPOCH).total_seconds()
        # 把"量程百分比"的幅度换算到本测点的参照跨度上（物理量=1.0，污染物<1）
        k = params.span_ratio

        # 1) 日周期：用当地 0 点起的秒数，所以每天同一时刻形状一致
        day_seconds = _local_seconds_of_day(moment)
        diurnal = k * (
            _sine_wave(
                day_seconds, SECONDS_PER_DAY, DIURNAL_AMPLITUDE, params.diurnal_phase,
            ) + _sine_wave(
                day_seconds, SECONDS_PER_DAY / 2.0,
                DIURNAL_AMPLITUDE * DIURNAL_HARMONIC_RATIO, params.diurnal_harmonic_phase,
            )
        )

        # 2) 负荷耦合（正/负相关，见 COUPLING_BY_NAME）；负荷本身是量程量，同样要缩放
        load = self._load_signal(elapsed) * LOAD_SCALE * params.coupling * k

        # 3) 独立缓慢漂移：两个尺度的平滑噪声叠加
        drift = k * DRIFT_SCALE * (
            DRIFT_SPLIT * _value_noise(params.lattice_slow, elapsed / params.drift_period)
            + (1.0 - DRIFT_SPLIT) * _value_noise(
                params.lattice_fast, elapsed / params.drift_period_fast,
            )
        )

        # 4) 传感器毛刺（白噪声，幅度最小）
        jitter = k * NOISE_JITTER_SPAN * self._jitter(params.name, moment)
        return params.base_ratio + diurnal + load + drift + jitter

    # ---- 对外接口 ----

    def sample(self, name: str, now: Optional[datetime] = None) -> float:
        """取单个测点在 now 时刻的物理量（已裁进量程 [low, high]）。"""
        params = self._params_by_name.get(name)
        if params is None:
            raise KeyError(f"未知测点: {name}（测点契约见 src/common/points.py）")

        moment = now if now is not None else datetime.now(timezone.utc)
        raw = params.low + self._ratio_at(params, moment) * params.ref_span
        # ★ 必须裁进量程：超量程数据会被接入层整条拒收（subscriber_to_td.parse_payload），
        #   那样连"超标告警"演练都看不到数据，所以宁可顶部削平也不能越界。
        return min(max(raw, params.low), params.high)

    def sample_all(self, now: Optional[datetime] = None) -> dict[str, float]:
        """取全部测点在 now 时刻的物理量，返回 {测点名: 值}。"""
        moment = now if now is not None else datetime.now(timezone.utc)
        return {params.name: self.sample(params.name, moment) for params in self.params}

    def simulate_clock(self, now: Optional[datetime] = None) -> datetime:
        """返回加了 SIM_CLOCK_SKEW 之后的仿真时刻（默认取本机 UTC 时刻）。"""
        moment = now if now is not None else datetime.now(timezone.utc)
        return moment + timedelta(seconds=SIM_CLOCK_SKEW_SECONDS)


# 模块级默认实例：modbus_server 直接用；验证脚本另建实例或另传时刻即可
SIMULATOR: Final[CemsSimulator] = CemsSimulator()
