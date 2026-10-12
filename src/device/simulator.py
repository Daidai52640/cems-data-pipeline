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

============================ 信号模型（六项叠加） ============================
每个测点的值 = 参照跨度比例（污染物=限值比，物理量=量程比），算完再裁回 [low, high]：

    1. base      基线        每个测点一个固定水平（基荷工况），种子决定；
                            **污染物锚限值、物理量锚量程**（见下面的基线标定）
    2. diurnal   日周期      用实测时钟的"当天 0 点起的秒数"驱动正弦 + 12 小时谐波，
                            白天高、夜里低，峰值时刻按测点错开（烟气温度固定在 14 点）
    3. load * g  负荷耦合    全网共用一个缓慢的日负荷波形（流量/流速/温度正相关、
                            氧含量负相关），模拟"生产负荷一起涨落"
    4. drift     独立缓慢漂移 每测点自己的 value noise（多尺度平滑噪声），
                            相邻周期只挪一点点 → 这就是"惯性"
    5. jitter    传感器毛刺   白噪声，幅度只有日周期/漂移的**几十分之一**
    6. spike     偶发尖峰     **只在污染物上**、低概率触发的短暂事件，
                            单位是"限值比"（不乘 span_ratio，见 SPIKE_* 说明）
                            ⇒ 让"超标"成为可被下游观测到的事件，而不是永远达标或永远超标

============================ 可复现性的关键设计 ============================
本模块**没有随机游走状态**：每一点的读数都是 (种子, 时刻) 的纯函数。
    - 种子：SIM_SEED，未设置时用 DETERMINISTIC_DEFAULT_SEED（不是随机默认值）；
            每测点用 sha256(种子:测点名) 派生子种子 → 增删测点不影响其它测点序列
    - 时刻：`now` 参数显式传入（默认取本机时钟），测试时传假时钟即可
所以「同种子 + 同时刻参数 => 逐值一致」是构造上成立的，不需要额外对齐状态。

★ 多台设备（Modbus 多从站）如何做到"各自独立、肉眼可辨"：
    每个从站一个 CemsSimulator 实例 = 一个独立的种子 + 一个相位偏移
    （modbus_server 按 `SLAVE_IDS` 逐个派生，见该文件的 device_simulators()）。
    种子不同 ⇒ 基线水平、日周期峰值时刻、漂移形状、尖峰幅度全都不同；
    相位不同 ⇒ 即使种子相同，两条曲线也不会重合。
    从站 1 沿用 SIM_SEED 且相位为 0 ⇒ **单设备形态与改造前逐值一致**。

============================ 参数标定（不是拍脑袋） ============================
幅度都用**参照跨度比例**表示（污染物=限值，物理量=量程；两者用 span_ratio 换算）。
以 SO2(量程 0~200、限值 35、span_ratio=0.175) 为例（量程 → 物理量要 ×200）：

    基线        限值的 50%~68%   = 17.5~24 mg/m3   （平时达标，留约 30% 余量）
    日周期幅度  3.5% 量程         = ±1.2 mg/m3      （≈ 限值的 ±3.5%）
    独立漂移    3.0% 量程         = ±1.1 mg/m3
    毛刺        0.25% 量程        = ±0.09 mg/m3
    尖峰        限值的 0.30~0.55  = 10~19 mg/m3     （偶发，让折算值冲过限值）

日周期与漂移的幅度是毛刺的 **10 倍以上**（自检 C 段断言这一条），
时间尺度也分得开：24 小时（日周期）/ 数小时（漂移）/ 单次刷新（毛刺）/
数分钟（尖峰），所以曲线是"缓慢走势 + 末位小毛刺 + 偶发尖峰"，而不是随机数。

★ 参照物必须按**判据**选，不能一律按量程（这是修过的真缺陷）：
    污染物 → 锚"限值"（否则基线 = 量程 50%~90% = 限值的 3~10 倍 → 告警常亮）
    O2     → 锚"实测运行区间"（实测烟气氧含量 3%~8%；按量程 40%~70% 会得到 12.5%，
             而折算公式 折算 = 标干 × 15/(21-O2) 对 O2 极敏感：
             O2=12.5% 时分母只有 8.5，全测点折算值被放大 1.9 倍 → 折算口径 100% 越限）
    其他物理量 → 锚"量程"（没有法规判据，也没有折算敏感性）
============================================================================
"""

from __future__ import annotations

import hashlib
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
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
#   - 污染物（有环保限值，见 LIMITS[name] < high）：基线 = **限值 × 50%~68%**
#       ⇒ 平时达标；日周期+漂移+毛刺+偶发尖峰才偶尔越过限值 ⇒ **超标成为事件**
#   - 物理量（无限值判据）：基线 = 量程 × 40%~70%
#       ⇒ 按工况合理区间取，没有法规参照
# ⚠️ 上界别贪高（2026-10-01 实测教训）：先取到 0.75 时，NOx 的基线+漂移峰值就到了
#    限值的 1.0 倍 ⇒ 24 小时里 **20% 的时间在越限**，看着又像"常年超标"。
#    现在留出约 30% 余量：实测无尖峰时折算峰值 ≈ 0.62~0.71 倍限值、越限 0.0%，
#    越限完全由尖峰驱动。
# ⚠️ 换行业/换限值（points.py）时，这两个比例要跟着现场实际调整。
BASELINE_OF_LIMIT_LOW: Final[float] = 0.50       # 污染物：占限值的比例区间下限
BASELINE_OF_LIMIT_HIGH: Final[float] = 0.68
BASELINE_OF_RANGE_LOW: Final[float] = 0.40       # 物理量：占量程的比例区间下限
BASELINE_OF_RANGE_HIGH: Final[float] = 0.70
# ⚠️⚠️ 例外：O2 必须锚"实测运行区间"，不能按量程（2026-10-01 第二次修正）⚠️⚠️
#   O2 在契约里看起来像物理量（limit == high），所以按量程锚到 40%~70% → 10%~17.5%。
#   但**实测烟气氧含量是 3%~8%**（燃煤锅炉），而且折算公式对 O2 极敏感：
#       折算 = 标干 × (21-6)/(21-O2)
#       O2=12.5% → ×1.88 ；O2=6% → ×1.0 ；O2=5% → ×0.94
#   按量程锚定实测结果：O2 均值 12.55% → 全测点折算值被放大 1.9 倍
#       ⇒ Dust/SO2/NOx 折算口径越限占比 **100%**（又变成"告警常亮"）
#   ⇒ 这里显式把 O2 的基线区间钉在实测区间上。换行业（垃圾焚烧 8%~12% 等）要改这里。
BASELINE_RANGE_OVERRIDE: Final[Mapping[str, tuple[float, float]]] = {
    "O2": (0.18, 0.30),      # 量程 0~25% ⇒ 实测 4.5%~7.5%
}
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
#   ⚠️ 不能用"LSB（寄存器量化步长）"当单位：各测点 scale 差 10 倍（Flow=1、Dust=100），
#      同一个 LSB 幅度落到 Flow 上相当于 1/1=1.0 个物理单位、
#      落到 SO2 上只有 1/10=0.1 个物理单位，再乘参照跨度后前者会大到 ±0.25 倍量程。
#      按量程百分比取，各测点的相对幅度才一致。
#   取值 0.0025 → 物理量毛刺 ±0.125% 量程；与漂移(3.0%) 的比值 = 1/12，
#      满足"毛刺比漂移小一个数量级以上"的自检约束（这是硬约束，别为了曲线好看去破它）。
#      按污染物换算：毛刺 ±0.09 mg/m3 ≈ 限值的 0.25%——毛刺必须远小于尖峰(限值的 30%~55%)。
NOISE_JITTER_SPAN: Final[float] = 0.0025
# ---- SPIKE_*：污染物偶发尖峰（2026-10-01 新增）----
# 目的：让"超标"成为**事件**。没有它时污染物恒定低于限值、越限占比 0.00%，
#       告警链路虽然通但没有任何可被下游观测到的触发。
# ⚠️⚠️ 量纲：尖峰幅度是**限值比**，与日周期/漂移/毛刺的"量程比"不同，**不乘 span_ratio** ⚠️⚠️
#   为什么必须例外：span_ratio 的作用是把"量程比"换算成"参照跨度比"。
#   若尖峰也乘它（SO2 的 0.175），峰值只有 0.3×0.175 = 0.05 个限值 ⇒ 永远冲不破限值，
#   加了这个尖峰等于没加。尖峰的**业务定义**就是"把折算值顶过限值"，所以它天然以限值为单位。
#   （注：SO2 的 span_ratio = 0.175 与 BASELINE_OF_LIMIT_HIGH 恰好数值相近，容易看串。）
# 幅度区间：0.30~0.55 个限值 ⇒ 叠加基线(0.50~0.68)后折算值约 1.0~1.2 倍限值
#   ⇒ 真超标（能触发告警），但因为仍裁在量程内，不会被接入层拒收
SPIKE_AMPLITUDE_LOW: Final[float] = float(os.getenv("SIM_SPIKE_AMPLITUDE_LOW", "0.30"))
SPIKE_AMPLITUDE_HIGH: Final[float] = float(os.getenv("SIM_SPIKE_AMPLITUDE_HIGH", "0.55"))
# 触发概率：**每个刷新周期（2 秒）触发一个尖峰的独立概率**（只对污染物生效）。
#   ⚠️ 别按"每小时几次"直觉取值：判断当前时刻是否在尖峰内要扫 ±SPIKE_WIDTH_STEPS 步，
#      所以"某采样落在尖峰窗口内"的概率 ≈ 1-(1-p)^(2×宽度步数)，不是 p 本身。
#      实测教训：p=0.02 + 宽度 300 步 ⇒ 命中率 ≈ 1 ⇒ **几乎每个采样都在尖峰里**，
#      污染物折算均值被顶到限值 5~6 倍 —— 和"基线锚错"是同一类事故（常亮 = 没有告警）。
#   p=0.004 + 宽度 60 步 ⇒ 窗口命中率约 1/3，折算值越限时间占比约 1%~10%（随测点而不同）
#      ⇒ 既算"偶发事件"，又保证 24 小时内每个污染物都能见到若干次超标。
#   0 = 关闭尖峰（做基线标定/周期性自检时用 0，避免尖峰污染统计）。
SPIKE_PROBABILITY: Final[float] = float(os.getenv("SIM_SPIKE_PROB", "0.004"))
# 尖峰形状：以触发时刻为峰的对称三角包络，衰减到 SPIKE_WIDTH_STEPS 步时归零。
#   不直接加方波的理由：方波跳变会让"相邻周期跳变"自检失真（尖峰上下沿是假跳变）；
#   带包络后每个周期只爬升 峰高/步数（默认 ±0.55/60 ≈ 0.9% 限值 = 0.16% 量程），
#   既真实又不触发告警误判。±2 分钟也和真实 CEMS 的短时波动量级相当。
SPIKE_WIDTH_STEPS: Final[int] = int(os.getenv("SIM_SPIKE_WIDTH_STEPS", "60"))
SPIKE_STEP_SECONDS: Final[float] = 2.0           # 尖峰包络的步长（与设备刷新周期对齐）
# 总开关：SIM_SPIKE_ENABLE=0 时所有测点都不加尖峰。
#   用途（自检脚本的 A/B 对照）：基线标定、日周期形状、相邻跳变都要在"无尖峰"下量一次，
#   否则尖峰会污染这些统计（它不是常态信号，是偶发事件）。
SPIKE_ENABLED: Final[bool] = os.getenv(
    "SIM_SPIKE_ENABLE", "1",
).strip().lower() not in ("0", "false", "no", "off")
# LOAD_*：公共负荷波形（全网共享，"各测点随生产负荷同步涨落"）
LOAD_SCALE: Final[float] = 0.012
LOAD_PERIOD: Final[float] = 1800.0
LOAD_HARMONIC_RATIO: Final[float] = 0.40

# ---- 物理相关性系数（谁跟着负荷走）----
# 正相关：负荷涨 → 流量/流速/烟温涨；负相关：负荷涨 → 氧含量降（燃烧更充分）
COUPLING_BY_NAME: Final[Mapping[str, float]] = {
    "Flow": 1.00,
    "Velocity": 0.95,
    "Pressure": -0.35,      # 引风机后静压：负荷大时负压更深（本仿真采用表压，故取弱负相关）
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

    ⚠️ 2026-10-01 修正：原来只看 `limit < high*0.5` 这个**经验阈值**，
    一旦某个污染物的限值落到量程一半以上（真实场景：NOx 限值 200 mg/m3 + 量程 0~400，
    或老标准/非超低排放行业），它会被**静默判成物理量** → 基线改按量程锚定 →
    均值直接顶到限值之上（复核实测：NOx 100% 时间越限）。这类静默正是本次修正要消灭的。

    现在改为**显式声明 + 导入期校验**：声明在 LIMIT_ANCHORED_NAMES 里的测点必须
    真的有 limit，且 limit 明显小于量程上限（防止把量程上限当限值填进来）。
    """
    return point.name in LIMIT_ANCHORED_NAMES


# ---- 哪些测点的基线按环保限值锚定（显式声明，不靠经验阈值猜）----
# 加新污染物时：① 在 points.py 填上它的 limit  ② 把名字加进这里
LIMIT_ANCHORED_NAMES: Final[frozenset[str]] = frozenset({"Dust", "SO2", "NOx"})

# 声明为污染物时，limit 必须明显小于量程上限（否则多半是把量程上限误填成了限值）
_LIMIT_SANITY_RATIO: Final[float] = 0.5

for _name in LIMIT_ANCHORED_NAMES:
    _limit = LIMITS.get(_name)
    if _limit is None:
        raise ValueError(f"{_name} 被声明为按限值锚定，但 points.py 里没有它的 limit")
    _point = next((p for p in POINTS if p.name == _name), None)
    if _point is None:
        raise ValueError(f"LIMIT_ANCHORED_NAMES 里的 {_name} 不在测点契约里")
    if not (_limit < _point.high * _LIMIT_SANITY_RATIO):
        raise ValueError(
            f"{_name} 的 limit={_limit} 不小于量程上限 {_point.high} 的一半；"
            "限值应当明显小于量程（超量程=拒收，超限值=告警，两者不是一回事）。"
            "若确实如此，请从 LIMIT_ANCHORED_NAMES 里移除它或核对 points.py 的 limit。"
        )


def _baseline_span(point: Point) -> float:
    """基线可浮动的参照跨度：污染物 = 限值，物理量 = 量程。"""
    if _is_limit_anchored(point):
        return LIMITS[point.name]
    return point.high - point.low


def _baseline_band(point: Point) -> tuple[float, float]:
    """返回该测点的基线比例区间（占"参照跨度"的比例）。

    优先级：显式区间覆盖 > 污染物锚限值 > 物理量锚量程。

    ⚠️ override 的语义是**参照跨度比**（不是量程比）：污染物上 0.5 表示"限值的一半"，
    物理量上表示"量程的一半"。原因是 `_ratio_at()` 的返回值统一乘以 `ref_span`，
    若 override 单独用别的参照，就会与基线/幅度项量纲不一致（复核报告的 P4）。
    """
    override = BASELINE_RANGE_OVERRIDE.get(point.name)
    if override is not None:
        return override
    if _is_limit_anchored(point):
        return (BASELINE_OF_LIMIT_LOW, BASELINE_OF_LIMIT_HIGH)
    return (BASELINE_OF_RANGE_LOW, BASELINE_OF_RANGE_HIGH)



def _spike_probability(point: Point) -> float:
    """该测点是否参与偶发尖峰；不参与返回 0.0。

    只有**污染物**（按限值锚定的测点）才加尖峰：
      - 物理量（温度/压力/流量…）没有环保判据，"尖峰"没有业务含义
      - O2 的 limit 等于量程上限，本来就不满足 _is_limit_anchored，自然不会加

    SIM_SPIKE_ENABLE=0 可整体关闭（做基线标定、24h 周期性自检时用，
    避免尖峰把"平时达标/周期形状"的统计污染掉）。
    """
    if not SPIKE_ENABLED or not _is_limit_anchored(point):
        return 0.0
    return max(0.0, min(1.0, SPIKE_PROBABILITY))


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

    ⚠️ **必须先把 moment 转成容器本地时区**（2026-10-02 修）：
    调用方 `sample_all()` 传进来的是 `datetime.now(timezone.utc)`（UTC-aware），
    直接取 `.hour` 拿到的是 **UTC 小时** —— 实测容器 TZ=Asia/Shanghai 时
    日周期峰值落在**当地 21 点**（设计应为 13~14 点），恰好就是上一段警告的这个偏差。
    `astimezone()` 无参 = 转到系统本地时区；对 naive datetime 也安全
    （naive 会被当作本地时间，结果不变）。
    """
    local = moment.astimezone()
    return (
        local.hour * 3600.0 + local.minute * 60.0 + local.second + local.microsecond / 1e6
    )


def _spike_envelope(distance: int, width: int) -> float:
    """尖峰包络：distance=0 时取 1.0，到 ±width 步时线性衰减到 0。

    这里刻意不用二次/高斯形状：直接算 (1 - |d|/width) 就是三角包络，
    形状不影响"是否超标"，但可让每个参数都能手算验证。
    """
    return max(0.0, 1.0 - abs(distance) / width)


@lru_cache(maxsize=8192)
def _spike_hit(seed: int, name: str, index: int, probability: float) -> float:
    """第 index 步的尖峰峰值（未乘包络）；未触发返回 0.0。

    ⚠️ 必须带 lru_cache：判断"当前时刻是否落在某个尖峰窗口内"要扫描 ±SPIKE_WIDTH_STEPS
    步，而步长是 2 秒、窗口 300 步 ⇒ 每个采样点要问 601 次。
    不带缓存时 24 小时自检要算几千万次 sha256（分钟级）；
    带缓存后每个"步"只算一次。
    ⚠️ 缓存键里必须含 seed 与 probability：否则不同种子/不同 SIM_SPIKE_PROB 的调用
    会互相串味，破坏可复现性。
    """
    rng = _derive_rng(seed, f"spike:{name}:{index}")
    if rng.random() >= probability:
        return 0.0
    return 1.0


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
    spike_probability: float        # 每个刷新周期的尖峰触发概率（污染物 > 0，其余 = 0）
    spike_amplitude: float          # 尖峰峰值（单位：限值比）——**不乘 span_ratio**，见 SPIKE_*
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
        spike_probability=_spike_probability(point),
        spike_amplitude=rng.uniform(SPIKE_AMPLITUDE_LOW, SPIKE_AMPLITUDE_HIGH),
        lattice_slow=_build_lattice(rng),
        lattice_fast=_build_lattice(rng),
    )


def seed_from_env() -> int:
    """读 SIM_SEED；非法值抛 ValueError（启动阶段就炸，别静默换成别的种子）。"""
    raw = os.getenv("SIM_SEED", "").strip()
    if not raw:
        return DETERMINISTIC_DEFAULT_SEED
    return int(raw)


class CemsSimulator:
    """CEMS 测点仿真器：给定时刻，产出该时刻各测点的物理量（可复现）。

    seed=None → 读 SIM_SEED（未设置则用 DETERMINISTIC_DEFAULT_SEED）。

    phase_offset_seconds：给本实例的仿真时钟加一个固定相位（秒），用于**多台设备**：
        同一个种子 + 不同相位 ⇒ 两条曲线形状同源但错开，肉眼可分；
        不同种子 + 不同相位 ⇒ 基线、日周期峰值时刻、漂移形状全都不同（默认做法）。
    默认 0.0 = 与改造前逐值一致（单设备不受影响）；模块级 SIMULATOR 用的就是默认值。
    """

    def __init__(self, seed: Optional[int] = None, phase_offset_seconds: float = 0.0) -> None:
        self.seed: int = seed_from_env() if seed is None else seed
        self.phase_offset_seconds: float = phase_offset_seconds
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

    def _spike(self, params: PointSimParams, elapsed: float) -> float:
        """偶发尖峰（单位：限值比）；没有尖峰时返回 0.0。

        设计要点（都是为了"可复现 + 不破坏其它自检"）：
          1. **无状态**：把时间轴按 SPIKE_STEP_SECONDS 切成整数步，尖峰是否发生
             由 (种子, 测点名, 步号) 的 sha256 决定 ⇒ 同一时刻永远同一个答案，
             跨进程/跨机器一致，也不需要 random 的隐藏状态
          2. **中心 + 三角包络**：某一步触发后，把该步当作峰心，在 ±WIDTH 步内
             按三角包络衰减 ⇒ 尖峰有上升沿/下降沿，不会是一个 2 秒宽的方波；
             否则"相邻周期跳变"自检会被上下沿的假跳变带偏
          3. 只在触发步的 ±WIDTH 内才做哈希，平时每个采样只查 1 步
        """
        if params.spike_probability <= 0.0:
            return 0.0

        width = SPIKE_WIDTH_STEPS
        current = int(elapsed / SPIKE_STEP_SECONDS)
        peak = 0.0
        for offset in range(-width, width + 1):
            index = current + offset
            if index < 0:
                continue
            if _spike_hit(self.seed, params.name, index, params.spike_probability) <= 0.0:
                continue
            peak = max(peak, params.spike_amplitude * _spike_envelope(offset, width))
            if peak >= params.spike_amplitude:
                break               # 已经到峰心（包络最大就是 1.0），无需再扫
        return peak

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
        # 5) 偶发尖峰：单位已经是"限值比"，**不能**乘 k（乘了就永远冲不破限值，见 SPIKE_*）
        spike = self._spike(params, elapsed)
        return params.base_ratio + diurnal + load + drift + jitter + spike

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
        """返回加了 SIM_CLOCK_SKEW 与本实例相位偏移之后的仿真时刻（默认取本机 UTC 时刻）。

        ⚠️ 相位偏移只改**读数**的时刻，不改网关上的报文时间戳（时间戳由网关自己打），
        所以它只用于让多台设备的曲线错开，不会被误读成"设备时钟不准"。
        """
        moment = now if now is not None else datetime.now(timezone.utc)
        return moment + timedelta(
            seconds=SIM_CLOCK_SKEW_SECONDS + self.phase_offset_seconds
        )


# 模块级默认实例：modbus_server 直接用；验证脚本另建实例或另传时刻即可
SIMULATOR: Final[CemsSimulator] = CemsSimulator()
