# -*- coding: utf-8 -*-
"""测点契约：全链路唯一的一份定义（寄存器地址、量程、TDengine 列名、单位、超标限值）。

设备层、网关层、平台接入层、展示层都从这里取测点定义。
以前同一份契约散在 5 个文件里各抄一遍，加/改测点必然漏改；
现在只有这一处是真源，改完各层自动跟着变。

============================ 2026-09-30 改版说明 ============================
本文件已按**环保平台真实格式**重排：12 个烟气测点，只做烟气（不做水）。

测点顺序（与 HJ 212 上传报文的值序对应）：
    1 烟气流量   2 颗粒物   3 二氧化硫   4 氮氧化物   5 氧含量
    6 烟气流速   7 烟气温度 8 烟气湿度   9 烟气压力

★★ 折算值（颗粒物/SO2/NOx）**不占寄存器、也不进 TDengine 列**：它由公式从
   "实测(标干)浓度 + 氧含量"算出来，见 to_reference_o2()。理由：
     1. 折算值是**导出量**，存一份就可能与算出来的不一致（冗余必然会漂）
     2. Modbus 保持寄存器只有 9 个，省下来给真正的物理量
   下游要折算值时**调用函数算**，不要另建列。

★★ 标准状态与折算的关键约定（必须守住）：
  - "标干" = 标准状态（273.15 K、101.325 kPa）+ 干烟气（扣除水分）下的质量浓度
  - "折算" = 把标干浓度统一到**基准氧含量**下的浓度，公式：

        折算浓度 = 标干浓度 × (21 - 基准氧含量) / (21 - 实测氧含量)

    ⚠️ 本公式的**标准出处待库主核对**（来源只拿到二次转述，未取到一手条文）。
       常见表述见 HJ 75-2017 及地方环保部门折算要求；燃煤锅炉基准氧含量取 6%。
  - **标干 ≠ 折算**：一个是物理换算，一个是按基准氧含量的标准化；
    氧含量高于基准 → 折算值**变大**（稀释效应被修正回来）。
  - HJ 75-2017 基准氧含量：燃煤锅炉 6%、燃油/燃气锅炉 3%、生活垃圾焚烧 11%
    ⚠️ 本表按**燃煤锅炉 6%** 取值；换行业必须改 O2_REFERENCE

★ 单位一律用国标写法（避免非 ASCII 字符进代码）：
  烟气流量 m3/h ｜ 流速 m/s ｜ 温度 degC ｜ 湿度 % ｜ 氧含量 % ｜ 压力 kPa

⚠️ 两处**为工程可行性做的偏离，已标注**（不是笔误）：
  1. 烟气**静压**国标单位是 Pa 且常为负值（引风机后负压）；但 Modbus 保持寄存器是
     16 位无符号，负值编不进去。故本表演示为 **kPa 表压（85~105）**。
     真实项目若必须传 Pa 负值，需要改用有符号寄存器约定或加偏置。
  2. 温度单位写作 **degC**（ASCII），展示层渲染为 ℃；理由是脚本/编码安全。

★ scale 是**每个测点自带**的寄存器换算系数（不是全局一个值）：
  Modbus 保持寄存器 16 位无符号（0~65535），`量程 × scale` 必须留余量。
  2026-09-30 改版烟测实测：Flow 200000×10、Pressure 10000 → 双双溢出 → 故引入本字段。

★ 超标限值（limit）的出处与量程是两回事，别混：
  - 量程（low/high）= 仪器能测的物理范围 → 接入层据此**拒收**超量程数据
  - 限值（limit）    = 排放标准允许的上限  → 达标判定/告警据此**报警**
  - ⚠️ 本表 limit 取值（库主 2026-09-30 定，**燃煤电厂超低排放**）：
        颗粒物 5、二氧化硫 35、氮氧化物 50（mg/m3，标干、6% 基准氧）
    **限值比量程小一两个数量级是正常的**——超量程=表坏了要拒收，
    超限值=排超标要告警，两件事不能混。换行业/换地区必须改这张表。
============================================================================
"""

from __future__ import annotations

from typing import Final, NamedTuple


class Point(NamedTuple):
    """单个测点的完整契约。"""

    name: str      # 字段名（MQTT 报文字段名 + 寄存器日志名）
    address: int   # Modbus 保持寄存器地址
    column: str    # TDengine 列名
    low: float     # 量程下限（也是仿真取值范围下限）
    high: float    # 量程上限（超量程的数据会被接入层拒收）
    unit: str      # 单位
    limit: float   # 排放限值（达标判定/告警用）；无标准的物理量填量程上限
    code: str      # 环保平台参数编码（HJ 212 风格）；下游映射用真源仍是 COLUMNS
    scale: int = 10  # ⭐ 本测点专属的寄存器换算系数（默认 10）


# ---- 全链路唯一真源：加/改测点只改这里 ----
#
# ⚠️ scale 是 per-point 的，不是全局的：Modbus 保持寄存器是 16 位无符号（0~65535），
#    量程 × scale 必须留出余量，否则高频大数（流量、压力）会溢出。
#    2026-09-30 改版时烟测实测到 Flow 200000×10、Pressure 10000×10 双双溢出 → 故引入本字段。
#
POINTS: Final[tuple[Point, ...]] = (
    # 名称          地址  列名           量程下限   量程上限    单位     限值    平台编码     换算
    Point("Flow",       0, "flow",          0.0,   65000.0, "m3/h", 65000.0, "B01",        1),
    Point("Dust",       1, "dust",          0.0,     100.0, "mg/m3",      5.0, "a21001",    10),
    Point("SO2",        2, "so2",           0.0,     200.0, "mg/m3",     35.0, "a21002",    10),
    Point("NOx",        3, "nox",           0.0,     400.0, "mg/m3",     50.0, "a21003",    10),
    Point("O2",         4, "o2",            0.0,      25.0, "%",        25.0, "a21008",    10),
    Point("Velocity",   5, "velocity",      0.0,      30.0, "m/s",      30.0, "B02",       10),
    Point("Temp",       6, "temp",          0.0,     300.0, "degC",    300.0, "B03",       10),
    Point("Humidity",   7, "humidity",      0.0,     100.0, "%",       100.0, "B04",       10),
    Point("Pressure",   8, "pressure",     85.0,     105.0, "kPa",     105.0, "B05",       10),
)

# ---- 折算基准：燃煤锅炉 6%（HJ 75-2017）----
O2_REFERENCE: Final[float] = 6.0

# ---- 设备/网关共用的寄存器约定 ----
SCALE: Final[int] = 10        # 寄存器里存的是真实值 ×10 的整数（35.2 → 352）
REG_BASE: Final[int] = 0      # 寄存器起始地址

# ---- 各层按需取用的只读视图，避免各自再推导一遍 ----
NAMES: Final[tuple[str, ...]] = tuple(point.name for point in POINTS)
COLUMNS: Final[tuple[str, ...]] = tuple(point.column for point in POINTS)
REGISTER_MAP: Final[tuple[tuple[str, int], ...]] = tuple(
    (point.name, point.address) for point in POINTS
)
RANGES: Final[dict[str, tuple[float, float]]] = {
    point.name: (point.low, point.high) for point in POINTS
}
UNITS: Final[dict[str, str]] = {point.name: point.unit for point in POINTS}
LIMITS: Final[dict[str, float]] = {point.name: point.limit for point in POINTS}
CODES: Final[dict[str, str]] = {point.name: point.code for point in POINTS}
SCALES: Final[dict[str, int]] = {point.name: point.scale for point in POINTS}
REG_COUNT: Final[int] = max(point.address for point in POINTS) + 1

# 寄存器能表达的整数上限（16 位无符号留一点余量），供烟测/自检用
REG_INT_MAX: Final[int] = 65535

# 契约自检：任何测点的量程 × scale 超上限，导入时就应该炸，而不是等到运行期
_OVERFLOW: Final[tuple[str, ...]] = tuple(
    point.name for point in POINTS if point.high * point.scale > REG_INT_MAX
)
if _OVERFLOW:
    raise ValueError(
        f"测点 {_OVERFLOW} 的量程 × scale 超过寄存器上限 {REG_INT_MAX}，"
        "请调小量程或 scale（见 points.py 顶部说明）"
    )


# ---- 折算：标干浓度 → 折算浓度（氧含量修正）----

# 参与折算的污染物（列名）；O2 列的列名见 O2_COLUMN
ZS_TARGETS: Final[tuple[str, ...]] = ("dust", "so2", "nox")
O2_COLUMN: Final[str] = "o2"


def to_reference_o2(value: float, o2: float, o2_ref: float = O2_REFERENCE) -> float:
    """把**标干浓度**折算到**基准氧含量**下的浓度。

        折算浓度 = 标干浓度 × (21 - 基准氧含量) / (21 - 实测氧含量)

    ⚠️ 两条纪律：
      1. **折算值是算出来的，不存库、不进寄存器**（见本文件顶部说明）
      2. 实测氧含量 ≥ 21% 时分母 ≤ 0 → 折算无物理意义，返回 nan（不要瞎除）
      3. 本公式的标准出处**待库主核对**（只拿到二次转述，未取到一手条文）

    对照（基准氧 6%）：O2=6% → 折算=标干；O2=9% → 折算放大；O2=3% → 折算缩小。
    """
    denominator = 21.0 - o2
    if denominator <= 0.0:
        return float("nan")
    return value * (21.0 - o2_ref) / denominator


def over_limit(name: str, value: float) -> bool:
    """按测点契约的 limit 判定是否超标（达标判定/告警用，与量程拒收无关）。

    ⚠️ 调用方要自己保证传入的是**该判定的那个量**：环保口径判**折算值**（不是标干值）。
    """
    limit = LIMITS[name]
    return value > limit

