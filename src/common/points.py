# -*- coding: utf-8 -*-
"""测点契约：全链路唯一的一份定义（寄存器地址、量程、TDengine 列名、单位、超标限值）。

设备层、网关层、平台接入层、展示层都从这里取测点定义。
以前同一份契约散在 5 个文件里各抄一遍，加/改测点必然漏改；
现在只有这一处是真源，改完各层自动跟着变。

============================ 2026-09-30 改版说明 ============================
本文件已按**环保平台真实格式**重排：9 个烟气测点，只做烟气（不做水）。

★ 标准依据：**HJ 212-2025**《污染物自动监测监控系统数据传输技术要求》
  （2025-06-06 发布、2026-01-01 实施；**HJ 212-2017 同日废止**，生态环境部公告 2025 年第 12 号）。
  ⚠️ 项目里凡写"按 HJ 212 对齐"的地方，指的都是 **2025 版**。

⚠️ **本文件对齐的是标准里的"数据模型"，不是"传输报文"** —— 这条边界必须说清，别过度声明：
    - ✅ **已对齐**：9 个烟气测点的**集合与顺序**（与标准上传报文的值序一致）、
      参数编码可映射到标准附录 B、超标判定用**折算值**（标准与 HJ 75 的口径）。
    - ❌ **未实现**：HJ 212 的**通信包本身**（`##长度 QN=…;ST=…;CN=…;PW=…;MN=…;Flag=…;CP=&&…&& CRC ##`）、
      数据段加密（§6.4 SM4）、应答与补传标志（`RF`）。
      本项目的实际报文体是**内部简化格式**（`"时间戳 测点=值 … Flag=N"`，走 MQTT），
      **不是 HJ 212 报文**。对环保平台的 HJ212 出口属**预留能力**，见 `docs/adr/0006`。

测点顺序（与 HJ 212 上传报文的值序对应）：
    1 烟气流量   2 颗粒物   3 二氧化硫   4 氮氧化物   5 氧含量
    6 烟气流速   7 烟气温度 8 烟气湿度   9 烟气压力

★★ 折算值（颗粒物/SO2/NOx）在本项目里**不占寄存器、也不进 TDengine 列**：由公式从
   "实测(标干)浓度 + 氧含量"算出来，见 to_reference_o2()。理由：
     1. 折算值是**导出量**，存一份就可能与算出来的不一致（冗余必然会漂）
     2. Modbus 保持寄存器只有 9 个，省下来给真正的物理量
   下游要折算值时**调用函数算**，不要另建列。

   ⚠️ 这是**本项目的主动取舍，不是说现场不能传折算值**。现场确实常把折算值做成
      Modbus 保持寄存器（例：某省数据采集传输协议规定 40003 烟尘折算 / 40007 SO2 折算 /
      40011 NOx 折算，数采仪每 5 s 用功能码 03 读一次），也确实常在 CEMS 工控机上算
      （HJ 212-2025 附录 B 表 B.10 专设 i23003「经工控机接入数采仪」、i23004「数采仪不计算」
      来承认这种拓扑）。两种形态都合规；本项目选的是"代码算"，理由是上面的冗余问题。

★★ 标准状态与折算的关键约定（必须守住）：
  - "标干" = 标准状态（273.15 K、101.325 kPa）+ 干烟气（扣除水分）下的质量浓度
  - "折算" = 把标干浓度统一到**基准氧含量**下的浓度，公式：

        折算浓度 = 标干浓度 × (21 - 基准氧含量) / (21 - 实测干基氧含量)

    ★ 出处（2026-10-01 已取到一手条文）：**HJ 75-2017 附录 C §C.2.4 式(C8)**；
      同形公式另见 HJ 76-2017 B.4.3 式(B11)、HJ 212-2017 附录 D 式(28)。
      分母是**实测干基氧含量**；分子里的 21 是助燃空气初始氧含量。
      ⚠️ 干湿基须先用式(C5)统一——本项目数据本身即"标干值"，故不再做干湿换算。
  - **标干 ≠ 折算**：一个是物理换算，一个是按基准氧含量的标准化；
    氧含量高于基准 → 折算值**变大**（稀释效应被修正回来）。
  - **基准氧含量按行业取，不是一个固定值**（2026-10-01 查证，均有条文）：

    | 标准 | 适用 | 基准氧 |
    |---|---|---|
    | GB 13223-2011 表 4 | **火电燃煤**（本项目采用） | **6%** |
    | GB 13223-2011 表 4 | 火电燃油/燃气 | 3% |
    | GB 13271-2014 表 6 | 工业/民用锅炉（非火电）燃煤 | **9%** |
    | GB 4915-2013 §4.1.4 | 水泥窑 | 10% |
    | GB 18485-2014 §3.18 | 生活垃圾焚烧 | 11% |
    | GB 18484-2020 §3.20 | 危废焚烧 | 11% |

    ⚠️ 本表按**火电燃煤 6%** 取值（见 O2_REFERENCE）；**换行业必须改它**。
      另：`i13108 基准氧含量` 在 HJ 212-2025 里是**由现场端上报**的字段，不是平台写死的。

★ 单位一律用国标写法（避免非 ASCII 字符进代码）：
  烟气流量 m3/h ｜ 流速 m/s ｜ 温度 degC ｜ 湿度 % ｜ 氧含量 % ｜ 压力 kPa

⚠️ 两处**为工程可行性做的偏离，已标注**（不是笔误）：
  1. 烟气**静压**国标单位是 Pa 且常为负值（引风机后负压）；但 Modbus 保持寄存器是
     16 位无符号，负值编不进去。故本仿真采用 **kPa 表压（85~105）**。
     真实项目若必须传 Pa 负值，需要改用有符号寄存器约定或加偏置。
     ★ 2026-10-07 起契约**已能表达**有符号寄存器（`dtype="int16"` 等，见下方"线格式"一节），
       但**本仿真仍用 kPa 表压不变**——改它要动全链路既有数据与限值口径，不在本次范围。
  2. 温度单位写作 **degC**（ASCII），展示层渲染为 ℃；理由是脚本/编码安全。

★ scale 是**每个测点自带**的寄存器换算系数（不是全局一个值）：
  Modbus 保持寄存器 16 位无符号（0~65535），`量程 × scale` 必须留余量。
  2026-09-30 改版烟测实测：Flow 200000×10、Pressure 10000 → 双双溢出 → 故引入本字段。
  ⚠️ scale 不只影响溢出，**还决定曲线平滑度**：分辨率 = 1/scale。
     颗粒物限值只有 5 mg/m3，若 scale=10 分辨率 0.1 = 限值的 2%，
     仿真信号的漂移/毛刺全部小于一个台阶 → 读数会整段冻在同一个值（2026-10-01 实测）。
     故颗粒物 scale 取 100（分辨率 0.01 = 限值的 0.2%）。

★ 超标限值（limit）的出处与量程是两回事，别混：
  - 量程（low/high）= 仪器能测的物理范围 → 接入层据此**拒收**超量程数据
  - 限值（limit）    = 排放标准允许的上限  → 达标判定/告警据此**报警**
  - ⚠️ 本表 limit 取值（**燃煤电厂超低排放**要求，2026-09-30 定）：
        颗粒物 5、二氧化硫 35、氮氧化物 50（mg/m3，标干、6% 基准氧）
    **限值比量程小一两个数量级是正常的**——超量程=表坏了要拒收，
    超限值=排超标要告警，两件事不能混。换行业/换地区必须改这张表。

★★ 线格式（2026-10-07 新增）：`dtype` / `endian` / `addr_notation`
  —— 契约要能表达"寄存器里的字节**怎么变成数**"，不只是"数是什么"。

  起因（真机实测）：RS485 温湿度变送器手册原文 **"16bits 数据高字节在前"**。
  在此之前 Point 只有"地址 + 量程 + scale"，**字节序与符号性无处安放**：
  全项目读寄存器只有 `int(raw)` 一种写法（`struct` 仅出现在 scripts/measure_clock_consistency.py，
  且那是 NTP 时钟，与寄存器无关）。真机一旦不是"无符号大端"，契约**描述不了**，
  只能靠现场口头约定 —— 这就是本次要补的洞。

  - `dtype`：`uint16` / `int16` / `int32` / `uint32` / `float32`，默认 `uint16`
      ⚠️ 符号性**必须显式**：湿度 0~100% 用 uint16 够；**温度可以是负的**（int16）。
         ⚠️ 16 位有符号是 **-32768~32767**，不是 0~65535（见 REG_INT_RANGES 与导入期自检）。
  - `endian`：16 位 `AB` / `BA`；32 位 `ABCD` / `CDAB` / `BADC` / `DCBA`，默认 `AB`
      ⚠️ 四个字母就是**线上字节的先后顺序**：ABCD=大端、DCBA=小端、CDAB=字交换（低字在前）、
         BADC=字内字节交换。**默认 AB 是 16 位的默认**：32 位 dtype 必须显式选 4 字母形式，
         选了 AB 属契约错误，**导入期自检即炸**，不留给运行期猜。
  - `addr_notation`：`"0-based"` / `"40001-style"` / `"1-based"`，默认 `"0-based"`
      它只说明**字段 address 是按哪种记法写的**，不改 address 的值：
          `线上 0 基地址 = point.address - ADDR_NOTATION_BASE[point.addr_notation]`
      ⚠️ 与 `REG_BASE` 的关系（**两者不是一回事，别合并**）：
          - `REG_BASE=0` 是**本项目仿真/网关读写保持寄存器块的起始地址**
            （`modbus_server` / `gateway` 用它拼 `read_holding_registers(REG_BASE, REG_COUNT)`），
            全局、只对本项目自己的 Modbus 服务端有意义；
          - `addr_notation` 是**测点契约字段**，描述"面对真机时 address 怎么读"
            （手册若给 40001，减 40001 才是线上 PDU 地址）。
          两者当前**取值一致**（0 基、基址 0），故 9 个测点行为零变化；
          将来多设备混用（0 基/1 基/40001 混在一起）时才需要分别对待，
          见 docs/架构与可维护性说明.md 的"地址基准"条目。
  ⚠️ **本次只加字段，不改任何读数路径**：9 个测点仍是 uint16 + AB + "0-based"，
     网关/仿真仍走 `int(raw)`；`decode_register()` / `encode_register()` 是**新增纯函数**，
     **当前无调用方**——真机接入时由接入层显式调用，不许顺手改网关既有行为。
============================================================================
"""

from __future__ import annotations

import math
import os
import struct
import sys
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
    code: str      # 环保平台参数编码：HJ 212-2025 附录 B.2（气监测参数编码表）
                   # 权威表 + 原文名称/单位/数据类型/原编码列 + 页码，见 src/protocol/hj212/factors.py
    scale: int = 10  # ⭐ 本测点专属的寄存器换算系数（默认 10）

    # ---- 线格式（2026-10-07 新增）----
    # 三个字段**全部带默认值**，取值就是现有 9 个测点的实际形态（uint16 + AB + 0-based），
    # 所以既有行为零变化；真机接入时按手册改这几个字段即可，不必另建一份映射表。
    dtype: str = "uint16"           # 寄存器数据类型（符号性与宽度）：uint16/int16/int32/uint32/float32
    endian: str = "AB"              # 线上字节序：16 位 AB/BA；32 位 ABCD/CDAB/BADC/DCBA
    addr_notation: str = "0-based"  # address 的记法：0-based / 40001-style / 1-based


# ---- 全链路唯一真源：加/改测点只改这里 ----
#
# ⚠️ scale 是 per-point 的，不是全局的：Modbus 保持寄存器是 16 位无符号（0~65535），
#    量程 × scale 必须留出余量，否则高频大数（流量、压力）会溢出。
#    2026-09-30 改版时烟测实测到 Flow 200000×10、Pressure 10000×10 双双溢出 → 故引入本字段。
#
# ⚠️ code（平台参数编码）2026-10-02 修正过一次，之前 9 个全错（错法：按顺序抄错一位）：
#      Dust 曾写 a21001（原文是「氨（氨气）」）、SO2 曾写 a21002（原文是氮氧化物）、
#      NOx 曾写 a21003（原文是一氧化氮）、O2 曾写 a21008（原文无此码）、
#      Flow/Velocity/Temp/Humidity/Pressure 曾写 B01~B05（那是附录 B.1「原编码」列的旧编码）。
#    现值依据 HJ 212-2025 附录 B.2（页码见 factors.py），并与附录 C 的官方废气报文示例
#    （用 a00000 / a34013 / a21002）交叉核对一致。
#    修正前该字段全仓库无消费者（只有本文件与派生的 CODES），所以不影响任何既有行为。
#
POINTS: Final[tuple[Point, ...]] = (
    # 名称          地址  列名           量程下限   量程上限    单位     限值    平台编码     换算
    Point("Flow",       0, "flow",          0.0,   65000.0, "m3/h", 65000.0, "a00000",        1),
    Point("Dust",       1, "dust",          0.0,      100.0, "mg/m3",      5.0, "a34013",   100),
    Point("SO2",        2, "so2",           0.0,     200.0, "mg/m3",     35.0, "a21026",    10),
    Point("NOx",        3, "nox",           0.0,     400.0, "mg/m3",     50.0, "a21002",    10),
    Point("O2",         4, "o2",            0.0,      25.0, "%",        25.0, "a19001",    10),
    Point("Velocity",   5, "velocity",      0.0,      30.0, "m/s",      30.0, "a01011",       10),
    Point("Temp",       6, "temp",          0.0,     300.0, "degC",    300.0, "a01012",       10),
    Point("Humidity",   7, "humidity",      0.0,     100.0, "%",       100.0, "a01014",       10),
    Point("Pressure",   8, "pressure",     85.0,     105.0, "kPa",     105.0, "a01013",       10),
)

# ---- 折算基准：燃煤锅炉 6%（HJ 75-2017）----
O2_REFERENCE: Final[float] = 6.0

# ---- 设备/网关共用的寄存器约定 ----
SCALE: Final[int] = 10        # 寄存器里存的是真实值 ×10 的整数（35.2 → 352）
# ⚠️ REG_BASE 是**本项目仿真/网关读写寄存器块的起始地址**（modbus_server/gateway 用它拼
#    read_holding_registers(REG_BASE, REG_COUNT)），**不是**"测点地址的记法"。
#    测点地址按哪种记法书写由 Point.addr_notation 表达，两者关系：
#        线上 0 基地址 = point.address - ADDR_NOTATION_BASE[point.addr_notation]
#    当前 REG_BASE=0 且 9 个测点 addr_notation 均为 "0-based"，两者取值一致、互相印证。
#    用法与取值**均未改动**（本次只新增字段与注释）。
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
DTYPES: Final[dict[str, str]] = {point.name: point.dtype for point in POINTS}
ENDIANS: Final[dict[str, str]] = {point.name: point.endian for point in POINTS}
ADDR_NOTATIONS: Final[dict[str, str]] = {
    point.name: point.addr_notation for point in POINTS
}
REG_COUNT: Final[int] = max(point.address for point in POINTS) + 1

# 寄存器能表达的整数上限（16 位无符号留一点余量），供烟测/自检用
# ⚠️ 它是**16 位无符号**的上限，只对 dtype="uint16" 成立；
#    别拿它去校验 int16（会在 32768~65535 这一段**漏检**），见 REG_INT_RANGES。
REG_INT_MAX: Final[int] = 65535

# ---- 线格式：合法取值、表示范围、字节序映射（2026-10-07 新增）----

#: 支持的寄存器数据类型（**符号性与宽度都由它表达**）
DTYPE_CHOICES: Final[tuple[str, ...]] = (
    "uint16", "int16", "int32", "uint32", "float32",
)

#: dtype → 字节宽度
_DTYPE_WIDTH: Final[dict[str, int]] = {
    "uint16": 2, "int16": 2, "int32": 4, "uint32": 4, "float32": 4,
}

#: dtype → struct 格式字符。**只写值类型、不写字节序**：字节序统一由 WIRE_BYTE_ORDER
#: 重排成规范大端序后再用 ">" 解析。这样"字节序"只有一处定义，
#: 不会出现"格式串里的 >/</@ 和 endian 字段各说一套"的分裂。
_DTYPE_STRUCT_CODE: Final[dict[str, str]] = {
    "uint16": "H", "int16": "h", "int32": "i", "uint32": "I", "float32": "f",
}

#: dtype → 能表达的整数区间（**导入期自检**与 encode_register 的范围校验共用）。
#: ⚠️ 16 位**有符号是 -32768~32767**，不是 0~65535：拿 REG_INT_MAX 校验 int16 测点会漏检
#:    （32768~65535 这段根本编不进 int16，却是"看起来合法"的数）。这就是自检要分 signed/unsigned。
REG_INT_RANGES: Final[dict[str, tuple[int, int]]] = {
    "uint16": (0, 65535),
    "int16": (-32768, 32767),
    "int32": (-2147483648, 2147483647),
    "uint32": (0, 4294967295),
}
REG_INT_MIN_16_SIGNED: Final[int] = REG_INT_RANGES["int16"][0]   # -32768
REG_INT_MAX_16_SIGNED: Final[int] = REG_INT_RANGES["int16"][1]   #  32767

#: float32 能**精确**表示的整数上限：IEEE-754 单精度尾数 24 位 → 2**24 以内的整数无舍入。
#: float32 没有整数溢出、只有精度丢失，故自检拿它当上限比拿 REG_INT_MAX 贴切。
FLOAT32_EXACT_INT_MAX: Final[int] = 1 << 24     # 16777216

#: endian → **线上字节顺序**：元组第 i 项 = "线上第 i 个字节"在规范大端序里的位置。
#:   16 位：AB（高字节在前，真机手册原文"16bits 数据高字节在前"）/ BA（低字节在前）
#:   32 位：ABCD（大端）/ CDAB（字交换，低字在前）/ BADC（字内字节交换，字序不变）/ DCBA（小端）
#: ⚠️ 这四个排列都是自逆的，所以同一张表既能"线上→规范"也能"规范→线上"，编解码不会走偏。
WIRE_BYTE_ORDER: Final[dict[str, tuple[int, ...]]] = {
    "AB": (0, 1),
    "BA": (1, 0),
    "ABCD": (0, 1, 2, 3),
    "CDAB": (2, 3, 0, 1),
    "BADC": (1, 0, 3, 2),
    "DCBA": (3, 2, 1, 0),
}

#: addr_notation → **地址基准**：`线上 0 基地址 = point.address - ADDR_NOTATION_BASE[记法]`
#: ⚠️ 与 REG_BASE 不是一回事（REG_BASE 是仿真/网关读写寄存器块的起始地址，见上方注释）。
ADDR_NOTATION_BASE: Final[dict[str, int]] = {
    "0-based": 0,          # address 就是线上 PDU 地址（本项目现状）
    "40001-style": 40001,  # 手册传统记法：40001 = 第 1 个保持寄存器
    "1-based": 1,          # 1 = 第 1 个保持寄存器
}


def _register_value_bounds(dtype: str) -> tuple[int, int]:
    """按 dtype 返回寄存器能表达的整数区间（导入期自检与 encode_register 共用）。"""
    if dtype == "float32":
        return (-FLOAT32_EXACT_INT_MAX, FLOAT32_EXACT_INT_MAX)
    return REG_INT_RANGES[dtype]


def _wire_layout(dtype: str, endian: str) -> tuple[str, tuple[int, ...]]:
    """校验 (dtype, endian) 组合，返回 (struct 格式字符, 线上字节重排表)。

    ⚠️ 非法组合**当场抛 ValueError**，不静默兜底：字节序猜错**不会报错**，
       只会读成另一个完全合法的数（比崩溃危险得多）。
    """
    if dtype not in _DTYPE_WIDTH:
        raise ValueError(f"未知 dtype {dtype!r}，只支持 {DTYPE_CHOICES}")
    if endian not in WIRE_BYTE_ORDER:
        raise ValueError(
            f"未知 endian {endian!r}：16 位用 AB/BA，32 位用 ABCD/CDAB/BADC/DCBA"
        )
    order = WIRE_BYTE_ORDER[endian]
    if len(order) != _DTYPE_WIDTH[dtype]:
        raise ValueError(
            f"dtype={dtype}（{_DTYPE_WIDTH[dtype]} 字节）与 endian={endian}"
            f"（{len(order)} 字节）不匹配：16 位 dtype 用 AB/BA，32 位 dtype 用 ABCD/CDAB/BADC/DCBA"
        )
    return _DTYPE_STRUCT_CODE[dtype], order


# 契约自检：任何测点的量程 × scale 越界，导入时就应该炸，而不是等到运行期。
# ★ 2026-10-07 扩展：上限**按 dtype 取**（signed / unsigned / float 各不相同），
#   且**下界一起查**——有符号 dtype 的意义就是能表达负值（如 Pa 负压），
#   "负值放不放得下"必须一并校验，否则加了 int16 也只算加了一半。
def _check_point_register_bounds(point: Point) -> str | None:
    """该测点的量程 × scale 越界则返回说明文字，合法返回 None。"""
    low_bound, high_bound = _register_value_bounds(point.dtype)
    if point.high * point.scale > high_bound:
        return (
            f"{point.name}: 量程上限 {point.high} × scale {point.scale} "
            f"超过 {point.dtype} 上限 {high_bound}"
        )
    if point.low * point.scale < low_bound:
        return (
            f"{point.name}: 量程下限 {point.low} × scale {point.scale} "
            f"低于 {point.dtype} 下限 {low_bound}"
        )
    return None


# 自检 1：线格式字段本身必须合法（dtype 认得出、endian 与该 dtype 的宽度匹配）
_BAD_WIRE_FORMAT: Final[tuple[str, ...]] = tuple(
    point.name
    for point in POINTS
    if point.dtype not in _DTYPE_WIDTH
    or point.endian not in WIRE_BYTE_ORDER
    or len(WIRE_BYTE_ORDER[point.endian]) != _DTYPE_WIDTH[point.dtype]
)
if _BAD_WIRE_FORMAT:
    raise ValueError(
        f"测点 {_BAD_WIRE_FORMAT} 的 (dtype, endian) 组合非法："
        f"dtype 只支持 {DTYPE_CHOICES}；16 位 endian 用 AB/BA，"
        "32 位 endian 用 ABCD/CDAB/BADC/DCBA（见 points.py 顶部的线格式说明）"
    )

# 自检 2：地址记法必须认识（不认识的记法会让 address 被按错误基准解释）
_BAD_ADDR_NOTATION: Final[tuple[str, ...]] = tuple(
    point.name for point in POINTS if point.addr_notation not in ADDR_NOTATION_BASE
)
if _BAD_ADDR_NOTATION:
    raise ValueError(
        f"测点 {_BAD_ADDR_NOTATION} 的 addr_notation 非法：只支持 {tuple(ADDR_NOTATION_BASE)}"
    )

# 自检 3：量程 × scale 必须装得进该 dtype（signed/unsigned 各按各自范围判）
_OUT_OF_RANGE: Final[tuple[str, ...]] = tuple(
    msg for msg in (_check_point_register_bounds(point) for point in POINTS) if msg
)
if _OUT_OF_RANGE:
    raise ValueError(
        f"测点量程 × scale 超出寄存器表示范围：{_OUT_OF_RANGE}。"
        f"请调小量程或 scale，或改用装得下的 dtype"
        f"（uint16 上限 {REG_INT_MAX}；int16 {REG_INT_MIN_16_SIGNED}~{REG_INT_MAX_16_SIGNED}；"
        f"float32 精确整数上限 {FLOAT32_EXACT_INT_MAX}）。见 points.py 顶部说明"
    )


# ---- 线格式编解码：寄存器字节流 ↔ 数值（**纯函数，不做 scale 换算**）----

def decode_register(raw: bytes, dtype: str = "uint16", endian: str = "AB") -> float:
    """字节流 → 数值：按 dtype 定符号与宽度、按 endian 定字节序。

    ★ 依据：真机手册（RS485 温湿度变送器）**"16bits 数据高字节在前"** → `endian="AB"`；
      各字段语义与"为什么必须能表达"见本文件顶部"线格式"一节。

    真机两个例子（均为手册取值，本函数的输入/输出原样对应）：
        b"\\x01\\xe6" + uint16 + AB → 486 → ÷10 = 48.6    （湿度，地址 0）
        b"\\xff\\x9f" + int16  + AB → -97 → ÷10 = -9.7    （温度，地址 1，**负值**）

    ⚠️ 三条边界，故意不模糊：
      1. **不做 scale 换算**：寄存器存的是 `真实值 × scale` 的整数，÷scale 是**调用方**的事
         （scale 由 Point 自带，见 SCALES）。分开是为了让"字节序错"和"忘除 scale"
         两种故障各自可见，而不是糊进一个函数里。
      2. `raw` 必须是**恰好**该 dtype 宽度的字节（16 位 2、32 位 4）；长度不符**抛 ValueError**，
         不做自动截断/补零——补出来的数看不出错。
      3. 整数 dtype 也返回 **float**（uint32 上限 4294967295 < 2**53，转换不丢精度），
         好处是四种 dtype 的返回类型一致，调用方不必分支。

    ⚠️ 32 位量跨两个寄存器，**字序由 endian 显式指定**（CDAB 在实际设备里很常见）；
      本函数只认 "线上字节顺序"，不猜设备型号。
    """
    code, order = _wire_layout(dtype, endian)
    data = bytes(raw)
    if len(data) != len(order):
        raise ValueError(
            f"decode_register: dtype={dtype} 需要 {len(order)} 字节，收到 {len(data)} 字节"
        )
    canonical = bytes(data[index] for index in order)   # 重排成规范大端序
    return float(struct.unpack(">" + code, canonical)[0])


def encode_register(value: float, dtype: str = "uint16", endian: str = "AB") -> bytes:
    """数值 → 字节流：`decode_register` 的严格反向（同一 (dtype, endian) 下往返一致）。

    ⚠️ 三条纪律，都是"宁可抛错，不要静默出错数"：
      1. **整数 dtype 不接受小数**：`486.5` 抛 ValueError。寄存器存的是 `真实值 × scale`
         后的整数，取整是**调用方**的语义（本函数不替它决定舍还是入）。
      2. **超范围抛错，绝不回绕**：uint16 编 `-1`、int16 编 `40000` 都抛 ValueError。
         回绕出来的数看起来完全合法，是最难查的一类故障（正是 REG_INT_RANGES 要拦的）。
      3. float32 溢出（超出 ±3.4e38）同样抛 ValueError，不让 struct 的 OverflowError 漏出去。

    ⚠️ float32 的 `nan` / `inf` **原样编码**（IEEE-754 位模式确定，往返一致）；
      它们在本项目里**不用于**表达"数据不可用"——那是协议层的事，
      见 ZS_UNAVAILABLE_SENTINEL 及其说明。
    """
    code, order = _wire_layout(dtype, endian)
    if dtype == "float32":
        try:
            canonical = struct.pack(">" + code, float(value))
        except (OverflowError, struct.error) as exc:
            raise ValueError(
                f"encode_register: {value!r} 超出 float32 表示范围"
            ) from exc
    else:
        low_bound, high_bound = _register_value_bounds(dtype)
        as_float = float(value)
        if not as_float.is_integer():
            raise ValueError(
                f"encode_register: {value!r} 不是整数，{dtype} 不接受小数"
                "（寄存器存的是 真实值 × scale 的整数，先乘 scale 再编码）"
            )
        as_int = int(as_float)
        if not low_bound <= as_int <= high_bound:
            raise ValueError(
                f"encode_register: {as_int} 超出 {dtype} 表示范围 "
                f"{low_bound}~{high_bound}（**不回绕**）"
            )
        canonical = struct.pack(">" + code, as_int)
    wire = bytearray(len(order))
    for wire_pos, canonical_pos in enumerate(order):
        wire[wire_pos] = canonical[canonical_pos]
    return bytes(wire)


# ---- 折算：标干浓度 → 折算浓度（氧含量修正）----

# 参与折算的污染物（列名）；O2 列的列名见 O2_COLUMN
ZS_TARGETS: Final[tuple[str, ...]] = ("dust", "so2", "nox")
O2_COLUMN: Final[str] = "o2"

#: 折算分母的基准常数：`21 - 实测干基氧含量`（HJ 75-2017 式(C8) 里的 21）。
#: ★ 单独取个名字是为了让"分母 = 21 - O2"这件事在**调用方**（小时结算的下限判据，
#:   见 `alarm_judge.judge_hour`）也能引用同一个常数，而不是各写一个字面量 21.0。
O2_DRY_BASIS: Final[float] = 21.0


def to_reference_o2(value: float, o2: float, o2_ref: float = O2_REFERENCE) -> float:
    """把**标干浓度**折算到**基准氧含量**下的浓度。

        折算浓度 = 标干浓度 × (21 - 基准氧含量) / (21 - 实测干基氧含量)

    ★ 出处：**HJ 75-2017 附录 C §C.2.4 式(C8)**（同形：HJ 76-2017 B.4.3 式(B11)、
      HJ 212-2017 附录 D 式(28)）。

    ⚠️ 四条纪律：
      1. **折算值是算出来的，不存库、不进寄存器**（见本文件顶部说明）
         —— 例外：**告警事件表**里会存"判定快照"（当时的标干值/O2/基准氧/限值/折算值），
            那是**判定证据**不是数据冗余，与本条不冲突
      2. 实测氧含量 ≥ 21% 时分母 ≤ 0 → 折算无物理意义，**本函数返回 nan**（不瞎除）
         ✅ **合规缺口已处理（2026-10-01）**：HJ 212-2025 §8.1.1 d) 规定"无法计算折算浓度时，
            按缺省数据类型的最大值传输"（如 9999.99 或 -9999.99），**不是 0、不是实测值、
            不是报无效**。本函数**继续保持返回 nan**（数学语义）；出站由传输出口显式调用
            **to_transmit_value()** 编码成哨兵值 ZS_UNAVAILABLE_SENTINEL，见下一个函数。
            ⚠️ 严禁把哨兵值塞回本函数：内部判定会因此把"算不出来"当成"严重超标"。
      3. 本公式已于 2026-10-01 **锁定**（出处已取到一手条文）；改它属于契约变更
      4. 干湿基须先用 HJ 75 式(C5) 统一；本项目数据本身即"标干"，故不再做干湿换算

    对照（基准氧 6%）：O2=6% → 折算=标干；O2=9% → 折算放大；O2=3% → 折算缩小。

    ⚠️ **本函数只处理"分母 <= 0"这一种不可用**（O2 ≥ 21% ⇒ nan）。分母很小但为正时
      （O2=20% ⇒ 15 倍；20.99% ⇒ 1500 倍）本函数照算不误 —— 这是**数学语义**，不动。
      "分母过小 ⇒ 折算结果不可信"属于**判定语义**，下限判据在 `alarm_judge.judge_hour`
      里（`ALARM_O2_DENOM_MIN`），它把这类样本并入"无效样本"，见那边的说明。
    """
    denominator = O2_DRY_BASIS - o2
    if denominator <= 0.0:
        return float("nan")
    return value * (O2_DRY_BASIS - o2_ref) / denominator


# ---- 传输编码：无法折算时的哨兵值（**协议层**，不是数学层）----

# ★ 出处：**HJ 212-2025 §8.1.1 d)** —— 无法计算折算浓度时，按**缺省数据类型的最大值**传输
#    （标准举例 9999.99 / -9999.99），不是 0、不是实测值、不是报无效。
# 取值说明：本链路所有浓度类测点（颗粒物/SO2/NOx）非负，故取 **+9999.99**，不用 -9999.99
#   （负号与"负值/异常低值"难以区分，且浓度本身不出现负值）。
# ⚠️ 它是**协议编码值，不是物理量**：比任何限值都大，严禁参与比较、统计、达标/超标判定，
#    否则"算不出来"会被判成"严重超标"。判定口径用 to_reference_o2() 的 nan 语义。
ZS_UNAVAILABLE_SENTINEL: Final[float] = 9999.99


def to_transmit_value(zs: float) -> float:
    """把折算值的**内部语义**映射成**可传输值**（协议编码层）。

    ★ 出处：**HJ 212-2025 §8.1.1 d)** —— 无法计算折算浓度时，按缺省数据类型的最大值
      传输（标准举例 9999.99 / -9999.99）；不是 0、不是实测值、不是报无效。

    规则：
      - 非有限值（nan / ±inf，即"算不出来"）→ ZS_UNAVAILABLE_SENTINEL（+9999.99）
      - 有限值 → 原样返回（折算结果是物理量，不做任何缩放、舍入或裁剪）

    ⚠️ 为什么不让 to_reference_o2() 直接返回哨兵值（本函数存在的唯一理由）：
      这两件事的语义完全不同，混在一个函数里迟早被改错——
        1. to_reference_o2() 承担**数学语义**：O2 ≥ 21% 时分母 ≤ 0，折算浓度不存在，
           返回 nan 是数学上唯一正确的答案，也是内部判定/小时结算的口径（nan 不参与比较）。
        2. 本函数承担**协议语义**：HJ 212 规定的是"往外传什么"，它不改变计算结果本身。
           一旦把哨兵塞进 to_reference_o2()，所有内部调用方都会拿到 9999.99 ——
           它大于任何限值，会被判成"严重超标"，而事实是"这次算不出来"。
           标准要求的动作是"传输时用最大值"，不是"把算不出来的数据当超标"。
      所以：**算归算（nan），传归传（哨兵）**，由传输出口显式调用本函数
      （接入层见 subscriber_to_td.transmit_reference_values()）。
    """
    if not math.isfinite(zs):
        return ZS_UNAVAILABLE_SENTINEL
    return zs


def over_limit(name: str, value: float) -> bool:
    """按测点契约的 limit 判定是否超标（达标判定/告警用，与量程拒收无关）。

    ⚠️ 调用方要自己保证传入的是**该判定的那个量**：环保口径判**折算值**（不是标干值）。
    """
    limit = LIMITS[name]
    return value > limit


# ============================================================================
# 点集选择：让网关与接入层能按配置切换"这一实例采/收哪套点位"
# ============================================================================
#
# 起因（2026-10-11 真机接入）：温湿度变送器只有 **2 个**测点，而本模块是 **9 个**烟气测点。
# 把真机硬塞进这 9 个地址会**语义错位**（湿度会被当成颗粒物），所以必须有"第二套点集"。
#
# ⭐ 设计取舍：**两套点集刻意保持同一套属性名**（`POINTS` / `NAMES` / `COLUMNS` /
#    `REG_BASE` / `REG_COUNT` / `RANGES` / `LIMITS` …）。
#    这样网关与接入层**只需要换一个"取值来源"，不必分叉代码路径** ——
#    改动是"把 `from ... import POINTS` 换成 `active_point_set().POINTS`"，而不是复制一套逻辑。
#
# ⚠️ **默认 `cems`**：不设 `POINT_SET` 时返回本模块，**既有部署行为完全不变**
#    （这是不破坏 25 项既有验收的前提）。
#
# ⚠️ **返回的是模块对象**（不是数据副本）：调用方直接读属性即可。
#    用惰性 import 避免 `points_transmitter` ↔ `points` 的循环导入。

#: 选择点集的环境变量名。
POINT_SET_ENV: Final[str] = "POINT_SET"

#: 支持的点集名（新增设备类型时在这里登记）。
SUPPORTED_POINT_SETS: Final[tuple[str, ...]] = ("cems", "transmitter")


def active_point_set(name: str | None = None):
    """按 `POINT_SET` 环境变量返回**当前点集模块**。

    | `POINT_SET` | 返回 | 说明 |
    |---|---|---|
    | 不设 / `cems` | 本模块 | 9 个烟气测点（默认，行为与既有完全一致） |
    | `transmitter` | `src.common.points_transmitter` | RS485 温湿度变送器（2 个测点，int16/AB） |

    Args:
        name: 显式指定点集名（测试用）；不给则读环境变量 `POINT_SET`。

    Raises:
        ValueError: 点集名不在 `SUPPORTED_POINT_SETS` 里。
            ⚠️ **刻意 raise 而不是回落到 cems** —— 静默回落会让"配错点集"变成一个
            采集正常、入库正常、但**数据是另一台设备**的静默错（本项目踩过同类坑）。
    """
    resolved = (name if name is not None else os.getenv(POINT_SET_ENV, "cems")).strip().lower()
    if resolved == "cems":
        return sys.modules[__name__]
    if resolved == "transmitter":
        from src.common import points_transmitter  # 惰性：避免循环导入

        return points_transmitter
    raise ValueError(
        f"未知的 {POINT_SET_ENV}={resolved!r}；只支持 {SUPPORTED_POINT_SETS}"
    )

