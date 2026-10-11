# -*- coding: utf-8 -*-
"""RS485 温湿度变送器（Modbus RTU）点位契约 —— **本设备的唯一真源**。

============================ 本文件是什么 ============================
一台**真实在用设备**的点位契约：温湿度变送器，Modbus RTU，`4800 / 8 / N / 1`、
从站 `0x01`、**只读**、**2 个保持寄存器**。
设备层/网关层/接入层/展示层要读这台设备时，**只从这里取**：设备标识、通讯参数、
寄存器表、量程、单位、换算系数、线格式（符号性/字节序/地址记法）。

============================ 与 points.py 的关系 ============================
- `src/common/points.py` 是**9 个烟气测点（CEMS）**的契约，同时也是**共用实现的所在地**。
  本文件**复用**它的 `Point` 类型、`decode_register()`，以及两个契约自检函数
  （`_check_point_register_bounds` / `_wire_layout`），**不复制、不重写**。
  → 线格式（`dtype` / `endian` / `addr_notation`）的语义**仍然只有一处实现**。
- 本文件是**另一份契约**（另一台设备），不是 points.py 的补丁：
  **实现共享，契约分文件 —— 一个文件一份设备契约。**

============================ 为什么单独一个文件，而不是塞进 points.py ============================
1. ⚠️ **硬约束，不是审美**：`points.POINTS` 的派生视图被全链路直接消费
   （网关按 `REG_COUNT` 一次读整块寄存器；接入层 `TD_COLUMNS = COLUMNS` 建超表并按
   `point.name` 取值入库，见 `src/platform/subscriber_to_td.py`；展示层按 `COLUMNS` 出列），
   且另有 25 项验收 + 301 个测试在保护它。往里加这 2 个点 = 改
   `REG_COUNT`(9→11)、改超表列、改大屏列 → **必然破坏既有验收**。
2. 这台设备与烟气测点**量纲、从站、传输方式都不同**（环境温湿度 vs 污染物浓度；
   RTU 串口 vs 项目 1 原有的 Modbus TCP 仿真），存续期与故障模式也不同；
   混成一张表后 `NAMES` / `COLUMNS` / `REG_COUNT` 这类"全局"视图就不再自洽。

============================ 命名：为什么加设备作用域前缀 ============================
点名取 `AirHumidity` / `AirTemp`（列名 `air_humidity` / `air_temp`），
**不**直接叫 `Humidity` / `Temp` —— 这两个名字在 9 点烟气契约里**已占用**，
且含义完全不同（烟气 `Temp` 是烟温，量程 0~300 degC、limit 300；本设备是环境温度，
-40~125 degC）。点名的实际用途是**字典键**（`values[point.name]`，见 subscriber_to_td）
与 MQTT 报文字段名，两台设备共用同一个键就是**静默串线**：
27.3 degC 的环境温度会被当成烟温写进烟温列。这是本项目最在意的一类故障
（"看起来完全合法、没有任何报错"），所以键名必须**设备作用域**、不允许重叠。

============================ 设备事实（手册 + 2026-10-07 实测）============================
出处：`docs/runbooks/真机接入.md`（S2 / S3 / S4 执行记录，均贴原始收发字节）。

寄存器表（手册原文 + 实测一致）：

    寄存器地址   PLC/组态记法   内容   类型                标度   单位
    0x0000       40001          湿度   uint16               /10    %
    0x0001       40002          温度   **int16（有符号）**   /10    degC

    ⚠️ 温度**必须有符号**：否则 -9.7 degC 会被读成 6543.9 degC（同一串字节 FF 9F）。
       手册明写"**16bits 数据高字节在前**" → 字节序 `AB`。

实测样本（原始字节，CRC 已独立复核）：

    01 03 04 02 31 01 11 6B D8  → 湿 561 → 56.1 %RH；温 273 → 27.3 degC
    01 03 04 00 00 00 00 FA 33  → 读**不存在的地址**返回**全 0**（不报异常）

⚠️ 由第二条样本导出的一条**已知边界**（本文件只记录，不在此处实现）：
    "寄存器地址配错"的表现是 **0 %RH / 0 degC**，而 0 落在两个测点的量程内 →
    **量程白名单拦不住这种假数据**，接入层必须另加跳变/合理性判断。
    本文件的 `RANGES` 只回答"值是否在量程内"，**不回答"值是否可信"**。

通讯参数（S2 实测；默认波特率是 **4800**，填 9600 会完全静默）：

    4800 / 8 / N / 1，从站 0x01，功能码只支持 03（只读）。
    ⚠️ 实测：发功能码 06 会被**原样回显**，但寄存器值不变（runbook S4 EXP1）
       → **"回显"不等于"写入生效"**。本契约因此**不提供写路径**（READ_ONLY=True）。

★★ 语义不适用字段的处置（**主动取舍，显式声明，不含糊**）
    `Point` 有三个字段是为**烟气/CEMS**语义设计的，对一台环境量变送器并不适用：
    `limit`（排放限值）、`code`（HJ 212 平台参数编码）、`addr_notation`（地址记法）。
    `addr_notation` 适用（实测就是 0 基线上地址，手册另给 40001 记法）——另两个不适用：

    (1) `limit` = **排放限值**（达标判定/告警用）。温湿度是**环境参数**，
        不存在"排放标准允许的上限"，也不参与基准氧折算 → **该字段对本设备没有语义**。
        处置：
          a. 按 points.py 字段注释既有的约定"**无标准的物理量填量程上限**"，
             填 `high`（湿度 100.0、温度 125.0）；
          b. 把"不参与判定"做成**可被代码查询的事实**，不只写在注释里：
                 EMISSION_LIMIT_APPLICABLE = False        # 本设备整体不参与达标判定
                 LIMIT_APPLICABLE         = {点名: False}  # 逐点查询
          c. 导入期自检**强制 `limit == high`**：谁把 limit 改成别的数
             （例如拿 90.0 当湿度告警阈值），**导入即炸** —— 这样"限值语义"有可执行
             证据，而不是靠评审时的记忆；
          d. ⚠️ **禁止**把本设备测点喂给 `points.over_limit()` / 达标结算：
             `limit == high` 会让"温度超过 125 degC"被判成"**超标**"，
             而事实是"**超量程**"（传感器坏/接线错）。points.py 顶部专门写过
             "超量程=表坏了要拒收，超限值=排超标要告警，两件事不能混"；
          e. 真要环境阈值（如湿度 >90% 告警）时**另建阈值来源**（配置/告警规则），
             **不许改这个字段** —— 一改就把 (d) 的防线拆了。

    (2) `code` = **HJ 212-2025 附录 B 的平台参数编码**。本设备是**辅助环境量**，
        不是 CEMS 监测项，**没有环保平台出口** → `HJ212_OUTLET_APPLICABLE = False`。
        字段仍填**最接近的既有编码**（温度 `a01012`、湿度 `a01014`，与 points.py 的
        烟温/烟湿同码）以免出现空串被下游当成"未匹配参数"；这只是**占位**，
        不构成"本设备按 HJ 212 上报"的声明。要接环保平台须先由协议方确认参数编码。

============================ 本文件不做的事 ============================
- ❌ 不改 `points.py`，也不动它任何派生视图（那是 9 点契约，另受验收保护）。
- ❌ 不做 RTU 切帧 / CRC 校验 / 串口读写（网关层的事）；本文件只有契约与**纯解码函数**。
- ❌ 不做"写寄存器"（设备只读，见上）。
- ❌ 不做量程拒收 / 跳变判断 / 告警（接入层的事）；本文件只给出 `RANGES` 与 `LIMIT_APPLICABLE`。
====================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from src.common.points import (
    ADDR_NOTATION_BASE,
    WIRE_BYTE_ORDER,
    Point,
    _check_point_register_bounds,
    _wire_layout,
    decode_register,
)

# ---- 设备标识 ----
#: 设备标识（MQTT 报文/MQTT 主题/日志/缓存键里的设备名）。**全链路只此一处**。
DEVICE_ID: Final[str] = "transmitter"

#: 设备型号说明（ASCII，便于进日志；中文说明见本文件顶部）。
DEVICE_MODEL: Final[str] = "RS485 Modbus RTU temperature/humidity transmitter"

# ---- 通讯参数（S2 实测：4800 / 8 / N / 1，从站 0x01）----
#: 波特率。⚠️ 实测默认值是 **4800 而不是 9600**；填错这一个字就完全静默（不报错）。
BAUD_RATE: Final[int] = 4800
DATA_BITS: Final[int] = 8
PARITY: Final[str] = "N"       # 无校验（None）
STOP_BITS: Final[int] = 1
#: 串口参数串（日志/错误信息里直接用，避免三处各拼一遍）
SERIAL_SETTINGS: Final[str] = f"{BAUD_RATE}/{DATA_BITS}/{PARITY}/{STOP_BITS}"

#: 从站地址（实测 0x01；发 0x02 完全静默）。
SLAVE_ADDRESS: Final[int] = 0x01

#: 功能码：只支持 03（读保持寄存器）。
READ_FUNCTION_CODE: Final[int] = 3
SUPPORTED_FUNCTION_CODES: Final[tuple[int, ...]] = (READ_FUNCTION_CODE,)
#: **只读设备**：实测发 06 会被原样回显但值不变 → 不存在可用的写路径，
#: 任何"写成功"的判据都不成立（要写必须回读验证）。见本文件顶部 S4 EXP1。
READ_ONLY: Final[bool] = True

#: 帧间静默**下限**（字节时间）。手册原文"初始/结束结构 >=4 字节的时间"，
#: **比 Modbus 标准的 3.5 字符更保守** → 切帧逻辑按 4 留间隔。
#: ⚠️ 它是**设备事实**（不是网关的偏好），故放在契约里；用法由网关层决定。
FRAME_GAP_MIN_BYTES: Final[int] = 4

#: 本设备寄存器块起始地址（线上 0 基 / PDU 地址）。
#: ⚠️ 与 `points.REG_BASE` 同名同值但**不是同一个东西**：那个是 9 点烟气仿真的块首。
REG_BASE: Final[int] = 0

# ---- 本设备唯一真源：加/改测点只改这里 ----
#
# 参数顺序（见 points.Point）：name, address, column, low, high, unit, limit,
#                              code, scale, dtype, endian, addr_notation
# 线格式依据：手册"16bits 数据高字节在前" → endian="AB"（见文件顶部）；
#             温度**有符号** → dtype="int16"；湿度 0~100 非负 → dtype="uint16"。
# limit 一律等于 high（**占位**，不参与判定，见文件顶部"语义不适用字段的处置"）。
POINTS: Final[tuple[Point, ...]] = (
    # 名称          地址  列名            量程下限  量程上限   单位    限值     平台编码   换算   dtype      endian  addr_notation
    Point("AirHumidity", 0, "air_humidity",   0.0,   100.0, "%",    100.0, "a01014",   10, "uint16", "AB", "0-based"),
    Point("AirTemp",     1, "air_temp",     -40.0,   125.0, "degC", 125.0, "a01012",   10, "int16",  "AB", "0-based"),
)

# ---- 各层按需取用的只读视图（照 points.py 的写法）----
NAMES: Final[tuple[str, ...]] = tuple(point.name for point in POINTS)
COLUMNS: Final[tuple[str, ...]] = tuple(point.column for point in POINTS)
REGISTER_MAP: Final[tuple[tuple[str, int], ...]] = tuple(
    (point.name, point.address) for point in POINTS
)
RANGES: Final[dict[str, tuple[float, float]]] = {
    point.name: (point.low, point.high) for point in POINTS
}
UNITS: Final[dict[str, str]] = {point.name: point.unit for point in POINTS}
SCALES: Final[dict[str, int]] = {point.name: point.scale for point in POINTS}
DTYPES: Final[dict[str, str]] = {point.name: point.dtype for point in POINTS}
ENDIANS: Final[dict[str, str]] = {point.name: point.endian for point in POINTS}
LIMITS: Final[dict[str, float]] = {point.name: point.limit for point in POINTS}
CODES: Final[dict[str, str]] = {point.name: point.code for point in POINTS}
ADDR_NOTATIONS: Final[dict[str, str]] = {
    point.name: point.addr_notation for point in POINTS
}
POINTS_BY_NAME: Final[dict[str, Point]] = {point.name: point for point in POINTS}

#: 与 `points.SCALE` **同名**的全局便捷值：设备层按 `SCALE` 编码寄存器块
#: （见 `src/device/modbus_server.py` 的 `from src.common.points import ... SCALE`），
#: 点集可切换时它必须存在，否则换点集就 AttributeError。
#: ⚠️ **真源仍是每个 Point 自带的 scale**（points.py 的原始设计）；这里只是
#: "本设备两个测点恰好同系数"的便捷入口，导入期自检强制它与各点一致。
#: 将来本设备出现 per-point 不同的 scale 时，删掉本常量并让设备层逐点取 `SCALES`。
SCALE: Final[int] = 10

#: 线上 0 基地址（PDU 地址）= 字段值 - 该记法的基准。
#: 公式与 points.py 一致（**不另立一份地址换算规则**），记法基准也取自那里。
RAW_ADDRESSES: Final[dict[str, int]] = {
    point.name: point.address - ADDR_NOTATION_BASE[point.addr_notation]
    for point in POINTS
}

#: 手册的"PLC/组态记法"列（40001 / 40002）：由**0 基地址**推导，
#: 不是第二份地址真源（基准同样取自 points.ADDR_NOTATION_BASE）。
PLC_ADDRESSES: Final[dict[str, int]] = {
    point.name: raw + ADDR_NOTATION_BASE["40001-style"]
    for point in POINTS
    for raw in (RAW_ADDRESSES[point.name],)
}

#: 寄存器块长度（**按 0 基地址算**，故本设备与 points.py 的 `max(address)+1` 写法不同：
#: 那种写法只在 addr_notation="0-based" 时成立；这里显式换算，换成 40001 记法也不会算错）。
REG_COUNT: Final[int] = max(RAW_ADDRESSES.values()) - REG_BASE + 1

#: 一次"读整块"应答的**数据段**字节数（每个测点按其线宽，16 位 = 2 字节）。
DATA_BYTES: Final[int] = sum(len(WIRE_BYTE_ORDER[point.endian]) for point in POINTS)

#: 完整 RTU 应答帧字节数 = 从站地址 1 + 功能码 1 + 字节数 1 + 数据 + CRC 2。
#: ⚠️ 实测值 **9**（`01 03 04 xx xx xx xx CRC CRC`）—— 本式必须与实测自洽，测试会钉住。
EXPECTED_RESPONSE_BYTES: Final[int] = 3 + DATA_BYTES + 2

# ---- 语义不适用字段的显式声明（见文件顶部"语义不适用字段的处置"）----
#: 本设备测点**是否参与"排放限值达标判定"**：否（温湿度没有排放限值）。
EMISSION_LIMIT_APPLICABLE: Final[bool] = False
#: 逐测点的判定适用性（值全为 False）。调用方按点名查询它，
#: **不要**再自己写一份"哪些点不判超标"的名单（那就是第二份真源）。
LIMIT_APPLICABLE: Final[dict[str, bool]] = {point.name: False for point in POINTS}
#: 本设备测点是否有 **HJ 212 环保平台出口**：否（辅助环境量，非 CEMS 监测项）。
HJ212_OUTLET_APPLICABLE: Final[bool] = False


def _check_points(points: Sequence[Point]) -> tuple[str, ...]:
    """返回契约问题清单（空元组 = 合法）。导入期自检与测试**共用同一套判据**。

    ⚠️ 判据不在这里重写：量程/线格式直接调用 `points.py` 的
       `_check_point_register_bounds` / `_wire_layout` —— 抄一份判据必然漂。
    """
    problems: list[str] = []

    # 1) 线格式：(dtype, endian) 必须合法且宽度匹配（16 位用 AB/BA；32 位用 4 字母）。
    for point in points:
        try:
            _wire_layout(point.dtype, point.endian)
        except ValueError as exc:
            problems.append(f"{point.name}: 线格式非法 —— {exc}")

    # 2) 量程 × scale 必须装得进该 dtype。
    #    ⚠️ signed / unsigned 各按各的范围判：温度 -40 ×10 = -400 装不进 uint16，
    #       这正是"温度忘了写 int16"的第一个拦路点。
    for point in points:
        message = _check_point_register_bounds(point)
        if message:
            problems.append(message)

    # 3) scale 必须为正数（0 会让 decode_point 直接除零）。
    for point in points:
        if point.scale <= 0:
            problems.append(f"{point.name}: scale={point.scale} 必须为正数")
        elif point.scale != SCALE:
            # 3b) 全局便捷值 SCALE 与逐点 scale 必须一致（见 SCALE 的注释）。
            problems.append(
                f"{point.name}: scale={point.scale} 与全局 SCALE={SCALE} 不一致"
                "（SCALE 只是便捷值；若本设备确实需要 per-point 不同的 scale，"
                "请删掉 SCALE 并让设备层逐点取 SCALES）"
            )

    # 4) code 不能为空（空串会让下游把它当成"未匹配参数"，静默丢一个点）。
    for point in points:
        if not point.code:
            problems.append(f"{point.name}: code 为空（平台参数编码不可缺省）")

    # 5) 点名 / 列名不得重复：重复会让 dict 派生视图**静默吞掉**一个测点。
    for label, values in (
        ("name", [point.name for point in points]),
        ("column", [point.column for point in points]),
    ):
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            problems.append(f"{label} 重复：{duplicates}（dict 派生视图会静默吞点）")

    # 6) 线上 0 基地址必须**从 REG_BASE 起唯一且连续**。
    #    依据是本设备的读法：一次 03 请求读整块。地址有洞只会读到 0，
    #    而**实测空洞返回全 0、不报异常**、且 0 落在量程内 → 量程检查拦不住，
    #    所以必须在**契约期**拦住（这类假数据没有任何后续信号）。
    raw_addresses: list[int] = []
    for point in points:
        base = ADDR_NOTATION_BASE.get(point.addr_notation)
        if base is None:
            problems.append(
                f"{point.name}: addr_notation={point.addr_notation!r} 非法，"
                f"只支持 {tuple(ADDR_NOTATION_BASE)}"
            )
            continue
        raw_addresses.append(point.address - base)
    if len(raw_addresses) == len(points):
        expected = list(range(REG_BASE, REG_BASE + len(points)))
        if sorted(raw_addresses) != expected:
            problems.append(
                f"线上 0 基地址必须从 REG_BASE={REG_BASE} 起唯一且连续：期望 {expected}，"
                f"实际 {sorted(raw_addresses)}"
                "（本设备一次读整块；地址有洞只会读到 0，实测不报异常）"
            )

    # 7) limit 必须是量程上限（= 占位值）。见文件顶部"语义不适用字段的处置"：
    #    本设备无排放限值语义，谁把它当阈值用，导入期就炸。
    for point in points:
        if point.limit != point.high:
            problems.append(
                f"{point.name}: limit={point.limit} 必须等于量程上限 high={point.high}"
                "（本设备无排放限值语义，该字段只作占位、不参与判定；"
                "需要环境阈值请另建阈值来源，见本文件顶部说明）"
            )

    return tuple(problems)


def validate_contract(points: Sequence[Point] = POINTS) -> None:
    """契约自检：不合法**当场抛 ValueError**，不静默兜底。

    ⚠️ 为什么是"导入期就炸"：字节序/符号性猜错**不会报错**，只会读成另一个
       "看起来完全合法"的数（-9.7 degC → 6543.9 degC），运行期没有任何信号。
       与 points.py 的导入期自检同一风格（那边也是 raise，不是打日志）。
    """
    problems = _check_points(points)
    if problems:
        raise ValueError(
            f"温湿度变送器点位契约非法（DEVICE_ID={DEVICE_ID}）：{list(problems)}。"
            "依据与取舍见 src/common/points_transmitter.py 顶部说明"
        )


# 导入期自检：本文件的 POINTS 必须自洽（与 points.py 的做法一致）。
validate_contract(POINTS)


# ---- 纯解码：寄存器字节 → 工程值（复用 points.decode_register，不重写字节序逻辑）----

def decode_point(name: str, raw: bytes) -> float:
    """单个测点：该测点的寄存器原始字节 → **工程值（已 ÷scale）**。

    ⚠️ 两步（解线格式 / 除 scale）**故意不合并**：points.decode_register 的注释已写明，
       合并会让"字节序错"和"忘除 scale"两种故障糊在一起、各自不可见。
       本函数只负责把**本设备的 scale** 接上，让调用方不必自己记"这个点要不要除 10"。

    ⚠️ 不做量程判断：超量程是**接入层**的拒收口径（见本文件顶部 RANGES 的说明）。
    """
    point = POINTS_BY_NAME[name]
    return decode_register(raw, point.dtype, point.endian) / point.scale


def decode_points(raw: bytes) -> dict[str, float]:
    """整块**数据段** → `{点名: 工程值}`（按线上 0 基地址升序切片）。

    ⚠️ `raw` 是应答的**数据段**：`01 03 04 | 02 31 01 11 | 6B D8` 中间那 4 字节，
       **不含**从站地址/功能码/字节数/CRC —— 切帧与 CRC 是网关层的事，本文件不做。

    ⚠️ 长度必须**恰好** `DATA_BYTES`：截断/补零都会解出"看起来合法"的数，故抛 ValueError。
    """
    if len(raw) != DATA_BYTES:
        raise ValueError(
            f"decode_points: 本设备数据段应为 {DATA_BYTES} 字节"
            f"（{REG_COUNT} 个 16 位测点），收到 {len(raw)} 字节"
        )
    values: dict[str, float] = {}
    offset = 0
    for point in sorted(POINTS, key=lambda item: RAW_ADDRESSES[item.name]):
        width = len(WIRE_BYTE_ORDER[point.endian])
        values[point.name] = decode_point(point.name, raw[offset : offset + width])
        offset += width
    return values
