# -*- coding: utf-8 -*-
"""测点契约：全链路唯一的一份定义（寄存器地址、量程、TDengine 列名、单位）。

设备层、网关层、平台接入层、展示层都从这里取测点定义。
以前同一份契约散在 5 个文件里各抄一遍，加/改测点必然漏改；
现在只有这一处是真源，改完各层自动跟着变。
"""

from __future__ import annotations

from typing import Final, NamedTuple


class Point(NamedTuple):
    """单个测点的完整契约。"""

    name: str      # MQTT 报文字段名，如 SO2
    address: int   # Modbus 保持寄存器地址
    column: str    # TDengine 列名，如 so2
    low: float     # 量程下限（也是仿真取值范围下限）
    high: float    # 量程上限（超量程的数据会被接入层拒收）
    unit: str      # 单位


# ---- 唯一的测点定义表：加测点只改这里 ----
POINTS: Final[tuple[Point, ...]] = (
    Point("SO2",      0, "so2",      20.0,  50.0,  "mg/m3"),
    Point("NOx",      1, "nox",      10.0,  30.0,  "mg/m3"),
    Point("Flow",     2, "flow",     80.0,  120.0, "m3/s"),
    Point("Dust",     3, "dust",     0.0,   10.0,  "mg/m3"),
    Point("O2",       4, "o2",       5.0,   12.0,  "%"),
    Point("Temp",     5, "temp",     100.0, 180.0, "℃"),
    Point("Humidity", 6, "humidity", 5.0,   15.0,  "%"),
    Point("Pressure", 7, "pressure", 95.0,  105.0, "kPa"),
)

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
REG_COUNT: Final[int] = max(point.address for point in POINTS) + 1
