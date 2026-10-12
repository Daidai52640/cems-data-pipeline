# -*- coding: utf-8 -*-
"""正式验收前的一次性环境探针：量两台设备的数据新鲜度与采集间隔。只读。"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts._td_ops import taos_sql  # noqa: E402

for tag in ("device1", "device2"):
    rows = taos_sql(
        "tdengine",
        f"SELECT ts FROM cems.cems_data WHERE device='{tag}' ORDER BY ts DESC LIMIT 8;",
    )
    stamps = [r[0] for r in rows]
    print(tag, stamps)

span = taos_sql(
    "tdengine",
    "SELECT CAST(ts AS BIGINT) FROM cems.cems_data WHERE device='device1' "
    "ORDER BY ts DESC LIMIT 13;",
)
ms = [int(r[0]) for r in span]
if len(ms) > 1:
    deltas = [(ms[i] - ms[i + 1]) / 1000.0 for i in range(len(ms) - 1)]
    print("device1 最近 12 个间隔(秒):", deltas, "中位", sorted(deltas)[len(deltas) // 2])

recent = taos_sql(
    "tdengine",
    "SELECT device, COUNT(*) FROM cems.cems_data WHERE ts >= now - 120s GROUP BY device;",
)
print("最近 120 s 各设备条数:", recent)
print("host now:", time.strftime("%Y-%m-%d %H:%M:%S"))
