# -*- coding: utf-8 -*-
"""压测后的生产链路复核：两台设备的最新数据 + loadtest 表行数。只读。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts._td_ops import taos_sql  # noqa: E402

for tag in ("device1", "device2"):
    rows = taos_sql(
        "tdengine",
        f"SELECT ts FROM cems.cems_data WHERE device='{tag}' ORDER BY ts DESC LIMIT 2;",
    )
    print(tag, [r[0] for r in rows])
rows = taos_sql(
    "tdengine",
    "SELECT COUNT(*) FROM cems.cems_data WHERE tbname LIKE 'loadtest%';",
)
print("loadtest 表行数:", rows)
