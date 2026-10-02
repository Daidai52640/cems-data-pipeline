# -*- coding: utf-8 -*-
"""量"库内最新一条"与宿主墙上时钟的距离（生产链路新鲜度）。只读。"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts._td_ops import LOCAL_TZ, taos_sql  # noqa: E402

for i in range(3):
    now = datetime.now(LOCAL_TZ)
    out = {}
    for tag in ("device1", "device2"):
        rows = taos_sql(
            "tdengine",
            f"SELECT CAST(ts AS BIGINT) FROM cems.cems_data WHERE device='{tag}' "
            "ORDER BY ts DESC LIMIT 1;",
        )
        ms = int(rows[0][0])
        newest = datetime.fromtimestamp(ms / 1000.0, LOCAL_TZ)
        out[tag] = {"newest": newest.strftime("%Y-%m-%d %H:%M:%S"),
                    "age_s": round((now - newest).total_seconds(), 2)}
    print(now.strftime("%Y-%m-%d %H:%M:%S"), out)
    if i < 2:
        time.sleep(5)
