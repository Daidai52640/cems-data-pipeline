# -*- coding: utf-8 -*-
"""一次性回填：用新的（coverage 封顶 1.0）公式重算指定的历史小时结论。

背景：`alarm_judge` 原先把 coverage 裸算成 n_valid/expected，于是 n_total=722 的满小时
得到 1.00278（"100.28%"）。后来改为 min(1.0, ...)，但**只改了公式、没回填历史行**，
库里仍留有 3 行 coverage>1.0。

做法：复用接入层**真实**的 `settle_hour()`（结论表幂等键 (子表, ts) → 覆盖）。
对象构造照抄 `subscriber_to_td.main()`：AlarmWriter(表) / AlarmJudge(配置)。
默认只预览，加 --apply 才写库。
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

sys.path.insert(0, "/app")

from src.platform import subscriber_to_td as sub  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="回填历史小时结论（coverage 封顶 1.0）")
    ap.add_argument("hours", nargs="+", help="要回填的小时起点，如 '2026-10-02 16:00:00'")
    ap.add_argument("--apply", action="store_true", help="真的写库（默认只打印）")
    args = ap.parse_args()

    tables = sub.ALARM_TABLES
    writer = sub.AlarmWriter(tables)
    if not writer.connect():
        print("  ❌ 告警表连接失败")
        return 2
    judge = sub.AlarmJudge(sub.ALARM_CONFIG)

    print(f"  实例身份: db={tables.db} plant={tables.plant} device={tables.device}")
    for text in args.hours:
        hour = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        sql = (
            f"SELECT point, n_total, coverage FROM {tables.db}.{tables.verdict_stable} "
            f"WHERE ts = '{text}' AND device = '{tables.device}' ORDER BY point"
        )
        before = writer.query(sql)
        print(f"\n  ── {text} ──")
        print(f"    回填前: {before if before else '(无行)'}")
        if not args.apply:
            print("    （仅预览；加 --apply 才写库）")
            continue
        n_v, n_e = sub.settle_hour(writer, tables, judge, hour)
        after = writer.query(sql)
        print(f"    写入 verdict={n_v} event={n_e}")
        print(f"    回填后: {after if after else '(无行)'}")

    writer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
