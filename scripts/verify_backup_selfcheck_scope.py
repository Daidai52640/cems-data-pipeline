# -*- coding: utf-8 -*-
"""备份自检口径复核：`reported_rows` 到底该和谁比。

背景（2026-10-03 双设备验收实测）：一份**完全成功**的备份被判成
`rows_out_of_range`（pre=18570 dumped=19100 post=18570），因为
`taosdump` 的 `reported_rows` 是**整库**（cems 下全部 stable）的行数，
而被拿去比的是 `cems_data` **一张超级表**的 `COUNT(*)`。

两条判据：
  ① reported_rows == Σ(cems 库全部 stable 的 COUNT(*))  → 备份完整性（应相等）
  ② cems_data 的 COUNT(*) 落在 [pre_count, post_count]  → 主表窗口单调性
两件事分开判，才能既发现真问题又不误报。

只读：对现网库只有 SELECT。
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._td_ops import (  # noqa: E402
    LOCAL_TZ,
    TD_CONTAINER_DEFAULT,
    TD_DB_DEFAULT,
    TD_STABLE_DEFAULT,
    ensure_utf8_stdout,
    read_json,
    taos_scalar,
    taos_sql,
    write_json,
)

EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence" / "backup"


def stable_counts(container: str, db: str) -> dict[str, int]:
    names = [row[0] for row in taos_sql(container, f"SHOW {db}.STABLES;")]
    counts: dict[str, int] = {}
    for name in names:
        counts[name] = int(taos_scalar(container, f"SELECT COUNT(*) FROM {db}.{name};"))
    return counts


def main() -> int:
    ensure_utf8_stdout()
    parser = argparse.ArgumentParser(description="备份 reported_rows 口径复核")
    parser.add_argument("--backup", required=True, help="备份目录（读 manifest）")
    parser.add_argument("--container", default=TD_CONTAINER_DEFAULT)
    parser.add_argument("--db", default=TD_DB_DEFAULT)
    parser.add_argument("--stable", default=TD_STABLE_DEFAULT)
    parser.add_argument("--tag", default=None)
    parser.add_argument("--tolerance", type=int, default=20,
                        help="判据①允许的绝对差（dump 期间并发写入的行数）")
    args = parser.parse_args()

    backup_dir = Path(args.backup).resolve()
    manifest = read_json(backup_dir / "manifest.json")
    reported = int(manifest["dump"]["reported_rows"])
    pre, post = int(manifest["window"]["pre_count"]), int(manifest["window"]["post_count"])
    post_ts = int(manifest["window"]["post_max_ms"])

    counts = stable_counts(args.container, args.db)
    # ⚠️ 非主表（verdict / alarm）在 dump 之后**仍会新增**（告警与小时结算是持续运行的），
    #    所以"当前值"不等于"dump 时的值"，必须按 manifest 里的 **post_count** 设成**同一时刻**的
    #    快照来重建 —— 否则会把 dump 之后新写入的告警算进"备份应有的行数"。
    others = {k: v for k, v in counts.items() if k != args.stable}
    others_at_post = {}
    for name, current in others.items():
        at_post = int(taos_scalar(
            args.container, f"SELECT COUNT(*) FROM {args.db}.{name} WHERE ts <= {post_ts};"))
        others_at_post[name] = {"now": current, "at_post_ts": at_post,
                                "growth_after_post": current - at_post}
    reconstructed = pre + sum(v["at_post_ts"] for v in others_at_post.values())
    total_now = sum(counts.values())
    main_now = counts.get(args.stable)
    report = {
        "checked_at": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "backup_dir": str(backup_dir),
        "manifest_self_check": manifest["dump"]["self_check"],
        "reported_rows": reported,
        "aligned_instant": {"post_count": post, "post_max": manifest["window"]["post_max"],
                            "post_max_ms": post_ts},
        "stable_counts_now": counts,
        "sum_of_all_stables_now": total_now,
        "check_1_integrity": {
            "rule": "reported_rows == 主表 dump 前条数 + Σ(其余 stable 在 dump 后同一时刻的条数)",
            "main_table": args.stable,
            "main_table_at_dump": pre,
            "other_stables_same_instant": others_at_post,
            "reconstructed_total": reconstructed,
            "diff": reconstructed - reported,
            "note": "重建与 reported 的差（个位数）来自 dump 期间告警/小时结算的并发写入："
                    "taosdump 自报数是导出过程中实际写出的行数，"
                    "而主表项用的是『dump 前』快照，两者不在同一瞬间。",
            "ok": abs(reconstructed - reported) <= args.tolerance,
        },
        "check_2_main_table_window": {
            "rule": "pre_count ≤ cems_data COUNT(*) ≤ post_count（在线备份窗口内）",
            "pre_count": pre,
            "post_count": post,
            "cems_data_now": main_now,
            "cems_data_now_minus_pre": (main_now - pre) if main_now is not None else None,
            "note": "复核在备份之后，主表已继续增长，所以只核『≥ pre』；"
                    "备份脚本当时判的是 pre ≤ dumped ≤ post。",
            "ok": main_now is not None and main_now >= pre,
        },
    }
    report["ok"] = (report["check_1_integrity"]["ok"]
                    and report["check_2_main_table_window"]["ok"])

    print(f"备份：{backup_dir}")
    print(f"  manifest 自检：{manifest['dump']['self_check']}")
    print(f"  reported_rows（taosdump 整库自报）：{reported}")
    print(f"  当前 cems 各表行数：{counts}")
    print(f"  重建（对齐到 post_max={manifest['window']['post_max']}）：主表 dump 前 {pre} "
          f"+ 其余 stable {sum(v['at_post_ts'] for v in others_at_post.values())} = {reconstructed}"
          f" → 与 reported 差 {reconstructed - reported}（容差 {args.tolerance}）")
    for name, item in others_at_post.items():
        print(f"    {name}: dump 后同刻 {item['at_post_ts']} / 现在 {item['now']}"
              f"（之后又长了 {item['growth_after_post']}）")
    print(f"  主表 cems_data：dump 前 {pre} / dump 后 {post} / 现在 {main_now}")
    print(f"  判据①（整库完整性）={report['check_1_integrity']['ok']}；"
          f"判据②（主表窗口）={report['check_2_main_table_window']['ok']}")

    out = EVIDENCE_DIR / f"backup_selfcheck_scope_{args.tag or 'run1'}.json"
    write_json(out, report)
    print(f"结果已落盘：{out}")
    return 0 if report["ok"] else 4


if __name__ == "__main__":
    raise SystemExit(main())
