# =============================================================================
# 边缘侧资源采证的**汇总**脚本（读 `measure_edge_resources.py` 的产出，只读 + 落一份汇总）
# -----------------------------------------------------------------------------
# 采证脚本负责"长跑记原始数"，本脚本负责"把原始数变成能写进报告的结论"：
# 逐容器的 CPU / 内存 min / avg / max、是否触顶（贴上限）、采样覆盖率（缺采多少）。
#
# ⚠️ 为什么不把汇总塞进采证脚本：采证脚本要**跑 24 小时**，任何一处改动都会让
#    已经跑了一半的那段白跑；汇总随时可重跑、口径要反复调，两者生命周期完全不同。
#
# ⚠️ 触顶判据（`ceiling_hits`）——CPU 与内存分开算，因为两者的"顶"表现不同：
#   · 内存顶 = `mem_percent >= --ceiling-ratio×100`（docker 的 MemPerc 直接给了比值）；
#   · CPU 顶   = `cpu_percent >= --cpu-ceiling`（默认 100 = 用满 1 个 vCPU 的额度）。
#   两个都报出来，"没有触顶"才是可核对的结论，不是一句"看着不高"。
#
# ⚠️ "缺采"要单独报：容器被重建（换镜像 / 改编排）的采样点会缺列，
#    采证脚本对缺的容器留空一行 —— 汇总里 `missing_samples` 就是这些行，
#    它同时也是"这段时间谁被重启过"的旁证。
#
# 用法（在仓库根目录执行）：
#   python scripts/measure_edge_resource_summary.py
#   python scripts/measure_edge_resource_summary.py --csv docs/evidence/edge/resource-24h.csv
#
# 参数：
#   --csv                输入（默认 docs/evidence/edge/resource-24h.csv）
#   --out                汇总 json 落点（默认同目录 resource-summary.json）
#   --ceiling-ratio      内存触顶判据（占上限的比例，默认 0.90）
#   --cpu-ceiling        CPU 触顶判据（百分数，默认 100.0）
#   --json-only          只落 json，不打印表格
#
# 退出码：0 = 汇总成功（含"文件为空"这种情形，会在 json 里说明）；1 = 输入文件缺失/不可解析。
# 只读：不碰任何容器，也不写除 `--out` 以外的文件。
# =============================================================================
"""边缘侧资源采证汇总：逐容器 CPU/内存 min-avg-max + 是否触顶，落 json。"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = PROJECT_ROOT / "docs" / "evidence" / "edge" / "resource-24h.csv"
DEFAULT_OUT = PROJECT_ROOT / "docs" / "evidence" / "edge" / "resource-summary.json"

LOCAL_TZ = timezone(timedelta(hours=8))


def human_bytes(num: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(num) < 1024.0 or unit == "TiB":
            return f"{num:.1f}{unit}"
        num /= 1024.0
    return f"{num:.1f}TiB"


def to_float(text: str) -> float | None:
    text = (text or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def pick_limit(limits: set[float]) -> float:
    """从出现过的内存上限集合里选"真正的那个上限"。容器没在跑的采样点 docker stats 会把
    上限记成 0（不是"上限变了"），先剔掉 0，再取出现次数最多的值（并列取值小的，确定性优先）。"""
    if not limits:
        return 0.0
    values = sorted(v for v in limits if v > 0)
    if not values:
        return 0.0
    return values[0] if len(values) == 1 else _mode(values)


def _mode(values: list[float]) -> float:
    counts: dict[float, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    return max(sorted(counts), key=lambda v: counts[v])


def summarize(rows: list[dict[str, str]], ceiling_ratio: float, cpu_ceiling: float) -> dict:
    by_container: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_container.setdefault(row.get("container", "?"), []).append(row)

    ts_all = [r["ts_local"] for r in rows if r.get("ts_local")]
    report: dict[str, object] = {
        "input_rows": len(rows),
        "first_ts": min(ts_all) if ts_all else None,
        "last_ts": max(ts_all) if ts_all else None,
        "runs": sorted({r.get("run_id", "") for r in rows if r.get("run_id")}),
        "ceiling_ratio": ceiling_ratio,
        "cpu_ceiling_percent": cpu_ceiling,
        "containers": {},
    }
    if ts_all:
        t0 = datetime.fromisoformat(min(ts_all))
        t1 = datetime.fromisoformat(max(ts_all))
        report["span_hours"] = round((t1 - t0).total_seconds() / 3600.0, 3)

    for name, items in sorted(by_container.items()):
        cpu = [v for v in (to_float(r.get("cpu_percent", "")) for r in items) if v is not None]
        mem = [
            v for v in (to_float(r.get("mem_used_bytes", "")) for r in items) if v is not None
        ]
        limits = {
            v
            for v in (to_float(r.get("mem_limit_bytes", "")) for r in items)
            if v is not None and v > 0
        }
        pids = [v for v in (to_float(r.get("pids", "")) for r in items) if v is not None]
        missing = sum(1 for r in items if to_float(r.get("mem_used_bytes", "")) is None)
        entry: dict[str, object] = {
            "samples": len(items),
            "missing_samples": missing,
            # 上限取**出现次数最多的那个非零值**：网关被 kill 的采样点会记成 0，
            # 那不是"上限变了"，不能让它把上限算错。
            "mem_limit_bytes": pick_limit(limits),
            "mem_limit_human": human_bytes(pick_limit(limits)) if limits else None,
        }
        for label, series in (("cpu_percent", cpu), ("mem_bytes", mem), ("pids", pids)):
            if series:
                entry[label] = {
                    "min": round(min(series), 3),
                    "avg": round(statistics.fmean(series), 3),
                    "max": round(max(series), 3),
                }
                if label == "mem_bytes":
                    entry["mem_human"] = {
                        "min": human_bytes(min(series)),
                        "avg": human_bytes(statistics.fmean(series)),
                        "max": human_bytes(max(series)),
                    }
            else:
                entry[label] = None
        entry["cpu_ceiling_hits"] = sum(1 for v in cpu if v >= cpu_ceiling)
        if limits:
            limit = pick_limit(limits)
            entry["mem_ceiling_hits"] = sum(1 for v in mem if v >= limit * ceiling_ratio)
            entry["mem_max_ratio_of_limit"] = round(max(mem) / limit, 4) if mem else None
        else:
            entry["mem_ceiling_hits"] = None
            entry["mem_max_ratio_of_limit"] = None
        report["containers"][name] = entry  # type: ignore[index]
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="边缘侧资源采证汇总（只读 csv，落 json）")
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="输入 csv 路径")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="汇总 json 落点")
    parser.add_argument("--ceiling-ratio", type=float, default=0.90, help="内存触顶判据（比例）")
    parser.add_argument("--cpu-ceiling", type=float, default=100.0, help="CPU 触顶判据（百分数）")
    parser.add_argument("--json-only", action="store_true", help="只落 json，不打印表格")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print(f"[fatal] 输入不存在: {csv_path}", flush=True)
        return 1
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError as exc:
        print(f"[fatal] 读不了输入: {exc}", flush=True)
        return 1

    report = summarize(rows, args.ceiling_ratio, args.cpu_ceiling)
    report["csv"] = str(csv_path)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if not args.json_only:
        print(f"input={csv_path}")
        print(
            f"rows={report['input_rows']} span_hours={report.get('span_hours')} "
            f"runs={len(report['runs'])}"
        )
        print(f"window {report['first_ts']} .. {report['last_ts']}")
        for name, entry in report["containers"].items():  # type: ignore[union-attr]
            cpu = entry["cpu_percent"] or {}
            print(
                f"  {name:24s} n={entry['samples']:5d} miss={entry['missing_samples']:3d} "
                f"cpu% {cpu.get('min')}/{cpu.get('avg')}/{cpu.get('max')} "
                f"mem {entry.get('mem_human', {}).get('min')}/"
                f"{entry.get('mem_human', {}).get('avg')}/"
                f"{entry.get('mem_human', {}).get('max')} "
                f"of {entry.get('mem_limit_human')} "
                f"max_ratio={entry.get('mem_max_ratio_of_limit')} "
                f"ceil_hits cpu={entry['cpu_ceiling_hits']} "
                f"mem={entry['mem_ceiling_hits']}"
            )
        print(f"[out] {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
