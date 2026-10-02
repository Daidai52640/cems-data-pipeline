# -*- coding: utf-8 -*-
"""把 docker stats 采样 CSV 汇总成"压测窗口内的资源占用"。

输入：`docs/evidence/perf/loadgen_stats_<tag>.csv`（sample_time,container,cpu_perc,mem_usage,mem_perc）
输出：每个容器在采样窗口内的 CPU 均值 / 峰值（% × 1 核）、内存首末值与增量（MiB）。
⚠️ 只报"均值 + 峰值"而不是"首末两点"：采样是 5 s 一次的快照，
首末两点会被瞬时抖动主导（本项目 N=10 那一轮 emqx 首点 3.17%、末点 94.91%，
两点法会得出完全相反的结论）。
"""

from __future__ import annotations

import csv
import re
import statistics
import sys
from pathlib import Path

EVID = Path(__file__).resolve().parent


def parse_mib(text: str) -> float:
    match = re.match(r"([\d.]+)\s*([KMG]iB)", text.strip())
    if not match:
        return float("nan")
    value, unit = float(match.group(1)), match.group(2)
    return value * {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024}[unit]


def summarize(path: Path) -> None:
    rows = [r for r in csv.DictReader(path.open(encoding="utf-8"))
            if r.get("container") not in (None, "", "container")]
    by_container: dict[str, list[dict]] = {}
    for row in rows:
        by_container.setdefault(row["container"], []).append(row)
    print(f"\n== {path.name}（{len(rows)} 行采样，{len(by_container)} 个容器）==")
    print(f"{'容器':<26}{'CPU均值%':>10}{'CPU峰值%':>10}{'内存首/MiB':>12}{'内存末/MiB':>12}{'Δ内存/MiB':>12}")
    for name in sorted(by_container):
        series = by_container[name]
        cpu = [float(item["cpu_perc"].rstrip("%")) for item in series]
        mem = [parse_mib(item["mem_usage"].split("/")[0]) for item in series]
        print(f"{name:<26}{statistics.mean(cpu):>10.2f}{max(cpu):>10.2f}"
              f"{mem[0]:>12.1f}{mem[-1]:>12.1f}{mem[-1] - mem[0]:>12.1f}")
    # 汇总：接入实例组 + 关键基础设施
    lg = [k for k in by_container if "lgsub" in k]
    if lg:
        cpu_all = [float(item["cpu_perc"].rstrip("%"))
                   for k in lg for item in by_container[k]]
        print(f"  → 接入实例合计（{len(lg)} 个）：CPU 均值 {statistics.mean(cpu_all):.2f}% 、"
              f"峰值 {max(cpu_all):.2f}%（单实例）")


if __name__ == "__main__":
    targets = sys.argv[1:] or sorted(str(p) for p in EVID.glob("loadgen_stats_acc_n*.csv"))
    for item in targets:
        summarize(Path(item))
