# -*- coding: utf-8 -*-
"""标定报表覆盖率门限：用**实测周期分布**和**逐窗口条数分布**说话，不拍脑袋定阈值。

为什么需要这个脚本（要回答的问题）：
报表覆盖率 = 窗口实际条数 / 窗口应有条数，而"应有条数"按**名义**采集周期 `POLL_INTERVAL=5.0 s`
算（每分钟 12 条）。但实测真实周期是 5.146 s 左右（`docs/性能与可靠性指标.md` §2.1），
所以**覆盖率天然到不了 100%**：分母按名义周期算，分子按真实节奏给。
于是"门限设多少"不能拍脑袋 —— 设高了会把正常数据全标成"样本不足"，
设低了会把真断档放过去。本脚本把两个分布量出来，给出：
  1. 名义周期 vs 实测周期（含分位数），算出覆盖率的天花板；
  2. 每个窗口的条数分布，并**区分"干净窗口"与"跨断档的窗口"**，给出干净窗口的覆盖率下限；
  3. 与门限对比后的余量，以及"门限是否会误标正常窗口"的结论。

口径与 `scripts/measure_completeness.py` 保持一致（同一套断档判据），便于两个数字互相引用：
  断档判据 = max(3 × 中位间隔, 中位间隔 + 10 s)；窗口"跨断档"= 该窗口区间与任一断档区间相交。

只读：全部 SQL 都是 SELECT，不写库、不改缓存、不动服务。
原始数据落到 `docs/measurements/report_coverage_<tag>.json`（测法 + 原始分布 + 结论）。

用法：
    python scripts/measure_report_coverage.py
    python scripts/measure_report_coverage.py --threshold 0.9 --tag try_090
"""

from __future__ import annotations

import argparse
import statistics
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import _td_ops as ops   # noqa: E402

# 与 src/web/report.py 的默认口径保持一致
POLL_INTERVAL_NOMINAL: Final[float] = 5.0      # 名义采集周期（网关 POLL_INTERVAL）
DEFAULT_THRESHOLD: Final[float] = 0.75         # 候选门限 = docs/告警判据设计.md §3.4 的 ALARM_COVERAGE_MIN
WINDOW_SECONDS: Final[dict[str, int]] = {"1m": 60, "1h": 3600, "1d": 86400}


def percentile(sorted_values: list[float], ratio: float) -> float:
    """分位数：最近秩法（样本量小时不插值，给的是"真的观测到过"的值）。"""
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, max(0, int(round(ratio * len(sorted_values))) - 1))
    return sorted_values[index]


def histogram(values: list[int], bucket: int | None = None) -> dict[str, int]:
    """直方图；给了 bucket 就按桶宽归并（窗口条数很大时用）。"""
    counter = Counter(values if bucket is None else (v // bucket * bucket for v in values))
    return {str(key): counter[key] for key in sorted(counter)}


def window_counts(container: str, db: str, stable: str, unit: str) -> list[tuple[int, int]]:
    """按 `unit` 分段，返回 [(窗口起始 epoch ms, 窗口内条数)]。"""
    rows = ops.taos_sql(
        container,
        f"SELECT CAST(_wstart AS BIGINT) AS w, COUNT(*) AS n "
        f"FROM {db}.{stable} INTERVAL({unit});",
    )
    return [(int(row[0]), int(row[1])) for row in rows]


def analyse_unit(
    unit: str,
    counts: list[tuple[int, int]],
    gaps: list[tuple[int, int]],
    lo_ms: int,
    hi_ms: int,
) -> dict[str, Any]:
    """单个粒度的窗口条数分析：干净窗口 vs 跨断档窗口，各自的覆盖率分布。

    ⚠️ 首末两个不完整窗口必须排除：它们"条数少"是因为库里本来就只有那么多数据
    （例如最后一个分钟窗口才过了 20 秒），量的是数据边界而不是采样节奏。
    把它们算进"正常窗口下限"，会得出"正常窗口覆盖 41.7%"这种错误结论。
    （报表接口本身照样会标出这些窗口，那是它的职责；标定时要把它们摘掉。）
    """
    seconds = WINDOW_SECONDS[unit]
    span_ms = seconds * 1000
    expected = seconds / POLL_INTERVAL_NOMINAL
    clean: list[float] = []
    dirty: list[float] = []
    partial = 0
    for start_ms, count in counts:
        if start_ms < lo_ms or start_ms + span_ms > hi_ms:
            partial += 1
            continue
        crossing = any(gap_start < start_ms + span_ms and gap_end > start_ms for gap_start, gap_end in gaps)
        (dirty if crossing else clean).append(min(1.0, count / expected))
    clean_sorted = sorted(clean)
    dirty_sorted = sorted(dirty)
    return {
        "unit": unit,
        "window_seconds": seconds,
        "expected_per_window": expected,
        "windows": len(counts),
        "partial_edge_windows_excluded": partial,
        "count_histogram": histogram(
            [c for start, c in counts if lo_ms <= start and start + span_ms <= hi_ms],
            bucket=None if unit == "1m" else 50),
        "clean_windows": len(clean),
        "clean_coverage": {
            "min": round(min(clean), 4) if clean else None,
            "P01": round(percentile(clean_sorted, 0.01), 4) if clean else None,
            "P05": round(percentile(clean_sorted, 0.05), 4) if clean else None,
            "P50": round(percentile(clean_sorted, 0.50), 4) if clean else None,
        },
        "gap_windows": len(dirty),
        "gap_coverage": {
            "min": round(min(dirty), 4) if dirty else None,
            "P50": round(percentile(dirty_sorted, 0.50), 4) if dirty else None,
            "max": round(max(dirty), 4) if dirty else None,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="标定报表覆盖率门限（只读）")
    parser.add_argument("--container", default=ops.TD_CONTAINER_DEFAULT)
    parser.add_argument("--db", default=ops.TD_DB_DEFAULT)
    parser.add_argument("--stable", default=ops.TD_STABLE_DEFAULT)
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                        help="候选门限（默认 0.75，与 docs/告警判据设计.md §3.4 同值）")
    parser.add_argument("--tag", default="calibration", help="输出文件名后缀")
    parser.add_argument("--out-dir", default=str(PROJECT_ROOT / "docs" / "measurements"))
    parser.add_argument("--no-write", action="store_true", help="只打印，不落盘")
    args = parser.parse_args()

    ops.ensure_utf8_stdout()
    ops.log(f"读取 {args.db}.{args.stable}（容器 {args.container}）")

    # ---- 取数：全部时间戳（只读） ----
    lo_ms, hi_ms = ops.ts_bounds(args.container, args.db, args.stable)
    total_rows = ops.count_rows(args.container, args.db, args.stable)
    raw_ts = [int(row[0]) for row in ops.taos_sql(
        args.container, f"SELECT CAST(ts AS BIGINT) FROM {args.db}.{args.stable} ORDER BY ts ASC;")]

    # ---- 断档判据（与 scripts/measure_completeness.py 同口径） ----
    all_gaps_ms = sorted(b - a for a, b in zip(raw_ts, raw_ts[1:]))
    median_gap_ms = percentile([float(v) for v in all_gaps_ms], 0.50)
    gap_threshold_ms = max(3 * median_gap_ms, median_gap_ms + 10_000)
    gaps = [(a, b) for a, b in zip(raw_ts, raw_ts[1:]) if b - a > gap_threshold_ms]

    intervals_s = [v / 1000.0 for v in all_gaps_ms]
    # 在线段（未跨断档）的相邻间隔：断档本身会把均值彻底带偏，必须分开看
    online = sorted((b - a) / 1000.0 for a, b in zip(raw_ts, raw_ts[1:]) if b - a <= gap_threshold_ms)
    online_mean = statistics.fmean(online) if online else 0.0

    per_unit = {
        unit: analyse_unit(
            unit, window_counts(args.container, args.db, args.stable, unit), gaps, lo_ms, hi_ms)
        for unit in ("1m", "1h", "1d")
    }

    # ---- 换算：名义分母下覆盖率的天花板，以及干净窗口的下限 ----
    ceiling = POLL_INTERVAL_NOMINAL / online_mean if online_mean else None
    normal_floor = per_unit["1m"]["clean_coverage"]["min"]
    margin = None if normal_floor is None else round(normal_floor - args.threshold, 4)

    conclusion = {
        "nominal_poll_interval_s": POLL_INTERVAL_NOMINAL,
        "measured_interval_p50_s": round(percentile(online, 0.50), 3),
        "measured_interval_p95_s": round(percentile(online, 0.95), 3),
        "measured_interval_mean_s": round(online_mean, 4),
        "coverage_ceiling_nominal_denominator": round(ceiling, 4) if ceiling else None,
        "clean_minute_coverage_floor": normal_floor,
        "candidate_threshold": args.threshold,
        "margin_below_normal_floor": margin,
        "verdict": (
            "OK：门限低于实测正常窗口下限，正常数据不会被标成样本不足"
            if margin is not None and margin > 0
            else "风险：门限 >= 实测正常窗口下限，正常数据会被误标，必须调低门限"
        ),
    }

    payload = {
        "generated_at": datetime.now().strftime(ops.TS_FMT),
        "method": {
            "coverage_definition": "窗口实际条数 / 窗口应有条数",
            "expected_definition": "窗口秒数 / POLL_INTERVAL（名义周期，默认 5.0 s）",
            "gap_threshold_ms": int(gap_threshold_ms),
            "gap_threshold_rule": "max(3 × 中位间隔, 中位间隔 + 10 s)，与 scripts/measure_completeness.py 同口径",
            "clean_window": "窗口区间与任何断档区间都不相交，且窗口完整落在数据范围内（排除首末不完整窗口）",
            "note": "覆盖率按名义周期算分母，实测周期 5.146 s ⇒ 天花板 < 100%，门限必须留出余量",
        },
        "source": {"container": args.container, "db": args.db, "stable": args.stable},
        "data_range": {
            "first_ts": ops.ts_ms_to_str(lo_ms),
            "last_ts": ops.ts_ms_to_str(hi_ms),
            "span_hours": round((hi_ms - lo_ms) / 3_600_000.0, 2),
            "total_rows": total_rows,
            "gap_count": len(gaps),
        },
        "interval_distribution_s": {
            "samples": len(intervals_s),
            "min": round(min(intervals_s), 3) if intervals_s else None,
            "P50": round(percentile(sorted(intervals_s), 0.50), 3),
            "P90": round(percentile(online, 0.90), 3),
            "P95": round(percentile(online, 0.95), 3),
            "P99": round(percentile(online, 0.99), 3),
            "max": round(max(intervals_s), 3) if intervals_s else None,
            "histogram_s": histogram([round(v) for v in intervals_s if v <= 60]),
        },
        "windows": per_unit,
        "conclusion": conclusion,
    }

    print()
    print("=== 覆盖率门限标定（实测） ===")
    print(f"数据范围 : {payload['data_range']['first_ts']} ~ {payload['data_range']['last_ts']}"
          f"（{payload['data_range']['span_hours']} h，{total_rows} 条，断档 {len(gaps)} 处）")
    print(f"相邻间隔 : P50={percentile(online, 0.50):.3f}s P95={percentile(online, 0.95):.3f}s "
          f"P99={percentile(online, 0.99):.3f}s 在线段均值={online_mean:.3f}s")
    print(f"名义周期 : {POLL_INTERVAL_NOMINAL} s ⇒ 覆盖率天花板 = 名义/实测 = "
          f"{conclusion['coverage_ceiling_nominal_denominator']}")
    for unit, item in per_unit.items():
        print(f"[{unit}] 窗口={item['windows']}（排除首末不完整 {item['partial_edge_windows_excluded']}）"
              f" 干净={item['clean_windows']} 跨断档={item['gap_windows']} "
              f"干净窗覆盖率 min={item['clean_coverage']['min']} P05={item['clean_coverage']['P05']} "
              f"P50={item['clean_coverage']['P50']} | 断档窗 max={item['gap_coverage']['max']}")
    print(f"门限 {args.threshold} ⇒ 距实测正常下限 {normal_floor} 余量 {margin}")
    print(f"结论     : {conclusion['verdict']}")

    if not args.no_write:
        out = Path(args.out_dir) / f"report_coverage_{args.tag}.json"
        ops.write_json(out, payload)
        ops.log(f"原始数据已写入 {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
