# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读
# -----------------------------------------------------------------------------
# 1) 本脚本**只读**：只做 TDengine SELECT，不写库、不发 MQTT、不动缓存文件。
#    可反复执行，重复执行只覆盖同名输出文件。
# 2) 前置条件：tdengine 容器在跑、REST 端口 6041 可从宿主机访问。
# 3) 环境：Windows 上先设 $env:PYTHONIOENCODING="utf-8"。
#
# 口径定义（两个都算，别只报一个）
# -----------------------------------------------------------------------------
#   理论应有条数(名义) = 窗口时长 / POLL_INTERVAL（5 s → 720 条/小时）
#   理论应有条数(实测节奏) = 窗口时长 / 实测中位间隔
#     ⚠️ 为什么要有第二个：网关的节奏是 sleep(5) + 读设备 + 等 PUBACK，
#        实测周期略大于 5 s；拿 720 当分母会把"节奏偏慢"错算成"丢数据"。
#   完整率 = 实际条数 / 理论应有条数
#   有效完整率 = 实际条数 / (实际条数 + 断档推断缺失条数)
#     断档判据：相邻时间戳间隔 > max(gap_factor × 中位间隔, 中位间隔 + 10 s)
#
# 窗口选取
# -----------------------------------------------------------------------------
#   --auto 会扫描 --since 之后的所有时间戳，自动取**最长的一段无断档连续窗口**，
#   并把被排除的时段（断档、停机）一并打印出来，避免"拿断链期算完整率"。
# =============================================================================
"""数据完整率测量：实际入库条数 / 理论应有条数，含断档检测与窗口自动选取。"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import statistics
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

TD_REST_URL = "http://127.0.0.1:6041/rest/sql"
TD_AUTH = "Basic " + base64.b64encode(b"root:taosdata").decode()
TD_DB = "cems"
TD_STABLE = "cems_data"

LOCAL_TZ = timezone(timedelta(hours=8))
PAGE = 1000


def parse_iso_utc(text: str) -> float:
    cleaned = text.strip().replace("Z", "").replace("T", " ")
    if "." in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        ).timestamp()
    return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    ).timestamp()


def parse_local(text: str) -> float:
    text = text.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=LOCAL_TZ).timestamp()
        except ValueError:
            continue
    raise ValueError(f"无法解析时间: {text!r}")


def fmt_local(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def td_query(sql: str, timeout: float = 15.0) -> list[list[str]]:
    req = urllib.request.Request(
        TD_REST_URL, data=sql.encode("utf-8"), headers={"Authorization": TD_AUTH}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("code") != 0:
        raise RuntimeError(f"TDengine 返回错误: {payload}")
    return payload.get("data") or []


def fetch_timestamps(since: float, until: float) -> list[float]:
    """按 ts 游标分页取回窗口内全部时间戳（秒，整数）。"""
    out: list[float] = []
    cursor = since
    while True:
        sql = (
            f"SELECT ts FROM {TD_DB}.{TD_STABLE} "
            f"WHERE ts > {int(cursor * 1000)} AND ts <= {int(until * 1000)} "
            f"ORDER BY ts LIMIT {PAGE}"
        )
        rows = td_query(sql)
        if not rows:
            break
        batch = [parse_iso_utc(row[0]) for row in rows]
        out.extend(batch)
        if len(batch) < PAGE:
            break
        cursor = batch[-1]
    return sorted(set(out))


def gap_threshold(deltas: list[float], factor: float) -> float:
    med = statistics.median(deltas) if deltas else 5.0
    return max(factor * med, med + 10.0)


def analyze(stamps: list[float], interval: float, factor: float) -> dict:
    if len(stamps) < 3:
        return {"n": len(stamps), "error": "样本太少"}
    deltas = [b - a for a, b in zip(stamps, stamps[1:])]
    threshold = gap_threshold(deltas, factor)
    gaps = [
        {"start": fmt_local(a), "end": fmt_local(b), "seconds": b - a,
         "missing_estimate": max(0, round((b - a) / statistics.median(deltas)) - 1)}
        for a, b in zip(stamps, stamps[1:]) if b - a > threshold
    ]
    missing = sum(g["missing_estimate"] for g in gaps)
    duration = stamps[-1] - stamps[0]
    median_delta = statistics.median(deltas)
    nominal_expected = duration / interval + 1.0
    rhythm_expected = duration / median_delta + 1.0
    return {
        "start": fmt_local(stamps[0]),
        "end": fmt_local(stamps[-1]),
        "duration_s": duration,
        "rows": len(stamps),
        "delta_s": {
            "min": min(deltas),
            "p50": median_delta,
            "p95": sorted(deltas)[int(len(deltas) * 0.95)],
            "max": max(deltas),
            "mean": statistics.fmean(deltas),
        },
        "gap_threshold_s": threshold,
        "gaps": gaps,
        "missing_rows_inferred": missing,
        "expected_nominal": nominal_expected,
        "expected_by_observed_rhythm": rhythm_expected,
        "completeness_nominal": len(stamps) / nominal_expected,
        "completeness_by_rhythm": len(stamps) / rhythm_expected,
        "completeness_effective": len(stamps) / (len(stamps) + missing),
        "rows_per_hour": len(stamps) / duration * 3600.0,
    }


def longest_continuous_run(stamps: list[float], interval: float, factor: float) -> list[float]:
    if not stamps:
        return []
    deltas = [b - a for a, b in zip(stamps, stamps[1:])]
    threshold = gap_threshold(deltas, factor)
    best: list[float] = []
    current: list[float] = [stamps[0]]
    for a, b in zip(stamps, stamps[1:]):
        if b - a > threshold:
            if len(current) > len(best):
                best = current
            current = [b]
        else:
            current.append(b)
    return best if len(best) > len(current) else current


def per_hour(stamps: list[float]) -> list[dict]:
    buckets: dict[str, int] = {}
    for s in stamps:
        key = datetime.fromtimestamp(s, LOCAL_TZ).strftime("%Y-%m-%d %H:00")
        buckets[key] = buckets.get(key, 0) + 1
    return [{"hour": k, "rows": v} for k, v in sorted(buckets.items())]


def main() -> int:
    parser = argparse.ArgumentParser(description="数据完整率测量（只读）")
    parser.add_argument("--since", required=True, help="窗口起点（本地时间，如 2026-10-01 13:26:16）")
    parser.add_argument("--until", default="now", help="窗口终点，默认 now")
    parser.add_argument("--interval", type=float, default=5.0, help="名义采样间隔 POLL_INTERVAL（秒）")
    parser.add_argument("--gap-factor", type=float, default=3.0, help="断档判据倍数")
    parser.add_argument("--auto", action="store_true", help="自动选取最长连续窗口")
    parser.add_argument("--tag", default="window")
    parser.add_argument(
        "--out-prefix",
        default=str(PROJECT_ROOT / "docs" / "evidence" / "perf" / "completeness"),
    )
    args = parser.parse_args()

    since = parse_local(args.since)
    until = time.time() if args.until == "now" else parse_local(args.until)
    stamps = fetch_timestamps(since, until)
    print(f"[input] 窗口 {fmt_local(since)} → {fmt_local(until)}，取回 {len(stamps)} 条时间戳")

    all_stats = analyze(stamps, args.interval, args.gap_factor)

    chosen = stamps
    if args.auto:
        chosen = longest_continuous_run(stamps, args.interval, args.gap_factor)
        print(
            f"[auto] 最长连续窗口: {fmt_local(chosen[0])} → {fmt_local(chosen[-1])}"
            f"（{len(chosen)} 条）"
        )
    stats = analyze(chosen, args.interval, args.gap_factor) if chosen else all_stats

    prefix = Path(args.out_prefix)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = prefix.with_name(f"{prefix.name}_{args.tag}.json")
    json_path.write_text(
        json.dumps(
            {
                "tag": args.tag,
                "since": fmt_local(since),
                "until": fmt_local(until),
                "interval_s": args.interval,
                "gap_factor": args.gap_factor,
                "scan_all": all_stats,
                "chosen_window": stats,
                "per_hour_all": per_hour(stamps),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    deltas_path = prefix.with_name(f"{prefix.name}_{args.tag}_deltas.csv")
    with deltas_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(["ts_local", "epoch", "delta_to_prev_s"])
        prev = None
        for s in stamps:
            writer.writerow([fmt_local(s), f"{s:.0f}", "" if prev is None else f"{s - prev:.0f}"])
            prev = s

    print("\n===== 完整率 =====")
    for label, key in (("扫描全窗口", "scan_all"), ("选定窗口", "chosen_window")):
        st = all_stats if key == "scan_all" else stats
        if st.get("error"):
            print(f"  [{label}] {st['error']}")
            continue
        print(
            f"  [{label}] {st['start']} → {st['end']}（{st['duration_s'] / 60:.1f} 分钟）\n"
            f"      实际 {st['rows']} 条；间隔 min={st['delta_s']['min']:.0f}s "
            f"P50={st['delta_s']['p50']:.2f}s P95={st['delta_s']['p95']:.0f}s "
            f"max={st['delta_s']['max']:.0f}s\n"
            f"      理论(名义 {args.interval:.0f}s): {st['expected_nominal']:.1f} 条 → "
            f"完整率 {st['completeness_nominal'] * 100:.2f}%\n"
            f"      理论(实测节奏 {st['delta_s']['p50']:.2f}s): "
            f"{st['expected_by_observed_rhythm']:.1f} 条 → "
            f"完整率 {st['completeness_by_rhythm'] * 100:.2f}%\n"
            f"      断档判据 {st['gap_threshold_s']:.1f}s → 断档 {len(st['gaps'])} 处，"
            f"推断缺失 {st['missing_rows_inferred']} 条 → "
            f"有效完整率 {st['completeness_effective'] * 100:.2f}%"
        )
        for g in st["gaps"][:20]:
            print(f"        gap {g['start']} → {g['end']}  {g['seconds']:.0f}s"
                  f"（约缺 {g['missing_estimate']} 条）")
    print(f"\n[out] {json_path}")
    print(f"[out] {deltas_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
