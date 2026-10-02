# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读
# -----------------------------------------------------------------------------
# 1) 本脚本**只读**：读 `docker logs`（用 docker logs 自己拉，或读 --log-file 指定的
#    已导出日志文本）与 TDengine 的 COUNT/MAX(ts)，不写库、不改服务。
# 2) 它做一件在延迟测量里很关键的事：**只用网关自己的时钟**，量出
#    "一条数据在断网缓存里停留多久才被补传出去" —— 单时钟相减，没有跨容器时钟偏差。
#    原理：网关每个补传周期都会先"[补传] 开始补传 N 条"（取走上一周期的积压），
#    再把本轮新采的数据"[缓存] … 数据已入本地队列"。
#    所以第 i 条入队的行，是在第 i+1 个补传周期被发出去的 → 滞后 = T(i+1) - T(i)。
# 3) 环境：Windows 上先设 $env:PYTHONIOENCODING="utf-8"。
#
# 设备维度（必须按设备过滤）
# -----------------------------------------------------------------------------
#   "走补传路径占比"的分母是同窗口**库内条数**，而 `cems_data` 是多设备共用的超级表
#   （TAG = plant/device），两台设备在同时写。不过滤的话分母 = 各设备条数之和，
#   占比被系统性地算小 —— 实测同一个 5 分钟窗口：不过滤 290 行，device1 单独 59 行
#   （同样 59 条缓存行，占比会从 100% 变成 20% 量级的假象）。
#   默认 device1 ⇒ 单设备形态下与改造前逐位一致；多设备时用 --device 分开统计。
#   ⚠️ 日志侧不用传设备：每个网关实例只读自己那台从站（container 各自一份日志），
#      换设备时用 --container 指向对应实例（默认 cems-gateway = device1；
#      device2 用 cems-gateway-plant2，见 docker-compose.yml 的 plant2 profile）。
# =============================================================================
"""从网关日志量"缓存→补传"滞后（单时钟），并统计走补传路径的条目占比。"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import re
import statistics
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from _device_scope import add_device_argument, device_predicate, resolve_device   # noqa: E402

TD_REST_URL = "http://127.0.0.1:6041/rest/sql"
TD_AUTH = "Basic " + base64.b64encode(b"root:taosdata").decode()
LOCAL_TZ = timezone(timedelta(hours=8))
TS_FORMAT = "%Y-%m-%d %H:%M:%S"

RE_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \[(\w+)\] gateway: (.*)$")
RE_RESEND_START = re.compile(r"^\[补传\] 开始补传 (\d+) 条")
RE_CACHED = re.compile(r"^\[缓存\](.*?)，数据已入本地队列: (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def parse_iso_utc(text: str) -> float:
    cleaned = text.strip().replace("Z", "").replace("T", " ")
    if "." in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        ).timestamp()
    return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    ).timestamp()


def td_count_between(start_local: str, end_local: str, device: str = "") -> int:
    """同窗口**被测设备**的库内条数（"走补传路径占比"的分母）。

    ⚠️ **必须带 device 谓词**：不过滤时分母是所有设备之和，占比被系统性算小
    （见文件头的实测数字）。
    """
    start = datetime.strptime(start_local, TS_FORMAT).replace(tzinfo=LOCAL_TZ).timestamp()
    end = datetime.strptime(end_local, TS_FORMAT).replace(tzinfo=LOCAL_TZ).timestamp()
    sql = (f"SELECT COUNT(*) FROM cems.cems_data "
           f"WHERE ts >= {int(start * 1000)} AND ts <= {int(end * 1000)} "
           f"AND {device_predicate(device)}")
    req = urllib.request.Request(TD_REST_URL, data=sql.encode(), headers={"Authorization": TD_AUTH})
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return int(payload["data"][0][0])


def percentile(ordered: list[float], q: float) -> float:
    pos = (len(ordered) - 1) * q
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def main() -> int:
    parser = argparse.ArgumentParser(description="网关补传滞后分析（只读）")
    parser.add_argument("--log-file", default=None, help="已导出的日志文件；不给就现场 docker logs")
    parser.add_argument("--container", default="cems-gateway")
    parser.add_argument("--since", default=None, help="只分析该本地时间之后的行（如 2026-10-01 13:26:00）")
    parser.add_argument("--tag", default="resend_lag")
    add_device_argument(parser)
    parser.add_argument(
        "--out-prefix", default=str(PROJECT_ROOT / "docs" / "evidence" / "drill" / "gateway_resend")
    )
    args = parser.parse_args()

    device = resolve_device(args.device)

    if args.log_file:
        text = Path(args.log_file).read_text(encoding="utf-8", errors="replace")
    else:
        out = subprocess.run(["docker", "logs", args.container],
                             capture_output=True, text=True, timeout=120)
        text = out.stdout + out.stderr

    cache_events: list[dict] = []
    resend_events: list[dict] = []
    cache_reasons: dict[str, int] = {}
    for raw in text.splitlines():
        match = RE_LINE.match(raw.strip())
        if not match:
            continue
        stamp, level, message = match.groups()
        if args.since and stamp < args.since:
            continue
        start = RE_RESEND_START.match(message)
        if start:
            resend_events.append({"t": stamp, "batch": int(start.group(1))})
            continue
        cached = RE_CACHED.match(message)
        if cached:
            reason = cached.group(1).strip() or "(空)"
            cache_reasons[reason] = cache_reasons.get(reason, 0) + 1
            cache_events.append({"t": stamp, "payload_ts": cached.group(2), "reason": reason})

    # 第 i 条入队的行在第 i+1 个补传周期被发出 → 滞后 = T(i+1) - T(i)
    lags: list[dict] = []
    started = [e["t"] for e in resend_events]
    for i, event in enumerate(cache_events):
        sent_at = started[i + 1] if i + 1 < len(started) else None
        lag = None
        if sent_at:
            lag = (datetime.strptime(sent_at, TS_FORMAT)
                   - datetime.strptime(event["t"], TS_FORMAT)).total_seconds()
        lags.append({"cached_at": event["t"], "payload_ts": event["payload_ts"],
                     "sent_at": sent_at, "lag_s": lag, "reason": event["reason"]})

    lag_values = [r["lag_s"] for r in lags if r["lag_s"] is not None]
    lag_values_sorted = sorted(lag_values)

    prefix = Path(args.out_prefix)
    csv_path = prefix.with_name(f"{prefix.name}_lag_{args.tag}.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(lags[0].keys()) if lags else
                                ["cached_at", "payload_ts", "sent_at", "lag_s", "reason"])
        writer.writeheader()
        writer.writerows(lags)

    # 走补传路径的条目占比 = 缓存条目数 / 同窗口库内条数
    ratio = None
    window = None
    if cache_events:
        first, last = cache_events[0]["payload_ts"], cache_events[-1]["payload_ts"]
        try:
            rows = td_count_between(first, last, device)
            ratio = len(cache_events) / rows if rows else None
            window = {"first": first, "last": last, "db_rows": rows,
                      "cached_rows": len(cache_events)}
        except Exception as exc:
            print(f"[warn] 查库失败，跳过占比: {exc}")

    summary = {
        "tag": args.tag,
        "device": device,
        "log_window": {
            "first_cache_event": cache_events[0]["t"] if cache_events else None,
            "last_cache_event": cache_events[-1]["t"] if cache_events else None,
            "resend_cycles": len(resend_events),
            "cached_rows": len(cache_events),
        },
        "cache_reasons": cache_reasons,
        "resend_batch_sizes": sorted({e["batch"] for e in resend_events}),
        "lag_s": {
            "n": len(lag_values),
            "min": lag_values_sorted[0] if lag_values else None,
            "p50": percentile(lag_values_sorted, 0.5) if lag_values else None,
            "p95": percentile(lag_values_sorted, 0.95) if lag_values else None,
            "max": lag_values_sorted[-1] if lag_values else None,
            "mean": statistics.fmean(lag_values) if lag_values else None,
        },
        "resend_path_ratio": ratio,
        "ratio_window": window,
    }
    json_path = prefix.with_name(f"{prefix.name}_summary_{args.tag}.json")
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str),
                         encoding="utf-8")

    print("===== 网关日志：缓存→补传 滞后（单时钟，无跨容器偏差）=====")
    print(f"  被测设备 {device}（TAG 过滤，见 --device）")
    print(f"  补传周期数 {len(resend_events)}；入缓存行数 {len(cache_events)}；"
          f"每批条数 {summary['resend_batch_sizes']}")
    print(f"  入缓存原因分布: {cache_reasons}")
    if lag_values:
        s = summary["lag_s"]
        print(f"  滞后 n={s['n']} min={s['min']:.0f}s P50={s['p50']:.0f}s "
              f"P95={s['p95']:.0f}s max={s['max']:.0f}s mean={s['mean']:.1f}s")
    if ratio:
        print(f"  走补传路径占比: {ratio * 100:.1f}%（{window['cached_rows']}/{window['db_rows']}，"
              f"窗口 {window['first']} → {window['last']}）")
    print(f"[out] {csv_path}\n[out] {json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
