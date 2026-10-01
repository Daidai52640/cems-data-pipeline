# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读
# -----------------------------------------------------------------------------
# 1) 本脚本**只读**：只做 docker exec date / TDengine SELECT NOW() / NTP 查询，
#    不写库、不发 MQTT、不动缓存文件。可反复执行。
# 2) 前置条件：docker CLI 可用、六服务在跑、能访问 UDP/123（外网 NTP）。
#    若 NTP 不通，C 部分会打印"不可用"并继续（A/B 部分仍有效）。
# 3) 环境：Windows 上先设 $env:PYTHONIOENCODING="utf-8"。
# 4) 输出：--out 指定的 JSON（覆盖同名文件）+ 屏幕表格。
#
# 为什么必须先跑这个
# -----------------------------------------------------------------------------
#   端到端延迟 = 库内可见时刻(宿主机时钟) - 报文时间戳(网关容器时钟)。这是**两个
#   时钟相减**。若两个时钟存在偏移 θ(t)，测出来的"延迟"里就混着 θ(t)。
#   本脚本给出：容器之间是否同一时钟、容器与宿主机的偏移随时间怎么变、谁相对真值在跑偏。
# =============================================================================
"""跨容器/宿主机时钟一致性测量：三部分（容器间一致性、宿主机偏移时间序列、NTP 外部基准）。"""

from __future__ import annotations

import argparse
import base64
import json
import socket
import statistics
import struct
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

TD_REST_URL = "http://127.0.0.1:6041/rest/sql"
TD_AUTH = "Basic " + base64.b64encode(b"root:taosdata").decode()

CONTAINERS = ["tdengine", "cems-gateway", "cems-device", "cems-subscriber", "emqx"]

NTP_EPOCH_DELTA = 2208988800
NTP_SERVERS = ["ntp.aliyun.com", "cn.pool.ntp.org", "time.cloudflare.com"]


# ---------------- 基础工具 ----------------

def parse_iso_utc(text: str) -> float:
    """把 REST 返回的 ISO8601 UTC 串转成 epoch 秒。"""
    cleaned = text.strip().replace("Z", "").replace("T", " ")
    if "." in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        ).timestamp()
    return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    ).timestamp()


def td_now() -> tuple[float, float, float]:
    """TDengine 服务端时钟：返回 (server_epoch, t_before, t_after)。RTT 只有几毫秒。"""
    req = urllib.request.Request(
        TD_REST_URL, data=b"SELECT NOW()", headers={"Authorization": TD_AUTH}
    )
    t_before = time.time()
    with urllib.request.urlopen(req, timeout=5) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    t_after = time.time()
    return parse_iso_utc(payload["data"][0][0]), t_before, t_after


def exec_date(name: str) -> tuple[float, float, float]:
    """容器墙钟：返回 (container_epoch, 宿主侧 t_before, 宿主侧 t_after)。

    宿主时间会取在 exec 前后各一次，所以 (t_before, t_after) 是容器读数真实
    发生时刻的区间；用中点抵消大部分 exec 延迟，不消的部分对同一台机器是常数。
    """
    t_before = time.time()
    out = subprocess.run(
        ["docker", "exec", name, "date", "+%s.%N"],
        capture_output=True, text=True, timeout=20, check=True,
    )
    t_after = time.time()
    return float(out.stdout.strip()), t_before, t_after


def ntp_offset(server: str, timeout: float = 3.0) -> tuple[float, float] | None:
    """标准 NTP 四时戳法测"服务器时钟 - 本机时钟"；返回 (offset 秒, rtt 秒)。"""
    packet = b"\x1b" + 47 * b"\0"
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        t1 = time.time()
        sock.sendto(packet, (server, 123))
        data, _ = sock.recvfrom(512)
        t4 = time.time()
    except OSError:
        return None
    finally:
        sock.close()
    if len(data) < 48:
        return None
    t2 = struct.unpack("!I", data[32:36])[0] + struct.unpack("!I", data[36:40])[0] / 2**32
    t3 = struct.unpack("!I", data[40:44])[0] + struct.unpack("!I", data[44:48])[0] / 2**32
    t2 -= NTP_EPOCH_DELTA
    t3 -= NTP_EPOCH_DELTA
    return ((t2 - t1) + (t3 - t4)) / 2.0, (t4 - t1)


def slope_ms_per_s(samples: list[tuple[float, float]]) -> float:
    """最小二乘拟合 (t_host[秒], offset[毫秒]) 的斜率，单位 ms/s。"""
    if len(samples) < 3:
        return float("nan")
    xs = [s[0] for s in samples]
    ys = [s[1] for s in samples]
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return float("nan")
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom


# ---------------- A. 容器之间是否同一时钟 ----------------

def part_a(rounds: int, gap_s: float) -> dict:
    """每个样本都自带宿主侧时间括号，所以容器间差异不受"顺序调用"影响。

    多轮之间隔 gap_s 秒，用来观察各容器是不是**一起**在漂（同一 VM 内核 → 共享
    同一个 CLOCK_REALTIME，应该一起漂）。
    """
    per_container: dict[str, list[float]] = {name: [] for name in CONTAINERS}
    table: list[dict[str, float]] = []
    for _ in range(rounds):
        row: dict[str, float] = {"t_host": time.time()}
        for name in CONTAINERS:
            try:
                value, t_before, t_after = exec_date(name)
            except Exception as exc:
                print(f"[A] {name} exec 失败: {exc}")
                continue
            offset_s = value - (t_before + t_after) / 2.0   # 容器时钟 - 宿主机时钟
            per_container[name].append(offset_s)
            row[name] = offset_s * 1000.0
        table.append(row)
        time.sleep(gap_s)

    summary = {}
    for name, values in per_container.items():
        if not values:
            continue
        summary[name] = {
            "n": len(values),
            "min_ms": min(values) * 1000.0,
            "p50_ms": statistics.median(values) * 1000.0,
            "max_ms": max(values) * 1000.0,
        }
    # 两两差：同一轮的同一时刻相比，去掉"顺序调用"带来的时间差
    pairwise = {}
    names = [n for n in CONTAINERS if per_container.get(n)]
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            diffs = [
                (per_container[a][k] - per_container[b][k]) * 1000.0
                for k in range(min(len(per_container[a]), len(per_container[b])))
            ]
            pairwise[f"{a} - {b}"] = {
                "p50_ms": statistics.median(diffs),
                "span_ms": max(diffs) - min(diffs),
            }
    return {
        "rounds": rounds,
        "gap_s": gap_s,
        "per_container_offset_ms": summary,
        "pairwise_diff_ms": pairwise,
        "series": table,
    }


# ---------------- B. 宿主机 vs 容器时钟的时间序列 ----------------

def part_b(seconds: float, interval: float) -> dict:
    series = []
    end = time.time() + seconds
    while time.time() < end:
        server, t_before, t_after = td_now()
        host_mid = (t_before + t_after) / 2.0
        series.append(
            {
                "t_host": host_mid,
                "offset_ms": (server - host_mid) * 1000.0,
                "rtt_ms": (t_after - t_before) * 1000.0,
            }
        )
        time.sleep(interval)

    offsets = [s["offset_ms"] for s in series]
    steps = [
        (series[i]["offset_ms"] - series[i - 1]["offset_ms"], series[i]["t_host"])
        for i in range(1, len(series))
    ]
    big_steps = [s for s in steps if abs(s[0]) > 100.0]
    # 只对"没有大跳"的连续段分别拟合斜率
    segments: list[list[tuple[float, float]]] = [[]]
    for i, item in enumerate(series):
        if i > 0 and abs(offsets[i] - offsets[i - 1]) > 100.0:
            segments.append([])
        segments[-1].append((item["t_host"], item["offset_ms"]))
    rates = [slope_ms_per_s(seg) for seg in segments if len(seg) >= 3]

    return {
        "seconds": seconds,
        "interval_s": interval,
        "n": len(series),
        "rtt_ms": {
            "p50": statistics.median([s["rtt_ms"] for s in series]),
            "max": max(s["rtt_ms"] for s in series),
        },
        "offset_ms": {
            "min": min(offsets),
            "max": max(offsets),
            "p50": statistics.median(offsets),
            "span": max(offsets) - min(offsets),
        },
        "steps_gt_100ms": [{"delta_ms": d, "at": t} for d, t in big_steps],
        "segment_rates_ms_per_s": rates,
        "series": series,
    }


# ---------------- C. 外部真值：谁相对真值在跑偏 ----------------

def part_c(seconds: float, interval: float) -> dict:
    rounds = []
    end = time.time() + seconds
    while time.time() < end:
        t_round = time.time()
        offsets = []
        for server in NTP_SERVERS:
            result = ntp_offset(server)
            if result is not None:
                offsets.append(result[0] * 1000.0)
        # 同时取一次容器时钟，便于把 B/C 两段拼起来
        server, t_before, t_after = td_now()
        host_mid = (t_before + t_after) / 2.0
        rounds.append(
            {
                "t_host": t_round,
                "host_vs_true_ms": statistics.median(offsets) if offsets else None,
                "ntp_servers_ok": len(offsets),
                "container_vs_host_ms": (server - host_mid) * 1000.0,
            }
        )
        time.sleep(interval)

    usable = [r for r in rounds if r["host_vs_true_ms"] is not None]
    if not usable:
        return {"available": False, "rounds": rounds}
    true_offsets = [r["host_vs_true_ms"] for r in usable]
    vm_offsets = [r["host_vs_true_ms"] + r["container_vs_host_ms"] for r in usable]
    return {
        "available": True,
        "n": len(usable),
        "host_vs_true_ms": {
            "min": min(true_offsets),
            "max": max(true_offsets),
            "p50": statistics.median(true_offsets),
            "span": max(true_offsets) - min(true_offsets),
        },
        "container_vs_true_ms": {
            "min": min(vm_offsets),
            "max": max(vm_offsets),
            "p50": statistics.median(vm_offsets),
            "span": max(vm_offsets) - min(vm_offsets),
        },
        "rounds": rounds,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="跨容器时钟一致性测量（只读）")
    parser.add_argument("--rounds", type=int, default=12, help="A 部分采样轮数")
    parser.add_argument("--round-gap", type=float, default=3.0, help="A 部分轮间隔（秒）")
    parser.add_argument("--seconds", type=float, default=60.0, help="B/C 部分持续秒数")
    parser.add_argument("--interval", type=float, default=1.0, help="B/C 部分采样间隔")
    parser.add_argument(
        "--out",
        default=str(PROJECT_ROOT / "docs" / "measurements" / "clock_consistency.json"),
    )
    args = parser.parse_args()

    print("=== A. 容器之间是否同一时钟（exec date，每样本自带宿主括号）===", flush=True)
    a = part_a(args.rounds, args.round_gap)
    for name, stats in a["per_container_offset_ms"].items():
        print(
            f"  {name:16s} 容器-宿主机 [{stats['min_ms']:+8.1f}, {stats['max_ms']:+8.1f}] ms"
            f"  P50={stats['p50_ms']:+8.1f} ms"
        )
    print("  两两差（同一轮同时刻相比；应远小于 1 秒）：")
    for pair, stats in a["pairwise_diff_ms"].items():
        print(
            f"    {pair:34s} P50={stats['p50_ms']:+6.1f} ms  波动 {stats['span_ms']:5.1f} ms"
        )

    print(f"\n=== B. 容器时钟 vs 宿主机时钟：{args.seconds:.0f} 秒时间序列 ===", flush=True)
    b = part_b(args.seconds, args.interval)
    print(
        f"  REST 往返 P50={b['rtt_ms']['p50']:.1f} ms（→ 偏移测量不确定度约 ±"
        f"{b['rtt_ms']['p50'] / 2:.1f} ms）"
    )
    print(
        f"  偏移范围 [{b['offset_ms']['min']:+.1f}, {b['offset_ms']['max']:+.1f}] ms，"
        f"极差 {b['offset_ms']['span']:.1f} ms"
    )
    for rate in b["segment_rates_ms_per_s"]:
        print(f"  连续段斜率 {rate:+.1f} ms/s（≈ {rate * 1000:.0f} ppm）")
    for step in b["steps_gt_100ms"]:
        print(f"  跳变 {step['delta_ms']:+.1f} ms")

    print(f"\n=== C. 外部基准（NTP，{len(NTP_SERVERS)} 台）===", flush=True)
    c = part_c(args.seconds, args.interval)
    if not c.get("available"):
        print("  外网 NTP 不可达，本部分跳过（不影响 A/B 结论）")
    else:
        h = c["host_vs_true_ms"]
        v = c["container_vs_true_ms"]
        print(
            f"  宿主机时钟-真值: [{h['min']:+.0f}, {h['max']:+.0f}] ms 极差 {h['span']:.0f} ms"
        )
        print(
            f"  容器时钟-真值  : [{v['min']:+.0f}, {v['max']:+.0f}] ms 极差 {v['span']:.0f} ms"
        )
        print("  （正=本地时钟落后真值；NTP 往返会让单次结果抖动几十毫秒）")

    result = {
        "generated_at_host": time.strftime("%Y-%m-%d %H:%M:%S"),
        "A_container_agreement": a,
        "B_container_vs_host_series": b,
        "C_external_ntp": c,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[out] {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
