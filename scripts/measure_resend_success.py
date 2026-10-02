# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读（这是**唯一会中断服务**的测量脚本）
# -----------------------------------------------------------------------------
# 1) 它做一次故障演练：`docker stop emqx` → 静置 --outage 秒 → `docker start emqx`，
#    然后对账"该补的条数 vs 真的补上的条数"。
#    ⚠️ 演练期间：网关进断网缓存、接入层收不到数据、Web 大屏停更（都是预期现象）。
#    ⚠️ 演练期间**不要**有别的任务在跑（别的测量结果会被污染）。
#    ✅ 脚本用 try/finally 保证：无论中途报错，最后一定把 emqx 起回来并等服务 healthy。
# 2) 数据的唯一来源是**只读**：
#    - 演练前先记库内基线（SELECT MAX(ts)）
#    - 静置结束时把 data/cache.jsonl 与在途文件（inflight-*.jsonl / 旧 .sending）快照到 docs/evidence/drill/ 作证据
#    - 恢复后等网关把队列排空（轮询文件大小归零），再查库对账
#    脚本本身不写库、不改网关代码、不动缓存文件内容。
# 3) 前置条件：六服务在跑；docker CLI 可用；宿主机能读项目 data/ 目录。
# 4) 环境：Windows 上先设 $env:PYTHONIOENCODING="utf-8"。
#
# 对账口径（重点：分母是"该补的条数"）
# -----------------------------------------------------------------------------
#   该补条数 N   = 快照里的**去重报文时间戳**中，晚于"演练前库内 MAX(ts)"的那些
#                  （早于基线的行早就送达了，不属于"该补"）
#   补上条数 M   = 这 N 条里，恢复排空后在库里真的查到的条数
#   补传成功率   = M / N
#   另外单独报：快照里"其实已在库"的条数（paho 自身重投导致的重复投递）、
#              排空耗时、以及残留未补条数（队列没排空说明没补完）。
# =============================================================================
"""补传成功率演练：停 emqx → 恢复 → 按"该补条数"对账。"""

from __future__ import annotations

import argparse
import base64
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
CACHE_FILE = DATA_DIR / "cache.jsonl"
SENDING_FILE = DATA_DIR / "cache.jsonl.sending"

TD_REST_URL = "http://127.0.0.1:6041/rest/sql"
TD_AUTH = "Basic " + base64.b64encode(b"root:taosdata").decode()
TD_DB = "cems"
TD_STABLE = "cems_data"

LOCAL_TZ = timezone(timedelta(hours=8))
TS_FORMAT = "%Y-%m-%d %H:%M:%S"

SERVICES = ["emqx", "tdengine", "cems-device", "cems-gateway", "cems-subscriber", "cems-web"]


def now_local() -> str:
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_local(text: str) -> float:
    return datetime.strptime(text, TS_FORMAT).replace(tzinfo=LOCAL_TZ).timestamp()


def parse_iso_utc(text: str) -> float:
    cleaned = text.strip().replace("Z", "").replace("T", " ")
    if "." in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        ).timestamp()
    return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    ).timestamp()


def td_query(sql: str, timeout: float = 15.0) -> list[list[str]]:
    req = urllib.request.Request(
        TD_REST_URL, data=sql.encode("utf-8"), headers={"Authorization": TD_AUTH}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("code") != 0:
        raise RuntimeError(f"TDengine 返回错误: {payload}")
    return payload.get("data") or []


def td_max_ts() -> float:
    """库内最新时间戳。注意 TDengine 3.3.6 不支持 MAX(ts)（报 Invalid data type: max），
    用 ORDER BY ts DESC LIMIT 1 取。"""
    rows = td_query(f"SELECT ts FROM {TD_DB}.{TD_STABLE} ORDER BY ts DESC LIMIT 1")
    if not rows:
        raise RuntimeError("库内没有任何数据，无法建立演练基线")
    return parse_iso_utc(rows[0][0])


def fetch_ts_between(start: float, end: float) -> set[int]:
    rows = td_query(
        f"SELECT ts FROM {TD_DB}.{TD_STABLE} "
        f"WHERE ts >= {int(start * 1000)} AND ts <= {int(end * 1000)} ORDER BY ts"
    )
    return {int(parse_iso_utc(row[0])) for row in rows}


def in_flight_files() -> list[Path]:
    """在读途文件清单：旧版固定名 `.sending` + 新版在途段 `inflight-*.jsonl`。

    ⚠️ 网关的补传接管已改为**唯一段名**（`inflight-<seq>.jsonl`，永不覆盖，
    见 ADR-0007 / 提交 14b648f）。本脚本原先只读 `.sending`，
    改造后**段文件里的数据不会被统计到** —— 那会让"该补条数/剩余条数"偏小、
    对账恒等式失真。这里把两者都纳入，口径才与网关实际行为一致。

    排序：残留段按 seq 升序在前、`cache.jsonl` 在后（发送顺序）。
    """
    files: list[Path] = []
    if DATA_DIR.is_dir():
        files.extend(sorted(DATA_DIR.glob("inflight-*.jsonl")))
    if SENDING_FILE.exists():
        files.append(SENDING_FILE)
    if CACHE_FILE.exists():
        files.append(CACHE_FILE)
    return files


def read_cache_lines() -> list[str]:
    lines: list[str] = []
    for path in in_flight_files():
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        lines.extend(line.strip() for line in text.splitlines() if line.strip())
    return lines


def cache_bytes() -> int:
    total = 0
    for path in in_flight_files():
        try:
            total += path.stat().st_size
        except OSError:
            pass
    return total


def container_state(name: str) -> tuple[str, str]:
    out = subprocess.run(
        ["docker", "inspect", name, "--format", "{{.State.Status}} {{.State.Health.Status}}"],
        capture_output=True, text=True, timeout=30,
    )
    parts = out.stdout.strip().split()
    return (parts[0] if parts else "?"), (parts[1] if len(parts) > 1 else "-")


def wait_healthy(name: str, timeout: float = 180.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, health = container_state(name)
        if status == "running" and health in ("healthy", "-"):
            return True
        time.sleep(2.0)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="补传成功率演练（会中断 emqx）")
    parser.add_argument("--outage", type=float, default=180.0, help="断开时长（秒）")
    parser.add_argument("--drain-timeout", type=float, default=420.0, help="等待排空的上限（秒）")
    parser.add_argument("--tag", default="resend")
    parser.add_argument(
        "--out",
        default=str(PROJECT_ROOT / "docs" / "evidence" / "drill" / "resend_reconciliation.json"),
    )
    args = parser.parse_args()

    result: dict[str, object] = {"tag": args.tag, "outage_target_s": args.outage, "events": {}}
    emqx_stopped = False

    print("=== 前置检查 ===", flush=True)
    for name in SERVICES:
        print(f"  {name:16s} {container_state(name)}")

    baseline_max = td_max_ts()
    baseline_rows = td_query(f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE}")[0][0]
    result["baseline"] = {
        "at": now_local(),
        "max_ts": datetime.fromtimestamp(baseline_max, LOCAL_TZ).strftime(TS_FORMAT),
        "rows": int(baseline_rows),
        "cache_bytes": cache_bytes(),
        "cache_lines": len(read_cache_lines()),
    }
    print(
        f"  基线: 库内 {baseline_rows} 条，MAX(ts)="
        f"{result['baseline']['max_ts']}，缓存 {result['baseline']['cache_lines']} 行"
    )
    if result["baseline"]["cache_lines"]:
        print("  ⚠️ 演练前缓存非空：前面已积压（会把更早的积压一起算进分母）")

    try:
        print(f"\n=== 断开 MQTT：docker stop emqx（静置 {args.outage:.0f} s）===", flush=True)
        subprocess.run(["docker", "stop", "emqx"], check=True, timeout=120,
                       capture_output=True, text=True)
        emqx_stopped = True
        t_stop = time.time()
        result["events"]["emqx_stopped_at"] = now_local()
        print(f"  emqx 已停于 {result['events']['emqx_stopped_at']}", flush=True)

        # 静置期间每 15 秒看一眼网关是不是在往缓存里写
        while time.time() - t_stop < args.outage:
            time.sleep(min(15.0, max(1.0, args.outage - (time.time() - t_stop))))
            print(
                f"  +{time.time() - t_stop:5.0f}s 缓存 {cache_bytes():6d} B / "
                f"{len(read_cache_lines()):3d} 行",
                flush=True,
            )

        # 快照 = "该补的条数"的唯一证据
        snapshot_lines = read_cache_lines()
        cache_ts: list[int] = []
        malformed = 0
        for line in snapshot_lines:
            parts = line.split()
            if len(parts) < 2:
                malformed += 1
                continue
            try:
                cache_ts.append(int(parse_local(f"{parts[0]} {parts[1]}")))
            except ValueError:
                malformed += 1
        unique_ts = sorted(set(cache_ts))
        result["events"]["snapshot_at"] = now_local()
        result["snapshot"] = {
            "file_lines": len(snapshot_lines),
            "malformed_lines": malformed,
            "unique_ts": len(unique_ts),
            "first_ts": datetime.fromtimestamp(unique_ts[0], LOCAL_TZ).strftime(TS_FORMAT)
            if unique_ts else None,
            "last_ts": datetime.fromtimestamp(unique_ts[-1], LOCAL_TZ).strftime(TS_FORMAT)
            if unique_ts else None,
        }
        print(
            f"  快照（{result['events']['snapshot_at']}）: 文件 {len(snapshot_lines)} 行 / "
            f"去重后 {len(unique_ts)} 条报文"
        )
        snapshot_path = Path(args.out).with_name(f"resend_snapshot_{args.tag}.txt")
        snapshot_path.write_text("\n".join(snapshot_lines) + "\n", encoding="utf-8")
        result["snapshot"]["evidence_file"] = str(snapshot_path)

        # 该补条数：只算晚于基线的（早于基线的行演练前就已送达）
        should_resend = [t for t in unique_ts if t > baseline_max]
        already_in_db_before = [t for t in unique_ts if t <= baseline_max]
        result["reconciliation"] = {
            "should_resend": len(should_resend),
            "snapshot_rows_at_or_before_baseline": len(already_in_db_before),
        }
        print(
            f"  该补条数 N = {len(should_resend)}"
            f"（快照里 ≤ 基线的 {len(already_in_db_before)} 条不计入）"
        )

        print("\n=== 恢复 MQTT：docker start emqx ===", flush=True)
        subprocess.run(["docker", "start", "emqx"], check=True, timeout=120,
                       capture_output=True, text=True)
        emqx_stopped = False
        result["events"]["emqx_started_at"] = now_local()
        healthy = wait_healthy("emqx", 180.0)
        result["events"]["emqx_healthy_at"] = now_local()
        print(f"  emqx healthy={healthy} @ {result['events']['emqx_healthy_at']}", flush=True)

        # ---- 恢复确认（lead 要求）：六服务都 healthy + 库内条数确实在增长 ----
        print("\n--- 恢复确认：六服务状态 ---", flush=True)
        states_after = {}
        for name in SERVICES:
            states_after[name] = container_state(name)
            print(f"  {name:16s} {states_after[name]}", flush=True)
        result["states_after_recovery"] = {k: list(v) for k, v in states_after.items()}

        print("--- 恢复确认：连续两次 COUNT 必须递增 ---", flush=True)
        counts: list[int] = []
        deadline = time.time() + 150.0
        while time.time() < deadline and len(counts) < 2:
            sample = int(td_query(f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE}")[0][0])
            label = now_local()
            if not counts or sample > counts[-1]:
                counts.append(sample)
                print(f"  第 {len(counts)} 次 COUNT(*) = {sample} @ {label}", flush=True)
            if len(counts) < 2:
                time.sleep(12.0)
        result["recovery_count_check"] = {
            "counts": counts,
            "increasing": len(counts) >= 2 and counts[-1] > counts[0],
            "subscriber_state_at_check": container_state("cems-subscriber"),
        }
        print(
            f"  数据在流: {result['recovery_count_check']['increasing']}"
            f"（等待上限 150 s；恢复瞬间接入层刚重连，COUNT 会先平后涨）",
            flush=True,
        )

        # 等网关把队列排空
        drain_start = time.time()
        drained_at = None
        while time.time() - drain_start < args.drain_timeout:
            if cache_bytes() == 0:
                drained_at = time.time()
                break
            time.sleep(3.0)
        result["events"]["drained_at"] = (
            datetime.fromtimestamp(drained_at, LOCAL_TZ).strftime(TS_FORMAT) if drained_at else None
        )
        result["events"]["drain_seconds"] = (
            round(drained_at - parse_local(result["events"]["emqx_started_at"]), 1)
            if drained_at else None
        )
        result["reconciliation"]["drain_ok"] = drained_at is not None
        result["reconciliation"]["leftover_lines"] = len(read_cache_lines())
        print(
            f"  排空{'完成' if drained_at else '超时'}，耗时 "
            f"{result['reconciliation'].get('drain_seconds', result['events']['drain_seconds'])} s，"
            f"残留 {result['reconciliation']['leftover_lines']} 行"
        )

        # 多查一次，等一下最后几条的入库落定
        time.sleep(5.0)
        after_epoch = parse_local(now_local())
        if should_resend:
            window_start = min(should_resend) - 1
            window_end = after_epoch + 1
            db_ts = fetch_ts_between(window_start, window_end)
        else:
            window_start, window_end, db_ts = 0.0, 0.0, set()
        snapshot_set = set(unique_ts)
        delivered = [t for t in should_resend if t in db_ts]
        missing = [t for t in should_resend if t not in db_ts]
        # 恢复之后新产生、并且已经直发入库的行（不属于"该补"，但对账恒等式要用到）
        live_rows = sorted(t for t in db_ts if t not in snapshot_set)
        result["reconciliation"]["live_rows_after_recovery"] = [
            datetime.fromtimestamp(t, LOCAL_TZ).strftime(TS_FORMAT) for t in live_rows
        ]

        result["reconciliation"].update(
            {
                "delivered": len(delivered),
                "missing": len(missing),
                "success_rate": (len(delivered) / len(should_resend)) if should_resend else None,
                "missing_ts": [datetime.fromtimestamp(t, LOCAL_TZ).strftime(TS_FORMAT) for t in missing],
                "live_rows_after_recovery_count": len(live_rows),
            }
        )
        result["after"] = {
            "at": now_local(),
            "rows": int(td_query(f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE}")[0][0]),
            "cache_lines": len(read_cache_lines()),
        }

        # ---- 完整对账台账（lead 要求：把"该补条数"怎么算的全程贴出来）----
        outage_seconds = (
            parse_local(result["events"]["emqx_started_at"])
            - parse_local(result["events"]["emqx_stopped_at"])
        )
        db_growth = int(result["after"]["rows"]) - int(result["baseline"]["rows"])
        ledger = {
            "断开前库内基线(条)": int(result["baseline"]["rows"]),
            "断开前库内 MAX(ts)": result["baseline"]["max_ts"],
            "断开时长(s)": round(outage_seconds, 1),
            "断开期间理论产生(条, 时长/5.0)": round(outage_seconds / 5.0, 1),
            "断开期间实际产生=该补条数 N(条)": len(should_resend),
            "恢复后队列剩余(行)": result["reconciliation"]["leftover_lines"],
            "恢复后补上 M(条)": len(delivered),
            "快照之后新产生并最终入库(条)": len(live_rows),
            "库内新增(条)": db_growth,
            "恒等式 基线+N+新产生-剩余=新增": int(result["baseline"]["rows"]) + len(should_resend)
            + len(live_rows) - int(result["reconciliation"]["leftover_lines"]) == int(result["after"]["rows"]),
            "补传成功率 M/N": result["reconciliation"]["success_rate"],
        }
        result["ledger"] = ledger
        print("\n===== 对账台账 =====")
        for key, value in ledger.items():
            print(f"  {key:34s}: {value}")
        print("\n===== 补传对账 =====")
        print(f"  该补条数 N            : {len(should_resend)}")
        print(f"  恢复后补上 M          : {len(delivered)}")
        print(f"  补传成功率 M/N        : "
              f"{result['reconciliation']['success_rate'] * 100:.2f}%"
              if should_resend else "  补传成功率           : N=0（没有产生积压，演练无效）")
        print(f"  未补上的条数          : {len(missing)}")
        print(f"  快照之后新产生并入库  : {len(live_rows)} 条"
              f"（网关还没察觉恢复时产生的行，和积压一起在重连后同一批补出去）")
        print(f"  库内计数 {result['baseline']['rows']} → {result['after']['rows']}"
              f"（增加 {int(result['after']['rows']) - int(result['baseline']['rows'])}）")
        if missing:
            print(f"  ⚠️ 未补时间戳: {result['reconciliation']['missing_ts'][:20]}")
    finally:
        if emqx_stopped:
            print("\n[finally] 演练异常中断，正在把 emqx 起回来…", flush=True)
            subprocess.run(["docker", "start", "emqx"], capture_output=True, text=True, timeout=120)
            wait_healthy("emqx", 180.0)
        print("[finally] 结束前确认六服务状态：", flush=True)
        states = {}
        for name in SERVICES:
            states[name] = container_state(name)
            print(f"  {name:16s} {states[name]}", flush=True)
        result["final_states"] = {k: list(v) for k, v in states.items()}

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[out] {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
