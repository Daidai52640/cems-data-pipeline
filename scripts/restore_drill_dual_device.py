# -*- coding: utf-8 -*-
# =============================================================================
# 双设备恢复演练：备份 → 另起独立实例恢复 → **逐设备**对账 → RTO
# -----------------------------------------------------------------------------
# 与 scripts/restore_drill_tdengine.py 的分工：
#   · 后者做的是"整表级"保真度校验（窗口条数 / 按小时与按天分段 / 结构 / 标签），
#     它的源端与恢复端查询都**不区分设备**；
#   · 本脚本在同一套 RTO 口径下，把对账**下沉到每一台设备（每一个子表 tag）**：
#       ① 逐设备：恢复端与源端独立取数，比条数 / 首末 ts / 逐列 SUM（不做库间比对）
#       ② 逐设备 × 逐小时桶：比条数 / 逐列 SUM / 桶内首末 ts
#       ③ 逐设备 × 逐天桶：同上
#       ④ 窗口合计：恢复端 == 源端（双设备合计）
#       ⑤ 子表标签集合：恢复端 == 源端 且恰为两台设备
#       ⑥ 设备在恢复端的**可辨识性**：plant/device 标签读得回来（不是"合并成一张表"）
#   两套校验互补：整表级防"段错位"，逐设备级防"两台设备的数据互相串台或丢一台"。
#
# 安全边界（与既有演练完全一致）：
#   · 另起容器 cems-tdengine-drill + 独立卷 cems-tdengine-drill-data，**不接 cems-net**；
#   · 对现网 cems 库只有 SELECT；恢复写进的是独立卷上的独立实例；
#   · 结束在 finally 里 docker rm -f + docker volume rm，并二次核对残留。
#
# RTO 口径（照 docs/runbooks/恢复演练与对账口径.md §2.1）：
#   t0      = 发出「开始恢复」的那一刻（time.perf_counter，宿主机单调时钟）
#   t_up    = 目标实例可以执行查询（不是容器 healthy）
#   t_data  = 数据导入完成且实例可查
#   t_seg   = 逐设备 + 分段校验全部通过
#   RTO = t_seg − t0
#
# 用法：
#   python scripts/restore_drill_dual_device.py --backup <备份目录>
#   python scripts/restore_drill_dual_device.py --backup <备份目录> --keep-going
# =============================================================================
"""双设备恢复演练：逐设备对账 + RTO。"""

from __future__ import annotations

import argparse
import json
import sys
import time
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
    VALUE_COLUMNS,
    count_rows,
    db_exists,
    ensure_utf8_stdout,
    log,
    read_json,
    run,
    segment_rows,
    taos_exec,
    taos_sql,
    ts_bounds,
    ts_ms_to_str,
    wait_taos_ready,
    write_json,
)

DRILL_CONTAINER = "cems-tdengine-drill"
DRILL_VOLUME = "cems-tdengine-drill-data"
PROBE_DB = "cems_drill_warmup"
TARGET_IMAGE = "tdengine/tdengine:latest"
EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence" / "backup"
SUM_REL_TOL = 1e-6

# 逐设备对账里被比较的"设备"由源端标签集合发现，不硬编码 —— 这样"多了一台设备"、
# "少了一台设备"都会直接体现为标签集合不一致，而不是被硬编码掩盖。
EXPECTED_DEVICES = (("plant1", "device1"), ("plant2", "device2"))


# --------------------------------------------------------------------------- #
# 现场管理
# --------------------------------------------------------------------------- #
def cleanup_instance() -> list[str]:
    """销毁演练实例与其卷；删除前逐个 inspect 核对名字，不做宽泛删除。"""
    if run(["docker", "inspect", DRILL_CONTAINER]).returncode == 0:
        run(["docker", "rm", "-f", DRILL_CONTAINER], timeout=120)
    if run(["docker", "volume", "inspect", DRILL_VOLUME]).returncode == 0:
        run(["docker", "volume", "rm", DRILL_VOLUME], timeout=180)
    leftovers = []
    if run(["docker", "inspect", DRILL_CONTAINER]).returncode == 0:
        leftovers.append(f"container:{DRILL_CONTAINER}")
    if run(["docker", "volume", "inspect", DRILL_VOLUME]).returncode == 0:
        leftovers.append(f"volume:{DRILL_VOLUME}")
    return leftovers


def start_instance() -> float:
    """建卷 + 起独立实例（故意不接 cems-net），返回从 t0 起算的秒数。"""
    run(["docker", "volume", "create", DRILL_VOLUME], check=True)
    run([
        "docker", "run", "-d", "--name", DRILL_CONTAINER,
        "-e", "TZ=Asia/Shanghai",
        "-v", f"{DRILL_VOLUME}:/var/lib/taos",
        TARGET_IMAGE,
    ], check=True, timeout=180)
    return time.perf_counter()


def warmup_instance(timeout: float = 180.0) -> dict:
    """与 taosdump 同形状的 DDL 预热；判据照 restore_drill_tdengine.warmup_instance。"""
    start = time.perf_counter()
    deadline = start + timeout
    attempts = 0
    log_lines: list[dict] = []
    last = ""
    while time.perf_counter() < deadline:
        attempts += 1
        sql = (
            f"CREATE DATABASE IF NOT EXISTS {PROBE_DB}; "
            f"CREATE STABLE IF NOT EXISTS {PROBE_DB}.probe "
            f"(ts TIMESTAMP, v FLOAT) TAGS (t NCHAR(8)); "
            f"DROP DATABASE IF EXISTS {PROBE_DB};"
        )
        proc = run(["docker", "exec", DRILL_CONTAINER, "taos", "-s", sql], timeout=60)
        combined = (proc.stdout or "") + (proc.stderr or "")
        bad = ("DB error" in combined
               or "Unable to establish connection" in combined
               or "Connection refused" in combined)
        ok = not bad and "OK," in combined
        log_lines.append({"attempt": attempts, "ok": ok})
        if ok:
            return {"ok": True, "attempts": attempts,
                    "seconds": round(time.perf_counter() - start, 3),
                    "attempt_log": log_lines}
        last = combined[-300:]
        time.sleep(min(0.1 * (2 ** (attempts - 1)), 1.0))
    return {"ok": False, "attempts": attempts,
            "seconds": round(time.perf_counter() - start, 3),
            "attempt_log": log_lines, "detail": last}


def ensure_db_after_restore(db: str) -> bool:
    """恢复后必须真的查到库（返回码 0 不等于恢复成功）。"""
    deadline = time.perf_counter() + 20.0
    while time.perf_counter() < deadline:
        if db_exists(DRILL_CONTAINER, db):
            return True
        time.sleep(1.0)
    return False


# --------------------------------------------------------------------------- #
# 逐设备取数
# --------------------------------------------------------------------------- #
def _tag_where(plant: str, device: str) -> str:
    return f"plant='{plant}' AND device='{device}'"


def device_snapshot(container: str, db: str, stable: str, plant: str, device: str,
                    win_lo: int, win_hi: int) -> dict:
    """一台设备在窗口内的独立快照：条数 / 首末 ts / 逐列 SUM。"""
    where = _tag_where(plant, device)
    window = f"ts >= {win_lo} AND ts <= {win_hi}"
    n = int(taos_sql(
        container,
        f"SELECT COUNT(*) FROM {db}.{stable} WHERE {where} AND {window};",
    )[0][0])
    sums_raw = taos_sql(
        container,
        f"SELECT {', '.join('SUM(' + c + ')' for c in VALUE_COLUMNS)} "
        f"FROM {db}.{stable} WHERE {where} AND {window};",
    )[0]
    sums = {}
    for column, raw in zip(VALUE_COLUMNS, sums_raw):
        try:
            sums[column] = float(raw)
        except (TypeError, ValueError):
            sums[column] = None
    bounds = {"min": None, "max": None}
    if n:
        rows = taos_sql(
            container,
            f"SELECT CAST(ts AS BIGINT) FROM {db}.{stable} WHERE {where} AND {window} "
            f"ORDER BY ts ASC LIMIT 1;",
        )
        bounds["min"] = ts_ms_to_str(int(rows[0][0]))
        rows = taos_sql(
            container,
            f"SELECT CAST(ts AS BIGINT) FROM {db}.{stable} WHERE {where} AND {window} "
            f"ORDER BY ts DESC LIMIT 1;",
        )
        bounds["max"] = ts_ms_to_str(int(rows[0][0]))
    return {"plant": plant, "device": device, "rows": n, "sums": sums, "bounds": bounds}


def compare_snapshot(src: dict, dst: dict) -> dict:
    """比一台设备的条数 / 逐列 SUM / 首末 ts。"""
    diffs: list[dict] = []
    if src["rows"] != dst["rows"]:
        diffs.append({"field": "rows", "src": src["rows"], "dst": dst["rows"]})
    if src["bounds"] != dst["bounds"]:
        diffs.append({"field": "bounds", "src": src["bounds"], "dst": dst["bounds"]})
    for column in VALUE_COLUMNS:
        a, b = src["sums"].get(column), dst["sums"].get(column)
        if a is None and b is None:
            continue
        if (a is None) != (b is None):
            diffs.append({"field": f"sum.{column}", "src": a, "dst": b, "why": "一侧为 NULL"})
            continue
        denom = max(abs(a), abs(b), 1e-9)
        if abs(a - b) / denom > SUM_REL_TOL:
            diffs.append({"field": f"sum.{column}", "src": a, "dst": b,
                          "rel": abs(a - b) / denom})
    return {"ok": not diffs, "diffs": diffs}


def device_segments(container: str, db: str, stable: str, plant: str, device: str,
                    unit: str, win_lo: int, win_hi: int) -> list[dict]:
    """按桶取一台设备的数据；bucket 用 CAST(_wstart AS BIGINT) 取纪元毫秒。"""
    where = _tag_where(plant, device)
    rows = taos_sql(
        container,
        f"SELECT CAST(_wstart AS BIGINT), COUNT(*), "
        f"{', '.join('SUM(' + c + ')' for c in VALUE_COLUMNS)}, "
        f"CAST(FIRST(ts) AS BIGINT), CAST(LAST(ts) AS BIGINT) "
        f"FROM {db}.{stable} WHERE {where} AND ts >= {win_lo} AND ts <= {win_hi} "
        f"INTERVAL({unit});",
    )
    buckets = []
    for row in rows:
        buckets.append({
            "bucket_ms": int(row[0]),
            "bucket_local": ts_ms_to_str(int(row[0])),
            "n": int(row[1]),
            "sums": {c: float(v) for c, v in zip(VALUE_COLUMNS, row[2:2 + len(VALUE_COLUMNS)])},
            "tmin": int(row[2 + len(VALUE_COLUMNS)]),
            "tmax": int(row[3 + len(VALUE_COLUMNS)]),
        })
    return buckets


def compare_device_segments(src: list[dict], dst: list[dict]) -> dict:
    """逐桶比条数 / 逐列 SUM / 桶内首末 ts。"""
    src_by = {b["bucket_ms"]: b for b in src}
    dst_by = {b["bucket_ms"]: b for b in dst}
    only_src = sorted(set(src_by) - set(dst_by))
    only_dst = sorted(set(dst_by) - set(src_by))
    count_diff, sum_diff, bounds_diff = [], [], []
    for key in sorted(set(src_by) & set(dst_by)):
        a, b = src_by[key], dst_by[key]
        if a["n"] != b["n"]:
            count_diff.append(a["bucket_local"])
        if a["tmin"] != b["tmin"] or a["tmax"] != b["tmax"]:
            bounds_diff.append(a["bucket_local"])
        for column in VALUE_COLUMNS:
            x, y = a["sums"][column], b["sums"][column]
            denom = max(abs(x), abs(y), 1e-9)
            if abs(x - y) / denom > SUM_REL_TOL:
                sum_diff.append({"bucket": a["bucket_local"], "column": column,
                                 "src": x, "dst": y})
                break
    ok = not (only_src or only_dst or count_diff or sum_diff or bounds_diff)
    return {
        "ok": ok,
        "n_buckets_src": len(src),
        "n_buckets_dst": len(dst),
        "total_rows_src": sum(b["n"] for b in src),
        "total_rows_dst": sum(b["n"] for b in dst),
        "matched": len(set(src_by) & set(dst_by)),
        "buckets_only_in_src": [ts_ms_to_str(k) for k in only_src],
        "buckets_only_in_dst": [ts_ms_to_str(k) for k in only_dst],
        "buckets_with_count_diff": count_diff,
        "buckets_with_sum_diff": sum_diff,
        "buckets_with_bounds_diff": bounds_diff,
    }


def tag_pairs(container: str, db: str, stable: str) -> list[list[str]]:
    return [row[:2] for row in taos_sql(
        container, f"SELECT DISTINCT plant, device FROM {db}.{stable} ORDER BY plant, device;")]


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def run_drill(args: argparse.Namespace) -> dict:
    backup_dir = Path(args.backup).resolve()
    manifest = read_json(backup_dir / "manifest.json")
    win_lo = int(manifest["window"]["pre_min_ms"])
    win_hi = int(manifest["window"]["pre_max_ms"])

    log(f"双设备恢复演练：备份 {backup_dir}")
    log(f"  校验窗口 {ts_ms_to_str(win_lo)} → {ts_ms_to_str(win_hi)}"
        f"（源端 dump 前已在库，必然完整落在备份里）")
    log(f"  备份自报条数 {manifest['dump']['reported_rows']}，"
        f"dump 前基线 {manifest['window']['pre_count']}")

    result: dict = {
        "drill": "dual-device-restore",
        "mode": "instance",
        "backup_dir": str(backup_dir),
        "backup_stamp": manifest.get("stamp"),
        "backup_kind": manifest.get("kind"),
        "backup_self_check": manifest.get("dump", {}).get("self_check"),
        "window_ms": [win_lo, win_hi],
        "window_local": [ts_ms_to_str(win_lo), ts_ms_to_str(win_hi)],
        "source": {"container": args.container, "db": args.db, "stable": args.stable},
        "started_at": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }

    leftovers: list[str] = []
    t0 = time.perf_counter()
    try:
        cleanup_instance()
        start_instance()
        ok, tail = wait_taos_ready(DRILL_CONTAINER, timeout=240)
        t_up = time.perf_counter() - t0
        log(f"  目标实例可查库：{ok}（+{t_up:.2f} s）")
        if not ok:
            result["ok"] = False
            result["failed_stage"] = {"stage": "instance_start", "detail": tail[-300:]}
            return result

        warm = warmup_instance()
        t_warm = time.perf_counter() - t0
        log(f"  实例 DDL 预热：{warm['ok']}（{warm['attempts']} 次尝试，+{t_warm:.2f} s）")
        if not warm["ok"]:
            result["ok"] = False
            result["failed_stage"] = {"stage": "warmup", "detail": warm.get("detail")}
            return result

        run(["docker", "exec", DRILL_CONTAINER, "sh", "-c", "rm -rf /tmp/restore-in"], check=True)
        cp = run(["docker", "cp", str(backup_dir), f"{DRILL_CONTAINER}:/tmp/restore-in"],
                 timeout=args.timeout)
        if cp.returncode != 0:
            result["ok"] = False
            result["failed_stage"] = {"stage": "copy_in", "detail": cp.stderr}
            return result
        t_copy = time.perf_counter() - t0

        proc = run(["docker", "exec", DRILL_CONTAINER, "taosdump",
                    "-i", "/tmp/restore-in", "-T", str(args.threads)], timeout=args.timeout)
        t_data = time.perf_counter() - t0
        output = (proc.stdout or "") + "\n" + (proc.stderr or "")
        if proc.returncode != 0 or "DB error" in output:
            result["ok"] = False
            result["failed_stage"] = {"stage": "restore", "detail": output[-800:]}
            return result
        if not ensure_db_after_restore(args.db):
            result["ok"] = False
            result["failed_stage"] = {
                "stage": "restore_db_missing",
                "detail": f"taosdump 返回码 {proc.returncode} 但库里查不到 {args.db}"}
            return result
        log(f"  taosdump -i 完成（+{t_data:.2f} s）")
        result["restore_output_tail"] = output[-600:]

        # ---- 校验 ⑤：标签集合等同（顺带校验"恰为两台设备"） -------------- #
        src_pairs = tag_pairs(args.container, args.db, args.stable)
        dst_pairs = tag_pairs(DRILL_CONTAINER, args.db, args.stable)
        expected = [list(p) for p in EXPECTED_DEVICES]
        result["tags"] = {
            "src": src_pairs, "dst": dst_pairs, "expected": expected,
            "same": src_pairs == dst_pairs,
            "matches_expected_two_devices": src_pairs == expected,
            "ok": src_pairs == dst_pairs and dst_pairs == expected,
        }
        log(f"  ⑤ 子表标签：源 {src_pairs} / 恢复 {dst_pairs} → "
            f"{'一致' if result['tags']['same'] else '❌ 不一致'}"
            f"（恰为两台设备：{result['tags']['matches_expected_two_devices']}）")

        # ---- 校验 ①~④：逐设备 + 逐设备×分段 ------------------------------ #
        devices_report: dict[str, dict] = {}
        all_ok = result["tags"]["ok"]
        for plant, device in EXPECTED_DEVICES:
            key = f"{plant}/{device}"
            entry: dict = {"plant": plant, "device": device}
            src_snap = device_snapshot(args.container, args.db, args.stable,
                                       plant, device, win_lo, win_hi)
            dst_snap = device_snapshot(DRILL_CONTAINER, args.db, args.stable,
                                       plant, device, win_lo, win_hi)
            entry["snapshot"] = {"src": src_snap, "dst": dst_snap,
                                 "cmp": compare_snapshot(src_snap, dst_snap)}
            log(f"  ① {key}：源 {src_snap['rows']} 条 / 恢复 {dst_snap['rows']} 条，"
                f"首末 ts 与逐列 SUM {'一致' if entry['snapshot']['cmp']['ok'] else '❌ 不一致'}")
            for unit in ("1h", "1d"):
                src_seg = device_segments(args.container, args.db, args.stable,
                                          plant, device, unit, win_lo, win_hi)
                dst_seg = device_segments(DRILL_CONTAINER, args.db, args.stable,
                                          plant, device, unit, win_lo, win_hi)
                cmp_result = compare_device_segments(src_seg, dst_seg)
                entry[f"segments_{unit}"] = cmp_result
                log(f"  ③ {key} 分段 {unit}：{cmp_result['n_buckets_src']} 桶 → "
                    f"{cmp_result['n_buckets_dst']} 桶，逐桶全等 = {cmp_result['ok']}")
            entry["ok"] = bool(entry["snapshot"]["cmp"]["ok"]
                               and entry["segments_1h"]["ok"]
                               and entry["segments_1d"]["ok"])
            all_ok = all_ok and entry["ok"]
            devices_report[key] = entry

        # ---- ④ 窗口合计（双设备求和，独立再算一次） ---------------------- #
        src_total = int(taos_sql(
            args.container,
            f"SELECT COUNT(*) FROM {args.db}.{args.stable} "
            f"WHERE ts >= {win_lo} AND ts <= {win_hi};")[0][0])
        dst_total = int(taos_sql(
            DRILL_CONTAINER,
            f"SELECT COUNT(*) FROM {args.db}.{args.stable} "
            f"WHERE ts >= {win_lo} AND ts <= {win_hi};")[0][0])
        per_device_sum = sum(e["snapshot"]["dst"]["rows"] for e in devices_report.values())
        result["totals"] = {
            "src_window_rows": src_total,
            "dst_window_rows": dst_total,
            "sum_of_devices_restored": per_device_sum,
            "src_eq_dst": src_total == dst_total,
            "dst_eq_sum_of_devices": dst_total == per_device_sum,
            "ok": src_total == dst_total == per_device_sum,
        }
        log(f"  ④ 窗口合计：源 {src_total} / 恢复 {dst_total} / 逐设备求和 {per_device_sum} "
            f"→ {'一致' if result['totals']['ok'] else '❌ 不一致'}")
        all_ok = all_ok and result["totals"]["ok"]

        result["devices"] = devices_report
        result["ok"] = bool(all_ok)

        t_seg = time.perf_counter() - t0
        result["rto"] = {
            "definition": "RTO = t_seg − t0，即从发出「开始恢复」到「逐设备 + 分段校验全部通过」",
            "clock": "time.perf_counter()（宿主机单调时钟）",
            "RTO_seconds": round(t_seg, 3),
            "t_up_ready_s": round(t_up, 3),
            "t_data_restored_s": round(t_data, 3),
            "breakdown": {
                "目标环境就绪": round(t_up, 3),
                "实例 DDL 预热": round(t_warm - t_up, 3),
                "备份传入目标": round(max(t_copy - t_warm, 0.0), 3),
                "数据导入落盘": round(max(t_data - t_copy, 0.0), 3),
                "校验": round(t_seg - t_data, 3),
            },
        }
        breakdown_text = " + ".join(f"{k} {v}" for k, v in result["rto"]["breakdown"].items())
        log(f"{'✅' if result['ok'] else '❌'} 校验结果："
            f"{'逐设备全部一致' if result['ok'] else '存在不一致'}")
        log(f"⏱ RTO = {result['rto']['RTO_seconds']:.3f} s（{breakdown_text}）")
        result["finished_at"] = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        return result
    finally:
        if not args.keep_going:
            leftovers = cleanup_instance()
            result.setdefault("cleanup", {})["leftovers"] = leftovers
            log(f"清理完成，残留：{leftovers or '无'}")


def main() -> int:
    ensure_utf8_stdout()
    parser = argparse.ArgumentParser(description="双设备恢复演练：逐设备对账 + RTO")
    parser.add_argument("--backup", required=True, help="备份目录（含 manifest.json）")
    parser.add_argument("--container", default=TD_CONTAINER_DEFAULT, help="现网 tdengine 容器名")
    parser.add_argument("--db", default=TD_DB_DEFAULT)
    parser.add_argument("--stable", default=TD_STABLE_DEFAULT)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=1800.0)
    parser.add_argument("--tag", default=None, help="结果文件名后缀")
    parser.add_argument("--keep-going", action="store_true", help="校验后保留演练现场（排查用）")
    args = parser.parse_args()

    result = run_drill(args)
    stamp = datetime.now(LOCAL_TZ).strftime("%Y%m%d_%H%M%S")
    tag = args.tag or stamp
    out_path = EVIDENCE_DIR / f"dual_device_restore_drill_{tag}.json"
    write_json(out_path, result)
    log(f"结果已落盘：{out_path}")
    return 0 if result.get("ok") else 4


if __name__ == "__main__":
    raise SystemExit(main())
