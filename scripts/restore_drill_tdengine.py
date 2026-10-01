# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读
# -----------------------------------------------------------------------------
# 1) 这是**恢复演练**脚本，目的只有一个：证明"备份真的能恢复出可信的数据"，
#    并给出可复现的 RTO 数字。它按 mode 决定往哪里恢复：
#
#    --mode instance（默认，推荐）
#       另起一个**独立 TDengine 实例**（独立容器 cems-tdengine-drill +
#       独立卷 cems-tdengine-drill-data），把备份恢复进去。
#       ✅ 完全不碰现网 cems 库；实例故意**不接 cems-net**（默认 bridge 是另一个
#          二层域），避免与现网 tdengine 发生 dnode 发现/集群串扰。
#       ✅ 清理：docker rm -f cems-tdengine-drill && docker volume rm cems-tdengine-drill-data
#
#    --mode rename
#       在**现网实例内**恢复成另一个库名 cems_drill_restore（taosdump -W 重命名）。
#       不会覆盖 cems，但确实在现网实例里建了一个库。
#       ✅ 清理：脚本在 finally 里 DROP DATABASE cems_drill_restore（删前核对库名）
#
#    --mode physical
#       从备份里的卷级 tar（physical/var_lib_taos.tar.gz）解包成独立卷，再起独立实例。
#       ✅ 清理同 instance。
#
# 2) 三种模式都**只读现网数据**（rename 模式只多一个自建库），不改 cems 一行。
#    演练结束会打印残留检查（容器/卷/库是否还在）。
#
# 3) ⚠️ 校验窗口是 manifest 里 `window.pre_min_ms .. window.pre_max_ms`，
#    不是"库里当前的全部数据"。原因：备份是在线做的，源库在 dump 之后还在长；
#    用 dump **前**的窗口做比较，可以保证这个窗口完整地落在备份里，
#    源端继续写入不会污染结论（详见 docs/runbooks/恢复演练与对账口径.md §1.4）。
#    ⚠️ 只比总数会掩盖问题（丢了 A 段、多了 B 段，总数可能一样），
#    所以校验是**分段**的：按小时 + 按天，逐桶比条数、逐列比 SUM、比桶内首末 ts。
#
# 4) 环境：Windows 上先设 $env:PYTHONIOENCODING="utf-8"。
#
# 用法：
#   python scripts/restore_drill_tdengine.py --mode instance
#   python scripts/restore_drill_tdengine.py --mode rename
#   python scripts/restore_drill_tdengine.py --mode physical --backup <含 physical/ 的备份目录>
#   python scripts/restore_drill_tdengine.py --mode instance --keep-going   # 校验失败也保留现场
# =============================================================================
"""TDengine 恢复演练：真恢复一次到独立环境 + 记录 RTO + 分段校验保真度。"""

from __future__ import annotations

import argparse
import json
import os
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
    db_properties,
    ensure_utf8_stdout,
    log,
    newest_backup,
    run,
    segment_rows,
    table_schema,
    tag_values,
    taos_exec,
    taos_sql,
    ts_bounds,
    ts_ms_to_str,
    wait_taos_ready,
    write_json,
)

DRILL_CONTAINER = "cems-tdengine-drill"
DRILL_VOLUME = "cems-tdengine-drill-data"
RENAME_DB = "cems_drill_restore"
TARGET_IMAGE = "tdengine/tdengine:latest"
MEASURE_DIR = PROJECT_ROOT / "docs" / "evidence" / "drill"

# 逐列 SUM 的浮点比较容差：同一批值聚合，正常应逐位相等；
# 留一个相对容差只是为了不把"聚合顺序差异"误报成"数据不一致"。
SUM_REL_TOL = 1e-6


# --------------------------------------------------------------------------- #
# 校验
# --------------------------------------------------------------------------- #
def _cmp_sums(src: dict, dst: dict, rel_tol: float = SUM_REL_TOL) -> list[dict]:
    diffs = []
    for col in VALUE_COLUMNS:
        a, b = src.get(col), dst.get(col)
        if a is None and b is None:
            continue
        if (a is None) != (b is None):
            diffs.append({"column": col, "src": a, "dst": b, "why": "一侧为 NULL"})
            continue
        denom = max(abs(a), abs(b), 1e-9)
        if abs(a - b) / denom > rel_tol:
            diffs.append({"column": col, "src": a, "dst": b,
                          "rel": abs(a - b) / denom, "why": "SUM 不一致"})
    return diffs


def compare_segments(src_seg: dict, dst_seg: dict, max_report: int = 20) -> dict:
    """逐桶比较：条数 / 逐列 SUM / 桶内首末 ts。"""
    src_w, dst_w = src_seg["windows"], dst_seg["windows"]
    only_src = sorted(set(src_w) - set(dst_w))
    only_dst = sorted(set(dst_w) - set(src_w))
    count_diff: list[str] = []
    sum_diff: list[str] = []
    bounds_diff: list[str] = []
    samples: list[dict] = []

    for key in sorted(set(src_w) & set(dst_w)):
        a, b = src_w[key], dst_w[key]
        bad = False
        if a["n"] != b["n"]:
            count_diff.append(key)
            bad = True
        sd = _cmp_sums(a["sums"], b["sums"])
        if sd:
            sum_diff.append(key)
            bad = True
        if a["tmin"] != b["tmin"] or a["tmax"] != b["tmax"]:
            bounds_diff.append(key)
            bad = True
        if bad and len(samples) < max_report:
            samples.append({
                "window_local": ts_ms_to_str(int(key)),
                "src": {"n": a["n"], "tmin": a["tmin"], "tmax": a["tmax"]},
                "dst": {"n": b["n"], "tmin": b["tmin"], "tmax": b["tmax"]},
                "sum_diffs": sd,
            })

    ok = not (only_src or only_dst or count_diff or sum_diff or bounds_diff)
    return {
        "unit": src_seg["unit"],
        "ok": ok,
        "n_windows_src": src_seg["n_windows"],
        "n_windows_dst": dst_seg["n_windows"],
        "total_rows_src": src_seg["total_rows"],
        "total_rows_dst": dst_seg["total_rows"],
        "matched": len(set(src_w) & set(dst_w)),
        "windows_only_in_src": [ts_ms_to_str(int(k)) for k in only_src[:max_report]],
        "windows_only_in_dst": [ts_ms_to_str(int(k)) for k in only_dst[:max_report]],
        "windows_with_count_diff": [ts_ms_to_str(int(k)) for k in count_diff[:max_report]],
        "windows_with_sum_diff": [ts_ms_to_str(int(k)) for k in sum_diff[:max_report]],
        "windows_with_bounds_diff": [ts_ms_to_str(int(k)) for k in bounds_diff[:max_report]],
        "samples": samples,
    }


def verify_backup(src_container: str, dst_container: str, src_db: str, dst_db: str,
                  stable: str, win_lo: int, win_hi: int, times: dict) -> dict:
    """全量保真度校验：条数 → 按小时分段 → 按天分段 → 库属性/表结构/标签。"""
    report: dict = {"window_ms": [win_lo, win_hi],
                    "window_local": [ts_ms_to_str(win_lo), ts_ms_to_str(win_hi)]}

    # ---- 1. 窗口内条数（精确相等） -------------------------------------- #
    src_n = count_rows(src_container, src_db, stable, win_lo, win_hi)
    dst_n = count_rows(dst_container, dst_db, stable, win_lo, win_hi)
    times["count"] = time.perf_counter()
    report["count"] = {"src": src_n, "dst": dst_n, "ok": src_n == dst_n}
    log(f"  ① 窗口内条数：源 {src_n} / 恢复 {dst_n} → "
        f"{'一致' if src_n == dst_n else '❌ 不一致'}")

    # ---- 2. 恢复出的全局首末 ts（信息项：备份里可能还有窗口之后的行） ---- #
    try:
        dst_min, dst_max = ts_bounds(dst_container, dst_db, stable)
        report["restored_bounds"] = {
            "min_ms": dst_min, "max_ms": dst_max,
            "min": ts_ms_to_str(dst_min), "max": ts_ms_to_str(dst_max),
        }
        log(f"  ② 恢复库全局 ts ∈ [{ts_ms_to_str(dst_min)}, {ts_ms_to_str(dst_max)}]")
    except Exception as exc:  # pragma: no cover
        report["restored_bounds"] = {"error": str(exc)}

    # ---- 3. 分段（按小时 / 按天） --------------------------------------- #
    for unit in ("1h", "1d"):
        src_seg = segment_rows(src_container, src_db, stable, unit, win_lo, win_hi)
        dst_seg = segment_rows(dst_container, dst_db, stable, unit, win_lo, win_hi)
        cmp_result = compare_segments(src_seg, dst_seg)
        report[f"segments_{unit}"] = cmp_result
        log(f"  ③ 分段 {unit}：源 {cmp_result['n_windows_src']} 桶 / 恢复 "
            f"{cmp_result['n_windows_dst']} 桶，逐桶全等 = {cmp_result['ok']}"
            + ("" if cmp_result["ok"] else
               f"  ❌ 条数差 {len(cmp_result['windows_with_count_diff'])} 桶 / "
               f"SUM 差 {len(cmp_result['windows_with_sum_diff'])} 桶 / "
               f"边界差 {len(cmp_result['windows_with_bounds_diff'])} 桶"))

    # ---- 4. 结构保真（库属性 / 表结构 / 标签） -------------------------- #
    src_props, dst_props = db_properties(src_container, src_db), db_properties(dst_container, dst_db)
    # rename 模式下库名本来就不同，`name` 的差异是预期的，不计入不一致
    ignore = {"name"} if src_db != dst_db else set()
    props_diff = {k: {"src": src_props.get(k), "dst": dst_props.get(k)}
                  for k in set(src_props) | set(dst_props)
                  if src_props.get(k) != dst_props.get(k) and k not in ignore}
    report["db_properties"] = {"src": src_props, "dst": dst_props,
                               "expected_diff": sorted(ignore),
                               "diff": props_diff, "ok": not props_diff}
    report["db_properties"]["vgroup_note"] = (
        "若差异只有 vgroups，属正常：taosdump 的 dbs.sql 不含 VGROUPS，"
        "恢复时按目标实例默认值建库；条数与分段不受影响。")
    report["db_properties"]["vgroup_diff"] = (
        src_props.get("vgroups") != dst_props.get("vgroups"))
    log(f"  ④ 库属性差异：{list(props_diff) if props_diff else '无'}"
        f"（预期差异 {sorted(ignore) or '无'}；"
        f"vgroups {src_props.get('vgroups')} → {dst_props.get('vgroups')}）")

    src_schema = table_schema(src_container, src_db, stable)
    dst_schema = table_schema(dst_container, dst_db, stable)
    report["schema"] = {"ok": src_schema == dst_schema,
                        "src_cols": len(src_schema), "dst_cols": len(dst_schema)}
    log(f"  ⑤ 表结构逐行一致：{report['schema']['ok']}（{len(src_schema)} 列）")

    src_tags = tag_values(src_container, src_db, stable)
    dst_tags = tag_values(dst_container, dst_db, stable)
    report["tags"] = {"ok": src_tags == dst_tags, "src": src_tags, "dst": dst_tags}
    log(f"  ⑥ 子表标签一致：{report['tags']['ok']} {src_tags}")

    report["ok"] = bool(
        report["count"]["ok"]
        and report["segments_1h"]["ok"]
        and report["segments_1d"]["ok"]
        and report["schema"]["ok"]
        and report["tags"]["ok"]
    )
    return report


# --------------------------------------------------------------------------- #
# 演练现场管理
# --------------------------------------------------------------------------- #
def cleanup_instance() -> list[str]:
    """销毁演练实例与其卷。名字是常量，删除前逐字核对，不做宽泛删除。"""
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


def _run_drill_container() -> None:
    # 故意不接 cems-net：默认 bridge 与 cems-net 是不同的二层域，
    # 演练实例不可能与现网 tdengine 互相发现（dnode 身份/集群都不会串）。
    run([
        "docker", "run", "-d", "--name", DRILL_CONTAINER,
        "-e", "TZ=Asia/Shanghai",
        "-v", f"{DRILL_VOLUME}:/var/lib/taos",
        TARGET_IMAGE,
    ], check=True, timeout=180)


PROBE_DB = "cems_drill_warmup"


def warmup_instance(container: str, timeout: float = 180.0) -> dict:
    """用一次与 taosdump 同形状的 DDL 把实例"焐热"，并据此判定它真能执行 DDL。

    为什么必须要这一步（实测踩到）：全新实例刚起来时 `SHOW DATABASES` 就会成功，
    但 mnode 的首次 DDL 事务还在预热，`CREATE STABLE` 会先返回
    `Stable not exists` 再由 mnode 重试（taosd 日志里能看到成串的
    `Stable already exists` / `Stable not exists` 重试）。
    在这个窗口里跑 `taosdump -i`，会出现**返回码 0、但目标库里根本没有 cems**
    的情况（本项目实测到过一次，第二次重跑就好了）——如果只信返回码，
    就会把"没恢复成功"当成"恢复成功"。所以：
      ① 恢复前先跑一遍 CREATE DATABASE/CREATE STABLE/DROP；
      ② 恢复后必须真的查到 cems 库，否则重试。
    """
    start = time.perf_counter()
    deadline = start + timeout
    attempts = 0
    attempt_log: list[dict] = []
    last = ""
    while time.perf_counter() < deadline:
        attempts += 1
        t_attempt = time.perf_counter()
        sql = (
            f"CREATE DATABASE IF NOT EXISTS {PROBE_DB}; "
            f"CREATE STABLE IF NOT EXISTS {PROBE_DB}.probe (ts TIMESTAMP, v FLOAT) TAGS (t NCHAR(8)); "
            f"DROP DATABASE IF EXISTS {PROBE_DB};"
        )
        proc = run(["docker", "exec", container, "taos", "-s", sql], timeout=60)
        combined = (proc.stdout or "") + (proc.stderr or "")
        # 判据必须同时满足：没有 DB error、没有连不上、并且真的回了 OK。
        # ⚠️ DDL 回的是 "Create OK"/"Drop OK"，不是 "Query OK"——只找后者会永远判失败
        #   （本项目在这里空转了 180 s / 47 次才发现）。
        bad = ("DB error" in combined
               or "Unable to establish connection" in combined
               or "Connection refused" in combined)
        attempt_log.append({"attempt": attempts,
                            "seconds": round(time.perf_counter() - t_attempt, 3),
                            "ok": not bad and "OK," in combined})
        if not bad and "OK," in combined:
            return {"ok": True, "attempts": attempts,
                    "seconds": round(time.perf_counter() - start, 3),
                    "attempt_log": attempt_log}
        last = combined[-300:]
        # ⚠️ 退避要小：第 1 次失败是 mnode 首次 DDL 事务预热，通常几百毫秒后就绪。
        #   这里原来固定 sleep 2 s，实测给每次 instance 演练凭空加了约 2 s 的 RTO
        #   （4 s 里 2 s 是 sleep），那是工装缺陷不是系统慢。改成指数退避、上限 1 s。
        time.sleep(min(0.1 * (2 ** (attempts - 1)), 1.0))
    return {"ok": False, "attempts": attempts,
            "seconds": round(time.perf_counter() - start, 3),
            "attempt_log": attempt_log, "detail": last}


def ensure_db_after_restore(container: str, db: str) -> bool:
    """恢复后必须真的查到库；给 mnode 一点提交时间，最多等 20 s。"""
    deadline = time.perf_counter() + 20.0
    while time.perf_counter() < deadline:
        if db_exists(container, db):
            return True
        time.sleep(1.0)
    return False


# --------------------------------------------------------------------------- #
# 三种模式
# --------------------------------------------------------------------------- #
def mode_instance(args: argparse.Namespace, backup_dir: Path) -> dict:
    t0 = time.perf_counter()
    cleanup_instance()
    run(["docker", "volume", "create", DRILL_VOLUME], check=True)
    _run_drill_container()
    ok, tail = wait_taos_ready(DRILL_CONTAINER, timeout=240)
    t_up = time.perf_counter() - t0
    log(f"  恢复目标实例可查库：{ok}（+{t_up:.2f} s）")
    if not ok:
        return {"ok": False, "stage": "instance_start", "detail": tail[-300:]}
    warm = warmup_instance(DRILL_CONTAINER)
    t_warm = time.perf_counter() - t0
    log(f"  实例 DDL 预热：{warm['ok']}（{warm['attempts']} 次尝试，"
        f"耗时 {warm['seconds']} s，+{t_warm:.2f} s）")
    if not warm["ok"]:
        return {"ok": False, "stage": "warmup", "detail": warm.get("detail")}

    run(["docker", "exec", DRILL_CONTAINER, "sh", "-c", "rm -rf /tmp/restore-in"], check=True)
    cp = run(["docker", "cp", str(backup_dir), f"{DRILL_CONTAINER}:/tmp/restore-in"],
             timeout=args.timeout)
    if cp.returncode != 0:
        return {"ok": False, "stage": "copy_in", "detail": cp.stderr}
    t_copy = time.perf_counter() - t0

    proc = run(["docker", "exec", DRILL_CONTAINER, "taosdump",
                "-i", "/tmp/restore-in", "-T", str(args.threads)], timeout=args.timeout)
    t_data = time.perf_counter() - t0
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    if proc.returncode != 0 or "DB error" in output:
        return {"ok": False, "stage": "restore", "detail": output[-800:]}
    if not ensure_db_after_restore(DRILL_CONTAINER, args.db):
        return {"ok": False, "stage": "restore_db_missing",
                "detail": f"taosdump 返回码 {proc.returncode} 但库里查不到 {args.db}；"
                          f"输出尾部：{output[-400:]}"}
    log(f"  taosdump -i 完成（+{t_data:.2f} s）")
    return {"ok": True, "t0": t0, "t_up": t_up, "t_warm": t_warm, "t_copy": t_copy,
            "t_data": t_data, "warmup": warm, "restore_output_tail": output[-800:],
            "src": args.container, "dst": DRILL_CONTAINER,
            "src_db": args.db, "dst_db": args.db}


def mode_rename(args: argparse.Namespace, backup_dir: Path) -> dict:
    t0 = time.perf_counter()
    if args.db != TD_DB_DEFAULT:
        return {"ok": False, "stage": "guard", "detail": "--mode rename 只支持 cems 库改名"}
    if db_exists(args.container, RENAME_DB):
        log(f"  演练库 {RENAME_DB} 已存在（很可能是上次残留），先删除")
        taos_exec(args.container, f"DROP DATABASE IF EXISTS {RENAME_DB};")
    t_up = time.perf_counter() - t0  # rename 模式复用现网实例，没有"起实例"这一步
    log(f"  复用现网实例（无实例启动阶段），从 +{t_up:.2f} s 开始导入")

    run(["docker", "exec", args.container, "sh", "-c", "rm -rf /tmp/drill-restore-in"], check=True)
    cp = run(["docker", "cp", str(backup_dir), f"{args.container}:/tmp/drill-restore-in"],
             timeout=args.timeout)
    if cp.returncode != 0:
        return {"ok": False, "stage": "copy_in", "detail": cp.stderr}
    t_copy = time.perf_counter() - t0

    proc = run(["docker", "exec", args.container, "taosdump",
                "-i", "/tmp/drill-restore-in", "-W", f"{args.db}={RENAME_DB}",
                "-T", str(args.threads)], timeout=args.timeout)
    t_data = time.perf_counter() - t0
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    if proc.returncode != 0 or "DB error" in output:
        return {"ok": False, "stage": "restore", "detail": output[-800:]}
    if not ensure_db_after_restore(args.container, RENAME_DB):
        return {"ok": False, "stage": "restore_db_missing",
                "detail": f"taosdump 返回码 {proc.returncode} 但库里查不到 {RENAME_DB}；"
                          f"输出尾部：{output[-400:]}"}
    log(f"  taosdump -i -W {args.db}={RENAME_DB} 完成（+{t_data:.2f} s）")
    return {"ok": True, "t0": t0, "t_up": t_up, "t_copy": t_copy, "t_data": t_data,
            "restore_output_tail": output[-800:],
            "src": args.container, "dst": args.container,
            "src_db": args.db, "dst_db": RENAME_DB}


def mode_physical(args: argparse.Namespace, backup_dir: Path) -> dict:
    tar_src = backup_dir / "physical" / "var_lib_taos.tar.gz"
    if not tar_src.exists():
        return {"ok": False, "stage": "guard",
                "detail": f"备份里没有卷级 tar：{tar_src}（备份时需带 --physical）"}
    t0 = time.perf_counter()
    cleanup_instance()
    run(["docker", "volume", "create", DRILL_VOLUME], check=True)
    t_vol = time.perf_counter() - t0

    extract = run([
        "docker", "run", "--rm",
        "-v", f"{DRILL_VOLUME}:/dst",
        "-v", f"{tar_src.parent.resolve()}:/src:ro",
        "alpine", "sh", "-c", "tar xzf /src/var_lib_taos.tar.gz -C /dst && echo extracted",
    ], timeout=args.timeout)
    if extract.returncode != 0 or "extracted" not in (extract.stdout or ""):
        return {"ok": False, "stage": "extract",
                "detail": (extract.stdout or "")[-400:] + (extract.stderr or "")[-400:]}
    t_unpack = time.perf_counter() - t0
    log(f"  卷级 tar 解包完成（+{t_unpack:.2f} s）")

    _run_drill_container()
    ok, tail = wait_taos_ready(DRILL_CONTAINER, timeout=240)
    t_data = time.perf_counter() - t0
    if not ok:
        return {"ok": False, "stage": "instance_start", "detail": tail[-300:]}
    if not ensure_db_after_restore(DRILL_CONTAINER, args.db):
        return {"ok": False, "stage": "restore_db_missing",
                "detail": f"物理副本实例起来了，但查不到 {args.db}（卷拷可能撕裂或不完整）"}
    log(f"  物理恢复实例可查库（+{t_data:.2f} s）")
    return {"ok": True, "t0": t0, "t_up": t_data, "t_vol": t_vol, "t_unpack": t_unpack,
            "t_copy": t_unpack, "t_data": t_data,
            "src": args.container, "dst": DRILL_CONTAINER,
            "src_db": args.db, "dst_db": args.db}


# --------------------------------------------------------------------------- #
def build_summary() -> dict:
    """把 docs/evidence/drill/tdengine_restore_drill_*.json 汇总成一张表。

    汇总口径：同一 mode 的多次独立演练，给 min / P50 / max —— 只做一次给不出重复性。
    """
    runs: list[dict] = []
    for path in sorted(MEASURE_DIR.glob("tdengine_restore_drill_*.json")):
        if path.name.endswith("_summary.json"):
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        rto = data.get("rto") or {}
        runs.append({
            "file": path.name,
            "mode": data.get("mode"),
            "backup_kind": data.get("backup_kind"),
            "tag": data.get("backup_stamp"),
            "backup_dir": data.get("backup_dir"),
            "rows_in_backup": (data.get("backup_dump") or {}).get("reported_rows"),
            "window": (data.get("verification") or {}).get("window_local"),
            "ok": data.get("ok"),
            "rto_seconds": rto.get("RTO_seconds"),
            "t_up_ready_s": rto.get("t_up_ready_s"),
            "t_data_restored_s": rto.get("t_data_restored_s"),
            "t_count_verified_s": rto.get("t_count_verified_s"),
            "breakdown": rto.get("breakdown"),
            "cleanup_leftovers": (data.get("cleanup") or {}).get("leftovers"),
        })

    by_mode: dict[str, dict] = {}
    keys = sorted({f"{r['mode']}/{r['backup_kind'] or '?'}" for r in runs if r["mode"]})
    for key in keys:
        mode, _, bk = key.partition("/")
        vals = sorted(r["rto_seconds"] for r in runs
                      if f"{r['mode']}/{r['backup_kind'] or '?'}" == key
                      and r["rto_seconds"] is not None)
        data_vals = sorted(r["t_data_restored_s"] for r in runs
                           if f"{r['mode']}/{r['backup_kind'] or '?'}" == key
                           and r["t_data_restored_s"] is not None)
        if not vals:
            continue
        mid = vals[len(vals) // 2] if len(vals) % 2 else (vals[len(vals) // 2 - 1] + vals[len(vals) // 2]) / 2
        by_mode[key] = {
            "mode": mode,
            "backup_kind": bk,
            "n_runs": len(vals),
            "all_ok": all(r["ok"] for r in runs
                          if f"{r['mode']}/{r['backup_kind'] or '?'}" == key),
            "RTO_seconds": {"min": min(vals), "p50": round(mid, 3), "max": max(vals),
                            "spread": round(max(vals) - min(vals), 3)},
            "data_available_seconds": {"min": min(data_vals), "max": max(data_vals)},
        }

    summary = {
        "generated_at": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "note": "RTO = 从发出「开始恢复」到「数据可查且条数与分段校验通过」；"
                "含校验工装自身开销（每次 docker exec 调 taos 约 0.6 s，6 次判定约 4.6 s）。"
                "data_available_seconds = 数据真正可查的时刻（不含校验）。",
        "by_mode": by_mode,
        "runs": runs,
    }
    return summary


def main() -> int:
    ensure_utf8_stdout()
    parser = argparse.ArgumentParser(description="TDengine 恢复演练 + RTO 记录")
    parser.add_argument("--mode", default="instance",
                        choices=("instance", "rename", "physical", "summary"))
    parser.add_argument("--backup", default=None, help="备份目录（默认取最新一份）")
    parser.add_argument("--backup-root", default=None, help="备份根目录（配合默认取最新）")
    parser.add_argument("--container", default=TD_CONTAINER_DEFAULT, help="现网 tdengine 容器名")
    parser.add_argument("--db", default=TD_DB_DEFAULT)
    parser.add_argument("--stable", default=TD_STABLE_DEFAULT)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=1800.0)
    parser.add_argument("--tag", default=None, help="结果文件后缀（默认按 mode）")
    parser.add_argument("--keep-going", action="store_true",
                        help="校验失败也保留演练现场，便于排查（默认自动清理）")
    args = parser.parse_args()

    if args.mode == "summary":
        summary = build_summary()
        out_path = MEASURE_DIR / "tdengine_restore_drill_summary.json"
        write_json(out_path, summary)
        for mode, agg in summary["by_mode"].items():
            log(f"{mode:<10} n={agg['n_runs']} all_ok={agg['all_ok']} "
                f"RTO min/p50/max = {agg['RTO_seconds']['min']} / "
                f"{agg['RTO_seconds']['p50']} / {agg['RTO_seconds']['max']} s；"
                f"数据可查 {agg['data_available_seconds']['min']}~"
                f"{agg['data_available_seconds']['max']} s")
        log(f"汇总已落盘：{out_path}")
        return 0

    if args.backup:
        backup_dir = Path(args.backup).resolve()
    else:
        root = Path(args.backup_root or os.getenv("CEMS_BACKUP_DIR", r"F:\cems-backup"))
        backup_dir = newest_backup(root)
    if not (backup_dir / "manifest.json").exists():
        log(f"✗ {backup_dir} 里没有 manifest.json，不是一份备份")
        return 2
    manifest = json.loads((backup_dir / "manifest.json").read_text(encoding="utf-8"))
    win = manifest["window"]
    if win.get("mode") == "time-window":
        # 差量备份：只有这一段时间窗的数据能被恢复出来，
        # 所以校验窗口必须用"请求的时间窗"，而不是全表首末 ts。
        win_lo, win_hi = int(win["requested_lo_ms"]), int(win["requested_hi_ms"])
        win_note = "（差量备份：用请求时间窗做校验窗口）"
    else:
        win_lo, win_hi = int(win["pre_min_ms"]), int(win["pre_max_ms"])
        win_note = ""

    log(f"恢复演练：mode={args.mode}")
    log(f"  备份：{backup_dir}")
    log(f"  备份自报条数 {manifest['dump']['reported_rows']}；校验窗口 "
        f"{ts_ms_to_str(win_lo)} → {ts_ms_to_str(win_hi)}{win_note}")

    result: dict = {
        "mode": args.mode,
        "backup_kind": manifest.get("kind"),
        "backup_dir": str(backup_dir),
        "backup_stamp": manifest.get("stamp"),
        "backup_dump": manifest.get("dump"),
        "backup_window": manifest.get("window"),
        "started_at": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "verification": None,
        "rto": None,
        "cleanup": None,
    }

    runner = {"instance": mode_instance, "rename": mode_rename, "physical": mode_physical}[args.mode]
    times: dict = {}
    try:
        stage = runner(args, backup_dir)
        if not stage.get("ok"):
            result["ok"] = False
            result["failed_stage"] = {k: v for k, v in stage.items() if k != "ok"}
            log(f"✗ 恢复阶段失败：{stage.get('stage')} {str(stage.get('detail'))[:400]}")
            return 3

        dst_container, dst_db, t_data = stage["dst"], stage["dst_db"], stage["t_data"]
        log("开始校验（①条数 → ②首末 → ③按小时/按天分段 → ④库属性 → ⑤表结构 → ⑥标签）")
        verify = verify_backup(args.container, dst_container, args.db, dst_db,
                               args.stable, win_lo, win_hi, times)
        t_seg = time.perf_counter() - stage["t0"]
        t_count = times["count"] - stage["t0"] if "count" in times else t_seg

        result["verification"] = verify
        result["ok"] = bool(verify["ok"])
        result["rto"] = {
            "definition": "RTO = t_seg − t0，即从发出「开始恢复」到「数据可查且条数与分段校验通过」",
            "clock": "time.perf_counter()（宿主机单调时钟，不受容器时钟漂移影响）",
            "RTO_seconds": round(t_seg, 3),
            "t_up_ready_s": round(stage["t_up"], 3),
            "t_copy_done_s": round(stage.get("t_copy", t_data), 3),
            "t_data_restored_s": round(t_data, 3),
            "t_count_verified_s": round(t_count, 3),
            "breakdown": (
                {   # 物理恢复没有"导入数据"这一步：解包即恢复，起容器即可查
                    "卷创建": round(stage.get("t_vol", 0.0), 3),
                    "卷级 tar 解包": round(max(stage.get("t_unpack", 0.0) - stage.get("t_vol", 0.0), 0.0), 3),
                    "实例启动至可查": round(max(t_data - stage.get("t_unpack", t_data), 0.0), 3),
                    "校验": round(t_seg - t_data, 3),
                } if args.mode == "physical" else
                dict(
                    [("目标环境就绪", round(stage["t_up"], 3))]
                    # DDL 预热单独列出来：它是**校验工装为了让恢复有效**而付的代价，
                    # 不是恢复本身的耗时。混进"备份传入目标"会把 docker cp 的代价报大 25 倍。
                    + ([("实例 DDL 预热", round(stage["t_warm"] - stage["t_up"], 3))]
                       if stage.get("t_warm") else [])
                    + [
                        ("备份传入目标",
                         round(max(stage.get("t_copy", t_data) - stage.get("t_warm", stage["t_up"]), 0.0), 3)),
                        ("数据导入落盘", round(max(t_data - stage.get("t_copy", t_data), 0.0), 3)),
                        ("校验", round(t_seg - t_data, 3)),
                    ]
                )
            ),
        }
        breakdown_text = " + ".join(f"{k} {v}" for k, v in result["rto"]["breakdown"].items())
        if "warmup" in stage:
            result["warmup"] = stage["warmup"]
        if "restore_output_tail" in stage:
            result["restore_output_tail"] = stage["restore_output_tail"]
        log(f"{'✅' if result['ok'] else '❌'} 校验结果："
            f"{'全部一致' if result['ok'] else '存在不一致，见结果文件'}")
        log(f"⏱ RTO = {result['rto']['RTO_seconds']:.3f} s（{breakdown_text}）")

        result["finished_at"] = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        out_path = MEASURE_DIR / f"tdengine_restore_drill_{args.tag or args.mode}.json"
        write_json(out_path, result)
        log(f"结果已落盘：{out_path}")
        return 0 if result["ok"] else 4
    finally:
        if args.keep_going:
            log("--keep-going：保留演练现场。清理方式："
                f"docker rm -f {DRILL_CONTAINER}；docker volume rm {DRILL_VOLUME}；"
                f"并在现网实例执行 DROP DATABASE IF EXISTS {RENAME_DB};")
        else:
            leftovers = cleanup_instance()
            if args.mode == "rename":
                run(["docker", "exec", args.container, "sh", "-c",
                     "rm -rf /tmp/drill-restore-in"])
                if db_exists(args.container, RENAME_DB):
                    taos_exec(args.container, f"DROP DATABASE IF EXISTS {RENAME_DB};")
                if db_exists(args.container, RENAME_DB):
                    leftovers.append(f"database:{RENAME_DB}")
            result["cleanup"] = {"leftovers": leftovers}
            log(f"清理完成，残留：{leftovers or '无'}")


if __name__ == "__main__":
    raise SystemExit(main())
