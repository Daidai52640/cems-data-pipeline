# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读
# -----------------------------------------------------------------------------
# 1) 本脚本对 TDengine **只读**：唯一取数手段是容器内的 `taosdump -D cems`
#    与 `SELECT COUNT/…`。它不写库、不改表、不改网关/接入层任何文件。
# 2) 落盘位置默认 `F:\cems-backup`（可用 --out 或 CEMS_BACKUP_DIR 覆盖）：
#    - 本机 C: 是系统盘（Disk 1），F: 是**另一块物理 NVMe**（Disk 0）→ 满足
#      HJ 75-2017 附录 H（规范性）H.4 的"自动备份到非系统磁盘"。
#    - 备份**故意放在仓库外**：`git clean -xfd`、误删仓库目录、.gitignore 漏配
#      都不会波及它。脚本只往这个目录写，删除只发生在 --prune 且经过名字与
#      位置双重校验之后（见 prune_backups）。
# 3) 临时目录只开在 **容器内 /tmp**（`/tmp/cems-backup-<stamp>`），
#    try/finally 里删除；绝不往 /var/lib/taos（数据卷）或 /var/log/taos（日志卷）写。
# 4) 在线备份：tdengine 不停机。因此 dump 期间仍在新写入库，备份是一个
#    "窗口"而不是一个瞬间。manifest 记下 dump 前/后的 COUNT 与首末 ts，
#    校验时用 **dump 前** 的首末 ts 作为比较窗口（该窗口必然完整落在 dump 内），
#    这样"源端还在长"不会污染恢复校验。详见 docs/数据可靠性.md §2.4。
# 5) --physical 会额外做一次**卷级**拷贝（只读挂载卷 + tar）。
#    ⚠️ 它是**崩溃一致性**副本，不是事务一致性副本：taosd 不停机，拷贝过程中
#    文件可能处于半写状态。它只是逻辑备份的应急补充，验收口径以 taosdump 为准。
#
# 用法：
#   python scripts/backup_tdengine.py                    # 全量逻辑备份 → hourly/
#   python scripts/backup_tdengine.py --tier daily        # 落到 daily/
#   python scripts/backup_tdengine.py --physical          # 顺带做卷级拷贝
#   python scripts/backup_tdengine.py --prune             # 按保留策略清理旧备份
#   python scripts/backup_tdengine.py --list              # 列出已有备份
#   python scripts/backup_tdengine.py --out D:\cemsbak    # 换落点
#   # 时间窗差量导出（增量路径，见 docs/数据可靠性.md §1.2 的论证与阈值）：
#   python scripts/backup_tdengine.py --since "2026-10-01 20:00:00" --until "2026-10-01 21:00:00"
#
# 保留层与目录：
#   hourly/ daily/ monthly/ manual/   ← 全量，前缀 cems_full_<stamp>，受 --prune 管理
#   incremental/                      ← 时间窗差量，前缀 cems_inc_<stamp>，**不受 --prune 管理**
# =============================================================================
"""TDengine `cems` 库全量逻辑备份（taosdump）+ 清单 + 校验 + 保留期清理。"""

from __future__ import annotations

import argparse
import json
import re
import shutil
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
    count_rows,
    db_properties,
    dir_inventory,
    ensure_utf8_stdout,
    human_bytes,
    log,
    resolve_mounts,
    run,
    sha256_file,
    table_schema,
    tag_values,
    ts_bounds,
    ts_ms_to_str,
    volume_dir_size_bytes,
    write_json,
)

DEFAULT_OUT = r"F:\cems-backup"
STAMP_FMT = "%Y%m%d_%H%M%S"
BACKUP_NAME_RE = re.compile(r"^cems_full_(\d{8}_\d{6})$")
TIERS = ("hourly", "daily", "monthly", "manual")

# 保留期（默认值，可被命令行覆盖）。
# 依据见 docs/数据可靠性.md §1.4。取值刻意**偏少**，因为"多份全量 × 长保留"的体积
# 是 O(份数 × 当前数据量)：本机当前 184 KiB 无所谓，但 5 年后库里约 736 MiB，
# 每多留一份 hourly 就多 736 MiB。份数按"要覆盖哪种事故"定，不按"能留多少"定：
#   - hourly  24  = 1 天    → 覆盖"当天误操作/误删卷"
#   - daily   14  = 2 周    → 覆盖"过几天才发现"
#   - monthly 60  = 60 个月 → 直接对齐 HJ 76-2017 B.5.2(3) 的日均/月均 ≥60 个月
#   - manual  0   = 不自动删 → 变更前留档
RETENTION_DEFAULT = {"hourly": 24, "daily": 14, "monthly": 60, "manual": 0}  # 0 = 不自动删


def container_tmp(stamp: str) -> str:
    return f"/tmp/cems-backup-{stamp}"


def detect_orphan_volumes(active: str) -> list[dict]:
    """找出与在用卷"名字只差一个连字符/下划线"的卷 —— 本机真实存在这种坑。

    本机同时有 `cems_tdengine-data`（在用的，compose 项目名前缀）与
    `cems-tdengine-data`（孤儿的，14.1 MB，最后写入早于前者）。
    任何硬编码卷名的备份脚本都会静默备到孤儿卷上，而且**不报错**。
    """
    proc = run(["docker", "volume", "ls", "--format", "{{.Name}}"])
    if proc.returncode != 0:
        return []
    suspects = []
    swapped = active.replace("_", "-") if "_" in active else active.replace("-", "_")
    for name in proc.stdout.splitlines():
        name = name.strip()
        if not name or name == active:
            continue
        if name == swapped:
            suspects.append({
                "name": name,
                "reason": f"与在用卷 {active} 仅差下划线/连字符",
                "size_bytes": volume_dir_size_bytes(name),
            })
    return suspects


def taosdump_version(container: str) -> str:
    proc = run(["docker", "exec", container, "taosdump", "--version"])
    for line in (proc.stdout or "").splitlines():
        if line.startswith("taosdump version"):
            return line.strip()
    return "unknown"


def server_version(container: str) -> str:
    proc = run(["docker", "exec", container, "taos", "-s", "SELECT SERVER_VERSION();"])
    for line in (proc.stdout or "").splitlines():
        # taos 的表格行形如 " 3.3.6.13         |"，要先把竖线和空白都剥掉
        stripped = line.replace("|", " ").strip()
        if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", stripped):
            return stripped
    return "unknown"


def dir_size_in_container(container: str, path: str) -> int | None:
    proc = run(["docker", "exec", container, "sh", "-c", f"du -sb {path} | cut -f1"])
    try:
        return int((proc.stdout or "").strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None


def parse_dump_rows(stdout: str) -> int | None:
    """从 taosdump 输出里取 "OK: N row(s) dumped out!" —— 这是 dump 自报的条数。

    ⚠️ taosdump 把这行写在 **stderr**（进度条写在 stdout），所以调用方必须把
    两路输出合起来再解析；只查 stdout 会得到 None（本项目踩过）。
    """
    matches = re.findall(r"OK:\s*(\d+)\s*row\(s\)\s*dumped out!", stdout or "")
    return int(matches[-1]) if matches else None


def parse_local_time(text: str) -> float:
    """接受 `YYYY-MM-DD HH:MM:SS` 或 `YYYY-MM-DDTHH:MM:SS`（按项目时区 +08:00）。"""
    cleaned = text.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).replace(tzinfo=LOCAL_TZ).timestamp()
        except ValueError:
            continue
    raise ValueError(f"无法解析时间：{text!r}（应为 YYYY-MM-DD HH:MM:SS）")


def taosdump_time_arg(ms: int) -> str:
    """taosdump 的 -S/-E 用 ISO8601 + 偏移，例如 2026-10-01T20:00:00.000+0800。"""
    return datetime.fromtimestamp(int(ms) / 1000.0, LOCAL_TZ).strftime("%Y-%m-%dT%H:%M:%S.000+0800")


def do_backup(args: argparse.Namespace) -> int:
    out_root = Path(args.out).resolve()
    container = args.container
    db = args.db
    stable = args.stable
    stamp = datetime.now(LOCAL_TZ).strftime(STAMP_FMT)
    # 带 -S/-E 时是一次"时间窗差量导出"（增量路径），不带就是全量
    inc_lo_ms = int(parse_local_time(args.since) * 1000) if args.since else None
    inc_hi_ms = int(parse_local_time(args.until) * 1000) if args.until else None
    if (inc_lo_ms is None) != (inc_hi_ms is None):
        log("✗ --since 与 --until 必须同时给（时间窗差量导出）")
        return 2
    is_window = inc_lo_ms is not None
    kind = "taosdump-logical-window" if is_window else "taosdump-logical-full"
    prefix = "cems_inc" if is_window else "cems_full"
    # 差量层固定落到 incremental/，不参与自动清理（删一份会断链，见 prune_backups）
    tier = "incremental" if is_window else args.tier

    log(f"备份目标：{db}.{stable}  容器={container}  模式={'时间窗差量' if is_window else '全量'}")
    log(f"落点：{out_root}\\{tier}\\{prefix}_{stamp}")
    if is_window:
        log(f"  时间窗（本地）：{ts_ms_to_str(inc_lo_ms)} → {ts_ms_to_str(inc_hi_ms)}")

    # ---- 0. 前置：容器在跑 + 能查库 ------------------------------------- #
    state = run(["docker", "inspect", container, "--format", "{{.State.Status}}"])
    if state.returncode != 0 or state.stdout.strip() != "running":
        log(f"✗ 容器 {container} 不在 running（{state.stdout.strip() or state.stderr.strip()}），中止")
        return 2

    # ---- 1. 卷核对（防孤儿卷） ------------------------------------------ #
    mounts = resolve_mounts(container)
    data_mount = next((m for m in mounts if m["dst"] == "/var/lib/taos"), None)
    log_mount = next((m for m in mounts if m["dst"] == "/var/log/taos"), None)
    if data_mount is None:
        log("✗ 没找到 /var/lib/taos 的挂载点，中止（无法核对卷归属）")
        return 2
    active_volume = data_mount["name"] or data_mount["src"]
    log(f"在用的数据卷（从运行容器读出，非硬编码）：{active_volume}")
    orphans = detect_orphan_volumes(active_volume)
    for o in orphans:
        log(f"  ⚠️ 发现疑似孤儿卷 {o['name']}（{o['reason']}，"
            f"占用 {human_bytes(o['size_bytes'])}）—— 它**不是**在用的那个")

    # ---- 2. 取 dump 前基线 ---------------------------------------------- #
    pre_count = count_rows(container, db, stable)
    pre_min, pre_max = ts_bounds(container, db, stable)
    log(f"dump 前基线：{pre_count} 条，ts ∈ [{ts_ms_to_str(pre_min)}, {ts_ms_to_str(pre_max)}]")

    tmp_in_container = container_tmp(stamp)
    backup_dir = out_root / tier / f"{prefix}_{stamp}"
    physical_info: dict | None = None
    t_start = time.perf_counter()
    started_wall = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")

    try:
        # ---- 3. 容器内 dump（只写 /tmp） -------------------------------- #
        run(["docker", "exec", container, "sh", "-c", f"rm -rf {tmp_in_container}"], check=True)
        run(["docker", "exec", container, "sh", "-c", f"mkdir -p {tmp_in_container}"], check=True)
        dump_t0 = time.perf_counter()
        dump_cmd = ["docker", "exec", container, "taosdump", "-D", db, "-o", tmp_in_container,
                    "-T", str(args.threads)]
        if is_window:
            dump_cmd += ["-S", taosdump_time_arg(inc_lo_ms), "-E", taosdump_time_arg(inc_hi_ms)]
        proc = run(dump_cmd, timeout=args.timeout)
        dump_seconds = time.perf_counter() - dump_t0
        dump_output = (proc.stdout or "") + "\n" + (proc.stderr or "")
        if proc.returncode != 0 or "DB error" in dump_output:
            log(f"✗ taosdump 失败：\n{dump_output[-1500:]}")
            return 3
        dumped_rows = parse_dump_rows(dump_output)
        log(f"taosdump 完成：自报 {dumped_rows} 条，耗时 {dump_seconds:.2f} s")

        # ---- 4. 取 dump 后基线 ------------------------------------------ #
        post_count = count_rows(container, db, stable)
        post_min, post_max = ts_bounds(container, db, stable)
        log(f"dump 后基线：{post_count} 条，ts ∈ [{ts_ms_to_str(post_min)}, {ts_ms_to_str(post_max)}]")

        # 自检。
        #  - 全量：dump 覆盖的是 [pre_count, post_count] 之间的某一个瞬间，只能判区间；
        #  - 时间窗差量：窗口两端都是**已经封口的历史**，源端继续写入不影响它，
        #    所以可以直接对"窗口内条数"做**精确相等**校验（更强的判据）。
        self_check = "ok"
        expected_window_rows: int | None = None
        if is_window:
            expected_window_rows = count_rows(container, db, stable, inc_lo_ms, inc_hi_ms)
            if dumped_rows is None:
                self_check = "dumped_rows_unparsed"
            elif dumped_rows != expected_window_rows:
                self_check = "window_rows_mismatch"
            log(f"  时间窗自检：窗口内实有条数 {expected_window_rows} / dump 自报 {dumped_rows}")
        if self_check == "ok" and not is_window:
            if dumped_rows is None:
                self_check = "dumped_rows_unparsed"
            elif not (pre_count <= dumped_rows <= post_count):
                self_check = "rows_out_of_range"
        if self_check != "ok":
            log(f"⚠️ 自检未通过：{self_check}（pre={pre_count} dumped={dumped_rows} post={post_count}）")

        # ---- 5. 拷出到宿主机 -------------------------------------------- #
        backup_dir.parent.mkdir(parents=True, exist_ok=True)
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        cp = run(["docker", "cp", f"{container}:{tmp_in_container}", str(backup_dir)], timeout=args.timeout)
        if cp.returncode != 0:
            log(f"✗ docker cp 失败：{cp.stderr}")
            return 4

        # ---- 6.（可选）卷级物理拷贝 ------------------------------------- #
        if args.physical:
            physical_info = do_physical_copy(active_volume, backup_dir, args)

        # ---- 7. 清单 + 摘要 --------------------------------------------- #
        inventory = dir_inventory(backup_dir)
        total_seconds = time.perf_counter() - t_start
        manifest = {
            "kind": kind,
            "tier": tier,
            "stamp": stamp,
            "created_at": started_wall,
            "created_at_ms": int(time.time() * 1000),
            "backup_seconds": round(total_seconds, 3),
            "dump_seconds": round(dump_seconds, 3),
            "restore_cmd": (
                f"docker exec <目标容器> taosdump -i <本目录>"
                + (f" -W {db}=<目标库名>" if args.rename_hint else "")
            ),
            "source": {
                "container": container,
                "db": db,
                "stable": stable,
                "taosdump_version": taosdump_version(container),
                "server_version": server_version(container),
                "data_volume": active_volume,
                "data_mount": data_mount,
                "log_mount": log_mount,
                "data_dir_bytes": volume_dir_size_bytes(active_volume),
                "log_dir_bytes": dir_size_in_container(container, "/var/log/taos"),
            },
            "window": {
                "mode": "time-window" if is_window else "full",
                "requested_lo_ms": inc_lo_ms,
                "requested_hi_ms": inc_hi_ms,
                "requested_lo": ts_ms_to_str(inc_lo_ms) if is_window else None,
                "requested_hi": ts_ms_to_str(inc_hi_ms) if is_window else None,
                "pre_count": pre_count,
                "post_count": post_count,
                "pre_min_ms": pre_min,
                "pre_max_ms": pre_max,
                "post_min_ms": post_min,
                "post_max_ms": post_max,
                "pre_min": ts_ms_to_str(pre_min),
                "pre_max": ts_ms_to_str(pre_max),
                "post_min": ts_ms_to_str(post_min),
                "post_max": ts_ms_to_str(post_max),
            },
            "dump": {"reported_rows": dumped_rows, "self_check": self_check,
                     "expected_window_rows": expected_window_rows,
                     "output_tail": dump_output[-2000:]},
            "db_properties": db_properties(container, db),
            "schema": table_schema(container, db, stable),
            "tags": tag_values(container, db, stable),
            "physical": physical_info,
            "suspect_orphan_volumes": orphans,
            "inventory": dict(inventory, note="manifest.json 自身不计入本清单（写清单时它还没生成）"),
        }
        write_json(backup_dir / "manifest.json", manifest)

        write_json(out_root / "LATEST.json", {
            "kind": kind,
            "tier": tier,
            "path": str(backup_dir),
            "stamp": stamp,
            "created_at": started_wall,
            "rows": dumped_rows,
            "bytes": inventory["total_bytes"],
        })
        append_log(out_root, {
            "event": "backup", "kind": kind, "tier": tier, "stamp": stamp,
            "path": str(backup_dir), "rows": dumped_rows,
            "bytes": inventory["total_bytes"], "seconds": round(total_seconds, 3),
            "self_check": self_check, "physical": bool(physical_info),
            "data_volume": active_volume, "at": started_wall,
        })

        log(f"✅ 备份完成：{backup_dir}")
        log(f"   条数 {dumped_rows}（dump 前 {pre_count} / dump 后 {post_count}）"
            f"  体积 {human_bytes(inventory['total_bytes'])}"
            f"  文件 {inventory['file_count']}  总耗时 {total_seconds:.2f} s")
        return 0 if self_check == "ok" else 5
    finally:
        run(["docker", "exec", container, "sh", "-c", f"rm -rf {tmp_in_container}"])


def do_physical_copy(volume: str, backup_dir: Path, args: argparse.Namespace) -> dict:
    """卷级拷贝：**只读**挂载卷 → tar.gz。见文件头第 5 条的免责说明。"""
    dest = backup_dir / "physical"
    dest.mkdir(parents=True, exist_ok=True)
    tar_path = dest / "var_lib_taos.tar.gz"
    t0 = time.perf_counter()
    proc = run([
        "docker", "run", "--rm",
        "-v", f"{volume}:/src:ro",
        "-v", f"{dest.resolve()}:/dst",
        args.physical_image,
        "sh", "-c", "tar czf /dst/var_lib_taos.tar.gz -C /src . 2>/dev/null; echo done",
    ], timeout=args.timeout)
    seconds = time.perf_counter() - t0
    if not tar_path.exists():
        log(f"⚠️ 物理拷贝未产出文件：{proc.stdout}\n{proc.stderr}")
        return {"ok": False, "volume": volume, "seconds": round(seconds, 3),
                "stderr": proc.stderr[-500:]}
    info = {
        "ok": True,
        "volume": volume,
        "tar": tar_path.name,
        "bytes": tar_path.stat().st_size,
        "sha256": sha256_file(tar_path),
        "seconds": round(seconds, 3),
        "consistency": "crash-consistent（taosd 未停机，只读挂载卷拷贝；非事务一致）",
    }
    log(f"   卷级拷贝：{human_bytes(info['bytes'])}，{seconds:.2f} s（崩溃一致性）")
    return info


def append_log(out_root: Path, record: dict) -> None:
    logs_dir = out_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    with (logs_dir / "backup_log.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def prune_backups(args: argparse.Namespace) -> int:
    """按保留策略清理。删除前做**名字 + 位置**双重校验，绝不做宽泛删除。"""
    out_root = Path(args.out).resolve()
    keep = {
        "hourly": args.keep_hourly,
        "daily": args.keep_daily,
        "monthly": args.keep_monthly,
        "manual": args.keep_manual,
    }
    newest_overall = None
    all_dirs: list[Path] = []
    for tier in TIERS:
        base = out_root / tier
        if base.is_dir():
            all_dirs.extend(p for p in base.glob("cems_full_*") if p.is_dir())
    if all_dirs:
        newest_overall = max(all_dirs, key=lambda p: p.stat().st_mtime)

    removed, kept = [], []
    for tier in TIERS:
        base = (out_root / tier).resolve()
        if not base.is_dir():
            continue
        dirs = sorted(
            (p for p in base.glob("cems_full_*") if p.is_dir()),
            key=lambda p: p.name, reverse=True,
        )
        limit = keep[tier]
        for idx, path in enumerate(dirs):
            # ---- 删除前的两道校验（任何一条不过就保留） ----------------- #
            if not BACKUP_NAME_RE.match(path.name):
                kept.append((str(path), "名字不匹配 cems_full_YYYYMMDD_HHMMSS"))
                continue
            if path.resolve().parent != base:
                kept.append((str(path), "不在预期的 tier 目录下"))
                continue
            if newest_overall is not None and path.resolve() == newest_overall.resolve():
                kept.append((str(path), "最新的一个备份永不自动删除"))
                continue
            if limit == 0 or idx < limit:
                kept.append((str(path), "在保留期内"))
                continue
            shutil.rmtree(path)
            removed.append(str(path))
            log(f"  清理 {tier}/{path.name}")

    append_log(out_root, {
        "event": "prune", "removed": removed, "kept_count": len(kept),
        "policy": keep, "at": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    })
    inc_dir = out_root / "incremental"
    inc_count = len(list(inc_dir.glob("cems_inc_*"))) if inc_dir.is_dir() else 0
    log(f"保留策略：{keep}（0 = 不自动删）")
    log(f"删除 {len(removed)} 个，保留 {len(kept)} 个")
    log(f"差量层 incremental/ 有 {inc_count} 份，**不参与自动清理**："
        f"删掉链上任意一份会让它之后的差量全部无法单独恢复，"
        f"要清理必须先做链完整性核对（本轮未实现，见 docs/数据可靠性.md §1.5）")
    return 0


def list_backups(args: argparse.Namespace) -> int:
    out_root = Path(args.out).resolve()
    if not out_root.exists():
        log(f"{out_root} 不存在")
        return 0
    total = 0
    for tier in list(TIERS) + ["incremental"]:
        base = out_root / tier
        if not base.is_dir():
            continue
        dirs = sorted((p for p in base.glob("cems_*_*") if p.is_dir()), key=lambda p: p.name)
        for path in dirs:
            manifest_path = path / "manifest.json"
            rows = size = "?"
            if manifest_path.exists():
                m = json.loads(manifest_path.read_text(encoding="utf-8"))
                rows = m.get("dump", {}).get("reported_rows")
                size = human_bytes(m.get("inventory", {}).get("total_bytes"))
            total += 1
            print(f"  {tier:<12} {path.name}  rows={rows}  size={size}")
    print(f"共 {total} 份备份，落点 {out_root}")
    return 0


def main() -> int:
    ensure_utf8_stdout()
    parser = argparse.ArgumentParser(
        description="TDengine cems 库全量逻辑备份（taosdump）")
    parser.add_argument("--out", default=None,
                        help=f"备份落点（默认 {DEFAULT_OUT}，也可用 CEMS_BACKUP_DIR）")
    parser.add_argument("--tier", default="hourly", choices=TIERS, help="落到哪一档保留层")
    parser.add_argument("--container", default=TD_CONTAINER_DEFAULT)
    parser.add_argument("--db", default=TD_DB_DEFAULT)
    parser.add_argument("--stable", default=TD_STABLE_DEFAULT)
    parser.add_argument("--threads", type=int, default=4, help="taosdump 并发线程")
    parser.add_argument("--timeout", type=float, default=1800.0)
    parser.add_argument("--physical", action="store_true",
                        help="额外做一次卷级 tar（崩溃一致性，非事务一致）")
    parser.add_argument("--physical-image", default="alpine")
    parser.add_argument("--since", default=None,
                        help="时间窗差量导出起点（本地时间，如 '2026-10-01 20:00:00'）")
    parser.add_argument("--until", default=None, help="时间窗差量导出终点（与 --since 成对）")
    parser.add_argument("--rename-hint", action="store_true",
                        help="只在 manifest 的恢复命令示例里附上 -W 改名用法")
    parser.add_argument("--prune", action="store_true", help="只按保留策略清理，不备份")
    parser.add_argument("--prune-after", action="store_true",
                        help="备份成功后顺带按保留策略清理")
    parser.add_argument("--list", action="store_true", help="列出已有备份后退出")
    parser.add_argument("--keep-hourly", type=int, default=RETENTION_DEFAULT["hourly"])
    parser.add_argument("--keep-daily", type=int, default=RETENTION_DEFAULT["daily"])
    parser.add_argument("--keep-monthly", type=int, default=RETENTION_DEFAULT["monthly"])
    parser.add_argument("--keep-manual", type=int, default=RETENTION_DEFAULT["manual"])
    args = parser.parse_args()

    if args.out is None:
        import os
        args.out = os.getenv("CEMS_BACKUP_DIR", DEFAULT_OUT)

    if args.list:
        return list_backups(args)
    if args.prune:
        return prune_backups(args)
    rc = do_backup(args)
    if rc == 0 and args.prune_after:
        prune_backups(args)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
