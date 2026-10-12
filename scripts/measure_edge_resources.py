# =============================================================================
# 边缘侧资源占用采证脚本（**长跑**：默认每 30 秒采一次、跑 24 小时）
# -----------------------------------------------------------------------------
# 它解决的是"边缘计算"这个定位里唯一缺的那条证据：**这套东西跑在资源受限的机器上**。
# 光在 docker-compose.yml 里写 mem_limit / cpus 只是"声明"，本脚本采的是"实际占用"，
# 两者一起才说明 512 MiB / 1 vCPU 这个额度够不够。
#
# 它采谁：**只要边缘侧容器**（数采 `cems-device` + 网关 `cems-gateway` /
# `cems-gateway-plant2`）—— 平台侧（TDengine / EMQX / Redis / nginx / web）按定义
# 不跑在边缘盒子上，把它们算进来会把"边缘占用"这个数字直接稀释掉。
# 容器清单可用 `--containers` 覆盖；缺哪个容器只记一行 `container_missing`，
# 不当成失败（演练期间网关本来可能正在重建）。
#
# 产出：**追加**写 `docs/evidence/edge/resource-24h.csv`（CSV，可解析）。
#   ⚠️ 追加而不是覆盖：脚本被重启（或分几次跑）时上一段采样不能丢，
#      每行都带 `run_id` / `sample_no`，跨段也能分开统计。
#   ⚠️ 每行写盘后 `flush` + `os.fsync`：这台机器上进程可能被强杀，
#      不 fsync 的话最后一段采样会静静消失（采证脚本最忌讳这个）。
#   ⚠️⚠️ 为什么**长跑时另写一个 `--append-file`**：这份 CSV 是**入库的仓库证据**，
#      而采证还在跑 —— 每个采样点都在追加，`git status` 就永远不会干净，
#      评审看到的是"改了却没提交"。所以约定：**入库的是某个时刻的冻结快照**
#      （就是 `resource-24h.csv`），**继续采的那部分写 `resource-24h.append-<run_id>.csv`**
#      （该文件被 `.gitignore` 排除）。24 h 跑完后把两者拼起来覆盖 `resource-24h.csv`
#      再提交一次，即可得到"一份完整、且入库时是干净的"证据。
#      不传 `--append-file` 时行为完全照旧（只写 `--out`）。
#
# ⚠️ 必须**脱离 agent 会话**运行（会话一结束，挂在会话下的子进程会被一起收走）：
#     pwsh -NoProfile -Command "Start-Process -FilePath <python> `
#       -ArgumentList 'scripts\measure_edge_resources.py' -WindowStyle Hidden -PassThru"
#   验证它在跑、且**父进程已退出**见 runbook 的"采证脚本确实脱离会话"一节。
#
# 用法（在仓库根目录执行）：
#   python scripts/measure_edge_resources.py                    # 24h，每 30s
#   python scripts/measure_edge_resources.py --once             # 只采一次（自检用）
#   python scripts/measure_edge_resources.py --duration-hours 1 --interval 5
#
# 参数：
#   --out               产出 CSV 路径（默认 docs/evidence/edge/resource-24h.csv）
#   --append-file       额外再追加一份到该路径（给"长跑"用；见上面为什么要有它）
#   --interval          采样间隔秒数（默认 30）
#   --duration-hours    跑多久（默认 24；`--once` 时忽略）
#   --containers        逗号分隔的容器名（默认边缘侧三个）
#   --once              只采一次就退出（退出码 0 = 采到了）
#   --status-file       心跳文件（给"它是不是还活着"提供可核对的落点）
#
# 退出码：0 = 正常跑完 / `--once` 采到；1 = 参数或首次采样就失败（写盘不可用等）。
# 只读：只调 `docker stats --no-stream`，不 start/stop/inspect 之外不动任何容器。
# =============================================================================
"""边缘侧资源占用采证：每 30 秒记一次 docker stats（只记边缘侧容器），追加写 CSV。"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = PROJECT_ROOT / "docs" / "evidence" / "edge" / "resource-24h.csv"
DEFAULT_STATUS = PROJECT_ROOT / "docs" / "evidence" / "edge" / "resource-24h.status.json"

LOCAL_TZ = timezone(timedelta(hours=8))

#: 边缘侧容器（数采 + 网关）。第二台设备的网关同样跑在边缘，一起采。
DEFAULT_CONTAINERS = ["cems-device", "cems-gateway", "cems-gateway-plant2"]

FIELDS = [
    "ts_local",
    "run_id",
    "sample_no",
    "container",
    "cpu_percent",
    "mem_used_bytes",
    "mem_limit_bytes",
    "mem_percent",
    "pids",
]


def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def strip_unit(text: str) -> str:
    """`"12.3%"` → `"12.3"`：只剥掉百分号，不做单位换算。"""
    return text.strip().rstrip("%").strip()


#: docker stats 的内存单位后缀 → 字节倍数。
#: ⚠️ 只认 docker 自己会输出的那几个（`B/KiB/MiB/GiB/TiB`，1024 进制）；
#:    认不出来的**抛错**而不是当成字节返回 —— "23MiB 被记成 23 字节"这种
#:    静默错量级会让整份采证失效，宁可让这一行报错。
BYTE_UNITS = {
    "B": 1,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
}


def to_bytes(text: str) -> int:
    """`"123.4MiB"` → 字节数（int）。空串 → 0。"""
    token = text.strip()
    if not token:
        return 0
    digits = ""
    unit = ""
    for ch in token:
        if (ch.isdigit() or ch == ".") and not unit:
            digits += ch
        else:
            unit += ch
    if not digits:
        raise ValueError(f"无法解析的内存值: {text!r}")
    factor = BYTE_UNITS.get(unit.strip().upper())
    if factor is None:
        raise ValueError(f"未知的内存单位: {text!r}")
    return int(float(digits) * factor)


def stats_once(containers: list[str], timeout: float = 30.0) -> dict[str, dict[str, str]]:
    """采一次 docker stats，返回 {容器名: {cpu, mem_used, mem_limit, mem_percent, pids}}。

    ⚠️ 用 `--format json` 而不是 `--format "{{.CPUPerc}}..."`：
    后者要靠空格切列，容器名里一旦有计划外的空格就会串列；
    JSON 的输出结构由 docker 保证，且 `MemUsage` 自带 "已用 / 上限" 两个数
    （**上限那个数正好能反证 mem_limit 真的被 docker 认下来了**）。
    """
    cmd = ["docker", "stats", "--no-stream", "--format", "json"]
    # 显式点名要采的容器：不点名会把平台侧（tdengine/emqx/...）一起拉进来，
    # 它们不跑在边缘盒子上，混进来会把"边缘占用"稀释掉。
    cmd.extend(containers)
    out = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(f"docker stats 失败: {out.stderr.strip()}")
    result: dict[str, dict[str, str]] = {}
    for line in out.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        name = item.get("Name", "")
        result[name] = {
            "cpu": strip_unit(item.get("CPUPerc", "0")),
            "mem_used": to_bytes(item.get("MemUsage", "").partition("/")[0]),
            "mem_limit": to_bytes(item.get("MemUsage", "").partition("/")[2]),
            "mem_percent": strip_unit(item.get("MemPerc", "0")),
            "pids": int(float(strip_unit(item.get("PIDs", "0")) or 0)),
        }
    return result


def human_bytes(num: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(num) < 1024.0 or unit == "TiB":
            return f"{num:.1f}{unit}"
        num /= 1024.0
    return f"{num:.1f}TiB"


def open_csv(path: Path) -> tuple[object, csv.DictWriter, bool]:
    """打开（或新建）CSV；返回 (句柄, writer, 是否新文件)。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fresh = not path.exists() or path.stat().st_size == 0
    handle = path.open("a", encoding="utf-8", newline="")
    writer = csv.DictWriter(handle, fieldnames=FIELDS)
    if fresh:
        writer.writeheader()
        handle.flush()
        os.fsync(handle.fileno())
    return handle, writer, fresh


def write_status(path: Path, payload: dict[str, object]) -> None:
    """写心跳文件（小、可覆盖）：给"它是不是还在采"一个可核对的落点。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="边缘侧资源占用采证（只读 docker stats，追加写 CSV）"
    )
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="产出 CSV 路径")
    parser.add_argument(
        "--append-file",
        default="",
        help="额外再追加一份到该路径（给长跑用：入库的那份保持冻结快照）",
    )
    parser.add_argument("--interval", type=float, default=30.0, help="采样间隔秒数（默认 30）")
    parser.add_argument("--duration-hours", type=float, default=24.0, help="跑多久（默认 24 小时）")
    parser.add_argument(
        "--containers",
        default=",".join(DEFAULT_CONTAINERS),
        help="逗号分隔的容器名（默认边缘侧三个）",
    )
    parser.add_argument("--once", action="store_true", help="只采一次就退出（自检用）")
    parser.add_argument("--status-file", default=str(DEFAULT_STATUS), help="心跳文件路径")
    args = parser.parse_args()

    containers = [c.strip() for c in args.containers.split(",") if c.strip()]
    if not containers:
        print("[fatal] --containers 为空", flush=True)
        return 1

    out_path = Path(args.out)
    status_path = Path(args.status_file)
    run_id = f"{now_local().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    deadline = time.time() + args.duration_hours * 3600.0

    targets: list[Path] = [out_path]
    if args.append_file:
        targets.append(Path(args.append_file))
    handles: list[tuple[object, csv.DictWriter, Path]] = []
    try:
        for target in targets:
            handle, writer, fresh = open_csv(target)
            handles.append((handle, writer, target))
            print(f"[info] csv={target} fresh={fresh}", flush=True)
    except OSError as exc:
        print(f"[fatal] 产出文件不可写: {exc}", flush=True)
        for handle, _, _ in handles:
            handle.close()
        return 1

    print(f"[info] run_id={run_id}", flush=True)
    print(f"[info] out={out_path}", flush=True)
    print(f"[info] containers={containers}", flush=True)
    print(
        f"[info] interval={args.interval}s duration={args.duration_hours}h once={args.once}",
        flush=True,
    )

    sample_no = 0
    written = 0
    missing_hits = 0
    try:
        while True:
            sample_no += 1
            ts = now_local()
            try:
                stats = stats_once(containers)
            except Exception as exc:  # noqa: BLE001 —— 长跑脚本不能因"某次 docker 抖动"就退出
                print(f"[warn] sample {sample_no} 采样失败: {exc}", flush=True)
                stats = {}

            rows: list[dict[str, object]] = []
            for name in containers:
                row = stats.get(name)
                if row is None:
                    missing_hits += 1
                    rows.append(
                        {
                            "ts_local": ts.isoformat(timespec="seconds"),
                            "run_id": run_id,
                            "sample_no": sample_no,
                            "container": name,
                            "cpu_percent": "",
                            "mem_used_bytes": "",
                            "mem_limit_bytes": "",
                            "mem_percent": "",
                            "pids": "",
                        }
                    )
                    continue
                rows.append(
                    {
                        "ts_local": ts.isoformat(timespec="seconds"),
                        "run_id": run_id,
                        "sample_no": sample_no,
                        "container": name,
                        "cpu_percent": row["cpu"],
                        "mem_used_bytes": row["mem_used"],
                        "mem_limit_bytes": row["mem_limit"],
                        "mem_percent": row["mem_percent"],
                        "pids": row["pids"],
                    }
                )
                written += 1

            # 同一批行写进每一份目标文件（入库快照 + 长跑追加），再逐份 flush+fsync
            for handle, writer, _ in handles:
                writer.writerows(rows)
                handle.flush()
                os.fsync(handle.fileno())

            write_status(
                status_path,
                {
                    "run_id": run_id,
                    "pid": os.getpid(),
                    "started_ts": run_id.split("-")[0],
                    "last_sample_ts": ts.isoformat(timespec="seconds"),
                    "sample_no": sample_no,
                    "rows_written": written,
                    "missing_hits": missing_hits,
                    "interval_s": args.interval,
                    "duration_hours": args.duration_hours,
                    "containers": containers,
                    "out": str(out_path),
                    "append_file": args.append_file or None,
                },
            )

            head = stats.get(containers[0], {})
            print(
                f"[sample {sample_no:5d}] {ts.strftime('%H:%M:%S')} "
                f"{containers[0]} cpu={head.get('cpu', '-'):>6}% "
                f"mem={human_bytes(float(head.get('mem_used') or 0))}"
                f"/{human_bytes(float(head.get('mem_limit') or 0))}"
                f" rows={written}",
                flush=True,
            )
            if args.once:
                break
            if time.time() >= deadline:
                print("[info] duration reached", flush=True)
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("[info] interrupted by Ctrl+C", flush=True)
    finally:
        for handle, _, _ in handles:
            handle.close()

    print(f"[done] samples={sample_no} rows={written} missing={missing_hits}", flush=True)
    if args.once and written == 0:
        print("[fatal] --once 没采到任何容器", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
