# -*- coding: utf-8 -*-
"""TDengine 备份/恢复演练的共用底座（被 backup_tdengine.py 与 restore_drill_tdengine.py 引用）。

设计约束（为什么这么写）：

1. **一切 SQL 走容器内的 `taos` CLI，不走 taosAdapter REST。**
   理由：REST 返回的 `ts` 是 **UTC ISO 字符串**（本项目在
   `docs/工程完整度-nginx与redis.md` §8.2 第 10 条记录过由此造成的一次真实数据错位）。
   容器内 CLI 直接按容器 TZ 展示，而本模块更进一步：**所有时间边界与分段键都用
   `CAST(... AS BIGINT)` 取"纪元毫秒"**，两端比较的是同一个无量纲整数，
   时区、容器时钟漂移都不会混进结果（时钟漂移见 `docs/工程事实-容器时钟漂移.md`）。

2. **不假设容器名以外的东西。** 数据卷名一律从 `docker inspect` 里读，
   不硬编码 —— 本机同时存在 `cems_tdengine-data` 与 `cems-tdengine-data` 两个卷
   （连字符 vs 下划线），硬编码会静默备到那个陈旧的孤儿卷上（见 README/manifest 的卷核对字段）。

3. **本模块只读。** 没有任何写库语句；唯一的写操作由调用方在容器 `/tmp` 下做临时目录。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final, Iterable

# 项目时区：全部容器都以 Asia/Shanghai 运行（见 docker-compose.yml 的 TZ）
LOCAL_TZ: Final[timezone] = timezone(timedelta(hours=8))

TD_CONTAINER_DEFAULT: Final[str] = "tdengine"
TD_DB_DEFAULT: Final[str] = "cems"
TD_STABLE_DEFAULT: Final[str] = "cems_data"

# 业务列（与 src/common/points.py 的 COLUMNS 同源；tags 单列出来）。
# ⚠️ 这里只是"做校验时要逐列比对"的清单，不参与写入，改它不影响链路契约。
VALUE_COLUMNS: Final[tuple[str, ...]] = (
    "so2", "nox", "flow", "dust", "o2", "temp", "humidity", "pressure", "velocity",
)
TAG_COLUMNS: Final[tuple[str, ...]] = ("plant", "device")

TS_FMT: Final[str] = "%Y-%m-%d %H:%M:%S"


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #
def log(msg: str) -> None:
    print(f"[{datetime.now(LOCAL_TZ).strftime('%H:%M:%S')}] {msg}", flush=True)


def ensure_utf8_stdout() -> None:
    """Windows 控制台默认 GBK，中文日志会抛 UnicodeEncodeError（项目里踩过）。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass


def run(cmd: list[str], timeout: float = 600.0, check: bool = False) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"命令失败({proc.returncode}): {' '.join(cmd)}\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dir_inventory(root: Path) -> dict[str, Any]:
    """返回目录清单：文件数、总字节、逐文件 sha256。"""
    files: list[dict[str, Any]] = []
    total = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        size = path.stat().st_size
        total += size
        files.append({
            "rel": str(path.relative_to(root)).replace("\\", "/"),
            "bytes": size,
            "sha256": sha256_file(path),
        })
    return {"file_count": len(files), "total_bytes": total, "files": files}


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def ts_ms_to_str(ms: int | float) -> str:
    return datetime.fromtimestamp(int(ms) / 1000.0, LOCAL_TZ).strftime(TS_FMT)


# --------------------------------------------------------------------------- #
# docker / taos CLI
# --------------------------------------------------------------------------- #
def container_state(name: str) -> tuple[str, str]:
    proc = run(["docker", "inspect", name, "--format",
                "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}"])
    if proc.returncode != 0:
        return ("missing", "-")
    parts = proc.stdout.strip().split("|")
    return (parts[0] if parts else "?", parts[1] if len(parts) > 1 else "-")


def wait_container_healthy(name: str, timeout: float = 180.0) -> bool:
    """"healthy" 缺省时退化为 "running"（临时实例没有 healthcheck）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, health = container_state(name)
        if status == "running" and health in ("healthy", "-"):
            return True
        time.sleep(2.0)
    return False


def wait_taos_ready(container: str, timeout: float = 180.0) -> tuple[bool, str]:
    """判据是"能真的查出库列表"，不是容器 healthy。

    临时实例没有 healthcheck，而 tdengine 镜像的 healthy 只代表进程活着；
    能 `SHOW DATABASES` 才代表**可以开始恢复**，RTO 的起点才有意义。
    """
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        proc = run(["docker", "exec", container, "taos", "-s", "SHOW DATABASES;"], timeout=30)
        if proc.returncode == 0 and "Query OK" in proc.stdout:
            return True, proc.stdout
        last = (proc.stdout or "") + (proc.stderr or "")
        time.sleep(1.0)
    return False, last


def parse_taos_table(text: str) -> list[list[str]]:
    """解析 `taos -s` 的 ASCII 表格输出。

    格式固定为：
        <欢迎语>
        taos> SELECT ...
        <表头> |
        ========...
        <值>   |
        Query OK, N row(s) in set (...)

    ⚠️ **0 行时 taos 根本不打表头与分隔行**，只回一句 `Query OK, 0 row(s) in set`。
    所以"找不到分隔行"必须判成空结果集，而不是解析失败
    （本项目在这里踩过一次：db_exists 对一个不存在的库直接抛异常）。
    """
    lines = [ln.rstrip("\r") for ln in text.splitlines()]
    sep_idx = None
    for idx, ln in enumerate(lines):
        stripped = ln.strip()
        if stripped and set(stripped) == {"="}:
            sep_idx = idx
            break
    if sep_idx is None:
        if re.search(r"Query OK,\s*0\s+row", text):
            return []
        raise RuntimeError(f"无法解析 taos 输出（没有找到分隔行）:\n{text}")

    rows: list[list[str]] = []
    for ln in lines[sep_idx + 1:]:
        if ln.startswith("Query OK"):
            break
        if not ln.strip():
            continue
        cells = [c.strip() for c in ln.split("|")]
        while cells and cells[-1] == "":
            cells.pop()
        rows.append(cells)
    return rows


def taos_sql(container: str, sql: str, timeout: float = 120.0) -> list[list[str]]:
    proc = run(["docker", "exec", container, "taos", "-s", sql], timeout=timeout)
    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
    if "DB error" in combined:
        raise RuntimeError(f"TDengine 报错 SQL={sql!r}\n{combined}")
    return parse_taos_table(combined)


def taos_exec(container: str, sql: str, timeout: float = 120.0) -> str:
    """执行不返回结果集的语句（DDL / DROP），只校验没有报错。"""
    proc = run(["docker", "exec", container, "taos", "-s", sql], timeout=timeout)
    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
    if "DB error" in combined:
        raise RuntimeError(f"TDengine 报错 SQL={sql!r}\n{combined}")
    return combined


def taos_scalar(container: str, sql: str) -> str:
    rows = taos_sql(container, sql)
    if not rows or not rows[0]:
        raise RuntimeError(f"SQL 无返回: {sql!r}")
    return rows[0][0]


def db_exists(container: str, db: str) -> bool:
    rows = taos_sql(container, f"SELECT name FROM information_schema.ins_databases WHERE name='{db}';")
    return bool(rows) and rows[0][0] == db


# --------------------------------------------------------------------------- #
# 库内计数与分段（校验的唯一取数口径）
# --------------------------------------------------------------------------- #
def count_rows(container: str, db: str, stable: str,
               lo_ms: int | None = None, hi_ms: int | None = None) -> int:
    where = ""
    if lo_ms is not None and hi_ms is not None:
        where = f" WHERE ts >= {int(lo_ms)} AND ts <= {int(hi_ms)}"
    return int(taos_scalar(container, f"SELECT COUNT(*) FROM {db}.{stable}{where};"))


def ts_bounds(container: str, db: str, stable: str) -> tuple[int, int]:
    """首末时间戳（纪元毫秒）。

    ⚠️ 不用 `MAX(ts)`/`MIN(ts)`：TDengine 3.3.6 对 `MAX(ts)` 报
    `Invalid data type: max`（项目在 scripts/measure_resend_success.py 里记过同一条），
    所以统一走 `ORDER BY ts LIMIT 1`。
    """
    min_ms = int(taos_scalar(
        container, f"SELECT CAST(ts AS BIGINT) FROM {db}.{stable} ORDER BY ts ASC LIMIT 1;"))
    max_ms = int(taos_scalar(
        container, f"SELECT CAST(ts AS BIGINT) FROM {db}.{stable} ORDER BY ts DESC LIMIT 1;"))
    return min_ms, max_ms


def segment_rows(container: str, db: str, stable: str, unit: str,
                 lo_ms: int, hi_ms: int) -> dict[str, Any]:
    """按 `unit`（1h / 1d）分段汇总：条数 + 逐列 SUM + 桶内首末 ts。

    返回 {"windows": {<桶起始ms>: {...}}, "n_windows": n, "total_rows": m}

    ⚠️ 用 `FIRST(ts)/LAST(ts)` 而不是 `MIN(ts)/MAX(ts)`：TDengine 3.3.6 对
    timestamp 列不支持 MIN/MAX（报 `Invalid parameter data type : min`），
    而 FIRST/LAST 在 INTERVAL 查询里就是桶内首末时间戳（实测已核）。
    """
    sums = ", ".join(f"SUM({c}) AS s_{c}" for c in VALUE_COLUMNS)
    sql = (
        f"SELECT CAST(_wstart AS BIGINT) AS w, COUNT(*) AS n, {sums}, "
        f"CAST(FIRST(ts) AS BIGINT) AS tmin, CAST(LAST(ts) AS BIGINT) AS tmax "
        f"FROM {db}.{stable} WHERE ts >= {int(lo_ms)} AND ts <= {int(hi_ms)} "
        f"INTERVAL({unit});"
    )
    windows: dict[str, Any] = {}
    total = 0
    for row in taos_sql(container, sql):
        w = int(row[0])
        n = int(row[1])
        sums_map: dict[str, float | None] = {}
        for idx, col in enumerate(VALUE_COLUMNS):
            raw = row[2 + idx]
            sums_map[col] = None if raw.upper() in ("NULL", "") else float(raw)
        windows[str(w)] = {
            "n": n,
            "sums": sums_map,
            "tmin": int(row[2 + len(VALUE_COLUMNS)]),
            "tmax": int(row[3 + len(VALUE_COLUMNS)]),
        }
        total += n
    return {"unit": unit, "windows": windows, "n_windows": len(windows), "total_rows": total}


def table_schema(container: str, db: str, stable: str) -> list[list[str]]:
    return taos_sql(container, f"DESCRIBE {db}.{stable};")


def db_properties(container: str, db: str) -> dict[str, str]:
    """库属性（保留期/精度/副本数都在这）—— 恢复保真度必须核这几项。"""
    cols = ("name", "vgroups", "replica", "duration", "keep", "minrows", "maxrows",
            "comp", "precision", "strict", "wal_level")
    rows = taos_sql(
        container,
        "SELECT " + ", ".join(f"`{c}`" for c in cols)
        + f" FROM information_schema.ins_databases WHERE name='{db}';",
    )
    if not rows:
        return {}
    return {c: rows[0][i] for i, c in enumerate(cols) if i < len(rows[0])}


def tag_values(container: str, db: str, stable: str) -> list[list[str]]:
    tags = ", ".join(TAG_COLUMNS)
    sql = f"SELECT DISTINCT {tags} FROM {db}.{stable} ORDER BY {tags};"
    return taos_sql(container, sql)


# --------------------------------------------------------------------------- #
# 卷（物理级）
# --------------------------------------------------------------------------- #
def resolve_mounts(container: str) -> list[dict[str, str]]:
    """从运行中的容器读出**实际**挂载，绝不硬编码卷名。"""
    proc = run(["docker", "inspect", container, "--format", "{{json .Mounts}}"], check=True)
    mounts = json.loads(proc.stdout)
    return [
        {"type": m.get("Type", ""), "name": m.get("Name", ""),
         "src": m.get("Source", ""), "dst": m.get("Destination", "")}
        for m in mounts
    ]


def volume_dir_size_bytes(volume: str) -> int | None:
    """用一次性 alpine 容器量某个卷的占用（只读挂载）。"""
    proc = run([
        "docker", "run", "--rm", "-v", f"{volume}:/src:ro", "alpine",
        "sh", "-c", "du -sb /src | cut -f1",
    ], timeout=300)
    if proc.returncode != 0:
        return None
    try:
        return int(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None


def parse_backup_dir(path: Path) -> dict[str, Any]:
    """加载一个备份目录的 manifest，并做最小完整性核对。"""
    manifest = read_json(path / "manifest.json")
    files = list(path.rglob("*.avro"))
    manifest["_dir"] = str(path)
    manifest["_avro_files"] = len(files)
    manifest["_exists"] = True
    return manifest


def newest_backup(out_root: Path, tier: str | None = None) -> Path:
    if not out_root.exists():
        raise FileNotFoundError(f"备份根目录不存在：{out_root}")
    if tier:
        tiers = [tier]
    else:
        tiers = [d.name for d in out_root.iterdir() if d.is_dir()]
    candidates: list[Path] = []
    for t in tiers:
        base = out_root / t
        if not base.is_dir():
            continue
        candidates.extend(p for p in base.glob("cems_full_*") if (p / "manifest.json").exists())
    if not candidates:
        raise FileNotFoundError(f"{out_root} 下没有可用的备份（tier={tier or 'any'}）")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def human_bytes(n: int | float | None) -> str:
    if n is None:
        return "?"
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024.0 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024.0
    return f"{value:.1f} TiB"


def env_path(name: str, default: str) -> str:
    return os.getenv(name, default)


def iter_dirs(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.iterdir() if p.is_dir())
