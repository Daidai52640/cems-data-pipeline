# =============================================================================
# 边缘侧"自治"演练：断链（北向失联）与崩溃自愈 —— 两件事都**不碰宿主机网络**
# -----------------------------------------------------------------------------
# 项目定位是"边缘计算"，那么有一类能力必须自己证明：**边缘节点在失去云侧之后能不能
# 自己扛住，回来之后数据对不对得上**。本脚本做两个实验、贴原始输出：
#
#   ① `--mode linkcut` —— **断链自治**
#      手法：`docker stop emqx`（断 60 秒）→ `docker start emqx` → 对账。
#      ⚠️ 断的是**容器**，不是宿主网卡：不拔网线、不 `netsh`、不动 COM3，
#         宿主上的 `serial_bridge.py` 全程照跑，真机链路不受影响。
#
#      ⚠️⚠️ **为什么最后用这一手 —— 两种更"像边缘"的做法都实测失败，留档免得下次再试：**
#        (a) `docker network disconnect cems-net cems-gateway`（只断这一个节点的网络）
#            —— **实测失败**：网关只挂在 `cems-net` 一张网上，断掉它 = 连 **Modbus 的设备层
#            也一起断**（日志：`Connection to (device, 5020) failed: [Errno -3] Temporary
#            failure in name resolution`）。结果是**网关根本采不到数**，队列里 0 行、
#            "没丢数据"变成了"本来就没产生数据"，断链自治这条**根本没被验证到**。
#            见本次失败的原始输出：`docs/evidence/edge/edge_autonomy_linkcut_20261011T182933.txt`。
#        (b) 在容器里把 broker 的名字改指到黑洞（`/etc/hosts` 加 `127.0.0.1 emqx`，只断北向）
#            —— **实测失败**：paho 已经建好的 TCP 会话**不会因为改 hosts 而断开**
#            （hosts 只在下一次重连解析时生效），而 `is_connected()` 一直是 True →
#            网关继续"发得成功"，队列 0 行。实测持续 2 分钟无断开、无积压。
#        所以选"停 broker"：它**立刻**关掉 TCP，网关的 `on_disconnect` 当轮就触发、
#        立刻转本地缓存 —— 这是三种做法里唯一能在 60 秒窗口内真正造出积压的。
#      ⚠️ 代价（如实记）：停 broker 打掉的是**两台网关**（plant1 与 plant2）的北向，
#        不是"单台边缘节点失联"；对账仍按 `--device` 分设备做，两台的数据各算各的。
#
#   ② `--mode crash` —— **崩溃自愈**
#      手法：`docker kill cems-gateway`（SIGKILL，不给优雅退出的机会）→ 靠
#      `restart: unless-stopped` 自动拉起 → 验证数据继续入库、时间戳连续。
#
# 两个实验共用的对账口径（分母是"该补的条数"，与 `measure_resend_success.py` 一致）：
#   该补条数 N = 网关落盘队列（cache.jsonl + inflight-*.jsonl）里所有报文中，
#                时间戳**晚于**"故障前库内 MAX(ts)"的那些（更早的早就送达了）
#   补上条数 M = 恢复排空后在库里真的查到的条数
#   恒等式     = 故障前条数 + N + 恢复后新产生 - 残留 = 恢复后条数
#
# ⚠️ 设备维度：`cems_data` 是两台设备共用的超级表，所有读库都带 `--device` 谓词
#    （不过滤会把另一台设备的行算成"补上了"）。队列侧无设备维度：`GATEWAY_DATA_DIR`
#    是每实例一份，第一个网关读的是宿主 `./data`。
#
# 用法（在仓库根目录执行，需要 10 个容器在跑）：
#   python scripts/drill_edge_autonomy.py --mode linkcut --outage 60
#   python scripts/drill_edge_autonomy.py --mode crash
#   python scripts/drill_edge_autonomy.py --mode linkcut --dry-run     # 只打印将做什么
#
# 参数：
#   --mode          linkcut | crash
#   --outage        断链时长秒数（仅 linkcut，默认 60）
#   --device        读库按哪台设备对账（默认 device1，见 `_device_scope.py`）
#   --drain-timeout 等队列排空的上限秒数（默认 300）
#   --container     被演练的容器（默认 cems-gateway）
#   --tag           证据文件名后缀（默认用时间戳）
#   --out           json 落点（默认 docs/evidence/edge/edge_autonomy_<mode>_<tag>.json）
#   --dry-run       只打印计划，不动任何东西
#
# 退出码：0 = 演练完成且**对账通过**；2 = 演练完成但**对账不通过**（原始输出仍会落盘）；
#         1 = 前置条件不满足 / 中途异常（脚本会尽力把 emqx 与被演练容器恢复原状）。
# 只读性：脚本自身不写库、不改 `src/`；它只做 `docker stop|start emqx` / `docker kill`，
#         以及读宿主上的 `data/`、`docker logs`、`emqx ctl clients list`。
# =============================================================================
"""边缘侧自治演练：断链自治（停 broker 60 s）与崩溃自愈（docker kill）实测 + 对账。"""

from __future__ import annotations

import argparse
import base64
import itertools
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ⚠️ Windows 控制台默认 GBK：不显式重配的话，本脚本打的中文会变成一堆 `????`/乱码
#    （实测：`python -c "print('中文')"` 经 pwsh 捕获后是乱码）。
#    这里把 stdout/stderr 重配成 UTF-8 —— 只影响"怎么编码输出字节"，
#    不影响落盘证据（那些本来就是 `encoding="utf-8"`）。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from _device_scope import (
    add_device_argument,
    device_predicate,
    resolve_device,
)

DATA_DIR = PROJECT_ROOT / "data"
CACHE_FILE = DATA_DIR / "cache.jsonl"
EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence" / "edge"

NETWORK = "cems-net"
POLL_INTERVAL = 5.0                 # 与 docker-compose.yml 的 POLL_INTERVAL 一致
TD_REST_URL = "http://127.0.0.1:6041/rest/sql"
TD_AUTH = "Basic " + base64.b64encode(b"root:taosdata").decode()
TD_DB = "cems"
TD_STABLE = "cems_data"
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
LOCAL_TZ = timezone(timedelta(hours=8))

EMQX_API = "http://127.0.0.1:18083/api/v5"
EMQX_AUTH = "Basic " + base64.b64encode(b"admin:public").decode()
#: broker 侧要盯的两个网关 client_id（见 docker-compose.yml 的 MQTT_CLIENT_ID）
WATCH_CLIENTS = ["cems-gateway-plant1", "cems-gateway-plant2"]

ALL_CONTAINERS = [
    "emqx",
    "tdengine",
    "cems-device",
    "cems-gateway",
    "cems-gateway-plant2",
    "cems-subscriber",
    "cems-subscriber-plant2",
    "cems-redis",
    "cems-nginx",
    "cems-web",
]


# ---------------------------------------------------------------- 通用小工具

def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def stamp() -> str:
    return now_local().strftime("%Y%m%dT%H%M%S")


def parse_local(text: str) -> float:
    return datetime.strptime(text, TS_FORMAT).replace(tzinfo=LOCAL_TZ).timestamp()


def parse_iso_utc(text: str) -> float:
    """TDengine REST 返回的 ts 是 **UTC** 字符串（本仓库既有脚本的同一口径）。"""
    cleaned = text.strip().replace("Z", "").replace("T", " ")
    if "." in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        ).timestamp()
    return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    ).timestamp()


def run(cmd: list[str], timeout: float = 60.0, check: bool = True) -> subprocess.CompletedProcess:
    """跑一条 docker 命令。

    ⚠️ **必须显式 `encoding="utf-8"` + `errors="replace"`**：本机控制台是 GBK，
    `text=True` 默认按 GBK 解码，而 `docker logs` 里是 UTF-8 中文 ——
    实测会直接抛 `UnicodeDecodeError: 'gbk' codec can't decode byte 0xad`
    （在子进程的 reader 线程里抛，主流程只看到一个莫名的 `None`）。
    """
    out = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    if check and out.returncode != 0:
        raise RuntimeError(f"命令失败 {cmd}: {(out.stderr or '').strip()}")
    return out


def container_state(name: str) -> tuple[str, str, str]:
    """(status, health, started_at) —— StartedAt 是"自愈实验"的关键一列。"""
    out = run(
        [
            "docker",
            "inspect",
            name,
            "--format",
            (
                "{{.State.Status}}|"
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}|"
                "{{.State.StartedAt}}"
            ),
        ],
        check=False,
    )
    parts = out.stdout.strip().split("|")
    if len(parts) < 3:
        return ("?", "-", "?")
    return (parts[0], parts[1], parts[2])


def all_states() -> dict[str, tuple[str, str, str]]:
    return {name: container_state(name) for name in ALL_CONTAINERS}


def attach_states() -> dict[str, dict[str, str]]:
    """网关挂在哪几个网络上（断链实验的前后对照就靠它）。"""
    out = run(
        ["docker", "inspect", "cems-gateway", "--format",
         "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}"],
        check=False,
    )
    names = out.stdout.split()
    return {"cems-gateway_networks": names, "attached": NETWORK in names}


def emqx_clients_line() -> list[str]:
    """问 broker：现在有哪些客户端连着 —— 断链/恢复的**外部旁证**。

    ⚠️ 走容器内的 `emqx ctl clients list`，**不走 Dashboard HTTP API**：
    实测本机 EMQX 6.3.0 上 `/api/v5/clients` 返回 **401**
    （`/api/v5/status` 却正常），默认 `admin/public` 对 clients 端点不生效；
    再为它去建 API key 属于给演练引入新的认证依赖，不如直接用容器自带的管理命令。
    ⚠️ 只看队列排空有个漏洞：队列空也可能是"根本没有新数据"。
    这里直接读到"网关这个 client_id 在不在 broker 的客户端表里"，才是链路真回来的证据。
    """
    out = run(["docker", "exec", "emqx", "emqx", "ctl", "clients", "list"], timeout=60, check=False)
    rows: list[str] = []
    for line in out.stdout.splitlines():
        line = line.strip()
        if not line.startswith("Client("):
            continue
        name = line[len("Client("):].split(",", 1)[0]
        if name in WATCH_CLIENTS:
            rows.append(line)
    return rows


def emqx_connected_ids() -> set[str]:
    return {
        row[len("Client("):].split(",", 1)[0]
        for row in emqx_clients_line()
    }


# ---------------------------------------------------------------- 读库 / 读队列

def td_query(sql: str, timeout: float = 15.0) -> list[list[str]]:
    req = urllib.request.Request(
        TD_REST_URL, data=sql.encode("utf-8"), headers={"Authorization": TD_AUTH}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("code") != 0:
        raise RuntimeError(f"TDengine 返回错误: {payload}")
    return payload.get("data") or []


def count_rows(device: str) -> int:
    rows = td_query(
        f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE} WHERE {device_predicate(device)}"
    )
    return int(rows[0][0])


def max_ts(device: str) -> float:
    rows = td_query(
        f"SELECT ts FROM {TD_DB}.{TD_STABLE} WHERE {device_predicate(device)} "
        f"ORDER BY ts DESC LIMIT 1"
    )
    if not rows:
        raise RuntimeError("库内无数据，无法建立基线")
    return parse_iso_utc(rows[0][0])


def ts_set_between(start: float, end: float, device: str) -> set[int]:
    rows = td_query(
        f"SELECT ts FROM {TD_DB}.{TD_STABLE} "
        f"WHERE ts >= {int(start * 1000)} AND ts <= {int(end * 1000)} "
        f"AND {device_predicate(device)} ORDER BY ts"
    )
    return {int(parse_iso_utc(r[0])) for r in rows}


def queue_files() -> list[Path]:
    """落盘队列 = 在途段（按 seq 升序）+ cache.jsonl。"""
    files: list[Path] = []
    if DATA_DIR.is_dir():
        files.extend(sorted(DATA_DIR.glob("inflight-*.jsonl")))
    if CACHE_FILE.exists():
        files.append(CACHE_FILE)
    return files


def queue_lines() -> list[str]:
    lines: list[str] = []
    for path in queue_files():
        try:
            lines.extend(l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip())
        except OSError:
            continue
    return lines


def queue_bytes() -> int:
    total = 0
    for path in queue_files():
        try:
            total += path.stat().st_size
        except OSError:
            pass
    return total


def queue_snapshot() -> dict[str, object]:
    lines = queue_lines()
    ts: list[int] = []
    malformed = 0
    for line in lines:
        parts = line.split()
        if len(parts) < 2:
            malformed += 1
            continue
        try:
            ts.append(int(parse_local(f"{parts[0]} {parts[1]}")))
        except ValueError:
            malformed += 1
    return {
        "files": [p.name for p in queue_files()],
        "bytes": queue_bytes(),
        "lines": len(lines),
        "malformed_lines": malformed,
        "unique_ts": len(set(ts)),
        "ts": sorted(set(ts)),
    }


def wait_healthy(name: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, health, _ = container_state(name)
        if status == "running" and health in ("healthy", "-"):
            return True
        time.sleep(2.0)
    return False


def wait_drained(timeout: float) -> float | None:
    """等落盘队列排空，返回排空时刻的时间戳（None = 超时）。"""
    start = time.time()
    while time.time() - start < timeout:
        if queue_bytes() == 0:
            return time.time()
        time.sleep(2.0)
    return None


# ---------------------------------------------------------------- 演练主体

def do_linkcut(args, result: dict, lines: list[str]) -> None:
    device = result["device"]

    lines.append("")
    lines.append("=== [0] 前置：10 个容器状态 + broker 客户端表 ===")
    before_states = all_states()
    for name, (status, health, started) in before_states.items():
        lines.append(f"  {name:22s} {status:8s} {health:9s} {started}")
    result["states_before"] = {k: list(v) for k, v in before_states.items()}
    clients_before = emqx_clients_line()
    result["broker_clients_before"] = clients_before
    lines.append("  --- emqx ctl clients list（网关两实例）---")
    for row in clients_before or ["  （没读到网关客户端 —— 前置不满足，继续但结果不可信）"]:
        lines.append(f"  {row}")

    base_rows = count_rows(device)
    base_max = max_ts(device)
    result["baseline"] = {
        "at": now_local().strftime(TS_FORMAT),
        "rows": base_rows,
        "max_ts": datetime.fromtimestamp(base_max, LOCAL_TZ).strftime(TS_FORMAT),
        "queue": {k: v for k, v in queue_snapshot().items() if k != "ts"},
    }
    lines.append(
        f"  基线 [{device}]: 库内 {base_rows} 条，MAX(ts)="
        f"{result['baseline']['max_ts']}，落盘队列 {result['baseline']['queue']['lines']} 行"
    )

    lines.append("")
    lines.append("=== [1] 断开北向：docker stop emqx（边缘与云之间唯一的北向出口）===")
    t_cut = time.time()
    run(["docker", "stop", "emqx"], timeout=120)
    result["cut"] = {
        "at": now_local().strftime(TS_FORMAT),
        "method": "docker stop emqx",
        "emqx_state": list(container_state("emqx")),
    }
    lines.append(f"  已停 @ {result['cut']['at']} → emqx {result['cut']['emqx_state']}")
    lines.append("  设备侧不受影响：Modbus 走容器内网，与 broker 无关（下面每轮都会核）")

    # 断链期间：每 15 秒看一次队列（必须看到它在长）+ 设备侧是否还在采
    while time.time() - t_cut < args.outage:
        time.sleep(min(15.0, max(1.0, args.outage - (time.time() - t_cut))))
        snap = queue_snapshot()
        alive = Path(DATA_DIR / ".gateway_alive")
        age = round(time.time() - alive.stat().st_mtime, 1) if alive.exists() else None
        lines.append(
            f"  +{time.time() - t_cut:5.0f}s 落盘队列 {snap['bytes']:7d} B / "
            f"{snap['lines']:3d} 行 / 去重 {snap['unique_ts']:3d} 条"
            f" | 网关心跳 age={age}s（本地采集循环仍在转）"
        )
    outage_s = time.time() - t_cut

    snap = queue_snapshot()
    result["snapshot"] = {k: v for k, v in snap.items() if k != "ts"}
    lines.append(
        f"  快照 @ {now_local().strftime(TS_FORMAT)}: 文件 {snap['lines']} 行 / "
        f"去重 {snap['unique_ts']} 条报文"
    )
    snap_path = EVIDENCE_DIR / f"edge_linkcut_queue_snapshot_{result['tag']}.txt"
    snap_path.write_text("\n".join(queue_lines()) + "\n", encoding="utf-8")
    result["snapshot"]["evidence_file"] = snap_path.name
    lines.append(f"  队列快照落盘: {snap_path.name}")

    should_resend = [t for t in snap["ts"] if t > base_max]
    before_base = [t for t in snap["ts"] if t <= base_max]
    result["reconciliation"] = {
        "should_resend_N": len(should_resend),
        "snapshot_at_or_before_baseline": len(before_base),
        "outage_seconds": round(outage_s, 1),
    }
    lines.append(
        f"  该补条数 N = {len(should_resend)}（快照里 ≤ 基线的 {len(before_base)} 条不计入）"
    )
    # 网关日志：断开那一刻它自己怎么说的（原始输出）
    logs = run(
        ["docker", "logs", "cems-gateway", "--since", f"{int(outage_s) + 30}s"],
        timeout=60, check=False,
    )
    log_lines = [l for l in (logs.stdout + logs.stderr).splitlines() if l.strip()]
    result["gateway_logs_during_outage"] = log_lines[-15:]
    lines.append("  --- cems-gateway 日志（断链窗口内，原始输出）---")
    for row in result["gateway_logs_during_outage"] or ["  （窗口内无日志）"]:
        lines.append(f"  {row}")

    lines.append("")
    lines.append("=== [2] 恢复北向：docker start emqx ===")
    t_restore = time.time()
    run(["docker", "start", "emqx"], timeout=120)
    healthy = wait_healthy("emqx", 180)
    result["restore"] = {
        "at": now_local().strftime(TS_FORMAT),
        "emqx_healthy": healthy,
        "emqx_state": list(container_state("emqx")),
    }
    lines.append(f"  已启动 @ {result['restore']['at']}，emqx healthy={healthy}")

    # 等网关与 broker 重新握手（外部旁证：broker 的客户端表里重新出现它）
    reconnected = False
    clients_after: list[str] = []
    deadline = time.time() + 180
    while time.time() < deadline:
        clients_after = emqx_clients_line()
        if len(clients_after) >= len(WATCH_CLIENTS):
            reconnected = True
            break
        time.sleep(3.0)
    result["broker_reconnect"] = {
        "connected": reconnected,
        "clients": clients_after,
        "at": now_local().strftime(TS_FORMAT),
        "seconds_after_restart": round(time.time() - t_restore, 1),
    }
    lines.append(f"  broker 客户端表恢复: {reconnected}（+{result['broker_reconnect']['seconds_after_restart']}s）")
    for row in clients_after:
        lines.append(f"  {row}")

    drained_at = wait_drained(args.drain_timeout)
    result["drain"] = {
        "drained": drained_at is not None,
        "seconds": round(drained_at - t_restore, 1) if drained_at else None,
        "leftover_lines": len(queue_lines()),
    }
    lines.append(
        f"  队列排空 {'完成' if drained_at else '超时'}，耗时 "
        f"{result['drain']['seconds']} s，残留 {result['drain']['leftover_lines']} 行"
    )

    time.sleep(5.0)
    after_rows = count_rows(device)
    after_max = max_ts(device)
    window_start = (min(should_resend) - 1) if should_resend else base_max - 1
    window_end = time.time() + 1
    db_ts = ts_set_between(window_start, window_end, device) if should_resend else set()
    snap_set = set(snap["ts"])
    delivered = [t for t in should_resend if t in db_ts]
    missing = [t for t in should_resend if t not in db_ts]
    live_rows = sorted(t for t in db_ts if t not in snap_set)

    result["after"] = {
        "at": now_local().strftime(TS_FORMAT),
        "rows": after_rows,
        "max_ts": datetime.fromtimestamp(after_max, LOCAL_TZ).strftime(TS_FORMAT),
        "queue_lines": len(queue_lines()),
    }
    result["reconciliation"].update(
        {
            "delivered_M": len(delivered),
            "missing": len(missing),
            "success_rate": (len(delivered) / len(should_resend)) if should_resend else None,
            "live_rows_after_recovery": len(live_rows),
            "db_growth": after_rows - base_rows,
        }
    )
    ledger = {
        "断开前库内(条)": base_rows,
        "断开前 MAX(ts)": result["baseline"]["max_ts"],
        "断开时长(s)": round(outage_s, 1),
        "理论应产生(条, 时长/5.0)": round(outage_s / POLL_INTERVAL, 1),
        "实际积压=该补 N(条)": len(should_resend),
        "恢复后补上 M(条)": len(delivered),
        "补传成功率 M/N": result["reconciliation"]["success_rate"],
        "快照后新产生并入库(条)": len(live_rows),
        "库内新增(条)": after_rows - base_rows,
        "残留(行)": result["drain"]["leftover_lines"],
        "恒等式 基线+N+新产生-残留==恢复后": base_rows + len(should_resend) + len(live_rows)
        - result["drain"]["leftover_lines"] == after_rows,
        "恢复后 MAX(ts)": result["after"]["max_ts"],
    }
    result["ledger"] = ledger
    lines.append("")
    lines.append("===== 对账台账（断链自治）=====")
    for key, value in ledger.items():
        lines.append(f"  {key:34s}: {value}")
    if missing:
        lines.append(
            "  ⚠️ 未补上的时间戳: "
            + ", ".join(
                datetime.fromtimestamp(t, LOCAL_TZ).strftime(TS_FORMAT) for t in missing[:20]
            )
        )
    result["verdict"] = {
        "backlog_created": len(should_resend) > 0,
        "no_data_loss": len(should_resend) > 0 and not missing,
        "identity_ok": bool(ledger["恒等式 基线+N+新产生-残留==恢复后"]),
        "queue_drained": drained_at is not None,
        "reconnected": reconnected,
    }
    lines.append(f"  判定: {json.dumps(result['verdict'], ensure_ascii=False)}")


def do_crash(args, result: dict, lines: list[str]) -> None:
    device = result["device"]
    container = args.container

    lines.append(f"=== [0] 前置：{container} 当前状态 ===")
    status, health, started = container_state(container)
    result["before"] = {"status": status, "health": health, "started_at": started}
    lines.append(f"  {container}: status={status} health={health} started_at={started}")

    # 采集期内的逐条时间戳，用来判"有没有断档"
    base_rows = count_rows(device)
    base_max = max_ts(device)
    result["baseline"] = {
        "at": now_local().strftime(TS_FORMAT),
        "rows": base_rows,
        "max_ts": datetime.fromtimestamp(base_max, LOCAL_TZ).strftime(TS_FORMAT),
        "queue_lines": len(queue_lines()),
    }
    lines.append(
        f"  基线 [{device}]: 库内 {base_rows} 条，MAX(ts)={result['baseline']['max_ts']}"
    )

    lines.append("")
    lines.append(f"=== [1] 崩溃：docker kill {container}（SIGKILL，不给优雅退出机会）===")
    # 前置：容器必须先在跑，否则 kill 会直接报 "is not running"（演练就白做了）
    status0, health0, _ = container_state(container)
    if status0 != "running":
        lines.append(f"  前置：{container} 不在跑（{status0}），先拉起来再崩...")
        run(["docker", "start", container], timeout=180, check=False)
        if not wait_healthy(container, 180.0):
            raise RuntimeError(f"{container} 起不来，崩溃演练无法进行")
        status0, health0, _ = container_state(container)
        lines.append(f"  前置完成: status={status0} health={health0}")
    t_kill = time.time()
    run(["docker", "kill", container])
    result["kill"] = {"at": now_local().strftime(TS_FORMAT)}
    status, health, _ = container_state(container)
    result["kill"]["state_after_kill"] = {"status": status, "health": health}
    lines.append(f"  已 kill @ {result['kill']['at']} → status={status} health={health}")

    # ---- 阶段 1：看 `restart: unless-stopped` 会不会自己把它拉回来 ----
    # ⚠️ 实测（2026-10-11）：**不会**。`docker kill` 在 Docker 的语义里是"外部显式停止"，
    #    与"进程自己崩掉"不同；实测观察 180 s 始终 `exited`、RestartCount 恒为 0。
    #    所以下面分两段看：先如实记录"等 --auto-wait 秒没有自愈"，
    #    再走运维真实会走的那一步（`docker compose up -d <svc>`）证明**能恢复且数据不丢**。
    #    这个区分必须写进结论，否则"自愈"两个字是虚的。
    saw_down = status != "running"
    auto_up_at = None
    started_at_after = ""
    polls: list[str] = []
    deadline = time.time() + args.auto_wait
    while time.time() < deadline:
        time.sleep(3.0)
        st, hl, sa = container_state(container)
        polls.append(f"  +{time.time() - t_kill:5.0f}s status={st:9s} health={hl:9s} started_at={sa}")
        if st != "running":
            saw_down = True
        if st == "running" and hl in ("healthy", "-"):
            auto_up_at = time.time()
            started_at_after = sa
            break
    lines.extend(polls)
    lines.append(
        f"  等待 {args.auto_wait:.0f} s 的自动重启结果: "
        f"{'自己起来了（+%.0f s）' % (auto_up_at - t_kill) if auto_up_at else '未自动重启'}"
    )

    manual_up_at = None
    if auto_up_at is None:
        lines.append("")
        lines.append(f"=== [1b] 显式恢复：docker compose up -d {container.removeprefix('cems-')} ===")
        t_manual = time.time()
        svc = container.removeprefix("cems-")
        out = run(
            ["docker", "compose", "--profile", "plant2", "up", "-d", svc],
            timeout=180, check=False,
        )
        for row in (out.stdout or "").splitlines():
            if row.strip():
                lines.append(f"  {row.strip()}")
        run(["docker", "start", container], check=False)
        if wait_healthy(container, 180.0):
            manual_up_at = time.time()
            _, _, started_at_after = container_state(container)
        lines.append(
            f"  显式恢复: healthy={manual_up_at is not None} 用时 "
            f"{round(manual_up_at - t_manual, 1) if manual_up_at else None} s，"
            f"StartedAt={started_at_after}"
        )

    came_up_at = auto_up_at or manual_up_at
    result["recovery"] = {
        "saw_down": saw_down,
        "auto_restarted": auto_up_at is not None,
        "auto_wait_seconds": args.auto_wait,
        "came_up_at": datetime.fromtimestamp(came_up_at, LOCAL_TZ).strftime(TS_FORMAT)
        if came_up_at else None,
        "restart_seconds": round(came_up_at - t_kill, 1) if came_up_at else None,
        "recovered_by": "restart policy" if auto_up_at else ("docker compose up -d" if manual_up_at else "none"),
        "started_at_after": started_at_after,
        "restart_count": run(
            ["docker", "inspect", container, "--format", "{{.RestartCount}}"], check=False
        ).stdout.strip(),
        "mem_limit_after_recovery_bytes": run(
            ["docker", "inspect", container, "--format", "{{.HostConfig.Memory}}"], check=False
        ).stdout.strip(),
    }
    lines.append(
        f"  恢复结论: 由「{result['recovery']['recovered_by']}」恢复，"
        f"总停机 {result['recovery']['restart_seconds']} s，"
        f"RestartCount={result['recovery']['restart_count']}，"
        f"恢复后 mem_limit={result['recovery']['mem_limit_after_recovery_bytes']} B"
    )

    # 等数据重新流动，并取一段连续时间戳判断档
    lines.append("")
    lines.append("=== [2] 数据是否继续入库（连续两次 COUNT 必须递增）===")
    counts: list[int] = []
    deadline = time.time() + 180
    while time.time() < deadline and len(counts) < 2:
        sample = count_rows(device)
        if not counts or sample > counts[-1]:
            counts.append(sample)
            lines.append(f"  COUNT(*) = {sample} @ {now_local().strftime(TS_FORMAT)}")
        if len(counts) < 2:
            time.sleep(12.0)

    after_rows = count_rows(device)
    after_max = max_ts(device)
    # 断档检测：故障前后各取一段（故障前 60 s ~ 故障点，故障点 ~ 现在）
    rows_ts = td_query(
        f"SELECT ts FROM {TD_DB}.{TD_STABLE} WHERE {device_predicate(device)} "
        f"AND ts >= {int((t_kill - 120) * 1000)} ORDER BY ts"
    )
    series = sorted(int(parse_iso_utc(r[0])) for r in rows_ts)
    gaps: list[tuple[int, int]] = []
    for prev, cur in itertools.pairwise(series):
        if cur - prev > POLL_INTERVAL * 3:
            gaps.append((prev, cur))
    result["series"] = {
        "first": datetime.fromtimestamp(series[0], LOCAL_TZ).strftime(TS_FORMAT)
        if series else None,
        "last": datetime.fromtimestamp(series[-1], LOCAL_TZ).strftime(TS_FORMAT)
        if series else None,
        "count": len(series),
        "max_gap_seconds": max((b - a for a, b in gaps), default=0),
        "gaps_over_3x_poll_interval": [
            [
                datetime.fromtimestamp(a, LOCAL_TZ).strftime(TS_FORMAT),
                datetime.fromtimestamp(b, LOCAL_TZ).strftime(TS_FORMAT),
                b - a,
            ]
            for a, b in gaps
        ],
    }
    result["after"] = {
        "at": now_local().strftime(TS_FORMAT),
        "rows": after_rows,
        "max_ts": datetime.fromtimestamp(after_max, LOCAL_TZ).strftime(TS_FORMAT),
        "db_growth": after_rows - base_rows,
        "queue_lines": len(queue_lines()),
        "counts": counts,
        "counts_increasing": len(counts) >= 2 and counts[-1] > counts[0],
    }
    lines.append("")
    lines.append("=== [3] 时间戳连续性（断档 > 3×POLL_INTERVAL 才列出）===")
    lines.append(
        f"  窗口内 {result['series']['count']} 条：{result['series']['first']} .. "
        f"{result['series']['last']}"
    )
    lines.append(f"  最大间隔 {result['series']['max_gap_seconds']} s（POLL_INTERVAL=5 s）")
    # 断档要分两类说清楚：停机期间的断档是**崩溃的必然后果**（不叫自愈），
    # 恢复之后还断档才是真问题。混在一起报会得出"恢复了但一直缺数"的错误印象。
    gap_during_down = [g for g in gaps if g[0] < t_kill < g[1] or (came_up_at and t_kill <= g[0] <= came_up_at)]
    gap_after_up = [g for g in gaps if came_up_at and g[0] > came_up_at]
    result["series"]["gaps_during_outage"] = [
        [
            datetime.fromtimestamp(a, LOCAL_TZ).strftime(TS_FORMAT),
            datetime.fromtimestamp(b, LOCAL_TZ).strftime(TS_FORMAT),
            b - a,
        ]
        for a, b in gap_during_down
    ]
    result["series"]["gaps_after_recovery_over_15s"] = [
        [
            datetime.fromtimestamp(a, LOCAL_TZ).strftime(TS_FORMAT),
            datetime.fromtimestamp(b, LOCAL_TZ).strftime(TS_FORMAT),
            b - a,
        ]
        for a, b in gap_after_up
    ]
    for a, b, d in result["series"]["gaps_during_outage"]:
        lines.append(f"  · 停机期间断档 {d:4d} s : {a} → {b}（崩溃的直接后果，非自愈问题）")
    for a, b, d in result["series"]["gaps_after_recovery_over_15s"]:
        lines.append(f"  ⚠️ 恢复后仍断档 {d:4d} s : {a} → {b}")
    if not gaps:
        lines.append("  无断档（没有任何间隔超过 15 s）")
    lines.append(
        f"  库内 {base_rows} → {after_rows}（+{after_rows - base_rows}），"
        f"MAX(ts) {result['baseline']['max_ts']} → {result['after']['max_ts']}"
    )
    result["verdict"] = {
        "saw_down": saw_down,
        "recovered": came_up_at is not None,
        "data_continues": bool(result["after"]["counts_increasing"]),
        "no_gap_after_recovery": not result["series"]["gaps_after_recovery_over_15s"],
    }
    lines.append(f"  判定: {json.dumps(result['verdict'], ensure_ascii=False)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="边缘侧自治演练（断链 / 崩溃自愈）")
    parser.add_argument("--mode", choices=["linkcut", "crash"], required=True)
    parser.add_argument("--outage", type=float, default=60.0, help="断链秒数（仅 linkcut）")
    parser.add_argument("--container", default="cems-gateway", help="被演练容器")
    parser.add_argument(
        "--auto-wait",
        type=float,
        default=45.0,
        help="crash 模式：先等多少秒看 restart 策略会不会自己拉起（默认 45；实测不会）",
    )
    parser.add_argument("--drain-timeout", type=float, default=300.0, help="等排空上限秒数")
    parser.add_argument("--tag", default="", help="证据文件名后缀（默认取时间戳）")
    parser.add_argument("--out", default="", help="json 落点")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不动东西")
    add_device_argument(parser)
    args = parser.parse_args()

    device = resolve_device(args.device)
    tag = args.tag or stamp()
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else (
        EVIDENCE_DIR / f"edge_autonomy_{args.mode}_{tag}.json"
    )
    log_path = EVIDENCE_DIR / f"edge_autonomy_{args.mode}_{tag}.txt"

    if args.dry_run:
        print(f"[dry-run] mode={args.mode} container={args.container} device={device}")
        print(f"[dry-run] 会写: {out_path}")
        print(f"[dry-run] 会写: {log_path}")
        if args.mode == "linkcut":
            print("[dry-run] 会执行: docker stop emqx")
            print(f"[dry-run] 静置 {args.outage} s（网关转本地缓存）")
            print("[dry-run] 会执行: docker start emqx")
            print("[dry-run] 证据: emqx ctl clients list + 队列快照 + 逐条对账")
        else:
            print(f"[dry-run] 会执行: docker kill {args.container}")
        for name in ALL_CONTAINERS:
            print(f"[dry-run]   {name:22s} {container_state(name)}")
        return 0

    result: dict[str, object] = {
        "mode": args.mode,
        "tag": tag,
        "device": device,
        "container": args.container,
        "started_at": now_local().strftime(TS_FORMAT),
    }
    lines: list[str] = [
        f"# 边缘侧自治演练 - mode={args.mode}",
        f"# tag={tag} device={device} container={args.container}",
        f"# started_at={result['started_at']}",
        "",
    ]

    rc = 1
    try:
        if args.mode == "linkcut":
            do_linkcut(args, result, lines)
        else:
            do_crash(args, result, lines)
        verdict = result.get("verdict", {})
        rc = 0 if verdict and all(verdict.values()) else 2
        result["exit_code"] = rc
    except Exception as exc:  # noqa: BLE001 —— 演练必须落到 finally 里把 emqx 恢复原状
        result["error"] = repr(exc)
        lines.append(f"!! 异常: {exc!r}")
    finally:
        # 无论成败，北向一定要接回去（断链模式下这是硬要求）
        if args.mode == "linkcut":
            status, health, _ = container_state("emqx")
            if status != "running":
                lines.append("[finally] emqx 未在跑，正在恢复…")
                run(["docker", "start", "emqx"], timeout=120, check=False)
                status, health, _ = container_state("emqx")
            lines.append(f"[finally] emqx: status={status} health={health}")
            result["final_emqx_state"] = {"status": status, "health": health}
        if args.mode == "crash":
            wait_healthy(args.container, 120.0)
        states = all_states()
        result["final_states"] = {k: list(v) for k, v in states.items()}
        lines.append("")
        lines.append("[finally] 10 个容器状态:")
        for name, (status, health, started) in states.items():
            lines.append(f"  {name:22s} {status:8s} {health:9s} {started}")
        result["ended_at"] = now_local().strftime(TS_FORMAT)

        log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n".join(lines))
    print()
    print(f"[out] {out_path}")
    print(f"[out] {log_path}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
