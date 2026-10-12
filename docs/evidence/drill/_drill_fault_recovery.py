# -*- coding: utf-8 -*-
"""TC-E2E-002 故障恢复零丢失 · 演练驱动 + 逐条对账（2026-10-03）

用法（工作区根目录）：
    python docs/evidence/drill/_drill_fault_recovery.py <1a|1b|2a|2b>

做三件事：
  1. 注入故障（停/起 EMQX、断/接链路、重启网关），全过程记录时间线；
  2. 抓取"网关已入本地队列"的每一条时间戳（日志 + 缓存文件两条独立来源）；
  3. 逐条对账：每个时间戳在库里对应设备下必须存在（按秒比对），并落 JSON + 原始文本。

⚠️ 不改 src/、不改 compose：本脚本只做外部注入与观测。
"""

from __future__ import annotations

import base64
import json
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

PROJ = Path(__file__).resolve().parents[3]
OUTDIR = PROJ / "docs" / "evidence" / "drill"
TD_URL = "http://127.0.0.1:6041"

# 控制台可能是 GBK：把 stdout/stderr 固定成 utf-8，避免中文/emoji 让脚本自己崩掉
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, OSError):
        pass

# 缓存目录（宿主 bind mount，与容器内一一对应）
CACHE_DIRS = {"device1": PROJ / "data", "device2": PROJ / "data" / "plant2"}
# ⚠️ 对账必须同时按 plant + device 两个 tag 过滤：
#    本机并行还有别的演练线（tzdrill 等）用 device1 作为设备 tag，只按 device 过滤会把它们的行算进来。
PLANT_OF = {"device1": "plant1", "device2": "plant2"}
GATEWAY_CTNS = {
    "device1": "cems-gateway",
    "device2": "cems-gateway-plant2",
}
SUB_CTNS = {"device1": "cems-subscriber", "device2": "cems-subscriber-plant2"}
NETWORK = "cems-net"

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
CACHE_LINE_RE = re.compile(
    r"\[缓存\]\s*(?P<reason>[^，]+?)，数据已入本地队列:\s*(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
)
DRAIN_START_RE = re.compile(r"\[补传\]\s*开始补传\s*(\d+)\s*条")
DRAIN_DONE_RE = re.compile(r"\[补传完成\]\s*broker 已确认\s*(\d+)\s*条")
DRAIN_PART_RE = re.compile(r"\[补传未完成\]\s*(\d+)\s*条未确认")
PREFLIGHT_OK_RE = re.compile(r"启动预检通过:\s*从站(\d+) 可读")
PREFLIGHT_BAD_RE = re.compile(r"启动预检失败")
MODBUS_OK_RE = re.compile(r"Modbus 已连接:\s*device:5020 从站(\d+)")
MODBUS_BAD_RE = re.compile(r"连不上 Modbus 设备")


# --------------------------------------------------------------------------- #
# 基础设施
# --------------------------------------------------------------------------- #
def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class Recorder:
    """时间线 + 原始输出收集器：既打屏，也落盘（每步即时 flush，进程被杀也留下证据）。"""

    def __init__(self, name: str) -> None:
        self.name = name
        self.lines: list[str] = []
        self.events: dict[str, str] = {}
        self.t0 = time.time()
        OUTDIR.mkdir(parents=True, exist_ok=True)
        self.txt_path = OUTDIR / f"{name}.txt"
        self.json_path = OUTDIR / f"{name}.json"

    def log(self, text: str = "") -> None:
        stamp = f"[{now()}] " if text else ""
        line = f"{stamp}{text}"
        self.lines.append(line)
        print(line, flush=True)

    def section(self, title: str) -> None:
        self.log("")
        self.log("=" * 72)
        self.log(title)
        self.log("=" * 72)

    def raw(self, title: str, text: str) -> None:
        self.log(f"--- {title} ---")
        self.log(text.rstrip("\n") if text.strip() else "(无输出)")

    def event(self, key: str) -> str:
        stamp = now()
        self.events[key] = stamp
        self.log(f"EVENT {key} = {stamp}")
        return stamp

    def flush_txt(self) -> None:
        self.txt_path.write_text("\n".join(self.lines) + "\n", encoding="utf-8")

    def write_json(self, payload: dict) -> None:
        payload["scenario"] = self.name
        payload["events"] = self.events
        payload["raw_text"] = str(self.txt_path.relative_to(PROJ)).replace("\\", "/")
        self.json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self.log(f"JSON 已写入: {self.json_path}")


def sh(args: list[str], timeout: float = 180.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        args, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout
    )


def docker(*args: str, timeout: float = 180.0) -> str:
    proc = sh(["docker", *args], timeout=timeout)
    return (proc.stdout or "") + (proc.stderr or "")


def td(sql: str) -> dict:
    req = urllib.request.Request(
        f"{TD_URL}/rest/sql",
        data=sql.encode("utf-8"),
        method="POST",
        headers={
            "Authorization": "Basic " + base64.b64encode(b"root:taosdata").decode("ascii"),
            "Content-Type": "text/plain",
        },
    )
    payload = json.loads(urllib.request.urlopen(req, timeout=20).read().decode("utf-8"))
    if payload.get("code") != 0:
        raise RuntimeError(f"TDengine 错误 {payload}")
    return payload


def td_rows(sql: str) -> list[list]:
    return td(sql).get("data") or []


def db_ts_set(device: str, lo: str, hi: str) -> set[str]:
    """库里该 plant+device 在 [lo, hi] 内的全部时间戳（按秒字符串）。"""
    sql = (
        "SELECT to_char(ts,'yyyy-mm-dd hh24:mi:ss') FROM cems.cems_data "
        f"WHERE plant = '{PLANT_OF[device]}' AND device = '{device}' "
        f"AND ts >= '{lo}' AND ts <= '{hi}'"
    )
    return {row[0] for row in td_rows(sql)}


def device_counts(lo: str, hi: str) -> dict:
    out = {}
    for device in ("device1", "device2"):
        rows = td_rows(
            "SELECT COUNT(*) FROM cems.cems_data "
            f"WHERE plant = '{PLANT_OF[device]}' AND device = '{device}' "
            f"AND ts >= '{lo}' AND ts <= '{hi}'"
        )
        out[device] = rows[0][0] if rows else 0
    return out


def total_count() -> int:
    return td_rows("SELECT COUNT(*) FROM cems.cems_data")[0][0]


def last_ts(device: str) -> str:
    rows = td_rows(
        "SELECT to_char(LAST(ts),'yyyy-mm-dd hh24:mi:ss') FROM cems.cems_data "
        f"WHERE plant = '{PLANT_OF[device]}' AND device = '{device}'"
    )
    return rows[0][0] if rows and rows[0][0] else ""


def read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [ln.strip() for ln in path.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip()]


def cache_snapshot() -> dict:
    """缓存快照：cache.jsonl + 在途段 + 旧版 .sending，逐行取时间戳。"""
    snap = {}
    for device, directory in CACHE_DIRS.items():
        cache = directory / "cache.jsonl"
        legacy = directory / "cache.jsonl.sending"
        segments = sorted(directory.glob("inflight-*.jsonl")) if directory.is_dir() else []
        cache_lines = read_lines(cache)
        legacy_lines = read_lines(legacy)
        seg_lines = {seg.name: read_lines(seg) for seg in segments}
        seg_total = sum(len(v) for v in seg_lines.values())
        snap[device] = {
            "dir": str(directory),
            "cache_lines": len(cache_lines),
            "legacy_sending_lines": len(legacy_lines),
            "inflight_files": {k: len(v) for k, v in seg_lines.items()},
            "inflight_lines": seg_total,
            "pending_total": len(cache_lines) + len(legacy_lines) + seg_total,
            "cache_ts": [ln[:19] for ln in cache_lines if TS_RE.match(ln)],
            "inflight_ts": [
                ln[:19] for lines in seg_lines.values() for ln in lines if TS_RE.match(ln)
            ],
            "legacy_ts": [ln[:19] for ln in legacy_lines if TS_RE.match(ln)],
        }
    return snap


def cache_pending_total(snap: dict) -> int:
    return sum(item["pending_total"] for item in snap.values())


def gw_logs(container: str, since: str) -> str:
    """取容器日志。⚠️ docker 只认 RFC3339，'YYYY-MM-DD HH:MM:SS' 会被拒（extra text），
    必须把空格换成 T，否则拿到的是错误行而不是日志。"""
    return docker("logs", container, "--since", since.replace(" ", "T"))


def parse_gateway_log(text: str) -> dict:
    cached, reasons = [], {}
    for match in CACHE_LINE_RE.finditer(text):
        cached.append(match.group("ts"))
        reason = match.group("reason").strip()
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "cached_ts": cached,
        "reasons": reasons,
        "resend_start": [int(m.group(1)) for m in DRAIN_START_RE.finditer(text)],
        "resend_done": [int(m.group(1)) for m in DRAIN_DONE_RE.finditer(text)],
        "resend_partial": [int(m.group(1)) for m in DRAIN_PART_RE.finditer(text)],
        "preflight_ok": [int(m.group(1)) for m in PREFLIGHT_OK_RE.finditer(text)],
        "preflight_bad": len(PREFLIGHT_BAD_RE.findall(text)),
        "modbus_ok": [int(m.group(1)) for m in MODBUS_OK_RE.finditer(text)],
        "modbus_bad": len(MODBUS_BAD_RE.findall(text)),
    }


def sub_session_log(text: str) -> list[str]:
    return [ln for ln in text.splitlines() if "会话恢复" in ln]


def health(container: str) -> dict:
    state = docker("inspect", container, "--format", "{{.State.Status}}|{{.State.StartedAt}}").strip()
    hc = docker(
        "inspect", container, "--format",
        "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}",
    ).strip()
    status, _, started = state.partition("|")
    return {"status": status, "health": hc, "started_at": started}


def all_states() -> dict:
    out = {}
    for ctn in (
        "cems-device", "cems-gateway", "cems-gateway-plant2", "cems-subscriber",
        "cems-subscriber-plant2", "cems-web", "emqx", "cems-nginx", "cems-redis", "tdengine",
    ):
        out[ctn] = health(ctn)
    return out


def wait_health(container: str, timeout: float = 120.0, interval: float = 2.0) -> float:
    start = time.time()
    while time.time() - start < timeout:
        if health(container)["health"] == "healthy":
            return time.time() - start
        time.sleep(interval)
    return -1.0


def wait_drain(rec: Recorder, since: str, timeout: float = 300.0, quiet_rounds: int = 3) -> dict:
    """等到两个网关的缓存（含在途段）连续 quiet_rounds 次为空。"""
    start = time.time()
    empty_rounds = 0
    series = []
    while time.time() - start < timeout:
        snap = cache_snapshot()
        pending = cache_pending_total(snap)
        series.append({"at": now(), "pending": pending})
        rec.log(f"  排空探测: pending={pending}（{ {k: v['pending_total'] for k, v in snap.items()} }）")
        if pending == 0:
            empty_rounds += 1
            if empty_rounds >= quiet_rounds:
                return {"drained": True, "seconds": time.time() - start, "series": series}
        else:
            empty_rounds = 0
        time.sleep(5)
    return {"drained": False, "seconds": time.time() - start, "series": series}


def wait_delivery(rec: Recorder, cached_ts: dict[str, list[str]], lo: str,
                  timeout: float = 120.0, interval: float = 10.0) -> dict:
    """等"缓存时间戳全部出现在库里"，用来区分"投递慢"与"真丢"。"""
    start = time.time()
    series = []
    while True:
        detail, missing_total = {}, 0
        for device, stamps in cached_ts.items():
            present = db_ts_set(device, lo, now())
            missing = [t for t in stamps if t not in present]
            detail[device] = {"cached": len(stamps), "missing": len(missing)}
            missing_total += len(missing)
        series.append({"at": now(), "detail": detail})
        rec.log(f"  投递探测: {detail}")
        if missing_total == 0 or time.time() - start > timeout:
            return {"missing_total": missing_total, "seconds": time.time() - start, "series": series}
        time.sleep(interval)


def broker_state(rec: Recorder, title: str) -> dict:
    """抓 EMQX 侧会话/队列状态（durable session 是否真的留存 + 每个会话投递了多少）。"""
    clients = docker("exec", "emqx", "emqx", "ctl", "clients", "list")
    subs = docker("exec", "emqx", "emqx", "ctl", "subscriptions", "list")
    rec.raw(title, clients + "\n" + subs)
    out = {}
    for line in clients.splitlines():
        if line.startswith("Client("):
            body = line[len("Client("):].rstrip(")")
            parts = body.split(", ")
            if parts:
                client_id = parts[0]
                fields = {}
                for item in parts[1:]:
                    if "=" in item:
                        key, _, value = item.partition("=")
                        fields[key.strip()] = value
                out[client_id] = fields
    return out


def reconcile(rec: Recorder, cached_ts: dict[str, list[str]], lo: str, hi: str) -> dict:
    """逐条对账：每个缓存时间戳在对应设备下必须存在。"""
    result = {}
    for device, stamps in cached_ts.items():
        unique = sorted(set(stamps))
        present = db_ts_set(device, lo, hi)
        missing = [t for t in unique if t not in present]
        result[device] = {
            "cached_unique": len(unique),
            "cached_duplicates": len(stamps) - len(unique),
            "first_ts": unique[0] if unique else "",
            "last_ts": unique[-1] if unique else "",
            "delivered": len(unique) - len(missing),
            "missing": len(missing),
            "success_rate": (len(unique) - len(missing)) / len(unique) if unique else None,
            "missing_ts": missing,
            "db_rows_in_window": len(present),
            "reconciled_all": not missing,
        }
        rec.log(
            f"  对账 {device}: 缓存 {len(unique)} 条 → 库内存在 {len(unique) - len(missing)} 条，"
            f"缺失 {len(missing)} 条，成功率 "
            f"{result[device]['success_rate'] if unique else 'n/a'}"
        )
        if missing:
            rec.log(f"    ⚠️ 缺失时间戳: {missing}")
    return result


def baseline(rec: Recorder, tag: str) -> dict:
    rec.section(f"基线（{tag}）")
    rec.raw("docker compose ps", docker("compose", "ps", "--format", "{{.Name}}\t{{.Service}}\t{{.Status}}"))
    snap = cache_snapshot()
    rec.raw("缓存快照", json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "cache_ts" and kk != "inflight_ts" and kk != "legacy_ts"} for k, v in snap.items()}, ensure_ascii=False, indent=2))
    counts = {d: last_ts(d) for d in ("device1", "device2")}
    rec.log(f"库内总数={total_count()} 最近时间戳={counts}")
    return {"at": now(), "total": total_count(), "last_ts": counts, "cache": snap,
            "emqx_started_at": health("emqx")["started_at"]}


# --------------------------------------------------------------------------- #
# 场景 1a：EMQX 容器停止 / 重启（两台同时受影响）
# --------------------------------------------------------------------------- #
def scenario_1a(rec: Recorder) -> dict:
    base = baseline(rec, "1a 故障前")
    t_since = base["at"]
    broker_before = broker_state(rec, "EMQX 会话/订阅状态（故障前）")

    rec.section("故障注入：docker stop emqx")
    rec.raw("docker stop emqx", docker("stop", "emqx"))
    t_stop = rec.event("emqx_stopped_at")

    growth = []
    for index in range(7):
        time.sleep(20)
        snap = cache_snapshot()
        growth.append({
            "at": now(),
            "pending": {k: v["pending_total"] for k, v in snap.items()},
        })
        rec.log(f"  离线 {20 * (index + 1)} 秒: pending={growth[-1]['pending']}")

    snapshot_time = now()
    outage_seconds = (datetime.strptime(snapshot_time, "%Y-%m-%d %H:%M:%S")
                      - datetime.strptime(t_stop, "%Y-%m-%d %H:%M:%S")).total_seconds()

    rec.section("离线窗口快照")
    snap = cache_snapshot()
    logs = {device: gw_logs(ctn, t_since) for device, ctn in GATEWAY_CTNS.items()}
    parsed = {device: parse_gateway_log(text) for device, text in logs.items()}
    for device in GATEWAY_CTNS:
        rec.raw(
            f"{device} 缓存快照（cache.jsonl={snap[device]['cache_lines']} 行，"
            f"在途段={snap[device]['inflight_lines']} 行，旧版 .sending={snap[device]['legacy_sending_lines']} 行）",
            "\n".join(json.dumps(item, ensure_ascii=False) for item in [{
                "cache_ts": snap[device]["cache_ts"],
                "inflight_ts": snap[device]["inflight_ts"],
                "legacy_ts": snap[device]["legacy_ts"],
            }]),
        )
        rec.raw(
            f"{device} 网关日志（缓存/补传相关）",
            "\n".join(
                ln for ln in logs[device].splitlines()
                if ("[缓存]" in ln or "[补传" in ln or "PUBACK" in ln)
            ),
        )
        rec.log(f"  {device} 日志口径入队 {len(parsed[device]['cached_ts'])} 条，原因分布 {parsed[device]['reasons']}")

    rec.section("恢复：docker start emqx")
    rec.raw("docker start emqx", docker("start", "emqx"))
    t_start = rec.event("emqx_started_at_cmd")
    healthy_seconds = wait_health("emqx")
    t_healthy = rec.event("emqx_healthy_at")
    rec.log(f"  emqx 恢复 healthy 耗时 {healthy_seconds:.1f} 秒")
    # 这一刻网关/订阅端都还没重连上：正好观察 broker 从磁盘恢复了哪些会话与订阅
    broker_restored = broker_state(rec, "EMQX 会话/订阅状态（broker 刚恢复、客户端尚未重连）")

    drain = wait_drain(rec, t_since)
    t_drained = rec.event("drained_at")
    rec.log(f"  排空耗时 {drain['seconds']:.1f} 秒，drained={drain['drained']}")

    rec.section("恢复后日志与状态")
    logs_after = {device: gw_logs(ctn, t_since) for device, ctn in GATEWAY_CTNS.items()}
    parsed_after = {device: parse_gateway_log(text) for device, text in logs_after.items()}
    for device, text in logs_after.items():
        rec.raw(
            f"{device} 网关日志（补传相关）",
            "\n".join(ln for ln in text.splitlines() if "[补传" in ln or "MQTT 已连接" in ln),
        )
    for device, ctn in SUB_CTNS.items():
        rec.raw(f"{device} 订阅端会话日志", "\n".join(sub_session_log(gw_logs(ctn, t_since))))
    rec.raw("缓存快照（恢复后）", json.dumps({k: {"pending_total": v["pending_total"]} for k, v in cache_snapshot().items()}, ensure_ascii=False))

    lo = (datetime.strptime(t_since, "%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d %H:%M:%S")
    hi = now()
    cached_ts = {}
    cached_source = {}
    for device in GATEWAY_CTNS:
        log_set = set(parsed_after[device]["cached_ts"])
        file_set = set(snap[device]["cache_ts"]) | set(snap[device]["inflight_ts"]) | set(snap[device]["legacy_ts"])
        cached_ts[device] = sorted(log_set | file_set)
        cached_source[device] = {
            "log_ts": len(log_set),
            "file_ts": len(file_set),
            "log_minus_file": sorted(log_set - file_set),
            "file_minus_log": sorted(file_set - log_set),
        }

    rec.section("投递观察（区分'投递慢'与'真丢'）")
    delivery = wait_delivery(rec, cached_ts, lo, timeout=120.0)

    rec.section("逐条对账")
    recon = reconcile(rec, cached_ts, lo, hi)
    for device, info in cached_source.items():
        rec.log(f"  {device} 来源一致性: 日志 {info['log_ts']} / 文件 {info['file_ts']}，"
                f"仅日志有 {len(info['log_minus_file'])}，仅文件有 {len(info['file_minus_log'])}")
    broker_after = broker_state(rec, "EMQX 会话/订阅状态（恢复后）")

    rec.raw("docker compose ps（恢复后）", docker("compose", "ps", "--format", "{{.Name}}\t{{.Status}}"))
    states = all_states()
    rec.raw("容器状态（恢复后）", json.dumps(states, ensure_ascii=False, indent=2))

    outage_ledger = {}
    for device in GATEWAY_CTNS:
        outage_ledger[device] = {
            "cached_rows": len(cached_ts[device]),
            "resend_start_batches": parsed_after[device]["resend_start"],
            "resend_done_batches": parsed_after[device]["resend_done"],
            "resend_partial_batches": parsed_after[device]["resend_partial"],
            "confirmed_sum": sum(parsed_after[device]["resend_done"]),
            "reasons": parsed_after[device]["reasons"],
            "db_rows_in_window": recon[device]["db_rows_in_window"],
        }

    return {
        "outage_seconds": outage_seconds,
        "emqx_healthy_seconds": healthy_seconds,
        "recovery_seconds_stop_to_drained": (
            datetime.strptime(t_drained, "%Y-%m-%d %H:%M:%S")
            - datetime.strptime(t_stop, "%Y-%m-%d %H:%M:%S")
        ).total_seconds(),
        "growth": growth,
        "drain": drain,
        "delivery": delivery,
        "cached_ts_source": cached_source,
        "cached_ts": cached_ts,
        "reconciliation": recon,
        "outage_ledger": outage_ledger,
        "broker_before": broker_before,
        "broker_restored": broker_restored,
        "broker_after": broker_after,
        "residual_cache": {k: v["pending_total"] for k, v in cache_snapshot().items()},
        "baseline": {k: v for k, v in base.items() if k != "cache"},
        "states_after": states,
    }


# --------------------------------------------------------------------------- #
# 场景 1b：EMQX 重启 × 订阅端离线窗口重叠（durable_sessions 专项）
# --------------------------------------------------------------------------- #
def scenario_1b(rec: Recorder) -> dict:
    base = baseline(rec, "1b 故障前")
    t_since = base["at"]

    rec.section("第 1 步：停两个订阅端（制造离线窗口）")
    rec.raw("docker stop cems-subscriber cems-subscriber-plant2",
            docker("stop", "cems-subscriber", "cems-subscriber-plant2"))
    t_sub_down = rec.event("subscribers_stopped_at")

    rec.section("第 2 步：停 EMQX")
    rec.raw("docker stop emqx", docker("stop", "emqx"))
    t_stop = rec.event("emqx_stopped_at")

    growth = []
    for index in range(6):
        time.sleep(20)
        snap = cache_snapshot()
        growth.append({"at": now(), "pending": {k: v["pending_total"] for k, v in snap.items()}})
        rec.log(f"  离线 {20 * (index + 1)} 秒: pending={growth[-1]['pending']}")

    snapshot_time = now()
    outage_seconds = (datetime.strptime(snapshot_time, "%Y-%m-%d %H:%M:%S")
                      - datetime.strptime(t_stop, "%Y-%m-%d %H:%M:%S")).total_seconds()
    snap = cache_snapshot()
    logs = {device: gw_logs(ctn, t_since) for device, ctn in GATEWAY_CTNS.items()}
    parsed = {device: parse_gateway_log(text) for device, text in logs.items()}
    rec.section("离线窗口快照（订阅端与 broker 均不在线）")
    for device in GATEWAY_CTNS:
        rec.raw(f"{device} 缓存时间戳",
                json.dumps({"cache_ts": snap[device]["cache_ts"],
                            "inflight_ts": snap[device]["inflight_ts"],
                            "legacy_ts": snap[device]["legacy_ts"]}, ensure_ascii=False))
        rec.log(f"  {device} 日志口径入队 {len(parsed[device]['cached_ts'])} 条")

    rec.section("第 3 步：起 EMQX（订阅端仍离线 → 报文只可能进 broker 的持久队列）")
    rec.raw("docker start emqx", docker("start", "emqx"))
    t_start = rec.event("emqx_started_at_cmd")
    healthy_seconds = wait_health("emqx")
    t_healthy = rec.event("emqx_healthy_at")
    rec.log(f"  emqx 恢复 healthy 耗时 {healthy_seconds:.1f} 秒")

    drain = wait_drain(rec, t_since, timeout=240.0)
    t_resend_done = rec.event("gateway_resend_done_at")
    rec.log(f"  网关补传（broker 已确认）耗时 {drain['seconds']:.1f} 秒，drained={drain['drained']}")
    logs_mid = {device: gw_logs(ctn, t_since) for device, ctn in GATEWAY_CTNS.items()}
    parsed_mid = {device: parse_gateway_log(text) for device, text in logs_mid.items()}
    for device, text in logs_mid.items():
        rec.raw(f"{device} 网关日志（订阅端仍离线时的补传）",
                "\n".join(ln for ln in text.splitlines() if "[补传" in ln))

    # broker 侧队列证据（订阅端离线时该会话的排队情况）
    rec.raw("EMQX 队列（订阅端离线时）",
            docker("exec", "emqx", "emqx", "ctl", "mqtt", "list", "--limit", "20"))

    rec.section("第 4 步：起订阅端 → 消费 broker 持久队列")
    rec.raw("docker start cems-subscriber cems-subscriber-plant2",
            docker("start", "cems-subscriber", "cems-subscriber-plant2"))
    t_sub_up = rec.event("subscribers_started_at")

    lo = t_since
    cached_ts = {}
    for device in GATEWAY_CTNS:
        log_set = set(parsed_mid[device]["cached_ts"])
        file_set = set(snap[device]["cache_ts"]) | set(snap[device]["inflight_ts"]) | set(snap[device]["legacy_ts"])
        cached_ts[device] = sorted(log_set | file_set)

    # 等持久队列投递完成：库里出现全部缓存时间戳
    wait_start = time.time()
    delivered_all = False
    delivery_series = []
    while time.time() - wait_start < 240:
        present = {device: db_ts_set(device, lo, now()) for device in GATEWAY_CTNS}
        missing_total = sum(
            len([t for t in cached_ts[d] if t not in present[d]]) for d in GATEWAY_CTNS
        )
        delivery_series.append({"at": now(), "missing_total": missing_total})
        rec.log(f"  队列投递探测: 缺失 {missing_total} 条")
        if missing_total == 0:
            delivered_all = True
            break
        time.sleep(10)
    t_delivered = rec.event("durable_queue_delivered_at")

    rec.section("恢复后日志与状态")
    for device, ctn in SUB_CTNS.items():
        rec.raw(f"{device} 订阅端会话日志", "\n".join(sub_session_log(gw_logs(ctn, t_since))))
    for device in GATEWAY_CTNS:
        rec.raw(f"{device} 网关日志（补传相关）",
                "\n".join(ln for ln in gw_logs(GATEWAY_CTNS[device], t_since).splitlines()
                          if "[补传" in ln or "MQTT 已连接" in ln))
    rec.raw("缓存快照（恢复后）",
            json.dumps({k: {"pending_total": v["pending_total"]} for k, v in cache_snapshot().items()}, ensure_ascii=False))

    rec.section("逐条对账（持久会话投递）")
    recon = reconcile(rec, cached_ts, lo, now())
    states = all_states()
    rec.raw("容器状态（恢复后）", json.dumps(states, ensure_ascii=False, indent=2))

    return {
        "outage_seconds": outage_seconds,
        "subscriber_offline_seconds": (
            datetime.strptime(t_sub_up, "%Y-%m-%d %H:%M:%S")
            - datetime.strptime(t_sub_down, "%Y-%m-%d %H:%M:%S")
        ).total_seconds(),
        "emqx_healthy_seconds": healthy_seconds,
        "gateway_resend_seconds": drain["seconds"],
        "durable_delivery_seconds": (
            datetime.strptime(t_delivered, "%Y-%m-%d %H:%M:%S")
            - datetime.strptime(t_sub_up, "%Y-%m-%d %H:%M:%S")
        ).total_seconds(),
        "delivered_all": delivered_all,
        "growth": growth,
        "drain": drain,
        "delivery_series": delivery_series,
        "reconciliation": recon,
        "residual_cache": {k: v["pending_total"] for k, v in cache_snapshot().items()},
        "baseline": {k: v for k, v in base.items() if k != "cache"},
        "states_after": states,
    }


# --------------------------------------------------------------------------- #
# 场景 2a：双设备同时断链（emqx 与链路断，进程不重启）
# --------------------------------------------------------------------------- #
def scenario_2a(rec: Recorder) -> dict:
    base = baseline(rec, "2a 故障前")
    t_since = base["at"]

    rec.section("故障注入：docker network disconnect cems-net emqx（断链，进程不重启）")
    rec.raw("docker network disconnect cems-net emqx", docker("network", "disconnect", NETWORK, "emqx"))
    t_cut = rec.event("link_cut_at")

    growth = []
    for index in range(6):
        time.sleep(20)
        snap = cache_snapshot()
        growth.append({"at": now(), "pending": {k: v["pending_total"] for k, v in snap.items()}})
        rec.log(f"  断链 {20 * (index + 1)} 秒: pending={growth[-1]['pending']}")

    t_snapshot = now()
    outage_seconds = (datetime.strptime(t_snapshot, "%Y-%m-%d %H:%M:%S")
                      - datetime.strptime(t_cut, "%Y-%m-%d %H:%M:%S")).total_seconds()
    snap = cache_snapshot()
    logs = {device: gw_logs(ctn, t_since) for device, ctn in GATEWAY_CTNS.items()}
    parsed = {device: parse_gateway_log(text) for device, text in logs.items()}

    rec.section("断链窗口快照")
    for device in GATEWAY_CTNS:
        rec.raw(f"{device} 缓存快照",
                json.dumps({"cache_lines": snap[device]["cache_lines"],
                            "inflight_lines": snap[device]["inflight_lines"],
                            "legacy_sending_lines": snap[device]["legacy_sending_lines"],
                            "cache_ts": snap[device]["cache_ts"],
                            "inflight_ts": snap[device]["inflight_ts"],
                            "legacy_ts": snap[device]["legacy_ts"]}, ensure_ascii=False))
        rec.raw(f"{device} 网关日志（断链窗口）",
                "\n".join(ln for ln in logs[device].splitlines()
                          if "[缓存]" in ln or "[补传" in ln or "Modbus" in ln))
        rec.log(f"  {device} 入队 {len(parsed[device]['cached_ts'])} 条，原因分布 {parsed[device]['reasons']}")

    rec.raw("emqx 进程未重启的证据（StartedAt 不变）", json.dumps(health("emqx"), ensure_ascii=False))

    rec.section("恢复：docker network connect cems-net emqx")
    rec.raw("docker network connect cems-net emqx", docker("network", "connect", NETWORK, "emqx"))
    t_restore = rec.event("link_restored_at")

    drain = wait_drain(rec, t_since, timeout=300.0)
    t_drained = rec.event("drained_at")
    rec.log(f"  排空耗时 {drain['seconds']:.1f} 秒，drained={drain['drained']}")

    logs_after = {device: gw_logs(ctn, t_since) for device, ctn in GATEWAY_CTNS.items()}
    parsed_after = {device: parse_gateway_log(text) for device, text in logs_after.items()}
    rec.section("恢复后日志与状态")
    for device, text in logs_after.items():
        rec.raw(f"{device} 网关日志（补传相关）",
                "\n".join(ln for ln in text.splitlines()
                          if "[补传" in ln or "MQTT 已连接" in ln or "连接断开" in ln))
    rec.raw("缓存快照（恢复后）",
            json.dumps({k: {"pending_total": v["pending_total"]} for k, v in cache_snapshot().items()}, ensure_ascii=False))
    rec.raw("emqx 进程 StartedAt（恢复后）", json.dumps(health("emqx"), ensure_ascii=False))

    cached_ts = {}
    cached_source = {}
    for device in GATEWAY_CTNS:
        log_set = set(parsed_after[device]["cached_ts"])
        file_set = set(snap[device]["cache_ts"]) | set(snap[device]["inflight_ts"]) | set(snap[device]["legacy_ts"])
        cached_ts[device] = sorted(log_set | file_set)
        cached_source[device] = {
            "log_ts": len(log_set), "file_ts": len(file_set),
            "log_minus_file": sorted(log_set - file_set),
            "file_minus_log": sorted(file_set - log_set),
        }

    rec.section("投递观察（区分'投递慢'与'真丢'）")
    delivery = wait_delivery(rec, cached_ts, t_since, timeout=120.0)

    rec.section("逐条对账")
    recon = reconcile(rec, cached_ts, t_since, now())
    for device, info in cached_source.items():
        rec.log(f"  {device} 来源一致性: 日志 {info['log_ts']} / 文件 {info['file_ts']}，"
                f"仅日志有 {len(info['log_minus_file'])}，仅文件有 {len(info['file_minus_log'])}")

    ledger = {}
    for device in GATEWAY_CTNS:
        ledger[device] = {
            "cached_rows": len(cached_ts[device]),
            "resend_start_batches": parsed_after[device]["resend_start"],
            "resend_done_batches": parsed_after[device]["resend_done"],
            "resend_partial_batches": parsed_after[device]["resend_partial"],
            "confirmed_sum": sum(parsed_after[device]["resend_done"]),
            "reasons": parsed_after[device]["reasons"],
        }

    states = all_states()
    rec.raw("容器状态（恢复后）", json.dumps(states, ensure_ascii=False, indent=2))
    return {
        "outage_seconds": outage_seconds,
        "emqx_restarted": health("emqx")["started_at"] != base["emqx_started_at"],
        "recovery_seconds_cut_to_drained": (
            datetime.strptime(t_drained, "%Y-%m-%d %H:%M:%S")
            - datetime.strptime(t_restore, "%Y-%m-%d %H:%M:%S")
        ).total_seconds(),
        "growth": growth,
        "drain": drain,
        "delivery": delivery,
        "cached_ts": cached_ts,
        "cached_ts_source": cached_source,
        "reconciliation": recon,
        "outage_ledger": ledger,
        "residual_cache": {k: v["pending_total"] for k, v in cache_snapshot().items()},
        "baseline": {k: v for k, v in base.items() if k != "cache"},
        "states_after": states,
    }


# --------------------------------------------------------------------------- #
# 场景 2b：启动预检非回归
# --------------------------------------------------------------------------- #
def scenario_2b(rec: Recorder) -> dict:
    base = baseline(rec, "2b 故障前")
    t_since_pre = base["at"]
    out: dict = {"baseline": {k: v for k, v in base.items() if k != "cache"}}

    # ---- 2b-1 设备未起时重启网关：预检不得误伤（不得拒绝启动） ----
    rec.section("2b-1 设备未起时重启网关（确认预检不误伤'暂时连不上'）")
    rec.raw("docker stop cems-device", docker("stop", "cems-device"))
    t_dev_down = rec.event("device_stopped_at")
    time.sleep(3)
    rec.raw("docker restart cems-gateway cems-gateway-plant2",
            docker("restart", "cems-gateway", "cems-gateway-plant2", timeout=180))
    rec.event("gateways_restarted_device_down_at")
    time.sleep(25)
    logs = {device: gw_logs(ctn, t_since_pre) for device, ctn in GATEWAY_CTNS.items()}
    parsed = {device: parse_gateway_log(text) for device, text in logs.items()}
    for device, text in logs.items():
        rec.raw(f"{device} 网关日志（设备停用期间启动）",
                "\n".join(ln for ln in text.splitlines()
                          if "网关启动" in ln or "Modbus" in ln or "预检" in ln or "缓存目录" in ln))
    states_down = {ctn: health(ctn) for ctn in GATEWAY_CTNS.values()}
    rec.raw("网关容器状态（设备停用期间）", json.dumps(states_down, ensure_ascii=False))
    out["2b1_device_down_gateway_restart"] = {
        "gateway_states": states_down,
        "preflight_failed_count": {d: parsed[d]["preflight_bad"] for d in parsed},
        "modbus_unreachable_count": {d: parsed[d]["modbus_bad"] for d in parsed},
        "refused_to_start": any(v["status"] != "running" for v in states_down.values()),
        "row_gap_window": {},
    }

    rec.section("2b-1 恢复：起设备")
    rec.raw("docker start cems-device", docker("start", "cems-device"))
    t_dev_up = rec.event("device_started_at")
    wait_health("cems-device")
    # 等两台上数（各自最近一条时间戳在 60 秒内）
    resumed = {}
    wait_start = time.time()
    while time.time() - wait_start < 120:
        resumed = {d: last_ts(d) for d in ("device1", "device2")}
        fresh = all(
            v and (datetime.now() - datetime.strptime(v, "%Y-%m-%d %H:%M:%S")).total_seconds() < 45
            for v in resumed.values()
        )
        if fresh:
            break
        time.sleep(5)
    t_resumed = rec.event("both_devices_resumed_at")
    logs_after = {device: gw_logs(ctn, t_dev_up) for device, ctn in GATEWAY_CTNS.items()}
    parsed_after = {device: parse_gateway_log(text) for device, text in logs_after.items()}
    for device, text in logs_after.items():
        rec.raw(f"{device} 网关日志（设备恢复后）",
                "\n".join(ln for ln in text.splitlines()
                          if "Modbus" in ln or "预检" in ln or "[补传" in ln))
    out["2b1_device_down_gateway_restart"].update({
        "resumed_at": t_resumed,
        "gateways_restarted_at": rec.events.get("gateways_restarted_device_down_at"),
        "last_ts_after_resume": resumed,
        "modbus_connected_after_device_up": {d: parsed_after[d]["modbus_ok"] for d in parsed_after},
        "device_down_seconds": (
            datetime.strptime(t_resumed, "%Y-%m-%d %H:%M:%S")
            - datetime.strptime(t_dev_down, "%Y-%m-%d %H:%M:%S")
        ).total_seconds(),
        # 上游断（设备侧）窗口内采不到数 = 数据源消失，不属"断网续传"保护范围，如实列出口径
        "db_rows_device1_in_gap": device_counts(t_dev_down, t_resumed)["device1"],
        "db_rows_device2_in_gap": device_counts(t_dev_down, t_resumed)["device2"],
    })

    # ---- 2b-2 设备已起时重启网关：预检通过 ----
    rec.section("2b-2 设备已起时重启网关（确认预检通过 + 采集恢复）")
    last_before = {d: last_ts(d) for d in ("device1", "device2")}
    rec.log(f"  重启前各设备最近一条: {last_before}")
    rec.raw("docker restart cems-gateway cems-gateway-plant2",
            docker("restart", "cems-gateway", "cems-gateway-plant2", timeout=180))
    t_restart = rec.event("gateways_restarted_device_up_at")
    time.sleep(20)
    states_up = {ctn: health(ctn) for ctn in GATEWAY_CTNS.values()}
    logs2 = {device: gw_logs(ctn, t_restart) for device, ctn in GATEWAY_CTNS.items()}
    parsed2 = {device: parse_gateway_log(text) for device, text in logs2.items()}
    for device, text in logs2.items():
        rec.raw(f"{device} 网关日志（设备在线的重启）",
                "\n".join(ln for ln in text.splitlines()
                          if "网关启动" in ln or "Modbus" in ln or "预检" in ln or "缓存目录" in ln))
    rec.raw("网关容器状态（设备在线的重启后）", json.dumps(states_up, ensure_ascii=False))

    # 等两台恢复出数，量出"预检 + 启动"本身引入的断档
    wait_start = time.time()
    while time.time() - wait_start < 120:
        after = {d: last_ts(d) for d in ("device1", "device2")}
        if all(after[d] and after[d] > last_before[d] for d in after):
            break
        time.sleep(5)
    t_restart_resumed = rec.event("both_devices_resumed_after_restart_at")
    rec.log(f"  重启后各设备最近一条: { {d: last_ts(d) for d in ('device1', 'device2')} }")
    out["2b2_device_up_gateway_restart"] = {
        "gateway_states": states_up,
        "preflight_ok_units": {d: parsed2[d]["preflight_ok"] for d in parsed2},
        "preflight_failed_count": {d: parsed2[d]["preflight_bad"] for d in parsed2},
        "restart_at": t_restart,
        "resumed_at": t_restart_resumed,
        "restart_gap_seconds": (
            datetime.strptime(t_restart_resumed, "%Y-%m-%d %H:%M:%S")
            - datetime.strptime(t_restart, "%Y-%m-%d %H:%M:%S")
        ).total_seconds(),
        "last_ts_before": last_before,
        "last_ts_after": {d: last_ts(d) for d in ("device1", "device2")},
        "db_rows_device1_in_gap": device_counts(t_restart, t_restart_resumed)["device1"],
        "db_rows_device2_in_gap": device_counts(t_restart, t_restart_resumed)["device2"],
    }

    # ---- 2b-3 正向控制：未登记从站必须被预检拒绝 ----
    rec.section("2b-3 正向控制：把 MODBUS_UNIT 指向未登记从站 99，预检必须拒绝启动")
    probe_env = [
        "-e", "MODBUS_UNIT=99",
        "-e", "MQTT_PORT=1884",              # 打不到 broker：即便预检被绕过也不可能污染库内数据
        "-e", "MQTT_CLIENT_ID=preflight-probe-99",
        "-e", "GATEWAY_DATA_DIR=/tmp/preflight-probe",
    ]
    probes = {}
    for device, ctn in GATEWAY_CTNS.items():
        proc = sh(["docker", "exec", *probe_env, ctn, "python", "src/gateway/gateway.py"], timeout=90)
        combined = (proc.stdout or "") + (proc.stderr or "")
        probes[device] = {
            "container": ctn,
            "exit_code": proc.returncode,
            "output_tail": combined.strip().splitlines()[-6:] if combined.strip() else [],
            "preflight_failed": bool(PREFLIGHT_BAD_RE.search(combined)),
            "preflight_passed": bool(PREFLIGHT_OK_RE.search(combined)),
        }
        rec.raw(f"{ctn} 预检正向控制（MODBUS_UNIT=99）",
                f"exit_code={proc.returncode}\n" + combined.strip())
    out["2b3_positive_control"] = probes

    rec.raw("docker compose ps（2b 结束）",
            docker("compose", "ps", "--format", "{{.Name}}\t{{.Status}}"))
    out["states_after"] = all_states()
    return out


# --------------------------------------------------------------------------- #
def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] not in {"1a", "1b", "2a", "2b"}:
        print(__doc__)
        return 2
    key = sys.argv[1]
    names = {
        "1a": "emqx_restart_1a_reconcile_20261003",
        "1b": "emqx_restart_1b_durable_reconcile_20261003",
        "2a": "dual_device_linkcut_2a_reconcile_20261003",
        "2b": "gateway_preflight_2b_nonregression_20261003",
    }
    rec = Recorder(names[key])
    rec.log(f"TC-E2E-002 演练 {key} 开始；结果文件 {names[key]}.json / .txt")
    try:
        if key == "1a":
            result = scenario_1a(rec)
        elif key == "1b":
            result = scenario_1b(rec)
        elif key == "2a":
            result = scenario_2a(rec)
        else:
            result = scenario_2b(rec)
        rec.flush_txt()
        rec.write_json(result)
        rec.log("演练结束")
    finally:
        rec.flush_txt()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
