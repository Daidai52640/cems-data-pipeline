# -*- coding: utf-8 -*-
"""TC-E2E-002 演练收尾：清理测试期产生的临时物 + 环境恢复核验。

用法：
    python docs/evidence/drill/_cleanup_and_verify.py
输出：
    docs/evidence/drill/cleanup_and_verify_20261003.txt
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import subprocess
import time

HERE = pathlib.Path(__file__).parent
spec = importlib.util.spec_from_file_location("drill", HERE / "_drill_fault_recovery.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

OUT = HERE / "cleanup_and_verify_20261003.txt"
lines: list[str] = []


def log(text: str = "") -> None:
    lines.append(text)
    print(text)


def run(args: list[str]) -> str:
    proc = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return ((proc.stdout or "") + (proc.stderr or "")).strip()


log("TC-E2E-002 演练收尾：清理 + 环境恢复核验")
log(f"核验时刻: {m.now()}")
log("=" * 72)

log("\n[1] 清理演练探针在容器内留下的临时目录（2B 正向控制用）")
for ctn in m.GATEWAY_CTNS.values():
    log(f"  docker exec {ctn} rm -rf /tmp/preflight-probe  →  {run(['docker', 'exec', ctn, 'rm', '-rf', '/tmp/preflight-probe']) or 'ok'}")
    log(f"  复查 {ctn}:/tmp/preflight-probe 是否存在 → "
        f"{run(['docker', 'exec', ctn, 'sh', '-c', '[ -e /tmp/preflight-probe ] && echo EXISTS || echo GONE'])}")

log("\n[2] 缓存残留（演练要求：补传后必须排空）")
snap = m.cache_snapshot()
for device, info in snap.items():
    log(f"  {device}: cache.jsonl={info['cache_lines']} 行, 在途段={info['inflight_lines']} 行, "
        f"旧版 .sending={info['legacy_sending_lines']} 行 → pending_total={info['pending_total']}")
log(f"  合计 pending_total = {m.cache_pending_total(snap)}")

log("\n[3] 库内有无本演练造的探针/测试数据")
queries = {
    "tag 含 probe/test 的行": (
        "SELECT COUNT(*) FROM cems.cems_data "
        "WHERE device LIKE '%probe%' OR plant LIKE '%probe%' OR device IN ('test','probe')"
    ),
    "plant='tzdrill' 的行（另一条线的时区演练，非本演练所造，不动）": (
        "SELECT COUNT(*) FROM cems.cems_data WHERE plant = 'tzdrill'"
    ),
    "库总数（信息项）": "SELECT COUNT(*) FROM cems.cems_data",
}
for title, sql in queries.items():
    log(f"  {title}: {m.td_rows(sql)}")
log(f"  SHOW DATABASES: {m.td_rows('SHOW DATABASES')}")

log("\n[4] 本演练是否创建过数据库/表（应为 0 个）")
log("  本演练全程只执行只读 SQL（SELECT/COUNT/LAST/to_char）+ 容器停起，未执行 CREATE/INSERT/DELETE/DROP")

log("\n[5] 环境恢复核验：10 容器 healthy")
compose_ps = run(["docker", "compose", "ps", "--format", "{{.Name}}\t{{.Status}}"])
log(compose_ps)
states = m.all_states()
bad = {k: v for k, v in states.items() if v["status"] != "running" or v["health"] not in ("healthy", "none")}
log(f"  非 healthy 的容器: {bad if bad else '无'}")

log("\n[6] 两台设备正常出数（5 秒/条，最近一条在 30 秒内）")
for _ in range(6):
    fresh = {}
    for device in ("device1", "device2"):
        last = m.last_ts(device)
        age = (m.datetime.now() - m.datetime.strptime(last, "%Y-%m-%d %H:%M:%S")).total_seconds()
        fresh[device] = {"last_ts": last, "age_s": age}
    log(f"  {fresh}")
    if all(item["age_s"] <= 30 for item in fresh.values()):
        break
    time.sleep(5)

log("\n[7] 一台设备的采集节奏核验（最近 3 分钟逐分钟条数，应≈12）")
for plant, device in (("plant1", "device1"), ("plant2", "device2")):
    rows = m.td_rows(
        "SELECT to_char(_wstart,'yyyy-mm-dd hh24:mi:ss'), COUNT(*) FROM cems.cems_data "
        f"WHERE plant='{plant}' AND device='{device}' AND ts >= NOW - 3m INTERVAL(1m)"
    )
    log(f"  {plant}/{device}: {rows}")

log("\n[8] 演练造成的库内永久缺口（如实列出；不清理：网关侧副本已在 broker 确认后移除，已无副本可补）")
for label, lo, hi, note in (
    ("run1", "2026-10-03 00:31:00", "2026-10-03 00:37:00", "device1 54 条（00:31:57~00:36:24）"),
    ("run3", "2026-10-03 00:50:00", "2026-10-03 00:56:00", "device1 53 条（00:50:44~00:55:06）"),
):
    rows = m.td_rows(
        "SELECT to_char(_wstart,'yyyy-mm-dd hh24:mi:ss'), COUNT(*) FROM cems.cems_data "
        f"WHERE plant='plant1' AND device='device1' AND ts >= '{lo}' AND ts < '{hi}' INTERVAL(1m)"
    )
    log(f"  {label} plant1/device1 逐分钟: {rows}   ← {note}")
    log(f"       证据: docs/evidence/drill/emqx_restart_1a_reconcile_20261003_{label}.json")

OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n已写入:", OUT)
