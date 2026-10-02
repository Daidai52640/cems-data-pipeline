# -*- coding: utf-8 -*-
"""从容器日志（primary evidence）重建 1A 三次执行的逐条对账，并产出汇总。

为什么重建：驱动脚本按"每次执行同名覆盖"写 JSON/TXT，而第 3 次执行覆盖了第 2 次的产物。
网关容器自 00:49:59 起未再重启，**三次执行的『数据已入本地队列』日志行都还在容器日志里**，
因此可以按窗口把每一次的缓存时间戳集合重新抓出来，逐条对库内对账（无任何人工转录）。

用法：
    python docs/evidence/drill/_recheck_1a_from_logs.py
输出：
    emqx_restart_1a_reconcile_20261003_run{1,2,3}.json
    emqx_restart_1a_run{1,2,3}_logs_20261003.txt
    emqx_restart_1a_summary_20261003.json
"""

from __future__ import annotations

import importlib.util
import json
import pathlib

HERE = pathlib.Path(__file__).parent
spec = importlib.util.spec_from_file_location("drill", HERE / "_drill_fault_recovery.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

# 每个执行：日志/对账窗口（起=注入前基线时刻，止=补传排空之后）
RUNS = [
    {
        "tag": "run1",
        "lo": "2026-10-03 00:31:56", "hi": "2026-10-03 00:36:30",
        "emqx_stopped_at": "2026-10-03 00:31:59",
        "emqx_listener_started_at": "2026-10-03 00:34:27",
        "drained_at": "2026-10-03 00:36:37",
        "raw_txt": "emqx_restart_1a_reconcile_20261003_run1.txt",
    },
    {
        "tag": "run2",
        "lo": "2026-10-03 00:38:24", "hi": "2026-10-03 00:41:10",
        "emqx_stopped_at": "2026-10-03 00:38:27",
        "emqx_listener_started_at": "2026-10-03 00:39:56",
        "drained_at": "2026-10-03 00:41:04",
        "raw_txt": "",
    },
    {
        "tag": "run3",
        "lo": "2026-10-03 00:50:39", "hi": "2026-10-03 00:55:30",
        "emqx_stopped_at": "2026-10-03 00:50:42",
        "emqx_listener_started_at": "2026-10-03 00:53:11",
        "drained_at": "2026-10-03 00:55:23",
        "raw_txt": "emqx_restart_1a_reconcile_20261003_run3.txt",
    },
]

summary: dict = {"runs": [], "aggregate": {}}
totals = {"cached": 0, "missing": 0}
per_device = {"device1": {"cached": 0, "missing": 0}, "device2": {"cached": 0, "missing": 0}}

for run in RUNS:
    entry: dict = {k: v for k, v in run.items() if k != "raw_txt"}
    cached_ts: dict[str, list[str]] = {}
    recon: dict[str, dict] = {}
    log_dump: list[str] = [
        f"# 1A {run['tag']} 原始日志（窗口 {run['lo']} ~ {run['hi']}）",
        "# 来源：docker logs <容器> --since/--until（容器自 00:49:59 起未再重启，日志完整）",
        "",
    ]
    for device, ctn in m.GATEWAY_CTNS.items():
        text = m.docker(
            "logs", ctn, "--since", run["lo"].replace(" ", "T"), "--until", run["hi"].replace(" ", "T")
        )
        parsed = m.parse_gateway_log(text)
        stamps = sorted({t for t in parsed["cached_ts"] if run["lo"] <= t <= run["hi"]})
        cached_ts[device] = stamps
        present = m.db_ts_set(device, run["lo"], run["hi"])
        missing = [t for t in stamps if t not in present]
        recon[device] = {
            "cached": len(stamps),
            "delivered": len(stamps) - len(missing),
            "missing": len(missing),
            "success_rate": (len(stamps) - len(missing)) / len(stamps) if stamps else None,
            "missing_ts": missing,
            "first_ts": stamps[0] if stamps else "",
            "last_ts": stamps[-1] if stamps else "",
            "db_rows_in_window": len(present),
            "cache_reasons": parsed["reasons"],
            "resend_start": parsed["resend_start"],
            "resend_done": parsed["resend_done"],
            "resend_partial": parsed["resend_partial"],
        }
        totals["cached"] += len(stamps)
        totals["missing"] += len(missing)
        per_device[device]["cached"] += len(stamps)
        per_device[device]["missing"] += len(missing)
        log_dump.append(f"=== {device}（{ctn}）窗口内入队 {len(stamps)} 条，缺失 {len(missing)} 条 ===")
        log_dump.append("原因分布: " + json.dumps(parsed["reasons"], ensure_ascii=False))
        log_dump.append("补传批次: 开始 " + json.dumps(parsed["resend_start"])
                        + " / 完成 " + json.dumps(parsed["resend_done"])
                        + " / 未完成 " + json.dumps(parsed["resend_partial"]))
        log_dump.append("缓存时间戳: " + json.dumps(stamps, ensure_ascii=False))
        log_dump.append("缺失时间戳: " + json.dumps(missing, ensure_ascii=False))
        log_dump.append("")
        log_dump.append("--- 原始日志行 ---")
        log_dump.extend(ln for ln in text.splitlines() if "[缓存]" in ln or "[补传" in ln or "MQTT 已连接" in ln)
        log_dump.append("")

    entry["cached_ts"] = cached_ts
    entry["reconciliation"] = recon
    if run.get("raw_txt"):
        entry["driver_raw_txt"] = run["raw_txt"]
    # 逐分钟视图：断档在报表口径下长什么样（缓存窗口前后各多留 2 分钟）
    minute_lo = run["lo"][:16] + ":00"
    entry["per_minute"] = {}
    for plant, device in (("plant1", "device1"), ("plant2", "device2")):
        rows = m.td_rows(
            "SELECT to_char(_wstart,'yyyy-mm-dd hh24:mi:ss'), COUNT(*) FROM cems.cems_data "
            f"WHERE plant='{plant}' AND device='{device}' AND ts >= '{minute_lo}' "
            f"AND ts <= '{run['hi']}' INTERVAL(1m)"
        )
        entry["per_minute"][device] = rows
    entry["verdict"] = "PASS" if all(v["missing"] == 0 for v in recon.values()) else "FAIL"
    (HERE / f"emqx_restart_1a_reconcile_20261003_{run['tag']}.json").write_text(
        json.dumps(entry, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (HERE / f"emqx_restart_1a_{run['tag']}_logs_20261003.txt").write_text(
        "\n".join(log_dump) + "\n", encoding="utf-8"
    )
    summary["runs"].append(entry)

summary["aggregate"] = {
    "runs": len(RUNS),
    "failed_runs": sum(1 for r in summary["runs"] if r["verdict"] == "FAIL"),
    "cached_total": totals["cached"],
    "missing_total": totals["missing"],
    "loss_rate": totals["missing"] / totals["cached"] if totals["cached"] else None,
    "per_device": {
        device: {**counts, "loss_rate": counts["missing"] / counts["cached"] if counts["cached"] else None}
        for device, counts in per_device.items()
    },
}
(HERE / "emqx_restart_1a_summary_20261003.json").write_text(
    json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
)

print(json.dumps(summary["aggregate"], ensure_ascii=False, indent=2))
for run in summary["runs"]:
    print(f"{run['tag']}: {run['verdict']} | " + " | ".join(
        f"{d}: 缓存 {v['cached']} 缺失 {v['missing']}" for d, v in run["reconciliation"].items()))
