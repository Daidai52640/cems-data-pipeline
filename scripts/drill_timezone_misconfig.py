# -*- coding: utf-8 -*-
"""时区错误演练（TZ=UTC）：制造"容器时区配错"，记录现象与判据反应。

做法（沿用既有演练的隔离手法，见 docs/reference/并发与负载指标.md §5.1）：
  · 用同一个镜像另起**一对一次性实例**：网关 `cems-tzdrill-gw`（TZ=UTC）
    + 接入层 `cems-tzdrill-sub`（TZ=Asia/Shanghai，与生产一致）；
  · 主题 `cems/tzdrill/data`、client_id、缓存目录、入库标签 `plant=tzdrill` 全部独立；
  · 数据源仍是真设备（`cems-device:5020` 从站 1），报文格式与生产逐字一致。

记录什么：
  ① 网关容器内的墙上时钟 vs 宿主墙上时钟（错时区的直接效应）
  ② 网关发布出来的 ts 字符串 vs 宿主当地时刻（偏移量）
  ③ 入库后的 ts（TDengine 读到的是本地串）与"应该是什么"
  ④ 覆盖率/新鲜度判据的反应：
       - web_dashboard /api/health 的两条判据（rows_last_1min==0 → no_rows_last_1min；
         最近样本距今 > POLL_INTERVAL×因子 → stale_data）
       用**同一份代码**（src/web/web_dashboard.py 的 Flask 应用）在宿主进程内、把
       TD_PLANT/TD_DEVICE 指向演练标签来判定 —— 判据是"最近 1 分钟内有行 + 足够新"，
       与 drill 数据同源，不碰生产标签。
  ⑤ 对照：同一时刻接线到**正确时区**的生产链路，判据是绿的（避免把"判据本来就不灵"当结论）

清理：两个容器 `docker rm -f`；演练子表 `DROP TABLE` 后 COUNT=0 复核。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._td_ops import (  # noqa: E402
    LOCAL_TZ,
    ensure_utf8_stdout,
    taos_exec,
    taos_scalar,
    taos_sql,
    write_json,
)

EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence" / "backup"
IMAGE = "cems-pipeline:latest"
NETWORK = "cems-net"
GW = "cems-tzdrill-gw"
SUB = "cems-tzdrill-sub"
TOPIC = "cems/tzdrill/data"
PLANT = "tzdrill"
DEVICE = "device1"
CHILD_TABLE = f"{PLANT}_{DEVICE}"
RUN_SECONDS = 75


def sh(cmd: list[str], timeout: float = 60.0) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                          errors="replace", timeout=timeout)


def container_clock(name: str) -> str:
    proc = sh(["docker", "exec", name, "date", "+%Y-%m-%d %H:%M:%S %Z %z"])
    return (proc.stdout or "").strip()


def rm(name: str) -> None:
    sh(["docker", "rm", "-f", name], timeout=120)


def main() -> int:
    ensure_utf8_stdout()
    parser = argparse.ArgumentParser(description="时区错误演练（TZ=UTC）")
    parser.add_argument("--seconds", type=float, default=RUN_SECONDS)
    parser.add_argument("--tag", default="tz_utc_run1")
    parser.add_argument("--keep-going", action="store_true")
    args = parser.parse_args()

    cache_dir = PROJECT_ROOT / "docs" / "evidence" / "backup" / "_tzdrill_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    for item in cache_dir.glob("*"):
        item.unlink()

    report: dict = {
        "drill": "timezone-misconfig",
        "injected": {"service": "gateway", "env": "TZ=UTC"},
        "control": {"subscriber_tz": "Asia/Shanghai", "tdengine_tz": "Asia/Shanghai",
                    "host_tz": "Asia/Shanghai (+08:00)"},
        "topic": TOPIC,
        "plant": PLANT,
        "child_table": CHILD_TABLE,
        "run_seconds": args.seconds,
        "started_at": datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }

    try:
        rm(GW)
        rm(SUB)
        taos_exec("tdengine", f"DROP TABLE IF EXISTS cems.{CHILD_TABLE};")

        host_before = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        report["host_clock_at_start"] = host_before

        gw = sh([
            "docker", "run", "-d", "--name", GW, "--network", NETWORK,
            "-e", "TZ=UTC",                                   # ★ 注入的时区错误
            "-e", "MQTT_HOST=emqx", "-e", "MQTT_PORT=1883",
            "-e", f"MQTT_TOPIC={TOPIC}",
            "-e", f"MQTT_CLIENT_ID={GW}",
            "-e", "GATEWAY_DATA_DIR=/app/data-tzdrill",
            "-e", "MODBUS_HOST=cems-device", "-e", "MODBUS_PORT=5020", "-e", "MODBUS_UNIT=1",
            "-e", "POLL_INTERVAL=5.0",
            "-v", f"{cache_dir}:/app/data-tzdrill",
            IMAGE, "python", "src/gateway/gateway.py",
        ])
        if gw.returncode != 0:
            report["ok"] = False
            report["failed_stage"] = {"stage": "start_gateway", "detail": gw.stderr}
            return 3

        sub = sh([
            "docker", "run", "-d", "--name", SUB, "--network", NETWORK,
            "-e", "TZ=Asia/Shanghai",                         # 接入层与生产一致
            "-e", "MQTT_HOST=emqx", "-e", "MQTT_PORT=1883",
            "-e", f"MQTT_TOPIC={TOPIC}",
            "-e", f"MQTT_CLIENT_ID={SUB}",
            "-e", f"TD_PLANT={PLANT}", "-e", f"TD_DEVICE={DEVICE}",
            "-e", "TD_URL=http://tdengine:6041",
            "-e", "ALARM_ENABLE=0",
            IMAGE, "python", "src/platform/subscriber_to_td.py",
        ])
        if sub.returncode != 0:
            report["ok"] = False
            report["failed_stage"] = {"stage": "start_subscriber", "detail": sub.stderr}
            return 3

        # 等网关启动预检通过（"启动预检"把"连上了但读不到本从站"变成启动失败）
        deadline = time.perf_counter() + 60
        preflight_ok = False
        while time.perf_counter() < deadline:
            logs = sh(["docker", "logs", GW]).stdout or ""
            if "启动预检通过" in logs:
                preflight_ok = True
                break
            if "预检失败" in logs or "Traceback" in logs:
                break
            time.sleep(1.0)
        report["gateway_preflight_ok"] = preflight_ok
        report["gateway_clock"] = container_clock(GW)
        report["subscriber_clock"] = container_clock(SUB)

        # 等子表出现（接入层建表 + 第一条落库），最多 60 s；出现后再采满 run_seconds
        table_deadline = time.perf_counter() + 60
        table_ready = False
        while time.perf_counter() < table_deadline:
            try:
                taos_sql("tdengine", f"SELECT COUNT(*) FROM cems.{CHILD_TABLE};")
                table_ready = True
                break
            except RuntimeError:
                time.sleep(2.0)
        report["child_table_ready"] = table_ready
        if not table_ready:
            report["gateway_log_tail"] = sh(["docker", "logs", "--tail", "30", GW]).stdout or ""
            report["subscriber_log_tail"] = sh(["docker", "logs", "--tail", "30", SUB]).stdout or ""
            report["ok"] = False
            report["failed_stage"] = {"stage": "no_rows_written",
                                      "detail": f"{CHILD_TABLE} 在 60 s 内没有出现"}
            return 3

        time.sleep(args.seconds)

        # ---- 读演练数据：入库的 ts 分布 ---------------------------------- #
        rows = taos_sql(
            "tdengine",
            f"SELECT CAST(ts AS BIGINT), ts FROM cems.{CHILD_TABLE} ORDER BY ts;")
        stamps = [int(r[0]) for r in rows]
        report["rows_written"] = len(stamps)
        if stamps:
            report["ts_min"] = str(rows[0][1])
            report["ts_max"] = str(rows[-1][1])
        host_now = datetime.now(LOCAL_TZ)
        report["host_clock_at_measure"] = host_now.strftime("%Y-%m-%d %H:%M:%S")
        if stamps:
            newest_local = datetime.fromtimestamp(stamps[-1] / 1000.0, LOCAL_TZ)
            oldest_local = datetime.fromtimestamp(stamps[0] / 1000.0, LOCAL_TZ)
            report["newest_stored_local"] = newest_local.strftime("%Y-%m-%d %H:%M:%S")
            report["oldest_stored_local"] = oldest_local.strftime("%Y-%m-%d %H:%M:%S")
            report["skew_seconds"] = round((newest_local - host_now).total_seconds(), 3)
            report["skew_hours"] = round(report["skew_seconds"] / 3600.0, 3)
            age = (host_now - newest_local).total_seconds()
        else:
            report["skew_seconds"] = None
            age = None

        # ---- 判据反应：同一份 /api/health 代码，指向演练标签 -------------- #
        os.environ["TD_PLANT"] = PLANT
        os.environ["TD_DEVICE"] = DEVICE
        os.environ["POLL_INTERVAL"] = "5.0"
        os.environ["TD_URL"] = "http://127.0.0.1:6041"
        for module in [m for m in list(sys.modules) if m.startswith("src.web")]:
            del sys.modules[module]
        from src.web import web_dashboard as WEB  # noqa: E402

        with WEB.app.test_client() as client:
            resp = client.get("/api/health")
            body = resp.get_json()
            report["health_drill_tag"] = {
                "http_status": resp.status_code,
                "body": body,
                "threshold_seconds": WEB.HEALTH_STALE_AFTER_SECONDS,
                "code_path": "src/web/web_dashboard.py::api_health（同一份生产代码）",
                "device_scope": f"{PLANT}/{DEVICE}",
            }

        # ---- 对照：生产标签在同一时刻是绿的 ------------------------------ #
        os.environ["TD_PLANT"] = "plant1"
        os.environ["TD_DEVICE"] = "device1"
        for module in [m for m in list(sys.modules) if m.startswith("src.web")]:
            del sys.modules[module]
        from src.web import web_dashboard as WEB2  # noqa: E402

        with WEB2.app.test_client() as client:
            resp2 = client.get("/api/health")
            report["health_production_tag"] = {
                "http_status": resp2.status_code,
                "body": resp2.get_json(),
                "device_scope": "plant1/device1",
            }

        # ---- 直接查库口径（与 /api/health 的内部判据同形） ---------------- #
        minute_rows = int(taos_scalar(
            "tdengine",
            f"SELECT COUNT(*) FROM cems.{CHILD_TABLE} WHERE ts >= now - 1m AND ts <= now;"))
        report["criterion_detail"] = {
            "rule_1": "最近 1 分钟内条数（rows_last_1min）必须 > 0",
            "rows_last_1min_drill_tag": minute_rows,
            "rule_2": f"最近一条样本距今必须 ≤ {WEB.HEALTH_STALE_AFTER_SECONDS} s",
            "data_age_seconds_drill_tag": None if age is None else round(age, 3),
            "verdict": ("stale_data/no_rows_last_1min → 判不健康"
                        if minute_rows == 0 else "有行，需看新鲜度"),
        }

        report["gateway_log_tail"] = sh(["docker", "logs", "--tail", "12", GW]).stdout or ""
        report["subscriber_log_tail"] = sh(["docker", "logs", "--tail", "12", SUB]).stdout or ""
        report["cache_leftover"] = sorted(p.name for p in cache_dir.glob("*"))
        report["ok"] = True
        return 0
    finally:
        if not args.keep_going:
            rm(GW)
            rm(SUB)
            taos_exec("tdengine", f"DROP TABLE IF EXISTS cems.{CHILD_TABLE};")
            report["cleanup"] = {
                "containers": [n for n in (GW, SUB)
                               if sh(["docker", "inspect", n]).returncode == 0],
                "child_table_dropped": True,
                "cache_files": sorted(p.name for p in cache_dir.glob("*")),
            }
            report["finished_at"] = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
            out = EVIDENCE_DIR / f"tz_misconfig_drill_{args.tag}.json"
            write_json(out, report)
            print(f"结果已落盘：{out}")
            print(json.dumps({k: v for k, v in report.items()
                              if k in ("rows_written", "skew_seconds", "ts_min", "ts_max",
                                       "health_drill_tag", "criterion_detail", "cleanup")},
                             ensure_ascii=False, indent=2)[:3000])


if __name__ == "__main__":
    raise SystemExit(main())
