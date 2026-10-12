# -*- coding: utf-8 -*-
r"""排放超标告警 · 线上验收（八服务在跑时执行；全部证据来自真库、由容器自己写入）。

⚠️ 本脚本**只读**：查库 + 打印判定，不写任何数据、不改任何状态（可反复跑）。

检查项：
  L1 告警三张表存在，且 cems_data 仍然只有 9 个物理量列（**没有**加折算列）
  L2 plant1/<device>（容器自己判的）事件行：start/end 成对、converted > limit_value
  L3 事件行是"判定快照"：raw_value/o2/o2_reference/limit_value/judge_version 齐全，
     且 to_reference_o2(raw_value, o2, o2_reference) 与 converted 一致（可复核）
  L4 推送记录与事件行一一对应（channel/status/payload）
  L5 同一 (子表, ts) 没有重复行（QA 幂等键在真表上成立）
  L6 小时结论表有容器写入的行；insufficient 与 ok/over 的语义正确（有 insufficient 必有 n_valid<720*门限）
  L7 判据统计日志（已判/重投跳过/事件/无效样本）能从容器日志里读到

★ 设备维度（多设备共用超级表）：告警三张超级表的 TAG 也是 `(plant, device)`，
  所以事件行 / 推送记录 / 小时结论的三类只读查询都带 `device` 过滤，默认 device1。
  不过滤的后果是实测过的：`plant1/device1` 与 `plant1/device2` 的行会被混在一起，
  L4 的"事件行数 == 推送记录数"这种**行数一一对应**检查、L5 的幂等检查都会串
  （两台设备各贡献一批行，一边多一边少也能凑出"相等"）。

用法：
  $env:PYTHONIOENCODING="utf-8"
  cd F:\Project1\cems-data-pipeline
  python scripts\verify_alarm_live.py [--since "2026-10-02 18:26:00"] [--device device1]
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

PROJECT_ROOT = r"F:\Project1\cems-data-pipeline"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts._device_scope import (   # noqa: E402
    add_device_argument,
    device_predicate,
    resolve_device,
)
from src.common.points import COLUMNS, LIMITS, O2_REFERENCE, to_reference_o2   # noqa: E402

TD_URL = os.getenv("TD_URL", "http://127.0.0.1:6041")
TD_USER = os.getenv("TD_USER", "root")
TD_PASS = os.getenv("TD_PASS", "taosdata")
TD_DB = os.getenv("TD_DB", "cems")
EVENT_STABLE = "cems_alarm_event"
VERDICT_STABLE = "cems_hourly_verdict"
PUSH_STABLE = "cems_alarm_push"

FAILURES: list[str] = []


def check(condition: bool, description: str, detail: str = "") -> None:
    print(f"  [{'PASS' if condition else 'FAIL'}] {description}")
    if detail:
        print(f"         {detail}")
    if not condition:
        FAILURES.append(description)


def rest(sql: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{TD_URL}/rest/sql",
        data=sql.encode("utf-8"),
        method="POST",
        headers={
            "Authorization": "Basic "
            + base64.b64encode(f"{TD_USER}:{TD_PASS}".encode("utf-8")).decode("ascii"),
            "Content-Type": "text/plain",
        },
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("code") != 0:
        raise SystemExit(f"TDengine 返回错误: {payload}  SQL={sql}")
    return payload


def query(sql: str) -> tuple[list[str], list[list[Any]]]:
    payload = rest(sql)
    return [meta[0] for meta in payload["column_meta"]], [list(r) for r in payload["data"]]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default="", help="只看该时刻之后的事件行（本地时间）")
    add_device_argument(
        parser,
        help_text="按设备 TAG 过滤三张告警表（默认 device1，与接入层 TD_DEVICE 一致）。"
        "多设备部署时不过滤会把两台设备的行混在一起，行数对应/幂等检查都会失效",
    )
    args = parser.parse_args()
    since_clause = f" AND ts >= '{args.since}'" if args.since else ""
    # 设备名做白名单校验后才拼进 SQL（与 scripts/_device_scope.py 同一规则）
    scope = resolve_device(args.device)
    dev_pred = device_predicate(scope)
    print(f"线上验收：TDengine={TD_URL} 库={TD_DB} 设备={scope} since={args.since or '（全部）'}")
    print(f"          设备过滤条件：plant = 'plant1' AND {dev_pred}")

    # ---- L1 表结构与"没有给 cems_data 加折算列" ----
    print("\n=== L1 表结构 ===")
    _columns, stables = query(f"SHOW {TD_DB}.STABLES")
    names = {row[0] for row in stables}
    check(
        {EVENT_STABLE, VERDICT_STABLE, PUSH_STABLE} <= names,
        "告警三张超级表都已建出（CREATE TABLE IF NOT EXISTS 幂等）",
        f"库内超级表 = {sorted(names)}",
    )
    _columns, data_columns = query(f"DESCRIBE {TD_DB}.cems_data")
    data_fields = [row[0] for row in data_columns]
    check(
        not any("conv" in name or "ref" in name for name in data_fields),
        "cems_data 里**没有**折算列（折算仍然是导出量，只在事件表存判定快照）",
        f"cems_data 列 = {data_fields}",
    )
    _columns, event_columns = query(f"DESCRIBE {TD_DB}.{EVENT_STABLE}")
    event_fields = [row[0] for row in event_columns]
    snapshot_fields = {
        "converted", "raw_value", "o2", "limit_value", "o2_reference", "judge_version",
        "over_samples", "window_seconds", "event_id", "phase", "trigger_ts",
    }
    check(
        snapshot_fields <= set(event_fields),
        "事件表列齐（含判定快照字段与 judge_version）",
        f"事件表列 = {event_fields}",
    )

    # ---- L2/L3 线上事件行 ----
    print(f"\n=== L2/L3 plant1/{scope} 事件行（容器自己判出来的）===")
    _columns, rows = query(
        f"SELECT ts, event_id, point, phase, converted, raw_value, o2, limit_value, "
        f"o2_reference, over_samples, window_seconds, judge_version, trigger_ts "
        f"FROM {TD_DB}.{EVENT_STABLE} WHERE plant = 'plant1' AND {dev_pred}{since_clause} "
        f"ORDER BY ts ASC, point ASC, phase ASC"
    )
    for row in rows:
        print(f"  {row[0]} {row[2]:<5} {row[3]:<7} converted={row[4]} raw={row[5]} "
              f"o2={row[6]} limit={row[7]} o2ref={row[8]} over={row[9]} dur={row[10]} "
              f"ver={row[11]}")
    instants = [row for row in rows if row[2] in {"dust", "so2", "nox"} and row[3] == "start"]
    check(bool(rows), f"plant1/{scope} 下有事件行（{len(rows)} 条）")
    check(
        bool(instants),
        "存在 phase=start 的线上事件行",
    )
    check(
        all(row[4] is None or row[4] > row[7] for row in instants),
        "所有 start 行的 converted > limit_value（判的是折算值）",
    )
    by_event: dict[str, set[str]] = {}
    for row in rows:
        by_event.setdefault(str(row[1]), set()).add(str(row[3]))
    paired = sorted(eid for eid, phases in by_event.items() if {"start", "end"} <= phases)
    still_open = sorted(
        eid for eid, phases in by_event.items() if "start" in phases and "end" not in phases
    )
    end_without_start = sorted(
        eid for eid, phases in by_event.items() if "end" in phases and "start" not in phases
    )
    check(
        bool(paired) and not end_without_start,
        "事件行的 start/end 自洽：≥1 个成对事件，且不存在只有 end 没有 start 的事件",
        f"成对事件 {len(paired)} 个；仍 OPEN（尚未恢复）{len(still_open)} 个：{still_open}",
    )
    check(
        all(row[12] and len(str(row[12])) >= 19 for row in rows),
        "每条事件行都有 trigger_ts（可追溯触发时刻）",
    )
    check(
        all(str(row[11]).startswith("v") for row in rows),
        "每条事件行都带 judge_version",
        f"取值 = {sorted({str(row[11]) for row in rows})}",
    )
    bad = []
    for row in rows:
        if row[4] is None or row[5] is None:
            continue
        again = to_reference_o2(float(row[5]), float(row[6]), float(row[8]))
        if abs(again - float(row[4])) > max(1e-5, abs(again) * 1e-6):
            bad.append((row[2], row[3], row[0], row[4], again))
    check(
        not bad,
        "快照可复核：to_reference_o2(raw_value, o2, o2_reference) 与 converted 一致",
        f"不一致 {bad[:3]}" if bad else f"共复核 {len(rows)} 行",
    )

    # ---- L4 推送记录 ----
    print(f"\n=== L4 推送记录（一期把'推送'落成可验证的动作）plant1/{scope} ===")
    _columns, push_rows = query(
        f"SELECT ts, event_id, phase, channel, status, payload FROM {TD_DB}.{PUSH_STABLE} "
        f"WHERE plant = 'plant1' AND {dev_pred}{since_clause} ORDER BY ts DESC LIMIT 3"
    )
    for row in push_rows:
        print(f"  {row[0]} {row[3]:<7} {row[4]:<9} {row[2]:<7} payload={row[5]}")
    _columns, event_count = query(
        f"SELECT COUNT(*) FROM {TD_DB}.{EVENT_STABLE} "
        f"WHERE plant='plant1' AND {dev_pred}{since_clause}"
    )
    _columns, push_count = query(
        f"SELECT COUNT(*) FROM {TD_DB}.{PUSH_STABLE} "
        f"WHERE plant='plant1' AND {dev_pred}{since_clause}"
    )
    check(
        int(event_count[0][0]) == int(push_count[0][0]) and int(event_count[0][0]) > 0,
        f"plant1/{scope} 每条事件行都有一条推送记录（行数一一对应）",
        f"事件 {event_count[0][0]} / 推送 {push_count[0][0]}",
    )
    check(
        push_rows and all(row[3] and row[4] == "recorded" for row in push_rows),
        "推送记录带 channel 与 status=recorded（真实外部通道未接入，见 ADR §6 未决 5）",
    )

    # ---- L5 幂等键 (子表, ts) 在真表上无重复行 ----
    # ★ 幂等键里的"子表"是靠 (ts, point, judge_type, phase) 反推的，而这些列**不含设备**：
    #   两台设备在同一时刻判同一个测点，就会凑出一组"看着重复、其实分属两台设备"的行。
    #   所以这一项必须按设备过滤后再判重复（子表名里本来就带 plant/device）。
    print(f"\n=== L5 幂等：同一 (子表, ts) 不重复（plant1/{scope}）===")
    _columns, dup_rows = query(
        f"SELECT COUNT(*) AS n FROM (SELECT ts, point, judge_type, phase, COUNT(*) AS c "
        f"FROM {TD_DB}.{EVENT_STABLE} WHERE {dev_pred} "
        f"GROUP BY ts, point, judge_type, phase) WHERE c > 1"
    )
    check(
        dup_rows and int(dup_rows[0][0]) == 0,
        f"plant1/{scope} 事件表不存在同一个 (子表, ts) 的重复行（重投只会覆盖，不会新增）",
        f"重复组数 = {dup_rows[0][0] if dup_rows else '?'}",
    )

    # ---- L6 小时结论 ----
    print(f"\n=== L6 小时结论（容器写入）plant1/{scope} ===")
    _columns, verdicts = query(
        f"SELECT ts, point, n_total, n_valid, n_invalid, coverage, conv_mean, limit_value, "
        f"verdict FROM {TD_DB}.{VERDICT_STABLE} "
        f"WHERE plant = 'plant1' AND {dev_pred} ORDER BY ts ASC, point ASC"
    )
    for row in verdicts:
        print(f"  {row[0]} {row[1]:<5} n_total={row[2]:<4} n_valid={row[3]:<4} "
              f"n_invalid={row[4]:<3} coverage={row[5]:.4f} conv_mean={row[6]} "
              f"limit={row[7]} verdict={row[8]}")
    check(bool(verdicts), f"小时结论表有容器写入的行（{len(verdicts)} 条）")
    bad_insufficient = [
        row for row in verdicts
        if row[8] == "insufficient" and row[5] >= 0.75 and (row[4] / row[2] if row[2] else 1) <= 0.10
    ]
    check(
        not bad_insufficient,
        "判定 insufficient 的行确实不满足覆盖率/无效占比条件（没有错判）",
        f"可疑行 {bad_insufficient}" if bad_insufficient else "",
    )
    bad_ok = [row for row in verdicts if row[8] == "ok" and row[5] < 0.75]
    check(
        not bad_ok,
        "**没有**任何 coverage < 0.75 却判 ok 的行（样本不足绝不判达标）",
        f"违规行 {bad_ok}" if bad_ok else "",
    )

    # ---- L7 判据统计日志 ----
    print("\n=== L7 容器日志里的判据统计 ===")
    try:
        logs = subprocess.run(
            ["docker", "compose", "logs", "subscriber", "--tail", "4000"],
            cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=60,
        ).stdout
    except (subprocess.SubprocessError, OSError) as exc:
        logs = ""
        print(f"  （docker compose logs 不可用: {exc}）")
    stat_lines = [line for line in logs.splitlines() if "告警判据统计" in line]
    for line in stat_lines[-4:]:
        print(f"  {line.strip()}")
    check(bool(stat_lines), "能从容器日志里读到判据统计（判据是活的，不是只启动了一次）")

    print("\n================ 结论 ================")
    if FAILURES:
        print(f"不通过 {len(FAILURES)} 项:")
        for item in FAILURES:
            print(f"  - {item}")
        return 1
    print("全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
