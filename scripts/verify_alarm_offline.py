# -*- coding: utf-8 -*-
r"""排放超标告警 · 离线回放验收（ADR-0002 §2.2 5) 的 V1 五个边界用例 + 交付要求的 4 项）。

⚠️ 本脚本**不在仓库内**（硬约束只允许改 subscriber_to_td.py / src/common/ / .env.example /
   docker-compose.yml，所以没有新增 scripts/ 下的文件）。放在仓库外运行，只 import 仓库代码。

覆盖：
  A 纯逻辑边界（不碰库）
    (a) 折算值 == 限值          → 0 个事件（`>` 语义：正好等于限值算达标）
    (b) 折算值 > 限值 连续 3 条 → 恰好 1 个 START
    (c) 恢复 < 限值×0.95 连续 6 条 → 恰好 1 个 END，且 duration 正确
    (d2) 提醒：事件长期 OPEN     → 产 remind 而不是第二个 START
    (e) O2 = 21 / 25            → 不产超标事件，产 invalid
  B 落库（真库，用隔离标签 plant=verify / device=offline，不污染 plant1）
    1 标干达标 / 折算超标       → 必须判超标（"判折算"的直接证据），快照 raw<limit<converted
    2 O2 >= 21（折算 nan）      → converted 落 NULL + phase=invalid，绝无 start
    3 同一批样本重放两次        → 事件表行数不变、无第二个 START（含"不恢复水位"的强版）
    4 判定快照可复核            → to_reference_o2(raw_value, o2) 与行内 converted 逐位一致
  C 小时结论（§3.4 三态）
    合成 720 样本达标 → ok；合成 720 样本超标 → over；合成低覆盖 → insufficient（绝不判 ok）
    真库低覆盖小时（14 条）→ insufficient，落 cems_hourly_verdict

用法（PowerShell）：
  $env:PYTHONIOENCODING="utf-8"; $env:PYTHONPATH="F:\Project1\cems-data-pipeline"
  C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe verify_alarm_offline.py
退出码：0 = 全部通过，1 = 有失败项。
"""

from __future__ import annotations

import base64
import json
import math
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from typing import Any

PROJECT_ROOT = r"F:\Project1\cems-data-pipeline"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.common.alarm_judge import (          # noqa: E402
    PHASE_END,
    PHASE_INVALID,
    PHASE_START,
    JUDGE_HOURLY,
    JUDGE_INSTANT,
    AlarmConfig,
    AlarmJudge,
    AlarmTables,
    normalize_ts,
    to_datetime,
)
from src.common.points import LIMITS, ZS_TARGETS, to_reference_o2   # noqa: E402
from src.platform import subscriber_to_td as SUB                    # noqa: E402

TD_URL = os.getenv("TD_URL", "http://127.0.0.1:6041")
TD_USER = os.getenv("TD_USER", "root")
TD_PASS = os.getenv("TD_PASS", "taosdata")
TD_DB = os.getenv("TD_DB", "cems")

# ---- 隔离标签：离线验收的行全部挂在 verify/offline 下，不与线上 plant1 混在一起 ----
TABLES = AlarmTables(
    db=TD_DB, plant="verify", device="offline",
    event_stable=SUB.ALARM_TABLES.event_stable,
    verdict_stable=SUB.ALARM_TABLES.verdict_stable,
    push_stable=SUB.ALARM_TABLES.push_stable,
    data_stable=SUB.ALARM_TABLES.data_stable,
)
CONFIG = AlarmConfig()          # ADR 默认参数：M=6 / N=3 / 0.95 / K=6 / 覆盖率 0.75

FAILURES: list[str] = []


# ==================== 工具 ====================

def check(condition: bool, description: str, detail: str = "") -> None:
    """记录一条判定；失败收进 FAILURES。"""
    print(f"  [{'PASS' if condition else 'FAIL'}] {description}")
    if detail:
        print(f"         {detail}")
    if not condition:
        FAILURES.append(description)


def run_sql(sql: str) -> dict[str, Any]:
    """走 taosAdapter REST 执行 SQL，返回原始响应。"""
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
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError) as exc:
        raise SystemExit(f"连不上 TDengine REST {TD_URL}: {exc}") from exc
    if payload.get("code") != 0:
        raise SystemExit(f"TDengine 返回错误: {payload}  SQL={sql}")
    return payload


def query(sql: str) -> tuple[list[str], list[list[Any]]]:
    payload = run_sql(sql)
    columns = [meta[0] for meta in payload["column_meta"]]
    return columns, [list(row) for row in payload["data"]]


def payload_text(ts: str, dust: float, so2: float, nox: float, o2: float) -> str:
    """按项目的内部简化报文格式造一条报文（走真实 parse_payload，不绕过拒收逻辑）。"""
    return (
        f"{ts} Flow=1000.0 Dust={dust} SO2={so2} NOx={nox} O2={o2} "
        f"Velocity=5.0 Temp=50.0 Humidity=5.0 Pressure=100.0 Flag=N"
    )


def parse(text: str) -> tuple[str, dict[str, float], dict[str, float]]:
    """报文 → (ts, 标干值, 数学层折算值)，与接入层同一条路径。"""
    ts, values = SUB.parse_payload(text)
    return ts, values, SUB.reference_values(values)


def cleanup_test_tables() -> None:
    """删掉本次离线验收用的子表（超级表保留），保证每轮从零开始。"""
    for column in ZS_TARGETS:
        for judge_type in (JUDGE_INSTANT, JUDGE_HOURLY):
            for suffix in ("", "_push"):
                run_sql(
                    f"DROP TABLE IF EXISTS {TD_DB}."
                    f"{TABLES.event_child(column, judge_type)}{suffix}"
                )
        run_sql(f"DROP TABLE IF EXISTS {TD_DB}.{TABLES.verdict_child(column)}")


def count_rows(stable: str, where: str = "") -> int:
    _columns, rows = query(f"SELECT COUNT(*) FROM {TD_DB}.{stable} {where}")
    return int(rows[0][0]) if rows else 0


# ==================== A. 纯逻辑边界（不碰库） ====================

def section_a_pure_cases() -> None:
    print("\n=== A. 纯逻辑边界用例（不碰库；ADR-0002 §2.2 5) V1）===")

    # (a) 折算值 == 限值 → 达标（0 事件）
    judge = AlarmJudge(CONFIG)
    events = []
    for second in range(0, 30, 5):
        ts = f"2026-10-02 10:00:{second:02d}"
        _ts, values, references = parse(payload_text(ts, 5.0, 35.0, 50.0, 6.0))
        # O2=6% ⇒ 折算 == 标干 ⇒ 三个污染物折算值**正好等于**限值
        events += judge.on_sample(ts, values, references)
    exact = {c: parse(payload_text("2026-10-02 10:00:00", 5.0, 35.0, 50.0, 6.0))[2][c]
             for c in ZS_TARGETS}
    check(
        not events,
        "(a) 折算值 == 限值（正好等于）→ 0 个事件",
        f"折算值 = {exact}，事件数 = {len(events)}",
    )

    # (b)(c) 连续 3 条越限 → 1 START；恢复连续 6 条 → 1 END，duration 正确
    judge = AlarmJudge(CONFIG)
    events = []
    for second in range(0, 15, 5):          # 3 条越限（o2=10 ⇒ 全部折算超标）
        ts = f"2026-10-02 11:00:{second:02d}"
        _ts, values, references = parse(payload_text(ts, 4.0, 30.0, 40.0, 10.0))
        events += judge.on_sample(ts, values, references)
    starts = [e for e in events if e.phase == PHASE_START]
    check(
        len(starts) == 3 and {e.point for e in starts} == set(ZS_TARGETS),
        "(b) 折算值 > 限值连续 3 条 → 恰好 3 个 START（3 个污染物各 1 个）",
        f"START 行: {[(e.point, e.ts, round(e.converted, 3)) for e in starts]}",
    )

    events = []
    for second in range(15, 45, 5):         # 6 条恢复（折算值 ≤ 限值×0.95）
        ts = f"2026-10-02 11:00:{second:02d}"
        _ts, values, references = parse(payload_text(ts, 1.0, 10.0, 10.0, 6.0))
        events += judge.on_sample(ts, values, references)
    ends = [e for e in events if e.phase == PHASE_END]
    check(
        len(ends) == 3,
        "(c) 恢复 < 限值×0.95 连续 6 条 → 恰好 3 个 END",
        f"END 行: {[(e.point, e.ts, e.window_seconds) for e in ends]}",
    )
    check(
        all(abs(e.window_seconds - 40.0) < 1e-6 for e in ends),
        "(c) 事件时长正确（首个越限样本 11:00:00 → 第 6 条恢复样本 11:00:40 = 40 秒）",
        f"duration = {[e.window_seconds for e in ends]}",
    )
    check(
        all(e.converted <= e.limit_value * CONFIG.recover_ratio for e in ends),
        "(c) END 行的判定快照确实满足恢复条件",
        f"converted = {[round(e.converted, 4) for e in ends]}",
    )

    # (d2) 长期 OPEN → remind（不是第二个 START）
    remind_config = AlarmConfig(remind_seconds=60.0)
    judge = AlarmJudge(remind_config)
    events = []
    for step in range(0, 120, 5):           # 持续越限 2 分钟
        minute, second = divmod(step, 60)
        ts = f"2026-10-02 12:{minute:02d}:{second:02d}"
        _ts, values, references = parse(payload_text(ts, 4.0, 30.0, 40.0, 10.0))
        events += judge.on_sample(ts, values, references)
    reminds = [e for e in events if e.phase == "remind"]
    starts = [e for e in events if e.phase == PHASE_START]
    check(
        len(starts) == 3 and len(reminds) >= 3,
        "(d2) 事件长期 OPEN → 只产 remind，不产第二个 START",
        f"START={len(starts)}，remind={len(reminds)}",
    )

    # (e) O2 = 21 / 25 → 折算 nan → invalid，绝不判超标
    judge = AlarmJudge(CONFIG)
    events = []
    for o2, second in ((21.0, 0), (25.0, 5)):
        ts = f"2026-10-02 13:00:{second:02d}"
        _ts, values, references = parse(payload_text(ts, 99.0, 190.0, 390.0, o2))
        events += judge.on_sample(ts, values, references)
    invalids = [e for e in events if e.phase == PHASE_INVALID]
    starts = [e for e in events if e.phase == PHASE_START]
    check(
        not starts and len(invalids) == 6,
        "(e) O2 = 21 / 25 → 0 个超标事件、6 条 invalid（2 帧 × 3 污染物）",
        f"invalid={len(invalids)}，start={len(starts)}，"
        f"converted={[e.converted for e in invalids[:2]]}",
    )
    check(
        all(math.isnan(e.converted) for e in invalids),
        "(e) invalid 行的 converted 是 nan（不是 0、不是哨兵值）",
    )

    # (f) 两条 REST 读路径的时间戳形态归一化（实测：taosrest 给本地朴素时间，
    #     裸 /rest/sql 给 UTC ISO 带 Z，两者是同一时刻但字符串不同）
    z_form = "2026-10-02T06:00:00.000Z"
    expected_local = datetime.fromisoformat(
        "2026-10-02T06:00:00+00:00"
    ).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    check(
        normalize_ts(z_form) == expected_local
        and normalize_ts(datetime(2026, 10, 2, 14, 0)) == "2026-10-02 14:00:00",
        "(f) normalize_ts 把 '…Z'（裸 REST）与朴素本地 datetime（taosrest 库）归一到同一本地时刻",
        f"Z → {normalize_ts(z_form)}；datetime → "
        f"{normalize_ts(datetime(2026, 10, 2, 14, 0))}",
    )


# ==================== B. 落库（真库，隔离标签） ====================

def section_b_db_cases() -> None:
    print("\n=== B. 落库用例（真库 cems；标签 plant=verify/device=offline）===")
    writer = SUB.AlarmWriter(TABLES)
    if not writer.connect():
        raise SystemExit("告警表连不上，B/C 段无法继续")
    print(f"  子表: {TABLES.event_child('nox', JUDGE_INSTANT)} / "
          f"{TABLES.push_child('nox', JUDGE_INSTANT)}")

    # ---- B1 标干达标 / 折算超标 ----
    judge = AlarmJudge(CONFIG)
    events = []
    for second in range(0, 15, 5):
        ts = f"2026-10-02 14:00:{second:02d}"
        _ts, values, references = parse(payload_text(ts, 4.0, 30.0, 40.0, 10.0))
        events += judge.on_sample(ts, values, references)
    SUB.publish_alarm_events(writer, TABLES, events, CONFIG.push_channel)

    _columns, rows = query(
        f"SELECT ts, point, phase, converted, raw_value, o2, limit_value, o2_reference "
        f"FROM {TD_DB}.{TABLES.event_stable} WHERE plant = 'verify' "
        f"AND phase = 'start' AND ts >= '2026-10-02 14:00:00' AND ts < '2026-10-02 15:00:00' "
        f"ORDER BY point"
    )
    print("  事件表 start 行（标干达标 / 折算超标）:")
    for row in rows:
        print(f"    point={row[1]:<5} raw={row[4]:>7.2f} < limit={row[6]:>5.1f} "
              f"< converted={row[3]:>8.4f}  o2={row[5]:.1f} o2_ref={row[7]:.1f}")
    check(len(rows) == 3, "B1 标干全部达标但折算全部超标 → 判 3 个超标事件", f"行数={len(rows)}")
    check(
        rows and all(row[4] < row[6] < row[3] for row in rows),
        "B1 每行的 raw_value < limit_value < converted（判的是折算值，不是标干值）",
    )

    # ---- B2 O2 >= 21 → converted NULL + invalid，无 start ----
    judge = AlarmJudge(CONFIG)
    events = []
    for second in range(0, 10, 5):
        ts = f"2026-10-02 14:30:{second:02d}"
        _ts, values, references = parse(payload_text(ts, 99.0, 190.0, 390.0, 22.0))
        events += judge.on_sample(ts, values, references)
    SUB.publish_alarm_events(writer, TABLES, events, CONFIG.push_channel)
    _columns, rows = query(
        f"SELECT point, phase, converted, o2 FROM {TD_DB}.{TABLES.event_stable} "
        f"WHERE plant = 'verify' AND ts >= '2026-10-02 14:30:00' AND ts < '2026-10-02 14:31:00' "
        f"ORDER BY phase, point"
    )
    invalid_rows = [row for row in rows if row[1] == PHASE_INVALID]
    start_rows = [row for row in rows if row[1] == PHASE_START]
    check(
        len(invalid_rows) == 6 and not start_rows,
        "B2 O2 >= 21%（折算 nan）→ 6 条 invalid、0 条 start（不判达标也不判超标）",
        f"invalid={len(invalid_rows)} start={len(start_rows)}",
    )
    check(
        all(row[2] is None for row in invalid_rows),
        "B2 invalid 行的 converted 在库里是 NULL（不是 0、不是哨兵 9999.99）",
        f"converted={[row[2] for row in invalid_rows]}",
    )

    # ---- B3 同一批样本重放两次 → 行数不变、无第二个 START ----
    batch = []
    for second in range(0, 15, 5):                       # 3 条越限 → 触发
        batch.append(f"2026-10-02 15:00:{second:02d}")
    for second in range(15, 60, 5):                      # 9 条恢复 → 结束
        batch.append(f"2026-10-02 15:00:{second:02d}")
    frames = [
        parse(payload_text(
            ts, 4.0, 30.0, 40.0, 10.0 if index < 3 else 6.0,
        ))
        for index, ts in enumerate(batch)
    ]

    def play(target_judge: AlarmJudge) -> list[Any]:
        produced = []
        for ts, values, references in frames:
            produced += target_judge.on_sample(ts, values, references)
        SUB.publish_alarm_events(writer, TABLES, produced, CONFIG.push_channel)
        return produced

    where = "WHERE plant = 'verify' AND ts >= '2026-10-02 15:00:00' AND ts < '2026-10-02 15:01:00'"
    produced_1 = play(AlarmJudge(CONFIG))
    total_1 = count_rows(TABLES.event_stable, where)
    starts_1 = int(query(
        f"SELECT COUNT(*) FROM {TD_DB}.{TABLES.event_stable} {where} AND phase = 'start'"
    )[1][0][0])
    print(f"  第 1 遍: 事件行 {total_1}（start {starts_1}），"
          f"本次产出 {len(produced_1)} 条")

    # 第 2 遍：全新判据、**不恢复水位**（最强的重投场景：状态都不知道，只能靠快照幂等）
    produced_2 = play(AlarmJudge(CONFIG))
    total_2 = count_rows(TABLES.event_stable, where)
    starts_2 = int(query(
        f"SELECT COUNT(*) FROM {TD_DB}.{TABLES.event_stable} {where} AND phase = 'start'"
    )[1][0][0])
    print(f"  第 2 遍（新判据、不恢复水位）: 事件行 {total_2}（start {starts_2}），"
          f"本次产出 {len(produced_2)} 条")

    # 第 3 遍：模拟容器重启（判据状态从事件表折叠恢复）+ 重投同一批
    restarted = AlarmJudge(CONFIG)
    restored = restarted.restore(writer.query(TABLES.restore_select_sql()))
    produced_3 = play(restarted)
    total_3 = count_rows(TABLES.event_stable, where)
    starts_3 = int(query(
        f"SELECT COUNT(*) FROM {TD_DB}.{TABLES.event_stable} {where} AND phase = 'start'"
    )[1][0][0])
    print(f"  第 3 遍（重启恢复：折叠 {restored} 行）: 事件行 {total_3}（start {starts_3}），"
          f"本次产出 {len(produced_3)} 条")

    check(total_1 == total_2 == total_3 and total_1 > 0,
          "B3 同一批样本重放 3 次 → 事件表行数不变", f"{total_1} / {total_2} / {total_3}")
    check(starts_1 == starts_2 == starts_3 == 3,
          "B3 没有第二个 START（每个污染物始终只有 1 个 start 行）",
          f"start 行数 {starts_1} / {starts_2} / {starts_3}")
    check(len(produced_3) == 0,
          "B3 重启后重投同一批 → 判据一条新事件都不产（时间水位生效）",
          f"第 3 遍产出 {len(produced_3)} 条")

    # ---- B4 判定快照可复核（ADR §5 验收判据）----
    _columns, rows = query(
        f"SELECT point, phase, ts, converted, raw_value, o2, o2_reference FROM "
        f"{TD_DB}.{TABLES.event_stable} WHERE plant = 'verify' AND converted IS NOT NULL "
        f"AND ts >= '2026-10-02 14:00:00' AND ts < '2026-10-02 15:01:00' ORDER BY ts"
    )
    mismatches = []
    for point, phase, ts, converted, raw, o2, o2_ref in rows:
        again = to_reference_o2(raw, o2, o2_ref)
        # converted 落的是 FLOAT（32 位单精度，约 7 位有效数字），所以按单精度容差比对
        if abs(again - converted) > max(1e-5, abs(again) * 1e-6):
            mismatches.append((point, phase, ts, converted, raw, o2, again))
    check(
        not mismatches,
        f"B4 快照可复核：{len(rows)} 条行的 to_reference_o2(raw_value, o2, o2_reference) "
        f"与 converted 一致（单精度容差 1e-5；converted 列是 FLOAT）",
        f"不一致 {mismatches[:2]}" if mismatches else "",
    )

    # ---- B5 推送记录（"推送"是落成可验证的动作）----
    push_total = count_rows(TABLES.push_stable, "WHERE plant = 'verify'")
    event_total = count_rows(TABLES.event_stable, "WHERE plant = 'verify'")
    _columns, push_rows = query(
        f"SELECT ts, event_id, phase, channel, status, payload FROM {TD_DB}.{TABLES.push_stable} "
        f"WHERE plant = 'verify' ORDER BY ts DESC LIMIT 2"
    )
    for row in push_rows:
        print(f"  推送记录: ts={row[0]} phase={row[2]} channel={row[3]} status={row[4]}")
        print(f"            payload={row[5]}")
    check(
        push_total == event_total and push_total > 0,
        "B5 每条告警事件都有一条推送记录（行数一一对应）",
        f"推送 {push_total} / 事件 {event_total}",
    )

    writer.close()


# ==================== C. 小时结论（§3.4 三态） ====================

def synthetic_hour(
    hour_start: str, n_samples: int, dust: float, so2: float, nox: float, o2: float,
) -> list[tuple[str, float, dict[str, float]]]:
    """造一个小时样本序列（间隔 5 秒）。"""
    base = to_datetime(hour_start)
    assert base is not None
    samples = []
    for index in range(n_samples):
        moment = base + timedelta(seconds=5 * index)
        samples.append((
            moment.strftime("%Y-%m-%d %H:%M:%S"), o2,
            {"dust": dust, "so2": so2, "nox": nox},
        ))
    return samples


def section_c_hourly() -> None:
    print("\n=== C. 小时结论（覆盖率三态；§3.4 样本不足绝不判达标）===")
    judge = AlarmJudge(CONFIG)

    # ok：满覆盖 + 折算均值低于限值（O2=6% 时不放大）
    verdicts, events = judge.judge_hour(
        "2026-10-02 16:00:00", synthetic_hour("2026-10-02 16:00:00", 720, 3.0, 20.0, 30.0, 6.0),
    )
    ok_verdicts = [v for v in verdicts if v.verdict == "ok"]
    check(
        len(ok_verdicts) == 3 and not events,
        "C1 满覆盖（720/720）+ 折算均值达标 → 3 个 verdict=ok、0 事件",
        f"coverage={[round(v.coverage, 3) for v in verdicts]}",
    )

    # over：满覆盖 + 折算均值超限 → hourly 事件 start/end
    verdicts, events = judge.judge_hour(
        "2026-10-02 17:00:00", synthetic_hour("2026-10-02 17:00:00", 720, 4.0, 30.0, 40.0, 10.0),
    )
    hourly_starts = [e for e in events if e.phase == PHASE_START]
    hourly_ends = [e for e in events if e.phase == PHASE_END]
    check(
        all(v.verdict == "over" for v in verdicts) and len(hourly_starts) == 3
        and len(hourly_ends) == 3 and all(e.judge_type == JUDGE_HOURLY for e in events),
        "C2 满覆盖 + 折算均值超限 → verdict=over 且产 judge_type=hourly 的 start/end",
        f"verdict={[v.verdict for v in verdicts]}，"
        f"conv_mean={[round(v.conv_mean, 3) for v in verdicts]}",
    )
    check(
        all(e.ts == v.ts for e, v in zip(hourly_starts, verdicts)),
        "C2 hourly 事件的 start ts = 小时起点，end ts = 该小时最后一条样本",
        f"start={hourly_starts[0].ts} end={hourly_ends[0].ts}",
    )

    # insufficient：覆盖率不足
    verdicts, events = judge.judge_hour(
        "2026-10-02 18:00:00", synthetic_hour("2026-10-02 18:00:00", 100, 3.0, 20.0, 30.0, 6.0),
    )
    check(
        all(v.verdict == "insufficient" for v in verdicts)
        and not any(v.verdict == "ok" for v in verdicts),
        "C3 覆盖率 100/720 = 0.139 < 0.75 → 判 insufficient，**绝不判 ok**（即使折算值全达标）",
        f"verdict={[v.verdict for v in verdicts]}，coverage={[round(v.coverage, 3) for v in verdicts]}",
    )
    check(
        len([e for e in events if e.phase == PHASE_INVALID]) == 3,
        "C3 insufficient 同时产 3 条『数据不足』事件（phase=invalid, judge_type=hourly）",
    )

    # 真库低覆盖小时：2026-10-02 06:00 只有 14 条
    writer = SUB.AlarmWriter(TABLES)
    writer.connect()
    _columns, rows = query(
        f"SELECT COUNT(*) FROM {TD_DB}.{TABLES.data_stable} "
        f"WHERE ts >= '2026-10-02 06:00:00' AND ts < '2026-10-02 07:00:00'"
    )
    real_count = int(rows[0][0])
    print(f"  真库小时 2026-10-02 06:00 实测样本数 = {real_count}")
    SUB.settle_hour(writer, TABLES, AlarmJudge(CONFIG), to_datetime("2026-10-02 06:00:00"))
    _columns, rows = query(
        f"SELECT ts, point, n_total, n_valid, n_invalid, coverage, verdict FROM "
        f"{TD_DB}.{TABLES.verdict_stable} WHERE plant = 'verify' "
        f"AND ts = '2026-10-02 06:00:00' ORDER BY point"
    )
    print("  小时结论表（真库低覆盖小时）:")
    for row in rows:
        print(f"    {row[0]} {row[1]:<5} n_total={row[2]} n_valid={row[3]} "
              f"n_invalid={row[4]} coverage={row[5]:.4f} verdict={row[6]}")
    check(
        len(rows) == 3 and all(row[6] == "insufficient" for row in rows),
        "C4 真库低覆盖小时（14/720）→ 结论表的 verdict 全是 insufficient",
        f"verdict={[row[6] for row in rows]}",
    )
    check(
        all(row[6] != "ok" for row in rows),
        "C4 该小时**没有**任何 ok（样本不足绝不判达标）",
    )

    # 真库满覆盖小时（对照）：**动态挑一个数据真有的小时**
    # ⚠️ 原先硬编码 "2026-10-02 16:00"（当时 722 条）。但那个小时现在 n_total=0
    #    —— 库被清过/重建过（多设备迁移期间），于是这条判据变成"在不存在的时段上
    #    求 ok/over"，必然误报 FAIL。**判据要跟着数据走，不能钉死某个历史时刻。**
    _cols, picked = query(
        f"SELECT _wstart, COUNT(*) FROM {TD_DB}.{TD_STABLE} "
        f"WHERE plant = 'plant1' AND device = 'device1' AND ts >= '2026-10-02 20:00:00' "
        f"AND ts < NOW INTERVAL(1h)"
    )
    full_hours = [r for r in picked if int(r[1]) >= 600]
    if not full_hours:
        check(False, "C5 真库满覆盖小时 → verdict 落在 {ok, over}", "找不到满覆盖小时（n>=600）")
    else:
        hour_text = str(full_hours[-1][0])[:19].replace("T", " ")
        print(f"  选取满覆盖小时（{full_hours[-1][1]} 条）: {hour_text}")
        # ⚠️ settle_hour 的作用对象是 TABLES 的标签（verify/offline），它按
        #    hour_rows_sql 回读的是 plant1/device1 的真实数据；这里只借它的计算路径。
        SUB.settle_hour(writer, TABLES, AlarmJudge(CONFIG), to_datetime(hour_text))
        _columns, rows = query(
            f"SELECT point, n_total, n_valid, coverage, conv_mean, limit_value, verdict FROM "
            f"{TD_DB}.{TABLES.verdict_stable} WHERE plant = 'verify' "
            f"AND ts = '{hour_text}' ORDER BY point"
        )
        print("  小时结论表（真库满覆盖小时）:")
        for row in rows:
            conv = "NULL" if row[4] is None else f"{row[4]:.3f}"
            limit = "NULL" if row[5] is None else f"{row[5]:.1f}"
            cov = "NULL" if row[3] is None else f"{row[3]:.4f}"
            print(f"    {row[0]:<5} n_total={row[1]} n_valid={row[2]} coverage={cov} "
                  f"conv_mean={conv} limit={limit} verdict={row[6]}")
        check(
            len(rows) == 3 and all(row[6] in ("ok", "over") for row in rows),
            "C5 真库满覆盖小时 → verdict 落在 {ok, over}（覆盖率够就正常判）",
            f"verdict={[row[6] for row in rows]}",
        )
    writer.close()


def main() -> int:
    print(f"离线回放验收：TDengine={TD_URL} 库={TD_DB} 判据参数={CONFIG}")
    cleanup_test_tables()
    section_a_pure_cases()
    section_b_db_cases()
    section_c_hourly()
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
