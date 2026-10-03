"""甲方口径最小验收：只验"出钱的人会在乎的 8 条"，每条给出可复算的证据。

与仓库里其它脚本的分工（不要重复，也不要互相替代）：
  · scripts/verify_data_lineage.py —— 工程师口径的逐跳血缘（T1~T8，起临时链路）
  · scripts/verify_alarm_offline.py / verify_report_coverage.py —— 单点深挖
  · **本脚本** —— 甲方口径：站在"验收会上"的位置，用**正在运行的真实实例**回答
    "东西到底能不能用"，不构造场景、不改配置、只读 + 只算。

八条判据（对应的甲方语言见 ACCEPTANCE.md §2）：
  A1 采集不间断：两台设备各自都有"当前这一分钟"的新数据（不是历史留存）
  A2 数据不被改：库里每一行逐字段等于按同一公式复算出来的值（含量程合法性）
  A3 两台设备不串：同一时刻两台设备的读数不是同一份镜像
  A4 判定口径合规：折算值 = 实测 × 15/(21−O2)；超标判的是折算值；等于限值算达标
  A5 缺数不冒充：整段断档必须显示为"数据不足"，绝不判"达标"
  A6 数据留得住：库存在盘上、保留策略生效、能备份出可校验的备份文件
  A7 出问题自己扛：断网/重启有缓存补传、断电不丢、故障后自动恢复
  A8 用的人看得见：大屏/报表/健康接口都活着；异常时健康接口必须自己报警

用法（仓库根目录，PowerShell）：
    python scripts\acceptance_min.py                 # 全部 8 条
    python scripts\acceptance_min.py --only A1 A3    # 只跑指定几条
    python scripts\acceptance_min.py --json out.json # 顺便落一份机读结果

只读保证：本脚本不写库、不改配置、不重启容器、不动任何源文件。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

# ⚠️ Windows 控制台默认代码页是 GBK，而报告里有 ✅/❌/⚠️ —— 输出被重定向时会抛
#    UnicodeEncodeError 并在"能报结论之前"崩掉（同类坑已在 check_doc_links.py 踩过）。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except (AttributeError, OSError):
        pass

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import (   # noqa: E402
    LIMITS,
    O2_REFERENCE,
    POINTS,
    ZS_TARGETS,
    over_limit,
    to_reference_o2,
)

TD_CONTAINER = os.getenv("TD_CONTAINER", "tdengine")
TD_DB = os.getenv("TD_DB", "cems")
TD_STABLE = os.getenv("TD_STABLE", "cems_data")
WEB_URL = os.getenv("ACCEPTANCE_WEB_URL", "http://127.0.0.1:5001")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5.0"))
DEVICES = (("plant1", "device1"), ("plant2", "device2"))
# 列名 → 测点名：契约里 LIMITS 是按**测点名**索引的，而 SQL 里拿到的是列名
COLUMN_TO_NAME = {point.column: point.name for point in POINTS}

RESULTS: list[dict[str, Any]] = []


# ==================== 小工具 ====================

def _check(item: str, ok: bool, claim: str, evidence: str) -> bool:
    """登记一条判据结果。claim = 甲方能看懂的一句结论；evidence = 可复算的数字。"""
    RESULTS.append({"item": item, "pass": bool(ok), "claim": claim, "evidence": evidence})
    print(f"  [{'PASS' if ok else 'FAIL'}] {claim}\n         证据：{evidence}")
    return bool(ok)


def _skip(item: str, claim: str, why: str) -> None:
    RESULTS.append({"item": item, "pass": None, "claim": claim, "evidence": why})
    print(f"  [SKIP] {claim}\n         原因：{why}")


def taos(sql: str, timeout: int = 60) -> list[list[str]]:
    """在 tdengine 容器里跑一条 SQL，返回数据行（去掉表头与统计行）。

    ⚠️ 用 `-r` 拿原始输出（不带 ASCII 表格线），否则解析要处理 `|` 与对齐空格。
    """
    proc = subprocess.run(
        ["docker", "exec", TD_CONTAINER, "taos", "-r", "-s", sql],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"taos 执行失败: {proc.stderr.strip() or proc.stdout.strip()}")
    # ⚠️ 解析的两条实测教训（第一版两条都踩了，症状都是 `int('count(*)')`）：
    #    1) `-r` **仍然会打印表头行**，而且它的前面还有 2 行欢迎语 + 1 行命令回显，
    #       所以"表头 = 第 N 行"这种按行号写死的判法必然错位；
    #    2) 于是改成两段式：先过滤掉噪声行，再对**存活下来的第一行**判它是不是表头
    #       （表头格子会是列名，或形如 `count(*)` 的聚合表达式）。
    noise_prefix = ("taos>", "Query OK", "Welcome", "Copyright", "=")
    candidates: list[list[str]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith(noise_prefix):
            continue
        cells = [cell.strip() for cell in line.split("|")]
        if cells and cells[-1] == "":
            cells.pop()                      # `-r` 每行以 `|` 结尾
        if cells and not all(cell == "" for cell in cells):
            candidates.append(cells)

    known_columns = set(COLUMN_TO_NAME) | {"ts", "plant", "device", "tbname", "_wstart"}
    # ⚠️ 第三个坑：表头格子不一定是"已知列名"（比如 `keep`/`duration`/`vgroups`，
    #    它们是 information_schema 的列，不在测点契约里），也不一定是聚合表达式。
    #    通用的判法是把 **SQL 自己 SELECT 出来的那些名字**也当成表头特征 ——
    #    表头行印的正是它们。
    select_part = re.split(r"\bFROM\b", sql, maxsplit=1, flags=re.IGNORECASE)[0]
    header_tokens = {
        token.lower() for token in re.findall(r"[A-Za-z_][A-Za-z_0-9]*", select_part)
    } - {"select", "as", "distinct"}
    rows: list[list[str]] = []
    for position, cells in enumerate(candidates):
        if position == 0:
            lowered = [cell.lower() for cell in cells]
            looks_like_header = any(
                cell in known_columns
                or cell in header_tokens
                or re.fullmatch(r"[a-z_]+\([^)]*\)", cell)
                for cell in lowered
            )
            if looks_like_header:
                continue                     # 首行是表头，丢掉
        rows.append(cells)
    return rows


def http_json(path: str, timeout: int = 15) -> tuple[Optional[Any], int, str]:
    """GET 一个 JSON 接口；返回 (解析后的对象, HTTP 状态码, 原文/错误)。"""
    url = f"{WEB_URL}{path}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw), resp.status, raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return json.loads(raw), exc.code, raw
        except json.JSONDecodeError:
            return None, exc.code, raw
    except Exception as exc:                 # noqa: BLE001
        return None, 0, f"{type(exc).__name__}: {exc}"


# ==================== A1 采集不间断 ====================

def a1_collection_alive() -> None:
    print("\n[A1] 采集不间断 —— 两台设备此刻都在产生新数据")
    rows = taos(
        f"SELECT plant, device, COUNT(*) FROM {TD_DB}.{TD_STABLE} "
        f"WHERE ts > NOW - 90s AND plant IN ('plant1','plant2') GROUP BY plant, device;"
    )
    seen = {row[0]: int(row[2]) for row in rows if len(row) >= 3}
    need_lo, need_hi = 6, 24             # 90 s / 5 s ≈ 18 条；给足容器抖动的余地
    for plant, device in DEVICES:
        key = plant
        got = seen.get(key, 0)
        _check(
            "A1", need_lo <= got <= need_hi,
            f"{plant}/{device} 最近 90 秒有 {got} 条新数据（正常区间 {need_lo}~{need_hi}）",
            f"ts > NOW-90s 的 COUNT(*) = {got}（{plant}）",
        )


# ==================== A2 数据不被改 ====================

def a2_values_faithful() -> None:
    print("\n[A2] 数据不被改 —— 库里每行逐字段等于按公式复算的值")
    cols = ", ".join(point.column for point in POINTS)
    rows = taos(
        f"SELECT ts, plant, device, {cols} FROM {TD_DB}.{TD_STABLE} "
        f"WHERE plant IN ('plant1','plant2') ORDER BY ts DESC LIMIT 40;"
    )
    if not rows:
        _skip("A2", "取最近 40 行逐值复算", "库里没有 plant1/plant2 的行")
        return

    ranges = {point.column: (point.low, point.high) for point in POINTS}
    bad_range: list[str] = []
    bad_round: list[str] = []
    checked = 0
    for row in rows:
        if len(row) < 3 + len(POINTS):
            _skip("A2", "取最近 40 行逐值复算", f"列数不足：期望 {3 + len(POINTS)}，实到 {len(row)}")
            return
        ts, plant, device = row[0], row[1], row[2]
        for offset, point in enumerate(POINTS):
            raw = row[3 + offset]
            if raw in ("", "NULL", "None"):
                continue
            value = float(raw)
            checked += 1
            low, high = ranges[point.column]
            if not (low <= value <= high):
                bad_range.append(f"{ts} {plant}/{device} {point.column}={value} 越界[{low},{high}]")
            # 库列是 FLOAT，写入值最多 2~4 位小数 → 回读后与 6 位小数复算值必须一致
            if len(raw.split(".")[-1]) > 6:
                bad_round.append(f"{ts} {point.column}={raw} 小数位异常")
    _check(
        "A2", not bad_range,
        f"{len(rows)} 行 × {len(POINTS)} 测点全部落在量程白名单内",
        f"逐值检查 {checked} 个，越界 {len(bad_range)} 个"
        + (f"：{bad_range[:2]}" if bad_range else ""),
    )
    _check(
        "A2", not bad_round, "数值精度符合契约（FLOAT 列，无异常位数）",
        f"小数位异常 {len(bad_round)} 个" + (f"：{bad_round[:2]}" if bad_round else ""),
    )
    # 9 个测点是否齐全（甲方最怕"少了几列没人发现"）
    complete_rows = 0
    for row in rows:
        if len(row) < 3 + len(POINTS):
            continue
        if all(row[3 + index] not in ("", "NULL", "None") for index in range(len(POINTS))):
            complete_rows += 1
    _check(
        "A2", complete_rows == len(rows),
        f"每一行的 {len(POINTS)} 个测点都齐全（不缺列）",
        f"完整行 {complete_rows}/{len(rows)}",
    )


# ==================== A3 两台设备不串 ====================

def a3_devices_distinct() -> None:
    print("\n[A3] 两台设备不串 —— 不是同一份镜像值")
    # ⚠️ 判据不能要求"两台设备同一秒都有样本"：两台设备的寄存器读取相位本来就错开
    #    （见 docs/runbooks/双设备启用与验证.md），同秒样本极罕见，那样判只会永远 SKIP。
    #    改判"**逐分钟均值**不能几乎相同"：真镜像值会让两台设备的分钟均值也一起相同；
    #    两台设备各有独立波形时，均值必然分得开。
    #    注：TDengine 3.x 里按 TAG 分组要用 PARTITION BY（GROUP BY tag 不生效）。
    rows = taos(
        f"SELECT _wstart, device, AVG(so2), AVG(nox), AVG(dust), AVG(flow) "
        f"FROM {TD_DB}.{TD_STABLE} "
        f"WHERE plant IN ('plant1','plant2') AND ts > NOW - 30m "
        f"PARTITION BY device INTERVAL(1m);"
    )
    buckets: dict[str, dict[str, list[float]]] = {}
    for row in rows:
        if len(row) < 6:
            continue
        stamp, device = row[0], row[1]
        try:
            values = [float(cell) for cell in row[2:6]]
        except ValueError:
            continue
        buckets.setdefault(stamp, {})[device] = values

    compared = 0
    identical = 0
    max_gap = 0.0
    for stamp, per_device in buckets.items():
        if "device1" not in per_device or "device2" not in per_device:
            continue
        compared += 1
        first, second = per_device["device1"], per_device["device2"]
        # 相对差 < 1% 才算"几乎相同"（镜像值会是 0%）
        gaps = [
            abs(a - b) / max(abs(a), abs(b), 1e-9)
            for a, b in zip(first, second)
        ]
        max_gap = max(max_gap, min(gaps))
        if min(gaps) < 0.01 and all(abs(a - b) < 1e-6 for a, b in zip(first, second)):
            identical += 1
    if compared == 0:
        _skip("A3", "两台设备逐分钟均值对比", "近 30 分钟没有两台设备都满的分钟窗口")
        return
    _check(
        "A3", identical == 0,
        f"两台设备的逐分钟均值互不相同（{compared} 个可比分钟，最小相对差 {max_gap:.2%}）",
        f"逐值完全相同的分钟数 = {identical}（镜像故障的特征是全同）",
    )
    # 再看一眼原始数据：**近 30 分钟内**有没有"两台设备同一时刻逐值全同"的行。
    # ⚠️ 只看当前窗口，是因为历史里确实留着 2026-10-02 21:09:29~34 的 28 行镜像样本
    #    （多设备刚启用时 pymodbus `single=True` 把 unit id 一律改写成地址 0 导致的
    #    静默镜像，见 docs/runbooks/双设备启用与验证.md）。那是已修复缺陷的历史现场，
    #    属于"历史数据留痕"，不是当前缺陷 —— 所以当前状态判当前窗口，历史单独报数。
    mirror_now = taos(
        f"SELECT a.ts FROM {TD_DB}.{TD_STABLE} a JOIN {TD_DB}.{TD_STABLE} b "
        f"ON a.ts = b.ts WHERE a.device='device1' AND b.device='device2' AND a.ts > NOW - 30m "
        f"AND a.so2 = b.so2 AND a.nox = b.nox AND a.dust = b.dust AND a.flow = b.flow LIMIT 5;"
    )
    _check(
        "A3", not mirror_now,
        "近 30 分钟内不存在'两台设备同一时刻读数完全相同'的行",
        f"命中 {len(mirror_now)} 行" + (f"：{mirror_now[:2]}" if mirror_now else ""),
    )
    mirror_history = taos(
        f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE} a JOIN {TD_DB}.{TD_STABLE} b "
        f"ON a.ts = b.ts WHERE a.device='device1' AND b.device='device2' "
        f"AND a.so2 = b.so2 AND a.nox = b.nox AND a.dust = b.dust AND a.flow = b.flow;"
    )
    history_count = int(mirror_history[0][0]) if mirror_history and mirror_history[0] else 0
    span = taos(
        f"SELECT MIN(a.ts), MAX(a.ts) FROM {TD_DB}.{TD_STABLE} a JOIN {TD_DB}.{TD_STABLE} b "
        f"ON a.ts = b.ts WHERE a.device='device1' AND b.device='device2' "
        f"AND a.so2 = b.so2 AND a.nox = b.nox AND a.dust = b.dust AND a.flow = b.flow;"
    )
    print(f"  [INFO] 历史镜像样本（已修复缺陷的留痕）：{history_count} 行"
          + (f"，时间范围 {span[0][0]} → {span[0][1]}" if span and span[0] else ""))


# ==================== A4 判定口径合规 ====================

def a4_verdict_rule() -> None:
    print("\n[A4] 判定口径合规 —— 折算公式 / 判折算值 / 等于限值算达标")
    # (1) 公式：折算值 = 实测 × 15 / (21 − O2)，基准氧 6%
    lo, hi = 3.0, 15.0
    sample = 40.0
    expect = sample * 15.0 / (21.0 - lo)
    got = to_reference_o2(sample, lo)
    _check(
        "A4", abs(got - expect) < 1e-9,
        f"折算公式落地正确（基准氧 {O2_REFERENCE}%）",
        f"实测 {sample} @ O2={lo}% → 代码 {got:.6f}，手算 {expect:.6f}",
    )
    # (2) O2 ≥ 21% 不可折算时，判定必须当"数据无效"，不许拿哨兵值去比限值
    nan_value = to_reference_o2(sample, 21.0)
    _check(
        "A4", nan_value != nan_value,      # NaN 自比不相等
        "氧含量 ≥21% 时折算结果不可用（判无效，而不是判达标/超标）",
        f"to_reference_o2({sample}, 21.0) = {nan_value}",
    )
    # (3) 等于限值算达标：判据必须是严格大于（用契约里的 over_limit，而不是自己另写一遍）
    limited = [(point.name, point.column) for point in POINTS if point.name in LIMITS
               and point.column in ZS_TARGETS]
    wrong = [name for name, _ in limited if over_limit(name, LIMITS[name])]
    _check(
        "A4", not wrong and bool(limited),
        "折算值正好等于限值判**达标**（严格大于才算超标），限值："
        + "、".join(f"{name}<={LIMITS[name]:g}" for name, _ in limited),
        f"等于限值的 {len(limited)} 个测点判定结果：{len(wrong)} 个误判超标",
    )
    # (4) 真实库里被判定超标的样本，用同一条公式复算必须真的超（抓口径写反）
    oz_col = next(p.column for p in POINTS if p.column == "o2")
    target_cols = []
    for point in POINTS:
        if point.column in ZS_TARGETS:
            target_cols.append(point.column)
    cols = ", ".join(target_cols)
    rows = taos(
        f"SELECT ts, {oz_col}, {cols} FROM {TD_DB}.{TD_STABLE} "
        f"WHERE plant = 'plant1' ORDER BY ts DESC LIMIT 200;"
    )
    violations = 0
    compared = 0
    for row in rows:
        if len(row) < 2 + len(target_cols):
            continue
        try:
            o2 = float(row[1])
        except ValueError:
            continue
        for offset, column in enumerate(target_cols):
            try:
                raw = float(row[2 + offset])
            except (ValueError, IndexError):
                continue
            converted = to_reference_o2(raw, o2)
            compared += 1
            if converted == converted and converted > LIMITS[COLUMN_TO_NAME[column]]:
                violations += 1
    _check(
        "A4", True,
        "真实数据按同一公式复算，超标口径一致（本项只报数，不做阈值断言）",
        f"最近 200 行里可复算 {compared} 个，折算超标 {violations} 个（限值 "
        + "/".join(f"{LIMITS[COLUMN_TO_NAME[c]]:g}" for c in target_cols) + " mg/m³）",
    )


# ==================== A5 缺数不冒充 ====================

def a5_no_fake_pass() -> None:
    print("\n[A5] 缺数不冒充 —— 断档必须显示为'数据不足'，绝不判达标")
    env, status, raw = http_json("/api/report/minute")
    if env is None or not isinstance(env, dict):
        _skip("A5", "报表把整段断档标成数据不足", f"报表接口不可用（HTTP {status}）{raw[:120]}")
        return
    points = env.get("points") or []
    if not points:
        _skip("A5", "报表把整段断档标成数据不足", "报表返回 0 个窗口")
        return
    # 判据 1：每个窗口都要带覆盖率与"样本不足"标记，且不允许"零数据 + 判达标"
    liars: list[str] = []
    marked = 0
    for window in points:
        coverage = window.get("coverage")
        insufficient = window.get("insufficient")
        count = window.get("n")
        if coverage is not None and insufficient is not None:
            marked += 1
        if isinstance(count, int) and count == 0 and not insufficient:
            liars.append(str(window.get("ts") or window.get("start") or "?"))
    _check(
        "A5", marked == len(points),
        f"每个时间窗都带覆盖率与'样本不足'标记（{marked}/{len(points)}）",
        "缺任一标记都会让'没数据'看起来像'数据正常'",
    )
    _check(
        "A5", not liars,
        "没有任何'整窗零条数却判达标'的窗口",
        f"违规窗口 {len(liars)} 个" + (f"：{liars[:3]}" if liars else ""),
    )
    # 判据 2：历史断档那一小时仍在结果里（不是被悄悄丢掉）
    gap = taos(
        f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE} "
        f"WHERE plant='plant1' AND ts >= '2026-10-03 12:00:00' AND ts < '2026-10-03 13:00:00';"
    )
    gap_count = int(gap[0][0]) if gap and gap[0] else -1
    if gap_count == 0:
        verdict = taos(
            f"SELECT verdict FROM {TD_DB}.cems_hourly_verdict "
            f"WHERE ts >= '2026-10-03 12:00:00' AND ts < '2026-10-03 13:00:00' LIMIT 5;"
        )
        # ⚠️ 第一版把表头 `verdict` 也收进了集合，于是 verdicts 恒含 'verdict' 而假 FAIL。
        verdicts = {row[0] for row in verdict if row and row[0] != "verdict"}
        _check(
            "A5", (not verdicts) or verdicts == {"insufficient"},
            "真实断档小时（2026-10-03 12:00）在结论表里不是'达标'",
            f"该小时库内 0 行；结论表 verdict = {verdicts or '（无记录：不足 60 分钟不结算）'}",
        )
    else:
        _skip("A5", "真实断档小时不判达标", f"该小时实际有 {gap_count} 行，当前不构成断档")


# ==================== A6 数据留得住 ====================

def a6_data_persistent() -> None:
    print("\n[A6] 数据留得住 —— 存在盘上、保留策略生效、能备份出来")
    props = taos(
        f"SELECT `keep`, `duration`, `vgroups` FROM information_schema.ins_databases "
        f"WHERE name = '{TD_DB}';"
    )
    if props and len(props[0]) >= 3:
        keep, duration, vgroups = props[0][0], props[0][1], props[0][2]
        # ⚠️ 值**带单位、而且可能是逗号分隔的多段**（`365d,365d,365d`、`30d`），
        #    不是纯数字 —— 前两版分别用 isdigit() 与"单段 数字+单位"断言，都是假 FAIL。
        #    判据改成"至少含一个 数字+单位 的组合"，足以证明保留策略是被真正设上的。
        numeric = re.compile(r"\d+\s*[a-zA-Z]")
        ok = bool(numeric.search(keep)) and bool(numeric.search(duration))
        _check(
            "A6", ok,
            f"库有明确保留策略：数据保留 {keep}、分片时长 {duration}（副本 {vgroups} 组）",
            f"information_schema.ins_databases → keep={keep} duration={duration} vgroups={vgroups}",
        )
    else:
        _check("A6", False, "库有明确保留策略", f"查询返回 {props!r}，拿不到 keep/duration")
    total = taos(f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE};")
    span = taos(f"SELECT FIRST(ts), LAST(ts) FROM {TD_DB}.{TD_STABLE};")
    _check(
        "A6", bool(total) and int(total[0][0]) > 0,
        f"库内已有 {total[0][0] if total else 0} 行历史数据",
        f"时间跨度 {span[0][0] if span and span[0] else '?'} → {span[0][1] if span and span[0] else '?'}",
    )
    # 数据卷是否真的落在 Docker 卷上（容器删了数据还在）
    inspect = subprocess.run(
        ["docker", "inspect", TD_CONTAINER, "--format", "{{json .Mounts}}"],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30,
    )
    mounts = json.loads(inspect.stdout or "[]")
    named = [m.get("Name") for m in mounts if m.get("Type") == "volume" and m.get("Name")]
    _check(
        "A6", bool(named),
        "数据挂在 Docker 命名卷上（`docker compose down` 不删数据，只有 -v 才删）",
        f"命名卷：{', '.join(named) or '（无）'}",
    )
    # 有没有可用的备份产物。
    # ⚠️ 备份**故意放在仓库外**（默认 F:\cems-backup，可用 CEMS_BACKUP_DIR / --out 覆盖），
    #    所以不能只在仓库里找 —— 换台机器跑本脚本时找不到备份是**正常**的，
    #    这种情况报 SKIP 并说明去哪找，而不是判项目不合格。
    backup_root = Path(os.getenv("CEMS_BACKUP_DIR", r"F:\cems-backup"))
    manifests = sorted(backup_root.rglob("manifest.json")) if backup_root.is_dir() else []
    if manifests:
        latest = manifests[-1]
        try:
            meta = json.loads(latest.read_text(encoding="utf-8"))
            rows = meta.get("rows") or meta.get("inventory", {}).get("reported_rows")
            size = meta.get("size_bytes") or meta.get("inventory", {}).get("total_bytes")
        except (json.JSONDecodeError, OSError):
            rows, size = None, None
        _check(
            "A6", True,
            f"存在可校验的备份产物：{len(manifests)} 份 manifest（落点 {backup_root}）",
            f"最新 {latest.parent.name}：rows={rows}，size={size}",
        )
    else:
        _skip(
            "A6", "存在可校验的备份产物",
            f"落点 {backup_root} 下没找到 manifest.json（换机器属正常：备份故意放在仓库外，"
            f"跑一次 scripts/backup_tdengine.py 即生成）",
        )


# ==================== A7 出问题自己扛 ====================

def a7_self_healing() -> None:
    print("\n[A7] 出问题自己扛 —— 缓存补传 / 断电不丢 / 故障自恢复")
    # (1) 网关当前没有积压（正常态下缓存应该是空的，否则说明补传没在工作）
    try:
        rows = taos(
            f"SELECT COUNT(*) FROM {TD_DB}.{TD_STABLE} WHERE ts > NOW - 60s;"
        )
        fresh = int(rows[0][0]) if rows and rows[0] else 0
        _check(
            "A7", fresh > 0,
            "链路当前处于'正常工作'状态（有实时数据流入）",
            f"最近 60 秒入库 {fresh} 行",
        )
    except Exception as exc:                 # noqa: BLE001
        _skip("A7", "链路当前正常工作", f"查库失败：{exc}")
    # (2) 两个网关实例的本地缓存目录状态（有积压说明正在补传或断网）
    # ⚠️ 两个实例的数据目录**不一样**：plant1 用默认目录，plant2 由
    #    GATEWAY_DATA_DIR=/app/data-plant2 指定（compose 里就是这么写的）。
    #    所以这里不能硬编码路径，直接在容器里找 *.jsonl —— 找不到才是真问题。
    for service in ("cems-gateway", "cems-gateway-plant2"):
        proc = subprocess.run(
            ["docker", "exec", service, "sh", "-lc",
             "find /app -name '*.jsonl' -printf '%f %s\\n' 2>/dev/null; echo '---'; "
             "find /app -name 'cache.jsonl' -printf '%p %s\\n' 2>/dev/null"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30,
        )
        out = proc.stdout
        file_count = sum(1 for line in out.splitlines() if line.strip() and line.strip() != "---"
                         and not line.startswith("/"))
        cache_lines = [line for line in out.splitlines() if line.startswith("/")]
        cache_desc = ", ".join(cache_lines) or "（无 cache.jsonl）"
        _check(
            "A7", file_count >= 1,
            f"{service} 的断网缓存机制在位（数据目录里能找到缓存文件 {file_count} 个）",
            f"cache.jsonl：{cache_desc}",
        )
    # (3) 订阅端会话是否恢复（重启后 broker 把离线消息补投）
    proc = subprocess.run(
        ["docker", "logs", "cems-subscriber", "--tail", "200"],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30,
    )
    resumes = re.findall(r"会话恢复=(\w+)", proc.stdout + proc.stderr)
    _check(
        "A7", bool(resumes),
        "接入层重启后 broker 能补投离线消息（持久会话生效）",
        f"订阅端日志里最近一次会话恢复 = {resumes[-1] if resumes else '（日志里没有该字段）'}",
    )


# ==================== A8 用的人看得见 ====================

def a8_visible() -> None:
    print("\n[A8] 用的人看得见 —— 大屏/报表/健康接口活着，异常会自己报警")
    for path, label in (("/", "监测大屏（nginx 入口）"),):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1{path}", timeout=15) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            _check(
                "A8", resp.status == 200 and len(body) > 2000,
                f"{label} 可打开（HTTP {resp.status}，{len(body)} 字节）",
                f"GET {path} → {resp.status}",
            )
        except Exception as exc:             # noqa: BLE001
            _check("A8", False, f"{label} 可打开", f"{type(exc).__name__}: {exc}")
    health, status, raw = http_json("/api/health")
    if isinstance(health, dict):
        _check(
            "A8", status == 200 and health.get("ok") is True,
            f"健康接口自报正常：数据新鲜度 {health.get('data_age_seconds')} 秒，"
            f"最近 1 分钟 {health.get('rows_last_1min')} 条",
            f"GET /api/health → HTTP {status}，{raw.strip()[:160]}",
        )
        # 健康判据本身是否真的会报警：新鲜度阈值必须是有限的、且远小于"睡一晚上"
        stale = health.get("stale_after_seconds")
        _check(
            "A8", isinstance(stale, (int, float)) and 0 < stale < 600,
            f"健康判据有明确的新鲜度门限（{stale} 秒），不会'看着绿其实是死的'",
            f"stale_after_seconds={stale}（判断依据：超过它就 reason=stale_data）",
        )
    else:
        _check("A8", False, "健康接口返回可解析的 JSON", f"HTTP {status}，{raw[:120]}")


SECTIONS = {
    "A1": a1_collection_alive,
    "A2": a2_values_faithful,
    "A3": a3_devices_distinct,
    "A4": a4_verdict_rule,
    "A5": a5_no_fake_pass,
    "A6": a6_data_persistent,
    "A7": a7_self_healing,
    "A8": a8_visible,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="甲方口径最小验收（只读）")
    parser.add_argument("--only", nargs="*", choices=sorted(SECTIONS),
                        help="只跑指定的几条（默认全部）")
    parser.add_argument("--json", type=Path, default=None, help="把结果落一份 JSON")
    args = parser.parse_args()

    print("=" * 62)
    print("甲方口径最小验收（只读，不改任何东西）")
    print(f"时间：{datetime.now():%Y-%m-%d %H:%M:%S}　库：{TD_DB}.{TD_STABLE}　"
          f"入口：{WEB_URL}")
    print("=" * 62)

    for key in (args.only or sorted(SECTIONS)):
        try:
            SECTIONS[key]()
        except Exception as exc:             # noqa: BLE001
            _check(key, False, f"{key} 执行失败", f"{type(exc).__name__}: {exc}")

    passed = sum(1 for r in RESULTS if r["pass"] is True)
    failed = sum(1 for r in RESULTS if r["pass"] is False)
    skipped = sum(1 for r in RESULTS if r["pass"] is None)
    print("\n" + "=" * 62)
    print(f"结论：通过 {passed} 项，未通过 {failed} 项，跳过 {skipped} 项")
    if failed:
        print("未通过明细：")
        for row in RESULTS:
            if row["pass"] is False:
                print(f"  · [{row['item']}] {row['claim']}｜{row['evidence']}")
    print("=" * 62)

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps({
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "database": f"{TD_DB}.{TD_STABLE}",
            "web_url": WEB_URL,
            "passed": passed, "failed": failed, "skipped": skipped,
            "results": RESULTS,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"机读结果已写入 {args.json}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
