# -*- coding: utf-8 -*-
"""验收自检 4/4：全链路在线检查——库里新增的数据是否"5 秒一条 + 不再乱跳"。

用法（需要 docker compose 已经 up）：
    python scripts/verify_pipeline_live.py [--minutes 5] [--since "2026-09-30 16:52:04"]

前置条件：device / gateway / subscriber 三个容器在跑。
检查项：
    A. 采集节奏：相邻入库时间戳间隔是否稳定在 5 秒（网关 POLL_INTERVAL）
    B. 数据新鲜度：最近一条是否在 60 秒内
    C. 跳变幅度：相邻两条的差值是否在量程的合理比例内
    D. 结束码：全部通过 0，任一不通过 1

⚠️ --since 不是可选项而几乎是必须项：
    库里同时存在**改造前**的纯随机数据（random.uniform）。
    如果回看窗口跨过了切换时刻，A/C 两项会被旧数据的乱跳带偏
    —— 实测 5 分钟窗口里旧数据把 temp 相邻跳变拉到量程的 93%，
    而只看新数据时只有 1.2%。切换时刻取设备容器本次启动的时间：
        docker compose logs device | Select-String "仿真信号已启用"
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import statistics
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS                                 # noqa: E402

TD_URL: str = os.getenv("TD_URL", "http://127.0.0.1:6041")
TD_USER: str = os.getenv("TD_USER", "root")
TD_PASS: str = os.getenv("TD_PASS", "taosdata")
TD_DB: str = os.getenv("TD_DB", "cems")
TD_STABLE: str = os.getenv("TD_STABLE", "cems_data")

EXPECTED_INTERVAL: float = 5.0        # 网关 POLL_INTERVAL
INTERVAL_TOLERANCE: float = 1.5       # 允许 ±1.5s（网络/调度抖动）
FRESHNESS_LIMIT: float = 60.0         # 最近一条必须在 60 秒内
MAX_STEP_RATIO: float = 0.05          # 相邻两条的最大差值不得超过量程的 5%
MIN_5S_SHARE: float = 0.60            # 恰好 5 秒的间隔占比下限

FAILURES: list[str] = []


def _check(condition: bool, description: str) -> None:
    """记录一条验收判定；失败时收进 FAILURES，最后统一汇总。"""
    print(f"  [{'PASS' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def query(sql: str) -> dict[str, Any]:
    """通过 taosAdapter 的 REST 接口执行只读 SQL，返回原始响应。"""
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
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError) as exc:
        raise SystemExit(f"连不上 TDengine REST {TD_URL}: {exc}") from exc
    if payload.get("code") != 0:
        raise SystemExit(f"TDengine 返回错误: {payload}")
    return payload


def fetch_rows(
    minutes: int, since: Optional[datetime]
) -> tuple[list[str], list[list[Any]]]:
    """取最近 minutes 分钟的入库记录，按时间升序；since 非空时只保留其后的行。

    ⚠️ since 必须由**本脚本**过滤，不能写进 SQL：
    TDengine 对 `WHERE ts >= NOW - 5m AND ts >= '2026-09-30 16:52:05'` 这种
    双时间谓词会**静默忽略字面量那个**（实测返回的仍是全窗口数据），
    所以把筛选放在 Python 里做，结果可控。
    """
    sql = (
        f"SELECT ts, {', '.join(point.column for point in POINTS)} "
        f"FROM {TD_DB}.{TD_STABLE} WHERE ts >= NOW - {minutes}m ORDER BY ts ASC"
    )
    payload = query(sql)
    columns = [meta[0] for meta in payload["column_meta"]]
    rows = payload["data"]
    if since is not None:
        rows = [row for row in rows if parse_ts(row[0]) >= since]
    return columns, rows


def parse_ts(value: Any) -> datetime:
    """把 TDengine 返回的时间戳（'2026-09-30T16:52:04.000Z' 或 '2026/9/30 16:52:04'）解析成 UTC。"""
    text = str(value).strip().replace("/", "-")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        moment = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def parse_since(text: str) -> Optional[datetime]:
    """把 --since 的 'YYYY-MM-DD HH:MM:SS'（按 UTC 理解）解析成 datetime。"""
    if not text.strip():
        return None
    try:
        return datetime.strptime(text.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise SystemExit(f"--since 格式应为 'YYYY-MM-DD HH:MM:SS'（UTC）: {text!r}") from exc


def section_a_cadence(rows: list[list[Any]]) -> None:
    """A. 采集节奏：相邻时间戳间隔必须稳定在 5 秒。"""
    print("\n=== A. 入库节奏（网关 POLL_INTERVAL = 5s）===")
    times = [parse_ts(row[0]) for row in rows]
    gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
    if not gaps:
        _check(False, "样本不足，无法判断节奏")
        return
    share = sum(1 for gap in gaps if abs(gap - EXPECTED_INTERVAL) <= INTERVAL_TOLERANCE) / len(gaps)
    print(f"  样本 {len(rows)} 条，区间 {rows[0][0]} ~ {rows[-1][0]}")
    print(f"  间隔 n={len(gaps)} min={min(gaps):.0f}s max={max(gaps):.0f}s "
          f"median={statistics.median(gaps):.0f}s mean={statistics.mean(gaps):.2f}s")
    print(f"  落在 {EXPECTED_INTERVAL:.0f}±{INTERVAL_TOLERANCE:.1f}s 的占比 = {share:.1%}")
    _check(statistics.median(gaps) == EXPECTED_INTERVAL, "相邻入库间隔中位数为 5 秒")
    _check(share >= MIN_5S_SHARE, f"至少 {MIN_5S_SHARE:.0%} 的间隔落在 5±1.5 秒内")


def section_b_freshness(rows: list[list[Any]]) -> None:
    """B. 数据新鲜度：最近一条必须在 FRESHNESS_LIMIT 秒内。"""
    print("\n=== B. 数据新鲜度 ===")
    if not rows:
        _check(False, "库里最近没有数据")
        return
    last = parse_ts(rows[-1][0])
    age = (datetime.now(timezone.utc) - last).total_seconds()
    print(f"  最近一条: {rows[-1][0]}（{age:.0f} 秒前）")
    _check(0 <= age <= FRESHNESS_LIMIT, f"最近一条数据在 {FRESHNESS_LIMIT:.0f} 秒内")


def section_c_step_size(columns: list[str], rows: list[list[Any]]) -> None:
    """C. 跳变幅度：相邻两条的差值应远小于量程（改造前是随机的，会大幅越界）。

    除了判 PASS/FAIL，还把**最差的那一对**时间戳打出来：
    判定失败时能立刻看出是"信号真的在乱跳"还是"窗口跨到了改造前的数据"。
    """
    print("\n=== C. 相邻两条的跳变（连续两个采集周期 = 5 秒）===")
    print(f"{'列名':<10}{'测点':<10}{'量程':>10}{'最大跳变':>10}{'跳变/量程':>11}{'中位跳变':>10}")
    worst_ratio = 0.0
    worst_detail = ""
    for point in POINTS:
        index = columns.index(point.column)
        values = [float(row[index]) for row in rows]
        deltas = [abs(b - a) for a, b in zip(values, values[1:])]
        if not deltas:
            continue
        span = point.high - point.low
        max_ratio = max(deltas) / span
        if max_ratio > worst_ratio:
            worst_ratio = max_ratio
            position = deltas.index(max(deltas)) + 1
            worst_detail = (
                f"{point.column}: {rows[position - 1][0]}({values[position - 1]}) -> "
                f"{rows[position][0]}({values[position]})"
            )
        print(
            f"{point.column:<10}{point.name:<10}{span:>10.1f}{max(deltas):>10.2f}"
            f"{max_ratio:>11.2%}{statistics.median(deltas):>10.2f}"
        )
    print(f"  最差的一对: {worst_detail}（= 量程的 {worst_ratio:.1%}）")
    _check(worst_ratio <= MAX_STEP_RATIO, f"所有测点的相邻跳变都 ≤ 量程的 {MAX_STEP_RATIO:.0%}")


def main() -> int:
    """依次跑完三段检查，返回进程退出码。"""
    parser = argparse.ArgumentParser(description="检查库里新增数据的节奏与跳变幅度")
    parser.add_argument("--minutes", type=int, default=5, help="回看多少分钟（默认 5）")
    parser.add_argument(
        "--since",
        default=os.getenv("TD_SINCE", ""),
        help="只统计该 UTC 时刻之后的数据（'YYYY-MM-DD HH:MM:SS'），用来掐掉改造前的旧数据",
    )
    args = parser.parse_args()
    since = parse_since(args.since)

    print(f"全链路在线检查：TDengine={TD_URL} 库={TD_DB} 表={TD_STABLE} 回看={args.minutes} 分钟")
    if since:
        print(f"只统计 ts >= {since.isoformat()} 的数据（切掉改造前的纯随机数据）")
    else:
        print("⚠️ 未指定 --since：窗口若跨过切换时刻，会把改造前的随机数据算进来，判定会失真")
    columns, rows = fetch_rows(args.minutes, since)
    print(f"查询列: {columns}")
    if not rows:
        print("库里这段时间没有数据：先确认 device/gateway/subscriber 三个容器都 healthy")
        return 1

    section_a_cadence(rows)
    section_b_freshness(rows)
    section_c_step_size(columns, rows)

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
