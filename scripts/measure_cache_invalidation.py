# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读（这个脚本**会写库**，与其它测量脚本不同）
# -----------------------------------------------------------------------------
# 目的：实测"数据更新后，缓存多久刷新"。
#
# 设计（每一处都是为了把测量做成**确定性的**，不是图省事）：
#
#  1. 目标行放在"**当前分钟 + 3 min**"这个还在未来的分钟内，秒位取 :10。
#     这样基线是确定的 **n=0 / 值为 NULL**（那一分钟还没有数据，且不会有真实数据插进来），
#     刷新的结果也是确定的 **n=1 / so2=123.456（哨兵值）** —— 不存在"和真实数据撞车"。
#     ⚠️ 必须落在 /api/data 看得见的范围里（它的窗口是"最近 QUERY_MINUTES=10 分钟"），
#        否则写后失效的水位观测不到，测出来的只是 TTL 兜底。
#
#  2. 报告窗口取 [目标分钟-1 min, 目标分钟+4 min]，按分钟聚合 → 5 个点，
#     目标点在中间。**上界是未来时刻**不是问题：未来那一分钟库里没有数据 → NULL。
#     这个窗口内容稳定（未来分钟不会被别的东西写入），缓存键/内容都不会自己滚。
#
#  3. 先在缓存已生效的前提下确认基线（n=0），再执行插入。
#     插入后立刻记录精确时刻，然后在**同一秒内**先打一次 /api/data
#     （把"库里现在这一分钟的最大值"推给展示层），再打报告接口。
#     这样测到的是"缓存发现自己过期 → 回源"的耗时，不含轮询相位延迟。
#
#  4. 轮询期间后台持续打 /api/data（大屏本来就在 5 s 刷一次），
#     模拟真实场景：观测与报表请求是并发在跑的。
#
#  5. try/finally：无论成败，都 DELETE 掉那一分钟的行，并用 SQL 核对归零。
#     目标分钟是"未来 + 空"，删除不会碰到任何真实数据。
#
# ⚠️ 副作用（诚实写明）：库里会**凭空多出一个未来时间戳的数据点**，存在几十秒。
#    因为查询上界统一收了 "ts <= now"（report.clamp_to_now / query_recent 的 WHERE），
#    大屏和报表在它"变成过去"之前都看不到它；脚本会在它变成过去之前删掉。
#    为把风险降到最低，脚本在插入前会先确认该分钟确实是空的，
#    删除后会核对 count=0。
# =============================================================================
"""实测"数据更新后缓存多久刷新"：向一个未来空分钟注入 1 行哨兵，测完删除。"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = PROJECT_ROOT / "docs" / "measurements"

NGINX_BASE = "http://127.0.0.1:80"
DIRECT_BASE = "http://127.0.0.1:5001"
TD_CONTAINER = "tdengine"
CHILD_TABLE = "cems.plant1_device1"
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
COLUMNS = ("so2", "nox", "dust", "o2", "humidity", "flow", "temp", "pressure", "velocity")
TD_AUTH = "Basic cm9vdDp0YW9zZGF0YQ=="
SENTINEL_SO2 = 123.456
LOCAL_TZ = timezone(timedelta(hours=8))      # 容器时区 Asia/Shanghai（无夏令时）


def _margin_seconds() -> int:
    """读展示层的安全边界（与容器内 cache.MARGIN_SECONDS 同源：环境变量 + 同一默认值）。

    宿主侧不能直接 import src/web/cache.py —— 那会在导入时就去连 127.0.0.1:6379，
    而 Redis 只在内网暴露（有意如此）。所以这里只读环境变量。
    """
    import os
    return int(os.getenv("CACHE_MARGIN_SECONDS", "60"))


# ==================== 1. TDengine 访问 ====================

def tc_rest(sql: str, timeout: float = 20.0) -> dict[str, Any]:
    request = urllib.request.Request(
        "http://127.0.0.1:6041/rest/sql",
        data=sql.encode("utf-8"),
        headers={"Authorization": TD_AUTH, "Content-Type": "text/plain"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("code") != 0:
        raise RuntimeError(f"SQL 失败: {sql}\n{payload}")
    return payload


def taos(sql: str, timeout: float = 30.0) -> str:
    completed = subprocess.run(
        ["docker", "exec", TD_CONTAINER, "taos", "-s", sql],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
    )
    output = (completed.stdout or "") + (completed.stderr or "")
    if completed.returncode != 0 or "DB error" in output:
        raise RuntimeError(f"taos 执行失败: {sql}\n{output}")
    return output


def _pick_empty_minute(base_minute: datetime) -> Optional[datetime]:
    """从 base_minute 起向更早挑第一个**空分钟**（0 行）；没有就返回 None。"""
    for back in range(0, 4):
        candidate = base_minute - timedelta(minutes=back)
        start = candidate.strftime(TS_FORMAT)
        end = (candidate + timedelta(minutes=1)).strftime(TS_FORMAT)
        if count_window(start, end) == 0:
            return candidate
    return None


def _pick_injectable_minute(base_minute: datetime) -> Optional[dict[str, Any]]:
    """挑一个可以**无损注入**的分钟。

    本机数据是连续采集的（实测每分钟 10~12 条、无空档），找不到空分钟，
    所以改成"注入到已有分钟 + 测完精确还原"：
      - 读走该分钟的全部原始行（逐行 ts + 10 个测点值），用于还原；
      - 挑一个**没被占用、且比该分钟现有最大 ts 更晚**的秒放哨兵
        （TDengine 同 ts 的 INSERT 是覆盖不是新增，撞上就是改数据不是加数据；
         而不比现有最大值更晚的话，该分钟的最大值不变 → 缓存判据不会触发）。
      - 现有最大值已经到 :59 就没法再加更晚的，跳过这一分钟。
    """
    for back in range(0, 4):
        candidate = base_minute - timedelta(minutes=back)
        start = candidate.strftime(TS_FORMAT)
        end = (candidate + timedelta(minutes=1)).strftime(TS_FORMAT)
        rows = read_rows(start, end)
        if not rows:
            return {"minute": candidate, "rows": [], "sentinel_ts": None}
        known_max = max(row["ts"] for row in rows)
        start_second = int(known_max[17:19]) + 1
        used = {row["ts"][17:19] for row in rows}
        for second in range(start_second, 60):
            text = f"{second:02d}"
            if text not in used:
                return {
                    "minute": candidate,
                    "rows": rows,
                    "sentinel_ts": (candidate + timedelta(seconds=second)).strftime(TS_FORMAT),
                }
    return None


def read_rows(start: str, end: str) -> list[dict[str, Any]]:
    """读窗口内全部原始行（用于精确还原）。

    ⚠️⚠️ REST 返回的 ts 是 **UTC**（'2026-10-01T10:38:02.000Z'），
    这里必须显式转成容器时区（Asia/Shanghai）再交给调用方。
    早先版本直接截断那串 UTC 文本当"本地时间"用，后果是：
    DELETE 之后把 12 行又写回了 **10 小时前的时刻** —— 既丢了本来那一分钟，
    又在库里凭空造出一段错位数据。实测发生了两次，靠 docs/measurements/_fix_drill_rows*.py 修回。
    """
    columns = ", ".join(("ts", *COLUMNS))
    payload = tc_rest(
        f"SELECT {columns} FROM {CHILD_TABLE} "
        f"WHERE ts >= '{start}' AND ts < '{end}' ORDER BY ts ASC"
    )
    rows: list[dict[str, Any]] = []
    for raw in payload["data"]:
        row: dict[str, Any] = {"ts": parse_utc(raw[0]).astimezone(LOCAL_TZ).strftime(TS_FORMAT)}
        for index, name in enumerate(COLUMNS, start=1):
            row[name] = float(raw[index])
        rows.append(row)
    return rows


def restore_rows(rows: list[dict[str, Any]]) -> None:
    """逐行写回原始行（每行一条 INSERT，便于逐条核对）。

    ⚠️⚠️ 时区口径（这里连续踩了三次，务必按下面的写法保持）：
      - `rows[*]["ts"]` 是 **本地时区**（Asia/Shanghai）文本；
      - 往库里写时**直接用这个文本**，不要再做时区换算 ——
        TDengine 的时间字面量按会话时区解释，写 18:43:04 落库后 REST 读回就是 10:43:04Z，
        两者是同一个物理时刻。
      - 反面教材：一度"顺手"把本地时间再 astimezone(utc) 转一次，
        结果 11 行被写到 10:43（本地），整整偏了 8 小时。
    """
    for row in rows:
        values = ", ".join(str(row[name]) for name in COLUMNS)
        taos(
            f"INSERT INTO {CHILD_TABLE} (ts, {', '.join(COLUMNS)}) "
            f"VALUES ('{row['ts'][:19]}', {values});"
        )


def parse_utc(value: Any) -> datetime:
    """REST 返回的 UTC 时间戳 -> aware datetime（仅诊断用，写回路径不要用它）。"""
    text = str(value).strip().rstrip("Z").replace("T", " ").split(".")[0]
    return datetime.strptime(text, TS_FORMAT).replace(tzinfo=timezone.utc)


def count_window(start: str, end: str) -> int:
    payload = tc_rest(
        f"SELECT COUNT(*) FROM {CHILD_TABLE} WHERE ts >= '{start}' AND ts < '{end}'"
    )
    return int(payload["data"][0][0])


def insert_sentinel(ts_text: str) -> None:
    values = (
        f"{SENTINEL_SO2}, 111.111, 99.99, 5.55, 50.0, 100000.0, 120.0, 101.0, 10.0"
    )
    taos(
        f"INSERT INTO {CHILD_TABLE} (ts, {', '.join(COLUMNS)}) "
        f"VALUES ('{ts_text}', {values});"
    )


def delete_window(start: str, end: str) -> None:
    taos(f"DELETE FROM {CHILD_TABLE} WHERE ts >= '{start}' AND ts < '{end}';")


# ==================== 2. HTTP ====================

def get_json(base: str, path: str, timeout: float = 20.0) -> dict[str, Any]:
    with urllib.request.urlopen(f"{base}{path}", timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def report_point(report_start: str, report_end: str, target: str) -> Optional[dict[str, Any]]:
    query = urllib.parse.urlencode({"start": report_start, "end": report_end})
    payload = get_json(NGINX_BASE, f"/api/report/minute?{query}")
    for point in payload.get("points", []):
        if point["ts"] == target:
            return point
    return None


def cache_stats(base: str = DIRECT_BASE) -> dict[str, Any]:    return get_json(base, "/api/cache/stats")


def clear_cache(base: str = DIRECT_BASE) -> dict[str, Any]:
    request = urllib.request.Request(f"{base}/api/cache/clear", data=b"", method="POST")
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


# ==================== 3. 主流程 ====================

def main() -> int:
    parser = argparse.ArgumentParser(description="缓存失效实测：数据更新后多久刷新")
    parser.add_argument("--out", type=Path, default=DOCS_DIR / "cache_invalidation_drill.json")
    parser.add_argument("--timeout", type=float, default=120.0, help="最长等待秒数")
    parser.add_argument("--poll-interval", type=float, default=0.2)
    parser.add_argument("--sentinel-second", type=int, default=5,
                        help="哨兵行在目标分钟内的秒位（插在 :05，便于与 5 s 网格区分）")
    args = parser.parse_args()

    # ---- 选目标分钟（推导见下，**不要**随手改窗口）----
    # 四个条件必须同时满足（每一条都是实测撞出来的）：
    #   ① 目标分钟必须在过去，且报告窗口的对齐上界要**超过**它。
    #      因为 clamp_to_now 把上界收窄到 now、align_range 再向下取整到分钟；
    #      若目标分钟对齐后就是最后一个完整分钟，聚合不会返回这一行，
    #      报表里压根没有这个点，脚本会一直读到 null。
    #   ② 报告窗口的**对齐结果必须全程不变**，否则缓存键会自己滚动，
    #      脚本会一直盯着一条从没被预热过的新键（实测踩过：上界取 target+2min30s，
    #      脚本跑到 18:32 之后对齐上界从 18:31 滚到 18:32，于是"永远测不到刷新"）。
    #      取上界 = target + 3 min（已在过去）即可固定。
    #   ③ 插入时刻 t >= mid + 60 - MARGIN，插入后窗口才算已闭合、才可能进缓存。
    #   ④ mid 必须在 /api/data 的观测窗内（now - mid < QUERY_MINUTES=10 min），
    #      否则"这一分钟的最大值"永远传不到展示层，写后失效不可能触发。
    # 取 mid = now - 5 min（若不是空分钟则继续往前找，最多到 now - 8 min）。
    margin = _margin_seconds()
    now = datetime.now()
    base_minute = (now - timedelta(minutes=5)).replace(second=0, microsecond=0)
    picked = _pick_injectable_minute(base_minute)
    if picked is None:
        print("⛔ 在允许的分钟范围内找不到可无损注入的分钟，无法演练。")
        return 2
    target_minute = picked["minute"]
    original_rows: list[dict[str, Any]] = picked["rows"]
    target = target_minute.strftime(TS_FORMAT)
    window_end = (target_minute + timedelta(minutes=1)).strftime(TS_FORMAT)
    sentinel_ts = picked["sentinel_ts"] or (
        (target_minute + timedelta(seconds=args.sentinel_second)).strftime(TS_FORMAT)
    )

    # 见 ②：上界 target+3min（已经在过去）→ 对齐结果恒为 [target-2min, target+3min)，
    # 目标分钟落在正中间，不是首窗口（可能被裁）、也不是末窗口（可能没有数据）。
    report_start = (target_minute - timedelta(minutes=2)).strftime(TS_FORMAT)
    report_end = (target_minute + timedelta(minutes=3)).strftime(TS_FORMAT)

    record: dict[str, Any] = {
        "purpose": "实测数据更新后缓存多久刷新（写后失效 vs TTL 兜底，分开归因）",
        "started_at": now.strftime(TS_FORMAT),
        "mechanism_under_test": "cache.note_minute_max / observed_window_max（写后失效）",
        "sentinel": {"ts": sentinel_ts, "so2": SENTINEL_SO2},
        "target": {
            "target_window": target,
            "window_end_exclusive": window_end,
            "report_start": report_start,
            "report_end": report_end,
            "baseline_expectation": "n=0, so2=null（该分钟在插入前没有数据）",
            "after_insert_expectation": f"n=1, so2={SENTINEL_SO2}",
        },
        "known_side_effect": "插入后库里会短暂多出一个未来时间戳的数据点，脚本结束前已删除",
    }

    clear_cache()
    stats0 = cache_stats()
    record["cache_config"] = stats0["config"]
    record["watermarks_before"] = stats0["watermarks"]
    record["original_rows"] = original_rows
    record["original_rows_count"] = len(original_rows)
    print(f"[1] 配置 {stats0['config']} | data_ts 水位 {stats0['watermarks']['data_ts']}")

    existing = len(original_rows)
    record["rows_in_window_before"] = existing
    print(f"[2] 目标分钟 {target} 现有原始行 = {existing}（哨兵将插在 {sentinel_ts}）")

    try:
        # ---- 步骤 3：预热缓存，确认基线 ----
        # 关键自检：预热期间必须**至少出现一次命中**。否则说明这一版缓存压根没写进 Redis
        # （或者缓存键在滚动），后面的"刷新"就无从谈起 —— 实测踩过这个坑：
        # 上界滚动导致缓存键每次都变，预热三次全是 miss，脚本却以为已经预热好了。
        counter_start = cache_stats()["counters"]
        for _ in range(3):
            point_before = report_point(report_start, report_end, target)
        stats_warm = cache_stats()
        warm_delta = {
            key: stats_warm["counters"][key] - counter_start[key]
            for key in stats_warm["counters"]
        }
        record["baseline"] = {
            "point": point_before,
            "counters_before_warm": counter_start,
            "counters": stats_warm["counters"],
            "warm_delta": warm_delta,
            "cache_keys": stats_warm["cache_keys"],
            "minute_max_entries": stats_warm["minute_max_entries"],
        }
        print(f"[3] 缓存已预热：目标点 = {point_before} | 预热增量 = {warm_delta}")
        if warm_delta.get("hit", 0) < 1:
            print("⛔ 预热期间一次命中都没有：缓存没生效（或缓存键在滚动），本次演练无效。")
            record["aborted"] = f"no cache hit during warmup: {warm_delta}"
            return 2
        # 再取一次并比对：两次完全一致才能证明"基线确实来自缓存且内容稳定"
        repeat = report_point(report_start, report_end, target)
        record["baseline"]["repeat_point"] = repeat
        record["baseline"]["repeat_equal"] = repeat == point_before
        print(f"    基线可重复 = {repeat == point_before}")
        if repeat != point_before:
            print("⛔ 基线两次请求结果不一致，无法做确定性判定。")
            record["aborted"] = "baseline not reproducible"
            return 2
        if point_before is None or point_before.get("so2") is None:
            print("⛔ 基线点没有数值，无法判断窗口值有没有变。")
            record["aborted"] = f"baseline unusable: {point_before}"
            return 2

        # ---- 步骤 4：插哨兵（插在该分钟现有最大值之后，保证该分钟最大值真的前进）----
        insert_sentinel(sentinel_ts)
        inserted_at = time.time()
        record["rows_after_insert"] = count_window(target, window_end)
        print(f"[4] 已插入哨兵 ts={sentinel_ts}（该分钟行数 {existing} -> "
              f"{record['rows_after_insert']}）")
        if record["rows_after_insert"] != existing + 1:
            print(f"⛔ 插入后行数不是 {existing + 1}（可能覆盖了已有 ts），本次演练无效。")
            record["aborted"] = "insert did not add exactly one row"
            return 2

        # ---- 步骤 5：观测与轮询**并行**（这才是真实拓扑）----
        # 展示层知道"库里某一分钟的最大值变了"只靠 /api/data（大屏每 5 s 打一次）。
        # 所以刷新延迟由两段构成：① 大屏下一次打 /api/data 的等待 ② 报表请求发现失效并回源。
        # 这里刻意**不**在插入后同步补打一次 /api/data —— 那会把 ① 压成 0，
        # 测出来的 0.011 s 只代表 ②，不能当"数据更新后多久刷新"对外引用。
        observations: list[dict[str, Any]] = []
        changed_at: Optional[float] = None
        stop = threading.Event()

        def is_fresh(point: Optional[dict[str, Any]]) -> bool:
            """该点是否已经反映插入后的新数据。

            ⚠️ 不能写 `so2 == SENTINEL_SO2`：报告数值会按 report.ROUND_DIGITS=2 四舍五入，
            123.456 回来是 123.46，严格相等永远不成立 —— 实测就是这么漏判的
            （哨兵明明已经进结果，脚本却一直报"没观测到刷新"）。
            判据改成"与**预期均值**的差 < 0.01"，预期均值由原有行的均值 + 哨兵一起算出来。
            """
            if point is None or point.get("so2") is None:
                return False
            expected = (
                sum(row["so2"] for row in original_rows) + SENTINEL_SO2
            ) / (len(original_rows) + 1)
            if point.get("n") not in (None, len(original_rows) + 1):
                return False
            return abs(float(point["so2"]) - expected) < 0.01

        def watch_data() -> None:
            """后台持续打 /api/data（大屏本来就在 5 s 刷一次）。"""
            while not stop.is_set():
                try:
                    get_json(NGINX_BASE, "/api/data")
                except Exception:
                    pass
                stop.wait(1.0)

        watcher = threading.Thread(target=watch_data, daemon=True)
        watcher.start()
        try:
            while time.time() - inserted_at < args.timeout:
                elapsed = time.time() - inserted_at
                point = report_point(report_start, report_end, target)
                fresh = is_fresh(point)
                observations.append({"t_rel_s": round(elapsed, 3), "point": point, "fresh": fresh})
                if fresh:
                    changed_at = elapsed
                    break
                time.sleep(args.poll_interval)
        finally:
            stop.set()
            watcher.join(timeout=5)

        record["observations"] = observations
        record["observations_count"] = len(observations)
        record["refresh_latency_s"] = round(changed_at, 3) if changed_at is not None else None
        record["point_after_update"] = observations[-1]["point"] if observations else None
        record["cache_stats_after"] = cache_stats()
        print(f"[5] 轮询 {len(observations)} 次，刷新延迟 = {record['refresh_latency_s']} s")
        if changed_at is None:
            print(f"    ⚠️ {args.timeout} s 内没有观测到刷新（写后失效没兜住，"
                  f"只能等 TTL={stats0['config']['ttl_seconds']} s）")
    finally:
        # ---- 步骤 6：清理 + 精确还原原有行 ----
        print("[6] 清理并还原该分钟的原始行 …")
        delete_window(target, window_end)
        time.sleep(0.3)
        record["rows_after_delete"] = count_window(target, window_end)
        restore_rows(original_rows)
        time.sleep(0.5)
        restored = read_rows(target, window_end)
        record["restore"] = {
            "rows_after_delete": record["rows_after_delete"],
            "restored_rows_count": len(restored),
            "original_rows_count": len(original_rows),
            "row_by_row_equal": restored == original_rows,
        }
        record["finished_at"] = datetime.now().strftime(TS_FORMAT)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"    删除后行数={record['restore']['rows_after_delete']}，"
              f"还原后行数={record['restore']['restored_rows_count']}"
              f"（原 {record['restore']['original_rows_count']}），"
              f"逐行一致={record['restore']['row_by_row_equal']}")

    print(f"\n原始数据已写入: {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
