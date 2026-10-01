# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读
# -----------------------------------------------------------------------------
# 1) 本脚本**只读**：不写 TDengine、不碰网关缓存、不发 MQTT。
#    （失效演练在 scripts/measure_cache_invalidation.py，那个脚本会写库，单独隔离）
# 2) 前置条件：八服务在跑（docker compose ps 全 healthy）。
# 3) 输出：只写 --out 指定的 JSON（覆盖同名文件，不累积垃圾）。
# 4) 环境：Windows 上先设 $env:PYTHONIOENCODING="utf-8"。
#
# 测什么
# -----------------------------------------------------------------------------
#   ① 经 nginx 与直连后端的状态码 / 响应长度（证明反代不改变响应）
#   ② 反代层自身开销：同一接口 same-workload 走 80 与走 5001 的耗时分布
#   ③ 缓存前后对比：同一负载、同一路由（都走 nginx），只有 CACHE_ENABLED 不同
#      → 查库次数、命中率、响应时间分布，逐项 A/B
#
# 为什么 A/B 都走 nginx
# -----------------------------------------------------------------------------
#   如果 A 走直连 5001、B 走 nginx，那"差异"里混进了反代层开销（§②已单独量）。
#   本脚本要求两侧路由完全一致，唯一变量是缓存开关，差异才归因得干净。
#
# ⚠️ 时钟
#   本脚本只测**宿主机单时钟**内的耗时（一次 HTTP 往返的墙钟差），
#   不跨容器取时间戳相减，因此不受 docs/工程事实-容器时钟漂移.md 的锯齿影响。
#   唯一的例外是响应里的业务时间戳（ts），本脚本只用它做"内容是否同一份"的比对，
#   不做任何毫秒级推断。
# =============================================================================
"""测量 nginx 反代开销 + Redis 缓存前后对比；输出原始 JSON。"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = PROJECT_ROOT / "docs" / "measurements"

NGINX_BASE = "http://127.0.0.1:80"
DIRECT_BASE = "http://127.0.0.1:5001"
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


# ==================== 1. 基础请求工具 ====================

def http_get(base: str, path: str, timeout: float = 60.0) -> dict[str, Any]:
    """发一次 GET，返回 {status, bytes, ms, sha256, body}。

    计时用 time.perf_counter()，且**只覆盖 urlopen 到 read() 完成**这一段：
    DNS、连接建立都算在里面（这正是"客户端感知的耗时"）。
    """
    url = f"{base}{path}"
    request = urllib.request.Request(url)
    started = time.perf_counter()
    status = 0
    body = b""
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return {
        "url": url,
        "status": status,
        "bytes": len(body),
        "ms": elapsed_ms,
        "sha256": hashlib.sha256(body).hexdigest(),
        "body": body,
    }


def bench(base: str, path: str, count: int, gap_seconds: float = 0.0) -> dict[str, Any]:
    """跑 count 次；返回耗时分布 + 状态/长度/内容散列的一致性统计。"""
    samples: list[float] = []
    statuses: dict[str, int] = {}
    lengths: list[int] = []
    digests: set[str] = set()
    for index in range(count):
        result = http_get(base, path)
        samples.append(result["ms"])
        statuses[str(result["status"])] = statuses.get(str(result["status"]), 0) + 1
        lengths.append(result["bytes"])
        digests.add(result["sha256"])
        if gap_seconds and index < count - 1:
            time.sleep(gap_seconds)
    return summarize(samples, statuses, lengths, digests)


def summarize(
    samples: list[float],
    statuses: dict[str, int],
    lengths: list[int],
    digests: set[str],
) -> dict[str, Any]:
    """耗时样本 -> 分位数 + 状态码分布 + 长度/内容一致性。"""
    ordered = sorted(samples)
    return {
        "n": len(samples),
        "statuses": statuses,
        "bytes": {"min": min(lengths), "max": max(lengths)},
        "body_variants": len(digests),
        "ms": {
            "min": round(ordered[0], 2),
            "p50": round(statistics.median(ordered), 2),
            "p95": round(percentile(ordered, 0.95), 2),
            "max": round(ordered[-1], 2),
            "mean": round(statistics.mean(ordered), 2),
        },
        "samples_ms": [round(value, 2) for value in samples],
    }


def percentile(ordered: list[float], fraction: float) -> float:
    """最近秩法取分位（样本量小的时候比线性插值更保守，不会给出样本里没有的值）。"""
    if not ordered:
        return float("nan")
    index = max(0, min(len(ordered) - 1, int(round(fraction * len(ordered))) - 1))
    return ordered[index]


def cache_stats(base: str = DIRECT_BASE) -> dict[str, Any]:
    """取缓存统计（直连 web 端口：/api/cache/* 被 nginx 挡成 403，这是有意的）。"""
    result = http_get(base, "/api/cache/stats")
    try:
        return json.loads(result["body"].decode("utf-8"))
    except Exception as exc:
        return {"error": f"无法解析缓存统计: {exc}", "status": result["status"]}


def clear_cache(base: str = DIRECT_BASE) -> dict[str, Any]:
    """清空缓存条目（不动计数器），让每一轮 A/B 从同一起点开始。"""
    request = urllib.request.Request(f"{base}/api/cache/clear", data=b"", method="POST")
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


# ==================== 2. 负载定义 ====================

def build_workloads(now: datetime) -> list[dict[str, Any]]:
    """定义 A/B 两组共用的一组查询。

    设计要点（**这是"缓存前后对比"能不能成立的前提**）：
      - **只有"已闭合窗口"才会进缓存**（见 src/web/cache.py 的 _closure）。
        所以每个可缓存负载都显式给一个"上界早于 now-60s"的区间；
        凡是采用报表默认区间（上界 = now）的请求，按设计就是查库 —— 那样测出来的
        差异必然是 0，会被误读成"缓存没用"。
      - 观测表（分钟最大值）默认覆盖最近 50 h，所以"昨天日报"可以缓存，
        而"30 天曲线 / 上月月报"超出覆盖范围 → 按设计不写缓存。
        这里两种都留着，正好把"能缓存 / 不能缓存"的边界一起量出来。
      - `/api/data` 是**反例对照组**：它按定义包含当前秒，不该被缓存，
        两侧的查库次数必须完全一样 —— 这才是"缓存没有影响当前数据"的证据。
    """
    minute_start = (now - timedelta(hours=2)).replace(minute=0, second=0, microsecond=0)
    minute_end = minute_start + timedelta(minutes=60)          # 完整 60 个分钟窗口
    closed_end = (now - timedelta(minutes=90)).replace(second=0, microsecond=0)
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    last_month_first = (now.replace(day=1) - timedelta(days=1))
    return [
        {
            "name": "minute_report_closed_1h",
            "desc": "分钟报表·已闭合 1 小时窗口（高频刷新的主场景）",
            "path": "/api/report/minute?" + urlencode({
                "start": minute_start.strftime(TS_FORMAT),
                "end": minute_end.strftime(TS_FORMAT),
            }),
            "count": 60,
            "cacheable": True,
        },
        {
            "name": "minute_report_default_open",
            "desc": "分钟报表·默认口径（上界=now，**按设计不缓存**，用于对照）",
            "path": "/api/report/minute",
            "count": 30,
            "cacheable": False,
        },
        {
            "name": "day_report_yesterday",
            "desc": "日报表·昨天（24 个整点，已闭合；在 50 h 观测覆盖内）",
            "path": "/api/report/day?" + urlencode({"date": yesterday}),
            "count": 30,
            "cacheable": True,
        },
        {
            "name": "custom_report_closed",
            "desc": "自由报表·已闭合 6 小时窗口（按小时聚合）",
            "path": "/api/report/custom?" + urlencode({
                "start": (now - timedelta(hours=8)).strftime(TS_FORMAT),
                "end": closed_end.strftime(TS_FORMAT),
            }),
            "count": 30,
            "cacheable": True,
        },
        {
            "name": "curve_hourly_30d",
            "desc": "自由区间曲线·30 天 1 小时粒度（最重的一条聚合查询）",
            "path": "/api/curve?" + urlencode({
                "start": (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M"),
                "end": closed_end.strftime("%Y-%m-%d %H:%M"),
            }),
            "count": 20,
            "cacheable": True,
        },
        {
            "name": "month_report_last_month",
            "desc": f"月报表·上月（{last_month_first.strftime('%Y-%m')}，按天补齐）",
            "path": "/api/report/month?" + urlencode({
                "year": str(last_month_first.year),
                "month": str(last_month_first.month),
            }),
            "count": 20,
            "cacheable": True,
        },
        {
            "name": "live_api_data_control",
            "desc": "对照组：/api/data 实时接口（含当前秒，**按设计不进缓存**）",
            "path": "/api/data",
            "count": 30,
            "cacheable": False,
        },
    ]


# ==================== 3. 三段测量 ====================

def section_routes(nginx_base: str) -> dict[str, Any]:
    """① 经 nginx 的对外路由：状态码 + 响应长度（验收第 2 条）。"""
    routes = ["/", "/report", "/api/health", "/api/data", "/static/echarts.min.js", "/healthz"]
    out: dict[str, Any] = {}
    for route in routes:
        result = http_get(nginx_base, route)
        out[route] = {
            "status": result["status"],
            "bytes": result["bytes"],
            "ms": round(result["ms"], 2),
        }
    return out


def section_overhead(nginx_base: str, direct_base: str, count: int) -> dict[str, Any]:
    """② 反代层开销：同一接口、同样次数，走 80 vs 走 5001。"""
    targets = ["/", "/api/data", "/static/echarts.min.js"]
    out: dict[str, Any] = {}
    for target in targets:
        direct = bench(direct_base, target, count)
        proxied = bench(nginx_base, target, count)
        out[target] = {
            "direct_5001": direct,
            "nginx_80": proxied,
            "delta_p50_ms": round(proxied["ms"]["p50"] - direct["ms"]["p50"], 2),
        }
    return out


def section_ab(nginx_base: str, label: str) -> dict[str, Any]:
    """③ 缓存 A/B 的一组：清空缓存 → 预热水位 → 跑全部负载 → 记终点计数。

    ⚠️ **预热这一步不能省**（实测踩过）：写后失效的水位（data_ts / 分钟最大值）
    是由 `/api/data` 顺带喂的。如果一上来就先打报表接口，展示层还不知道
    "库里现在到哪一刻了"，`_closure` 判不出窗口闭合 → 全部请求计 bypass_future、
    缓存一次都不命中。实测就是这样：220 个请求里 190 个被判"未闭合"，
    看起来像"缓存完全没用"，其实是测量脚本没把前置条件铺好。
    所以这里先按大屏的真实行为打两次 /api/data，再开始计量。

    ⚠️ 计数器**不清零**：它记录"进程启动以来"的累计值。
    所以本组只比 `counters_delta`（本轮增量），不比绝对值 ——
    绝对值会混进上一轮、上一次测量的量。
    """
    clear_result = clear_cache()
    for _ in range(2):                      # 预热水位（不计入计量）
        http_get(nginx_base, "/api/data")
    before = cache_stats()
    workloads = build_workloads(datetime.now())
    results: dict[str, Any] = {}
    for item in workloads:
        # 有间隔地打：更接近"报表页每 5 秒刷一次"的真实节奏；
        # 缓存命中与否不依赖间隔（键由窗口决定），但间隔能让 Redis 的 TTL 语义更真实。
        # 每个负载**单独记一次计数增量**：否则总量对不上时无法判断是哪一类查询在绕过缓存。
        counter_here = cache_stats()["counters"]
        result = bench(nginx_base, item["path"], item["count"], gap_seconds=0.05)
        result.update({"desc": item["desc"], "path": item["path"], "cacheable": item["cacheable"]})
        result.pop("samples_ms")
        after_here = cache_stats()["counters"]
        result["counters_delta"] = {
            key: after_here[key] - counter_here[key] for key in after_here
        }
        results[item["name"]] = result
    after = cache_stats()
    requests_total = sum(item["count"] for item in workloads)
    queries = (after["counters"]["query"] - before["counters"]["query"])
    return {
        "label": label,
        "cache_enabled": before["enabled"],
        "clear_before_round": clear_result,
        "cache_config": before["config"],
        "watermarks_before": before["watermarks"],
        "watermarks_after": after["watermarks"],
        "counters_before": before["counters"],
        "counters_after": after["counters"],
        "counters_delta": {
            name: after["counters"][name] - before["counters"][name]
            for name in after["counters"]
        },
        "lookups": after["lookups"],
        "http_requests_total": requests_total,
        # 这是"查库次数下降"最直接的证据：本轮所有 HTTP 请求里，真正打到 TDengine 的有几次
        "tdengine_queries_during_round": queries,
        "results": results,
    }


# ==================== 4. 主流程 ====================

def main() -> int:
    parser = argparse.ArgumentParser(description="nginx 反代开销 + Redis 缓存前后对比")
    parser.add_argument("--tag", default="nginx_redis", help="输出文件名标签")
    parser.add_argument("--phase", default="both", choices=["both", "routes", "ab"],
                        help="只跑某一段（A/B 需要重启容器切换开关，故分开跑）")
    parser.add_argument("--overhead-count", type=int, default=12)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    out_path = args.out or (DOCS_DIR / f"nginx_redis_{args.tag}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 支持分多段跑后合并到同一个文件（A/B 两轮之间要重启容器切换缓存开关）
    document: dict[str, Any] = {}
    if out_path.exists():
        try:
            document = json.loads(out_path.read_text(encoding="utf-8"))
        except Exception:
            document = {}

    document.update({
        "measured_at": datetime.now().strftime(TS_FORMAT),
        "nginx_base": NGINX_BASE,
        "direct_base": DIRECT_BASE,
        "clock_note": "耗时全部为宿主机单时钟内的 perf_counter 差；不跨容器相减",
    })

    if args.phase in ("routes", "both"):
        document["routes"] = section_routes(NGINX_BASE)
        document["overhead"] = section_overhead(NGINX_BASE, DIRECT_BASE, args.overhead_count)

    if args.phase in ("ab", "both"):
        stats = cache_stats()
        label = "cache_on" if stats["enabled"] else "cache_off"
        document.setdefault("rounds", {})[label] = section_ab(NGINX_BASE, label)
        document.setdefault("round_order", [])
        if label not in document["round_order"]:
            document["round_order"].append(label)

    out_path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(document, ensure_ascii=False, indent=2))
    print(f"\n原始数据已写入: {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
