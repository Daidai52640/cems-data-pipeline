# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读
# -----------------------------------------------------------------------------
# 1) 本脚本**只读**，不污染任何数据：
#      - 不写 TDengine（只做 SELECT）
#      - 不写网关缓存文件（不碰 data/cache.jsonl）
#      - 不发 MQTT 消息；只作为**额外的一个订阅端**旁听 cems/plant1/data，
#        给"这条数据到达 broker 的时刻"打宿主时钟。它的 client_id 是独立新值，
#        不改动网关/接入层的 client_id、主题、QoS。
# 2) 前置条件：六服务在跑（docker compose ps 全 healthy）；宿主机 python 已装
#    paho-mqtt（项目既有依赖，见 requirements.txt）。
# 3) 运行期间**不要**停 emqx / 重启网关 / 跑补传演练，否则"延迟"里会混进故障恢复时间。
# 4) 输出：只写 --out 指定的 CSV 与同名 .summary.json（覆盖同名文件，不累积垃圾）。
# 5) 环境：Windows 上先设 $env:PYTHONIOENCODING="utf-8"。
#
# 测法（三条时间线，全部落在同一份逐条原始数据里）
# -----------------------------------------------------------------------------
#   ts            报文时间戳：网关读设备那一刻的**网关本地墙钟**，秒级精度
#   t_arrival     MQTT 旁听端收到该报文的**宿主机**墙钟
#   t_first_seen  REST 轮询首次查到该 ts 的**宿主机**墙钟
#
#   lat_visible = t_first_seen - ts        ← 端到端（设备读数时间戳 → 库内可查）
#   lat_arrival = t_arrival    - ts        ← 设备读数 → broker（受 ts 秒级量化限制）
#   delta2      = t_first_seen - t_arrival ← broker → 库内可查（**纯宿主机时钟，无量化**）
#
# 为什么要做时钟校正
# -----------------------------------------------------------------------------
#   ts 来自容器时钟、t_* 来自宿主机时钟，两者**不是同一个时钟**：实测存在
#   百毫秒量级的周期性相对漂移（见 scripts/measure_clock_consistency.py 的输出）。
#   本脚本每次轮询都用 `SELECT NOW()` 同期读一次库端（=容器）时钟，算出当时的
#   偏移 delta_ms，并把 lat_visible 校正成 lat_visible_corr = lat_visible + delta。
#   未校正前请勿引用任何"毫秒级"结论。
#
# 量化偏差怎么处理（重要）
# -----------------------------------------------------------------------------
#   ts 只到秒：真正关系是 lat_visible_corr = 真实延迟 + frac，frac ~ U(0,1)。
#   所以逐样本仍带 ±0.5 s 的均匀量化偏差；报告里给的是：
#     - P50/P95/max 的**原始观测值**（工程上可直接引用，偏保守）
#     - 去偏估计（减 0.5 s）以及"仅直发样本"的固定段中值（min/max 中点法）
#
# 设备维度（必须按设备过滤）
# -----------------------------------------------------------------------------
#   `cems_data` 是多设备共用的超级表（TAG = plant/device），两台设备在同时写。
#   本脚本的轮询 SQL 是 `ORDER BY ts DESC LIMIT n` —— 不过滤时取回的是**所有设备
#   混排**的最近 n 行，"库内可查"样本里会混进别的设备（实测 5 分钟窗口混读 290 行、
#   device1 只有 59 行），逐条延迟与 P95 都不再属于被测设备。
#   默认 device1 ⇒ 单设备形态下与改造前逐位一致；多设备时用 --device 分开测。
#   ⚠️ MQTT 旁听侧不用改：主题本身就是按厂区分的（device1 → cems/plant1/data、
#      device2 → cems/plant2/data，见 docker-compose.yml），默认只旁听 plant1，
#      所以 arrivals 字典天然只含被测设备；换设备时要把 MQTT_TOPIC 一起改。
# =============================================================================
"""测量端到端延迟（设备读数时间戳 → TDengine 可查），输出逐条原始数据 CSV。"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import statistics
import sys
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from _device_scope import add_device_argument, device_predicate, resolve_device   # noqa: E402

TD_REST_URL = os.getenv("TD_REST_URL", "http://127.0.0.1:6041/rest/sql")
TD_USER = os.getenv("TD_USER", "root")
TD_PASS = os.getenv("TD_PASS", "taosdata")
TD_DB = os.getenv("TD_DB", "cems")
TD_STABLE = os.getenv("TD_STABLE", "cems_data")

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "cems/plant1/data")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))

LOCAL_TZ = timezone(timedelta(hours=8))     # 网关/容器时区 Asia/Shanghai（无夏令时）
TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# 超过这个秒数还只是"到达 broker"，说明这条走的是缓存补传而不是实时直发
BACKLOG_THRESHOLD_S = 5.0


def parse_ts_local(text: str) -> int:
    """报文里的本地时间字符串 → epoch 整秒。"""
    return int(datetime.strptime(text, TS_FORMAT).replace(tzinfo=LOCAL_TZ).timestamp())


def parse_iso_utc(text: str) -> float:
    """REST 返回的 ISO8601 UTC 串 → epoch 秒。"""
    cleaned = text.strip().replace("Z", "").replace("T", " ")
    if "." in cleaned:
        return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        ).timestamp()
    return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    ).timestamp()


def td_query(sql: str, timeout: float = 5.0) -> tuple[float, float, dict]:
    req = urllib.request.Request(
        TD_REST_URL,
        data=sql.encode("utf-8"),
        headers={
            "Authorization": "Basic "
            + base64.b64encode(f"{TD_USER}:{TD_PASS}".encode()).decode(),
            "Content-Type": "text/plain",
        },
    )
    t_before = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    t_after = time.time()
    if payload.get("code") != 0:
        raise RuntimeError(f"TDengine 返回错误: {payload}")
    return t_before, t_after, payload


class MqttProbe:
    """旁听 MQTT 主题，给每条报文的到达时刻打宿主时钟。"""

    def __init__(self) -> None:
        self.arrivals: dict[int, float] = {}
        self.count = 0
        self.lock = threading.Lock()
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"cems-metrics-probe-{os.getpid()}",
            clean_session=True,
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            client.subscribe(MQTT_TOPIC, qos=MQTT_QOS)
            print(f"[probe] 已订阅 {MQTT_TOPIC} (QoS={MQTT_QOS})", flush=True)
        else:
            print(f"[probe] 连接失败 reason_code={reason_code}", flush=True)

    def _on_message(self, client, userdata, msg) -> None:
        now = time.time()
        parts = msg.payload.decode("utf-8", errors="replace").split()
        if len(parts) < 2:
            return
        try:
            epoch = parse_ts_local(f"{parts[0]} {parts[1]}")
        except ValueError:
            return
        with self.lock:
            self.count += 1
            self.arrivals.setdefault(epoch, now)

    def start(self) -> None:
        self.client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
        self.client.loop_start()

    def stop(self) -> None:
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass


def percentile(ordered: list[float], q: float) -> float:
    """线性插值分位数（与 numpy.percentile 默认口径一致）。"""
    if not ordered:
        return float("nan")
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def summarize(values: list[float]) -> dict[str, float]:
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "min": ordered[0],
        "p50": percentile(ordered, 0.50),
        "p95": percentile(ordered, 0.95),
        "max": ordered[-1],
        "mean": statistics.fmean(ordered),
        "stdev": statistics.stdev(ordered) if len(ordered) > 1 else 0.0,
    }


def fmt(stats: dict[str, float]) -> str:
    if not stats.get("n"):
        return "n=0（无样本）"
    return (
        f"n={int(stats['n']):4d} min={stats['min']:.3f}s P50={stats['p50']:.3f}s "
        f"P95={stats['p95']:.3f}s max={stats['max']:.3f}s "
        f"mean={stats['mean']:.3f}s stdev={stats['stdev']:.3f}s"
    )


def recompute(csv_path: Path) -> int:
    """从已落盘的逐条原始数据重新汇总（不改数据、不重测），用于复核文档里的数字。"""
    with csv_path.open(encoding="utf-8", newline="") as fp:
        records = list(csv.DictReader(fp))

    def pick(cls: str, field: str) -> list[float]:
        return [float(r[field]) for r in records if r["class"] == cls and r[field] != ""]

    direct = pick("direct", "lat_visible_corrected_s")
    resend = pick("resend", "lat_visible_corrected_s")
    allv = [float(r["lat_visible_corrected_s"]) for r in records if "clock-jump" not in r["class"]]
    d2 = pick("direct", "delta2_s")
    print(f"== 复核 {csv_path.name}：共 {len(records)} 条 ==")
    print(f"  直发(全样本)  : {fmt(summarize(allv))}")
    print(f"  直发 direct   : {fmt(summarize(direct))}")
    print(f"  补传 resend   : {fmt(summarize(resend))}")
    if direct:
        print(f"  直发延迟上界  : {min(direct):.3f} s（min(观测值)，真实值的合法上界）")
    if d2:
        print(f"  broker→库可见 : {fmt(summarize(d2))}（含 U(0,poll) 探测延迟）")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="CEMS 端到端延迟测量（只读）")
    parser.add_argument("--minutes", type=float, default=30.0, help="测量时长（分钟）")
    parser.add_argument("--poll", type=float, default=0.25, help="REST 轮询间隔（秒）")
    parser.add_argument("--limit", type=int, default=600, help="每轮回看的最近行数")
    parser.add_argument("--tag", default="run", help="本次运行标签")
    parser.add_argument("--recompute", default=None, help="只从已有 CSV 重新汇总，不测")
    add_device_argument(parser)
    parser.add_argument(
        "--out",
        default=str(PROJECT_ROOT / "docs" / "evidence" / "perf" / "latency_raw.csv"),
    )
    args = parser.parse_args()

    if args.recompute:
        return recompute(Path(args.recompute))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    probe = MqttProbe()
    probe.start()
    time.sleep(2.0)

    device = resolve_device(args.device)
    print(
        f"[run] 设备={device}（TAG 过滤）tag={args.tag} 时长={args.minutes} 分钟 "
        f"轮询间隔={args.poll}s",
        flush=True,
    )

    # ts -> (首次被查到时刻, 当时的时钟校正量 delta_ms, 是否落在时钟跳变窗口)
    seen: dict[int, tuple[float, float, bool]] = {}
    first_poll = True
    rest_rtt: list[float] = []
    clock_offsets: list[float] = []
    jump_count = 0
    prev_delta: float | None = None
    # ⚠️ 必须带 device 谓词：不加的话 LIMIT n 取回的是各设备混排的行（见文件头说明）
    sql = (
        f"SELECT NOW() AS t_now, ts FROM {TD_DB}.{TD_STABLE} "
        f"WHERE {device_predicate(device)} "
        f"ORDER BY ts DESC LIMIT {args.limit}"
    )

    deadline = time.time() + args.minutes * 60.0
    try:
        while time.time() < deadline:
            t0 = time.time()
            try:
                t_before, t_after, payload = td_query(sql)
            except Exception as exc:
                print(f"[warn] 查询失败（继续）: {exc}", flush=True)
                time.sleep(1.0)
                continue
            host_mid = (t_before + t_after) / 2.0
            rest_rtt.append(t_after - t_before)

            columns = [str(meta[0]) for meta in payload["column_meta"]]
            i_now = columns.index("t_now")
            i_ts = columns.index("ts")
            rows = payload.get("data") or []
            if rows:
                # 库端（容器）时钟 - 宿主机时钟；同期测得，用来校正跨时钟相减
                delta_s = parse_iso_utc(rows[0][i_now]) - host_mid
                clock_offsets.append(delta_s)
            else:
                delta_s = clock_offsets[-1] if clock_offsets else 0.0

            # 容器时钟实测存在周期性向前跳变（见 measure_clock_consistency.py）。
            # 跳变落在两次轮询之间时，本窗口内新出现的行无法判断"跳变前还是跳变后
            # 入库"，其校正量有歧义 → 打标并在统计里剔除。
            in_jump_window = False
            if prev_delta is not None and abs(delta_s - prev_delta) > 0.1:
                jump_count += 1
                in_jump_window = True
            prev_delta = delta_s

            keys = [int(parse_iso_utc(row[i_ts])) for row in rows]
            if first_poll:
                for key in keys:
                    seen.setdefault(key, (float("nan"), delta_s, False))
                first_poll = False
            else:
                for key in keys:
                    if key not in seen:
                        seen[key] = (host_mid, delta_s, in_jump_window)

            elapsed = time.time() - t0
            if elapsed < args.poll:
                time.sleep(args.poll - elapsed)
    except KeyboardInterrupt:
        print("[run] 收到中断，按已采集样本出结果", flush=True)

    probe.stop()

    with probe.lock:
        arrivals = dict(probe.arrivals)

    records: list[dict[str, object]] = []
    for epoch, (visible_at, delta_s, in_jump) in sorted(seen.items()):
        if visible_at != visible_at:        # nan = 运行前就存在的历史行
            continue
        arrival = arrivals.get(epoch)
        lat_visible = visible_at - epoch
        lat_corrected = lat_visible + delta_s
        if arrival is None:
            cls = "no-probe"
            lat_arrival = ""
            delta2 = ""
            lat_arrival_corr = ""
        else:
            lat_arrival = arrival - epoch
            lat_arrival_corr = lat_arrival + delta_s
            delta2 = visible_at - arrival
            cls = "resend" if lat_arrival > BACKLOG_THRESHOLD_S else "direct"
        if in_jump:
            cls += "+clock-jump"
        records.append(
            {
                "ts_epoch": epoch,
                "ts_local": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(epoch)
                ),
                "t_arrival": "" if arrival is None else f"{arrival:.3f}",
                "t_first_seen": f"{visible_at:.3f}",
                "clock_offset_ms": f"{delta_s * 1000:.1f}",
                "lat_visible_s": f"{lat_visible:.3f}",
                "lat_visible_corrected_s": f"{lat_corrected:.3f}",
                "lat_arrival_s": "" if lat_arrival == "" else f"{lat_arrival:.3f}",
                "lat_arrival_corrected_s": (
                    "" if lat_arrival_corr == "" else f"{lat_arrival_corr:.3f}"
                ),
                "delta2_s": "" if delta2 == "" else f"{delta2:.3f}",
                "class": cls,
            }
        )

    if records:
        with out_path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=list(records[0].keys()))
            writer.writeheader()
            writer.writerows(records)

    def pick(cls: str, field: str) -> list[float]:
        return [
            float(r[field])
            for r in records
            if r["class"] == cls and r[field] != ""
        ]

    direct_corr = pick("direct", "lat_visible_corrected_s")
    resend_corr = pick("resend", "lat_visible_corrected_s")
    all_corr = [
        float(r["lat_visible_corrected_s"])
        for r in records
        if "clock-jump" not in str(r["class"])
    ]
    lat_arr_corr = pick("direct", "lat_arrival_corrected_s")
    d2 = pick("direct", "delta2_s")

    def midrange(values: list[float]) -> tuple[float, float] | None:
        """均匀量化下的分布中心估计：真实值 ≈ (min+max)/2 - 0.5，误差量级 1/(N+1) 秒。

        ⚠️ 前提"量化份额 u 在样本间均匀独立"在本环境**不成立**：容器时钟相对宿主机
        以约 26 ms/s 漂移、又周期性跳变，而轮询周期与秒边界近似同步 → 单次运行内
        u 近似**冻结**在某个值上（同一次运行里连续多行的 u 只差 0.02 秒）。
        因此该方法只用于**量级参考**，不作为精确值；两次不同运行之间它可以相差
        100~150 ms（已实测）。要一个可靠结论请用下面的**上界法**：
        真实值 = 观测值(corrected) - u ≤ 观测值，所以 min(观测值) 就是真实值的
        一个合法上界（前提：时钟校正准确，实测 ±7 ms）。
        """
        if len(values) < 20:
            return None
        ordered = sorted(values)
        return (ordered[0] + ordered[-1]) / 2.0 - 0.5, 1.0 / (len(ordered) + 1)

    fixed_segment = midrange(direct_corr)
    device_to_broker = midrange(lat_arr_corr)
    # broker → 库内可查：观测值里含"轮询探测延迟 U(0, poll)"，减去其均值 poll/2 去偏
    d2_unbiased = [v - args.poll / 2.0 for v in d2]

    summary = {
        "tag": args.tag,
        "device": device,
        "minutes": args.minutes,
        "poll_interval_s": args.poll,
        "rows_in_db_window": len(records),
        "rows_seen_by_probe": probe.count,
        "class_counts": {
            cls: sum(1 for r in records if str(r["class"]).startswith(cls))
            for cls in ("direct", "resend", "no-probe")
        },
        "clock_jump_events": jump_count,
        "rows_excluded_for_clock_jump": sum(
            1 for r in records if "clock-jump" in str(r["class"])
        ),
        "probe_arrivals_without_db_row": sorted(set(arrivals) - set(seen)),
        "lat_visible_corrected_all": summarize(all_corr),
        "lat_visible_corrected_direct": summarize(direct_corr),
        "lat_visible_corrected_resend": summarize(resend_corr),
        "lat_arrival_corrected_direct": summarize(lat_arr_corr),
        "delta2_direct": summarize(d2),
        "delta2_direct_poll_debiased": summarize(d2_unbiased),
        "direct_fixed_segment_s": fixed_segment[0] if fixed_segment else None,
        "direct_fixed_segment_stderr_s": fixed_segment[1] if fixed_segment else None,
        "direct_upper_bound_s": min(direct_corr) if direct_corr else None,
        "device_to_broker_upper_bound_s": min(lat_arr_corr) if lat_arr_corr else None,
        "device_to_broker_fixed_segment_s": device_to_broker[0] if device_to_broker else None,
        "device_to_broker_stderr_s": device_to_broker[1] if device_to_broker else None,
        "clock_offset_ms": summarize(clock_offsets),
        "rest_rtt_ms": {
            k: v * 1000 for k, v in summarize(rest_rtt).items() if k != "n"
        },
        "quantization_note": (
            "ts 只到秒：lat_visible_corrected = 真实延迟 + U(0,1)。"
            "逐样本仍有 ±0.5 s 量化偏差；直接引用 P50/P95 偏保守 0.5 s，"
            "去偏估计 = 观测值 - 0.5 s。"
        ),
    }
    summary_path = out_path.with_suffix(".summary.json")
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )

    print("\n===== 端到端延迟（设备读数时间戳 → 库内可查），已做时钟校正 =====")
    print(f"  全部样本           : {fmt(summary['lat_visible_corrected_all'])}")
    print(f"  直发 direct        : {fmt(summary['lat_visible_corrected_direct'])}")
    print(f"  补传 resend        : {fmt(summary['lat_visible_corrected_resend'])}")
    if fixed_segment:
        print(
            f"  直发固定段（去量化）: {fixed_segment[0] * 1000:+.0f} ms"
            f"  ±{fixed_segment[1] * 1000:.0f} ms（min/max 中点法）"
        )
    print("\n===== 分段（仅直发样本，u=秒级量化份额对两段完全相同）=====")
    print(
        f"  设备读 → broker    : {fmt(summary['lat_arrival_corrected_direct'])}"
        "（含 U(0,1) 量化）"
    )
    if device_to_broker:
        print(
            f"     去量化固定段     : {device_to_broker[0] * 1000:+.0f} ms"
            f"  ±{device_to_broker[1] * 1000:.0f} ms"
        )
    print(f"  broker → 库内可查  : {fmt(summary['delta2_direct'])}（纯宿主机时钟）")
    print(
        f"     扣掉轮询探测延迟 : {fmt(summary['delta2_direct_poll_debiased'])}"
        f"（减去 U(0,{args.poll}s) 的均值）"
    )
    print(
        f"\n  跨时钟校正量       : {fmt(summarize(clock_offsets))}"
        f"  跳变事件 {jump_count} 次，剔除 "
        f"{summary['rows_excluded_for_clock_jump']} 行"
    )
    print(f"  REST 往返          : P50={summary['rest_rtt_ms'].get('p50', 0):.1f} ms")
    print(f"\n  分类计数           : {summary['class_counts']}")
    print(f"[out] 逐条原始数据: {out_path}")
    print(f"[out] 摘要         : {summary_path}")
    if summary["probe_arrivals_without_db_row"]:
        print(f"[!!] 旁听到但库内未见（疑似丢失）: {summary['probe_arrivals_without_db_row']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
