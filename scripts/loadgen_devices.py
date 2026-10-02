# -*- coding: utf-8 -*-
# =============================================================================
# 跑之前必读
# -----------------------------------------------------------------------------
# 1) 本脚本是**负载发生器**：模拟 N 台设备并发向 EMQX 上送报文。
#
#    ★★ 边界（必须连同任何数字一起引用）：
#       它**直发 EMQX，绕过了网关**。所以它压的是「接入层 + 存储层」
#       （subscriber_to_td + TDengine），**不是**「网关采集层」（Modbus 轮询 →
#       缓存/补传）。网关采集层的背压另有测法，见 docs/reference/并发与负载指标.md §3。
#
# 2) 不污染真实链路：
#      - 主题前缀默认 `cems/loadtest`，**永远不写 cems/plant1/data 或 cems/plant2/data**
#      - 入库写入独立标签（默认 plant=`loadtest`、device=`lt<i>`），与 plant1/plant2 完全分开
#      - `--cleanup` 能把本次造出来的子表全部删掉，见 --help 与文档 §6
#
# 3) 报文格式与真实网关**逐字一致**：直接复用 src/gateway/gateway.py 的 build_payload()
#    与 src/common/points.py 的 POINTS 顺序（9 个测点 + Flag=N），不另抄一份。
#    唯一差别是**数值来源**：真实网关的数值来自 Modbus 寄存器，这里按每台一个独立
#    种子 + 相位偏移构造（否则 N 台发同一条曲线，"N 台"就没有意义）。
#
# 4) 测什么（三条延迟，全部落在每条原始数据里）：
#      lat_gen      = t_publish_host - 计划发布时刻        ← 本机发送侧排队/拥塞（纯宿主时钟）
#      lat_broker   = t_arrival_host - t_publish_host      ← 发布 → broker 投回旁听端（纯宿主时钟）
#      lat_store    = t_first_seen_host - t_arrival_host   ← broker → 库内可查（纯宿主时钟）
#      lat_e2e      = t_first_seen_host - t_publish_host   ← 端到端（纯宿主时钟）
#    ⚠️ 报文里的 ts 是**秒级**的（与网关一致），所以凡是用它当起点算出来的延迟都带
#       U(0,1) s 的量化偏差 —— 本脚本不使用它做延迟计算，只在 --verify 对账时用。
#
# 5) 为什么要旁听 + 轮询两条线：
#      旁听探针（额外一个订阅端、独立 client_id、干净会话）给出"到达 broker 的时刻"；
#      REST 轮询给出"这条 ts 在库里首次可见的时刻"。两者都在**宿主机时钟**上取，
#      所以分段延迟不受容器虚拟时钟漂移影响（见 docs/reference/性能与可靠性指标.md §3）。
#
# 6) 输出（默认 docs/evidence/perf/）：
#      loadgen_<tag>_raw.csv            逐条原始数据
#      loadgen_<tag>_summary.json       汇总（含 throughput / 延迟分位 / 对账台账 / 资源快照）
# =============================================================================
"""负载发生器：模拟 N 台设备并发向 EMQX 上送，测吞吐/延迟，并可对账入库是否丢数。"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import math
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final, Optional

import paho.mqtt.client as mqtt

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import NAMES, POINTS, RANGES              # noqa: E402
from src.gateway.gateway import TS_FORMAT, build_payload         # noqa: E402

LOCAL_TZ: Final[timezone] = timezone(timedelta(hours=8))   # 容器时区 Asia/Shanghai（无夏令时）

# ---- 默认值（与仓库现有脚本、docker-compose 的默认口径一致）----
DEFAULT_MQTT_HOST: Final[str] = "127.0.0.1"
DEFAULT_MQTT_PORT: Final[int] = 1883
DEFAULT_TOPIC_PREFIX: Final[str] = "cems/loadtest"
DEFAULT_TD_URL: Final[str] = "http://127.0.0.1:6041/rest/sql"
DEFAULT_TD_USER: Final[str] = "root"
DEFAULT_TD_PASS: Final[str] = "taosdata"
DEFAULT_TD_DB: Final[str] = "cems"
DEFAULT_TD_STABLE: Final[str] = "cems_data"
DEFAULT_TD_PLANT: Final[str] = "loadtest"
DEFAULT_SEED: Final[int] = 20261002

# 旁听等待：发布后最多再等这么多秒，让"最后一条"的投递/入库被观测到
ARRIVAL_GRACE_S: Final[float] = 3.0
# 轮询间隔（秒）：t_first_seen 的量化误差 = U(0, poll_interval)
DEFAULT_POLL_INTERVAL_S: Final[float] = 0.3
# 批量写库的分块大小（一条 INSERT 多行 VALUES）
INSERT_CHUNK: Final[int] = 300


# ==================== 1. 数值构造（每台设备一条独立曲线） ====================

def _hash_unit(text: str, salt: int) -> float:
    """sha256(text) → [0,1) 的确定性伪随机数（不依赖 random 的全局状态）。"""
    import hashlib
    digest = hashlib.sha256(f"{text}:{salt}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / float(1 << 64)


@dataclass
class DeviceCurve:
    """一台虚拟设备的曲线：独立种子 + 独立相位 + 独立漂移频相。

    与真实设备层的仿真同构（基线 + 日周期 + 漂移 + 毛刺），但不依赖设备层实现，
    目的是"N 台曲线互不相同"这个前提**可被验证**（见 curve_digest()）。
    """

    index: int
    seed: int
    #: 报文时间戳的**同秒错峰量**（秒，0 = 不偏移）。
    #: 多个虚拟设备共用一个 device 标签时（--ingest-channels < N），TDengine 的主键是
    #: (子表, ts)、**同秒必覆盖**，于是"N 台"会塌成"1 台"。给每台一个 [0, interval) 内
    #: 不同的偏移，可以让同一通道内的各台落在不同的秒上（详见 make_curves 的说明）。
    ts_offset: float = 0.0

    def __post_init__(self) -> None:
        self.name = f"lt{self.index}"
        # 每台一个相位偏移（秒），台与台之间错开 → 曲线不重合
        self.phase_s = 0.0 if self.index == 1 else (self.index - 1) * 37.0
        self.drift_hz = 0.00007 * (1.0 + 0.13 * (self.index % 11))

    def values_at(self, when: datetime) -> dict[str, float]:
        """某一时刻该设备的 9 个测点值（与 points.POINTS 的键一致）。"""
        # 基准先截到整秒：报文 ts 只有秒级精度，值的取值点也跟着对齐秒，
        # 否则"值属于第几秒"在两侧会不一致（错峰偏移见 ts_offset）
        stamp = float(int(when.replace(tzinfo=LOCAL_TZ).timestamp())) + self.phase_s
        day_frac = (stamp % 86400.0) / 86400.0
        slow = math.sin(2.0 * math.pi * (day_frac + 0.07 * self.index))
        drift = math.sin(stamp * self.drift_hz + 1.7 * self.index)
        values: dict[str, float] = {}
        for point in POINTS:
            span = point.high - point.low
            if point.name in ("Dust", "SO2", "NOx"):
                # 污染物锚"限值"（与设备层同口径）：平时达标，偶发接近限值
                anchor = point.limit * (0.45 + 0.30 * _hash_unit(point.name + "base", self.seed))
                value = (
                    anchor
                    + 0.06 * point.limit * slow
                    + 0.05 * point.limit * drift
                    + 0.012 * point.limit * math.sin(stamp * 0.37 + self.index)
                )
            elif point.name == "O2":
                # O2 锚实测运行区间 3%~8%（折算公式对它极敏感，不能按量程取）
                anchor = 3.0 + 5.0 * _hash_unit("o2base", self.seed)
                value = anchor + 0.7 * slow + 0.5 * drift + 0.1 * math.sin(stamp * 0.41)
            else:
                anchor = point.low + span * (0.30 + 0.40 * _hash_unit(point.name + "base", self.seed))
                value = (
                    anchor
                    + 0.09 * span * slow
                    + 0.05 * span * drift
                    + 0.01 * span * math.sin(stamp * 0.29 + self.index * 1.3)
                )
            low, high = RANGES[point.name]
            # 收进量程：接入层对超量程是**整条拒收**，负载发生器不该造非法数据
            values[point.name] = round(min(max(value, low), high), 1)
        return values

    def payload_at(self, when: datetime, ts_base: Optional[int] = None) -> tuple[str, str]:
        """该设备在 when 时刻的报文（含同秒错峰），返回 (完整报文, 报文里的秒级 ts)。

        `ts_base`：本周期的**基准整秒**（epoch 秒）。给定时报文 ts = ts_base + ts_offset，
        取值点 = ts_base − ts_offset（两者互补，保证"值属于第几秒"与报文 ts 一致）；
        不给定时就按 when 的整秒算（ts_offset=0 时即本机当前秒）。

        ★ 格式由 `src.gateway.gateway.build_payload()` 原样产出，本函数**只把它的
          秒级时间戳换成错峰后的那一个字符串**，字段、顺序、量纲、Flag 全都不动。
          ⚠️ 为什么不改 build_payload 的时间源：本项目的硬约束是**不改 src/**；
             而这里的替换是"同一格式换一个秒值"，不引入任何格式差异。
        """
        offset = int(self.ts_offset)
        if ts_base is None:
            ts_base = int(when.replace(tzinfo=LOCAL_TZ).timestamp())
        values = self.values_at(datetime.fromtimestamp(ts_base - offset, tz=LOCAL_TZ))
        body = build_payload(values)[20:]          # 去掉 build_payload 生成的 19 秒 ts + 空格
        ts = time.strftime(TS_FORMAT, time.localtime(ts_base + offset))
        return f"{ts} {body}", ts

    def digest_at(self, when: datetime) -> str:
        """该时刻 9 个值的指纹（用于证明"各台曲线不同"）。"""
        import hashlib
        text = "|".join(f"{name}={self.values_at(when)[name]}" for name in NAMES)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def make_curves(count: int, seed: int, devices_per_channel: int = 1,
                interval: float = 5.0) -> list[DeviceCurve]:
    """造 count 台设备，每台一个派生种子（互不相同）。ts_offset 一律为 0。

    ⚠️ 错峰偏移**不在这里算**：它必须是"在本通道内排第几台"，
    而"谁和谁同通道"由 LoadGen 的按模分组决定 —— 在这里按全局下标取模会算错。
    （实测踩过：4 台按 k=2 分组时，按全局下标算出的偏移让两个通道内部都出现重复秒。）
    正确做法见 `build_channel_offsets()`。
    """
    return [
        DeviceCurve(index=index, seed=seed + (index - 1) * 7919)
        for index in range(1, count + 1)
    ]


def build_channel_offsets(devices: int, channels: int) -> tuple[dict[int, int], dict[int, list[int]]]:
    """按模分组算出"每台在本通道内的序位"，返回 ({设备号: 偏移秒}, {通道号: [设备号…]})。

    - 通道划分：`channel = (device-1) % k + 1`（按模，保证同通道内序位唯一）
    - 偏移：通道内第 j 台（0-based）偏移 = j 秒；每通道只有 1 台时偏移恒为 0
    """
    groups: dict[int, list[int]] = {}
    for index in range(1, devices + 1):
        groups.setdefault((index - 1) % channels + 1, []).append(index)
    offsets: dict[int, int] = {}
    for members in groups.values():
        for position, index in enumerate(members):
            offsets[index] = position if len(members) > 1 else 0
    return offsets, groups


# ==================== 2. TDengine REST（只读查询 + 批量写库） ====================

class TdRest:
    """TDengine REST 客户端：只做 SELECT / INSERT，不建库不建表（表在首次写入时由接入层建）。"""

    def __init__(self, url: str, user: str, password: str, db: str, stable: str) -> None:
        self.url = url
        self.db = db
        self.stable = stable
        self._auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def query(self, sql: str, timeout: float = 15.0) -> dict[str, Any]:
        """执行一条 SQL，返回 REST 的原始 JSON（失败抛异常，调用方决定是否吞掉）。"""
        request = urllib.request.Request(
            self.url,
            data=sql.encode("utf-8"),
            headers={"Authorization": self._auth, "Content-Type": "text/plain"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if payload.get("code") != 0:
            raise RuntimeError(f"TDengine 返回错误: {payload.get('desc')} | SQL: {sql[:200]}")
        return payload

    def child_table(self, td_plant: str, device: str) -> str:
        """接入层建的子表名规则：`<plant>_<device>` 且统一小写（见 subscriber_to_td.CHILD_TABLE）。"""
        return f"{td_plant}_{device}".lower()

    def select_device_ts(self, td_plant: str, device: str) -> list[str]:
        """读某台设备在库里的全部 ts（本地时间字符串）。

        用超级表 + `tbname` 过滤而不是直接查子表：子表名由接入层的命名规则推导，
        万一规则漂了会**静默查不到**；tbname 过滤能在库内精确剪枝到一张子表。
        """
        table = self.child_table(td_plant, device)
        payload = self.query(
            f"SELECT ts FROM {self.db}.{self.stable} WHERE tbname = '{table}' ORDER BY ts"
        )
        return [str(row[0]) for row in payload.get("data", [])]

    def select_devices_ts(self, td_plant: str, devices: list[str]) -> dict[str, list[str]]:
        """一次查询取回多台设备的 ts（tbname IN (...)），返回 {设备标签: [ts, ...]}。

        轮询用：每轮一条 SQL 而不是 N 条 —— 否则 N=50 时轮询本身就成了负载的一部分。
        """
        if not devices:
            return {}
        tables = ", ".join(f"'{self.child_table(td_plant, name)}'" for name in devices)
        payload = self.query(
            f"SELECT tbname, ts FROM {self.db}.{self.stable} "
            f"WHERE tbname IN ({tables}) ORDER BY ts"
        )
        out: dict[str, list[str]] = {}
        for row in payload.get("data", []):
            out.setdefault(str(row[0]).lower(), []).append(str(row[1]))
        return out


def _fmt_db_ts(text: str) -> str:
    """REST 返回的 ISO8601(UTC) 串 → 本地时间字符串（与报文 ts 同口径，便于对账）。"""
    cleaned = str(text).strip().replace("Z", "").replace("T", " ")
    if "." in cleaned:
        moment = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f")
    else:
        moment = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
    return moment.replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ).strftime(TS_FORMAT)


def iso_utc_to_host_epoch(text: str) -> float:
    """REST 返回的 ISO8601(UTC) 串 → 宿主机 epoch 秒（用于与旁听时刻相减）。"""
    cleaned = str(text).strip().replace("Z", "").replace("T", " ")
    if "." in cleaned:
        moment = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S.%f")
    else:
        moment = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
    return moment.replace(tzinfo=timezone.utc).timestamp()


# ==================== 3. 逐条在飞记录 ====================

@dataclass
class Sent:
    """一条已发布报文的全生命周期时间戳。"""

    seq: int
    device: int
    device_name: str
    payload_ts: str          # 报文里的秒级 ts
    planned: float           # 计划的发布时刻（宿主 monotonic）
    published: float = 0.0   # 实际交给 paho 的时刻（宿主 monotonic）
    arrived: float = 0.0     # 旁听端收到该报文的时刻（宿主 monotonic）
    seen: float = 0.0        # REST 轮询首次查到该 ts 的时刻（宿主 monotonic）
    payload: str = ""
    rc: int = 0

    def row(self) -> dict[str, Any]:
        """导出一行 CSV（延迟单位毫秒；未观测到的留空）。"""
        def ms(value: float) -> str:
            return f"{value * 1000.0:.1f}" if value > 0 else ""

        return {
            "seq": self.seq,
            "device": self.device_name,
            "topic_device_index": self.device,
            "payload_ts": self.payload_ts,
            "rc": self.rc,
            "lat_gen_ms": ms(self.published - self.planned) if self.published else "",
            "lat_broker_ms": ms(self.arrived - self.published) if (self.arrived and self.published) else "",
            "lat_store_ms": ms(self.seen - self.arrived) if (self.seen and self.arrived) else "",
            "lat_e2e_ms": ms(self.seen - self.published) if (self.seen and self.published) else "",
        }


def percentiles(values: list[float], points: tuple[float, ...] = (50, 95, 99)) -> dict[str, float]:
    """分位数（线性插值，与 numpy 默认口径一致），入参已按毫秒计。"""
    if not values:
        return {}
    ordered = sorted(values)
    out: dict[str, float] = {}
    for point in points:
        if len(ordered) == 1:
            out[f"p{point:g}"] = round(ordered[0], 1)
            continue
        position = (len(ordered) - 1) * point / 100.0
        low = math.floor(position)
        high = math.ceil(position)
        weight = position - low
        out[f"p{point:g}"] = round(ordered[low] * (1 - weight) + ordered[high] * weight, 1)
    out["min"] = round(ordered[0], 1)
    out["max"] = round(ordered[-1], 1)
    out["mean"] = round(statistics.fmean(ordered), 1)
    return out


# ==================== 4. 发布器 ====================

class LoadGen:
    """N 台设备的并发发布器 + 旁听探针 + 库内可见性轮询。"""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        # 入库通道划分：把 N 台按 **i mod k** 分成 k 组，每组由一个独立接入实例（独立
        # device 标签 / 独立子表）负责。默认 k=1 = 全部进同一张子表（单写入者口径）。
        #
        # ⚠️ 为什么按模分组、而不按"连续几台一组"：
        #   接入层是**一个实例只写一张子表**，子表主键是 (子表, ts)；
        #   同一张表里多台设备必须落在不同的秒上（见 make_curves 的 ts_offset）。
        #   ts_offset 按"在本通道内排第几台"取值，模分组让第 i 台在通道内第 (i-1)//k 位，
        #   于是偏移 = (i-1)//k，**同一通道内两两不同、不同通道之间互不干扰**；
        #   而"连续分组"在第 6 台就会重复偏移（实测 10 台只留 40 行、丢 360 行）。
        self.channels = max(1, min(args.ingest_channels, args.devices))
        self.channel_offsets, self.channel_devices = build_channel_offsets(
            args.devices, self.channels
        )
        self.devices_per_channel = max(len(v) for v in self.channel_devices.values())
        # ⚠️ 每通道 m 台共用一张子表时，ts 只能在 m 个整秒上错开；若上报周期比 m 短，
        #    不同周期的 ts 必然相撞，而子表主键 (子表, ts) 会**静默覆盖** → 库里条数变少，
        #    看起来像"丢数据"。这不是链路丢数，是**负载发生器的构造与表结构不匹配**：
        #    真实部署是"一台设备一个接入实例"（k=N），此时 ts_offset 恒为 0，不存在该问题。
        #    这里显式提醒，并要求 N/k ≤ interval；否则请把 --interval 调大或把 k 调到 N。
        if self.devices_per_channel > args.interval:
            print(
                f"[loadgen][WARN] 每通道 {self.devices_per_channel} 台 > 上报周期 "
                f"{args.interval}s：同一子表内 ts 会跨周期相撞并被 TDengine 覆盖，"
                f"库内条数会少于发布条数（不是链路丢数）。"
                f"请提高 --interval 或把 --ingest-channels 设为 {args.devices}（= 一台一实例）。",
                file=sys.stderr,
            )
        self.curves = make_curves(args.devices, args.seed,
                                  self.devices_per_channel, args.interval)
        for curve in self.curves:
            curve.ts_offset = float(self.channel_offsets[curve.index])
        self.topics = {
            curve.index: f"{args.topic_prefix}/device{curve.index}/data" for curve in self.curves
        }
        self.sent: list[Sent] = []
        self._lock = threading.Lock()
        self._seq = 0
        self._probe_seen: dict[tuple[int, str], float] = {}
        self.probe_messages = 0
        self.published_count = 0
        self.enqueue_failures = 0
        self.ack_failures = 0
        self.arrived_count = 0
        self.db_visible = 0
        self.stop_publisher = threading.Event()
        self.rate_buckets: dict[int, int] = {}
        self.publish_window: tuple[float, float] = (0.0, 0.0)
        #: 发布窗口起点的**墙钟整秒**（报文 ts 的基准；`monotonic` 不能当 epoch 用，
        #: 实测把它喂给 time.localtime 会得到 1970-01-01，整批数据被库判"越界"全丢）
        self.publish_base_epoch = 0
        #: 旁听探针是否真的订阅成功（SUBACK 已回）——数据完整性的一部分，写进汇总
        self.probe_ready = False
        self.probe_suback: list[int] = []

        tag = f"{args.tag or 'run'}-{os.getpid()}"
        self.client_id = f"cems-loadgen-{tag}"
        self.probe_id = f"cems-loadgen-probe-{tag}"

    # ---- 4.1 MQTT ----
    def _make_publisher(self) -> mqtt.Client:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        client.connect(self.args.mqtt_host, self.args.mqtt_port, keepalive=60)
        client.loop_start()
        return client

    def _make_probe(self) -> tuple[mqtt.Client, threading.Event, list[int]]:
        """旁听探针：额外一个订阅端，独立 client_id、干净会话，只订阅不发布。

        它是**读侧**：即便它掉线也不会让入库少一条（接入层是另一个独立会话）。

        返回 (client, ready_event, suback_rcs)；ready_event 在收到 SUBACK 之后置位，
        这样"发布起点"之前订阅一定已经生效，不会把开头的消息旁听丢。
        """
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=self.probe_id, clean_session=True,
        )
        ready = threading.Event()
        suback: list[int] = []

        def on_connect(_c: mqtt.Client, _u: Any, _f: Any, reason_code: Any, _p: Any) -> None:
            if reason_code != 0:
                print(f"[loadgen][probe] 连接失败 reason_code={reason_code}", file=sys.stderr)
                return
            for topic in self.topics.values():
                result = client.subscribe(topic, qos=self.args.qos)
                if self.args.debug_probe:
                    print(f"[loadgen][probe][dbg] subscribe {topic} qos={self.args.qos} -> {result}",
                          file=sys.stderr, flush=True)

        def on_subscribe(_c: mqtt.Client, _u: Any, _mid: int, rcs: Any, _p: Any) -> None:
            # paho 2.x 回调给的是 [ReasonCode]（不是 int）；1.x 给的是 [int]
            raw = [rcs] if isinstance(rcs, int) else list(rcs)
            suback.extend(int(getattr(value, "value", value)) for value in raw)
            if self.args.debug_probe:
                print(f"[loadgen][probe][dbg] suback {rcs}", file=sys.stderr, flush=True)
            ready.set()

        def on_message(_c: mqtt.Client, _u: Any, msg: mqtt.MQTTMessage) -> None:
            stamp = time.monotonic()
            with self._lock:
                self.probe_messages += 1
            if self.args.debug_probe:
                print(f"[loadgen][probe][dbg] message {msg.topic} {msg.payload[:24]!r}",
                      file=sys.stderr, flush=True)
            text = msg.payload.decode("utf-8", errors="replace")
            parts = text.split()
            if len(parts) < 3:
                return
            key = (_device_index_from_topic(msg.topic), f"{parts[0]} {parts[1]}")
            with self._lock:
                self._probe_seen.setdefault(key, stamp)

        def on_log(_c: mqtt.Client, _u: Any, _level: int, buf: str) -> None:
            if "error" in buf.lower() or "exception" in buf.lower():
                print(f"[loadgen][probe][paho] {buf}", file=sys.stderr)

        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.on_log = on_log
        client.connect(self.args.mqtt_host, self.args.mqtt_port, keepalive=60)
        client.loop_start()
        return client, ready, suback

    # ---- 4.2 主流程 ----
    def run(self) -> dict[str, Any]:
        args = self.args
        publisher = self._make_publisher()
        probe, probe_ready, suback = self._make_probe()
        # 等 SUBACK：订阅没生效就开跑，开头的消息会被旁听漏掉（延迟样本平白缺失）
        if not probe_ready.wait(timeout=10.0):
            print("[loadgen][probe] 10 秒内未收到 SUBACK，延迟分段数据将不完整", file=sys.stderr)
        self.probe_ready = probe_ready.is_set()
        self.probe_suback = suback
        time.sleep(0.5)          # SUBACK 之后 broker 才会真正把匹配的消息推过来

        stop_seen = threading.Event()
        poller = threading.Thread(
            target=self._poll_visibility, args=(stop_seen,), name="loadgen-poller", daemon=True,
        )
        poller.start()

        start = time.monotonic()
        self.publish_window = (start, start + args.duration)
        self.publish_base_epoch = int(time.time())   # 墙钟整秒：报文 ts 的周期基准
        workers = max(1, min(len(self.curves), args.parallel_publishers))
        threads = [
            threading.Thread(target=self._device_loop, args=(publisher, worker_index, workers),
                             name=f"loadgen-dev{worker_index}", daemon=True)
            for worker_index in range(workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        published_end = time.monotonic()
        # 收尾：必须等**发布出去的每一条都在库内可见**才能收工。
        # ⚠️ 不能只等固定秒数：高并发下 broker 队列 + 接入层单线程入库会明显滞后
        #    （实测 N=10、50 条/秒 时，发布结束还有上千条排在队列里没入库；
        #     此时若按固定 settle 收工，汇总里的"库内可见"会远小于"已发布"，
        #     看起来像丢数，其实只是还没查完）。
        #    这里的判据是"库内可见条数 = 已发布条数"，另加 idle 上限防止无限等。
        remaining_wait = max(args.settle, 10.0)
        idle = 0.0
        last_seen = -1
        drain_deadline = published_end + args.visibility_timeout
        while idle < remaining_wait and time.monotonic() < drain_deadline:
            with self._lock:
                total = len(self.sent)
                seen = self.db_visible
            if total and seen >= total:
                break
            if seen != last_seen:
                idle = 0.0
                last_seen = seen
            else:
                idle += 0.2
            time.sleep(0.2)
        self.drain_end = time.monotonic()
        self.drain_complete = bool(self.sent) and self.db_visible >= len(self.sent)

        stop_seen.set()
        poller.join(timeout=10.0)
        publisher.disconnect()
        publisher.loop_stop()
        probe.disconnect()
        probe.loop_stop()

        return self._summarise(start, published_end)

    def _device_loop(self, publisher: mqtt.Client, worker_index: int, workers: int) -> None:
        """发布线程：按 interval 节奏，轮流给"归我管"的设备发一条。

        一个 client 多个发布线程是 paho 支持的用法（publish 线程安全）；
        这样做的好处是：N=50 时仍然只有 1 条 TCP 连接，压的是链路本身而不是连接数。

        ★ 时钟模型：**所有线程共用一个"周期起点"**（`cycle i` 的起点 = 窗口起点 + i×interval），
          每台设备在自己的周期里发 1 条、报文 ts = 周期起点 + 该设备的 ts_offset。
          为什么不各自按 `time.monotonic()` 起算：那样 k 个线程的起点会差几十毫秒，
          加上 ts_offset 再取整秒时会**跨秒合并**（实测：10 台里有 5 台被挤到同一秒，
          子表主键 (子表, ts) 互相覆盖 → 300 条只留 39 行）。
          共用一个周期起点后，"第 i 周期第 j 台"的 ts 完全确定，且与墙钟的偏差 ≤ interval。
        """
        args = self.args
        interval = args.interval
        start, end = self.publish_window
        # ★ interval < 1 s：改成"**每秒发一台**"的轮询突发。
        #   为什么不能"每秒把 N 台都发一遍"：报文 ts 只有秒级精度，接入层子表主键是
        #   (子表, ts)，同一台在同一秒发两条就会被**静默覆盖**（实测 300 条只留 21 行）。
        #   轮询突发让"每秒恰好一条、ts 全局唯一"，代价是每台的有效采样周期被拉长到 N 秒
        #   （报告里必须写清这一点，否则吞吐数字会被误读）。
        if interval < 1.0:
            cycle = 0
            while not self.stop_publisher.is_set():
                cycle_start = start + cycle * interval
                if cycle_start >= end:
                    break
                delay = cycle_start - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                cycle_base = self.publish_base_epoch + int(round(cycle * interval))
                # 归我发的设备：worker_index, +workers, ...；每轮只发 1 台，按轮次取
                mine = list(range(worker_index, len(self.curves), workers))
                curve = self.curves[mine[cycle % len(mine)]]
                self._emit(publisher, curve, cycle_base, cycle)
                cycle += 1
            return

        cycle = 0
        while not self.stop_publisher.is_set():
            cycle_start = start + cycle * interval
            if cycle_start >= end:
                break
            # 等到本周期起点（落后了就立刻做，追赶不叠加：见下面的 next 判断）
            delay = cycle_start - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            # ★ 周期基准整秒 = 窗口起点墙钟 + cycle×interval（**由周期号唯一决定**）。
            #   绝不能在这里读 time.time()：一个周期要发 N 条、耗时可能跨过整秒边界，
            #   各线程读到的 base 会不同，加上 ts_offset 后互相撞在同一秒
            #   （实测：第 1 周期 lt2 与第 2 周期 lt1 撞在同一秒，逐秒累积 10 台塌成 1 台）。
            #   也绝不能用 time.monotonic() 当 epoch（它不是墙钟，会被库判越界）。
            cycle_base = self.publish_base_epoch + int(round(cycle * interval))

            # 本轮归我发的设备：worker_index, worker_index+workers, ...
            for index in range(worker_index, len(self.curves), workers):
                if self.stop_publisher.is_set():
                    break
                self._emit(publisher, self.curves[index], cycle_base, cycle)

            cycle += 1
            # ⚠️ 这里**绝不能**按墙上时间把 cycle 往前跳（写成 cycle = (now-start)/interval）：
            #    那样会把已经发过的 cycle 号再算一遍，cycle_base 重复 → 同一台同一秒发两条 →
            #    子表主键 (子表, ts) 直接覆盖。实测正是它把 100 条压成 21 行。
            #    cycle 只增 1；落后是 delay 的分内事（<0 就不睡，立刻追）。

    def _emit(self, publisher: mqtt.Client, curve: DeviceCurve,
              cycle_base: int, cycle: int) -> None:
        """发一条该设备的报文（含台账/计数/等 PUBACK）。"""
        args = self.args
        planned = time.monotonic()
        payload, payload_ts = curve.payload_at(datetime.now(tz=LOCAL_TZ), ts_base=cycle_base)
        if args.debug_probe:
            print(f"[dbg] cycle={cycle} base={cycle_base} dev={curve.index} "
                  f"off={int(curve.ts_offset)} ts={payload_ts}", file=sys.stderr, flush=True)

        with self._lock:
            self._seq += 1
            item = Sent(
                seq=self._seq, device=curve.index, device_name=curve.name,
                payload_ts=payload_ts, planned=planned, payload=payload,
            )
            self.sent.append(item)

        item.published = time.monotonic()
        try:
            info = publisher.publish(self.topics[curve.index], payload, qos=args.qos)
        except Exception:                       # 边界异常不能掀翻发布线程
            with self._lock:
                self.enqueue_failures += 1
            item.rc = -1
            return
        item.rc = int(info.rc)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            with self._lock:
                self.enqueue_failures += 1
            return
        with self._lock:
            self.published_count += 1
            bucket = int(item.published - self.publish_window[0])
            self.rate_buckets[bucket] = self.rate_buckets.get(bucket, 0) + 1
        if args.ack_timeout > 0:
            try:
                info.wait_for_publish(timeout=args.ack_timeout)
            except (ValueError, RuntimeError):
                pass
            if not info.is_published():
                with self._lock:
                    self.ack_failures += 1

    # ---- 4.3 库内可见性轮询 ----
    def _poll_visibility(self, stop: threading.Event) -> None:
        """轮询库内该批 ts，把"首次可见时刻"回填到对应的在飞记录上。

        ⚠️ 只查本次造的这些子表（tbname IN (...)），一轮一条 SQL、不扫全库；
        查不到就下轮再查。设备名 → 子表名的换算与接入层同规则（见 TdRest.child_table）。
        """
        td = TdRest(self.args.td_url, self.args.td_user, self.args.td_pass,
                    self.args.td_db, self.args.td_stable)
        device_tags = [curve.name for curve in self.curves]
        #: 已经匹配过的 (设备, ts) 与已回填过旁听时刻的 key —— **跨轮持久**，不随 pending 重建清空，
        #: 否则库内同一行会被反复计数（db_visible 虚高）、已回填的会被重复扫。
        matched: set[tuple[int, str]] = set()
        arrival_done: set[int] = set()
        pending: dict[tuple[int, str], Sent] = {}
        idle_after_publish = 0
        while True:
            # ⚠️ 循环退出条件必须看 `stop` 事件，**不能**用"本轮 pending 为空"：
            #    轮询线程是在发布开始**之前**启动的，那时一条都还没发，
            #    "pending 为空"会让它第一轮就退出 → 之后所有消息都拿不到库里可见时刻
            #    （实测踩过：db_visible / arrived_at_broker 恒为 0）。
            if stop.is_set():
                break
            publish_end = self.publish_window[1]
            past_window = publish_end > 0 and time.monotonic() > publish_end

            # ⚠️ 顺序要紧：**先取旁听快照、再扫全部在飞记录**。
            #    反过来的话，两条语句之间新到的消息既不在快照里（那一条没匹配上），
            #    又已经因为"被收进 pending"而不再参与后续匹配 → 永久漏掉。
            with self._lock:
                arrived = dict(self._probe_seen)
                candidates = list(self.sent)

            for item in candidates:
                if item.seq not in arrival_done:
                    stamp = arrived.get((item.device, item.payload_ts))
                    if stamp is not None:
                        item.arrived = stamp
                        arrival_done.add(item.seq)
                        with self._lock:
                            self.arrived_count += 1
                    elif item.published and time.monotonic() - item.published > 30.0:
                        # 30 秒还没旁听到 = 这条旁听侧真没收到（不计入延迟样本，但留在台账里）
                        arrival_done.add(item.seq)

            with self._lock:
                for item in self.sent:
                    key = (item.device, item.payload_ts)
                    if item.seen == 0 and key not in matched:
                        pending[key] = item

            # 再查库，回填可见时刻
            try:
                rows_by_table = td.select_devices_ts(self.args.td_plant, device_tags)
            except Exception:
                rows_by_table = {}
            for table, raw_rows in rows_by_table.items():
                for raw in raw_rows:
                    local = _fmt_db_ts(raw)
                    device = _device_index_of_table(table, self.args.td_plant)
                    key = (device, local)
                    if key in matched:
                        continue
                    item = pending.get(key)
                    if item is not None and item.seen == 0:
                        # ts 到秒：库内可见时刻取"查到的这一刻"，量化误差 ≤ poll_interval
                        item.seen = time.monotonic()
                        matched.add(key)
                        with self._lock:
                            self.db_visible += 1
                        pending.pop(key, None)

            # 发布结束后再给一段宽限：等尾包投递 + 查库回填；连续 3 轮没有未决项就收工
            if past_window:
                if pending:
                    idle_after_publish = 0
                else:
                    idle_after_publish += 1
                    if idle_after_publish >= 3:
                        break
            time.sleep(self.args.poll_interval)

    # ---- 4.4 汇总 ----
    def _summarise(self, start: float, published_end: float) -> dict[str, Any]:
        args = self.args
        with self._lock:
            items = list(self.sent)
        first_pub = min((item.published for item in items if item.published), default=start)
        last_pub = max((item.published for item in items if item.published), default=published_end)
        window = max(last_pub - first_pub, 1e-9)

        def collect(attr: str, only: list[Sent]) -> list[float]:
            out: list[float] = []
            for item in only:
                if attr == "e2e":
                    if item.seen and item.published:
                        out.append((item.seen - item.published) * 1000.0)
                elif attr == "broker":
                    if item.arrived and item.published:
                        out.append((item.arrived - item.published) * 1000.0)
                elif attr == "store":
                    if item.seen and item.arrived:
                        out.append((item.seen - item.arrived) * 1000.0)
                elif attr == "gen":
                    if item.published and item.planned:
                        out.append((item.published - item.planned) * 1000.0)
            return out

        observed = [item for item in items if item.seen and item.published]
        per_device: dict[str, Any] = {}
        for curve in self.curves:
            mine = [item for item in items if item.device == curve.index]
            mine_observed = [item for item in mine if item.seen]
            channel = next(
                (key for key, value in self.channel_devices.items() if curve.index in value), 1
            )
            per_device[curve.name] = {
                "publish_topic": self.topics[curve.index],
                "ingest_table": f"{args.td_plant}_{curve.name}",
                "ingest_channel": channel,
                "published": len(mine),
                "arrived_at_broker": sum(1 for item in mine if item.arrived),
                "visible_in_db": len(mine_observed),
                "curve_digest": curve.digest_at(datetime.now(tz=LOCAL_TZ)),
                "first_payload": mine[0].payload if mine else "",
            }

        summary: dict[str, Any] = {
            "tag": args.tag,
            "started_at": datetime.now(tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "config": {
                "devices": args.devices,
                "interval_seconds": args.interval,
                "duration_seconds": args.duration,
                "qos": args.qos,
                "topic_prefix": args.topic_prefix,
                "topic_example": self.topics[1],
                "mqtt": f"{args.mqtt_host}:{args.mqtt_port}",
                "publisher_client_id": self.client_id,
                "probe_client_id": self.probe_id,
                "td_db_stable": f"{args.td_db}.{args.td_stable}",
                "td_plant": args.td_plant,
                "td_device_tag": f"lt<i>（i=1..{args.devices}）",
                "ingest_channels": self.channels,
                "channel_device_ranges": {
                    f"lt{i}": f"device {min(v)}-{max(v)}（子表 {args.td_plant}_lt{i}）"
                    for i, v in sorted(self.channel_devices.items())
                },
                "settle_seconds": args.settle,
                "poll_interval_seconds": args.poll_interval,
                "parallel_publishers": max(1, min(args.devices, args.parallel_publishers)),
                "seed": args.seed,
                "probe_subscribed": self.probe_ready,
                "probe_suback_rc": self.probe_suback,
                "probe_messages_received": self.probe_messages,
                "probe_keys_seen": len(self._probe_seen),
                "probe_key_sample": [f"{d}@{t}" for d, t in sorted(self._probe_seen)[:3]],
                "ledger_key_sample": sorted(
                    f"{item.device}@{item.payload_ts}" for item in items[:3]
                ),
                "ts_offset_seconds": sorted({curve.ts_offset for curve in self.curves}),
                "ts_offset_note": "同一子表内多台设备共用一张表时，为避免 (子表, ts) 同秒覆盖，"
                                  "第 j 台的报文 ts 加了 j 秒的整数偏移（亚秒偏移无效：ts 只有秒级）；"
                                  "延迟口径不受影响（全部用宿主机时钟）",
            },
            "counts": {
                "published_enqueued": len(items),
                "published_ok": self.published_count,
                "enqueue_failures": self.enqueue_failures,
                "ack_failures": self.ack_failures,
                "arrived_at_broker": self.arrived_count,
                "visible_in_db": self.db_visible,
            },
            "throughput": {
                "publish_window_seconds": round(window, 3),
                "published_per_second": round(self.published_count / window, 3),
                "visible_in_db_per_second": round(self.db_visible / window, 3),
                "nominal_per_second": round(args.devices / args.interval, 3),
                "drain_seconds": round(getattr(self, "drain_end", published_end) - first_pub, 3),
                "drain_complete": getattr(self, "drain_complete", False),
                "e2e_rows_per_second": round(
                    self.db_visible / max(getattr(self, "drain_end", published_end) - first_pub, 1e-9), 3
                ),
                "per_second_buckets": {
                    str(second): count for second, count in sorted(self.rate_buckets.items())
                },
            },
            "latency_ms": {
                "note": "全部由宿主机时钟相减（不含跨容器时钟漂移）；"
                        "lat_gen = 计划发布 → 实际发布；lat_broker = 发布 → 旁听端收到；"
                        "lat_store = 旁听端收到 → 库内首次可见（含 ≤poll_interval 的轮询量化）；"
                        "lat_e2e = 发布 → 库内首次可见",
                "samples_observed": len(observed),
                "gen_to_publish": percentiles(collect("gen", items)),
                "publish_to_broker": percentiles(collect("broker", items)),
                "broker_to_db_visible": percentiles(collect("store", items)),
                "e2e_publish_to_db_visible": percentiles(collect("e2e", items)),
            },
            "per_device": per_device,
        }
        return summary


def _device_index_from_topic(topic: str) -> int:
    """从 `.../device<i>/data` 里取设备号；取不到返回 -1。"""
    for part in topic.split("/"):
        if part.startswith("device") and part[6:].isdigit():
            return int(part[6:])
    return -1


def _device_index_of_table(table: str, td_plant: str) -> int:
    """子表名 → 设备号：`<plant>_lt<i>` → i；不符合返回 -1。"""
    prefix = f"{td_plant}_lt"
    name = table.lower()
    if name.startswith(prefix) and name[len(prefix):].isdigit():
        return int(name[len(prefix):])
    return -1


# ==================== 5. 对账（发布 vs 入库，逐条） ====================

def verify(args: argparse.Namespace, published: dict[str, set[str]]) -> dict[str, Any]:
    """逐条对账：把"本次发布过的 (设备, ts)"与"库内该设备的全部 ts"比一遍。

    ⚠️ `published` 必须是**运行时实际发出去的**那些 ts（调用方从台账里取），
       不能按算法重算：报文 ts 取的是"发布那一刻的整秒"，重算会整体错位几秒，
       结果是"missing 与 extra 一样多"这种假丢数（实测踩过一次）。

    口径（三条都是"逐条"，不是抽样）：
      missing = 发布过但库里没有  → **真丢数**
      extra   = 库里有但本次没发（该子表的历史残留 / 别的运行留下的）→ 不计入丢数，但要报出来
      dup     = 库内同 ts 多行（QoS1 重投 + TDengine 覆盖后仍剩多行说明 ts 撞了）
    """
    td = TdRest(args.td_url, args.td_user, args.td_pass, args.td_db, args.td_stable)
    report: dict[str, Any] = {"per_device": {}, "totals": {}}
    total_missing = total_extra = total_dup = total_db = 0
    for device, want in sorted(published.items()):
        try:
            rows = [_fmt_db_ts(raw) for raw in td.select_device_ts(args.td_plant, device)]
        except Exception as exc:
            report["per_device"][device] = {"error": str(exc)}
            continue
        counts: dict[str, int] = {}
        for ts in rows:
            counts[ts] = counts.get(ts, 0) + 1
        have = set(counts)
        missing = sorted(want - have)
        extra = sorted(have - want)
        dup = sorted(ts for ts, count in counts.items() if count > 1)
        total_missing += len(missing)
        total_extra += len(extra)
        total_dup += len(dup)
        total_db += len(rows)
        report["per_device"][device] = {
            "published": len(want),
            "in_db_rows": len(rows),
            "in_db_distinct_ts": len(have),
            "missing": len(missing),
            "missing_examples": missing[:5],
            "extra": len(extra),
            "extra_examples": extra[:5],
            "duplicate_ts": len(dup),
        }
    # ⚠️ `extra` 大不等于丢数，但它有一个**极易误读的成因**：上一次运行遗留在
    #    EMQX 持久会话队列里的行，会在本次被投递进来。若本次发布集合里只有
    #    一部分运行期内发出，就会同时出现 extra 很大、missing=0 的形态。
    #    所以这里显式给出两侧时间窗，让读的人能自己判断 extra 是不是"别的运行留下的"。
    published_min = min((ts for s in published.values() for ts in s), default="")
    published_max = max((ts for s in published.values() for ts in s), default="")
    report["window"] = {
        "published_ts_min": published_min,
        "published_ts_max": published_max,
    }
    report["totals"] = {
        "published": sum(len(v) for v in published.values()),
        "in_db_rows": total_db,
        "missing": total_missing,
        "extra": total_extra,
        "duplicate_ts": total_dup,
        "loss_free": total_missing == 0,
        "extra_note": "extra = 库内落在本次发布 ts 集合之外的行；常见成因是上一轮运行"
                      "遗留在 EMQX 持久会话队列里的数据在本次被投递（用 client_id 区分运行可避免）",
    }
    return report


def cleanup(args: argparse.Namespace) -> dict[str, Any]:
    """删掉本次造出的子表（默认只删 `lt*`，即负载发生器专用设备标签，绝不碰 device1/device2）。"""
    td = TdRest(args.td_url, args.td_user, args.td_pass, args.td_db, args.td_stable)
    removed: list[str] = []
    failed: dict[str, str] = {}
    for index in range(1, args.devices + 1):
        table = td.child_table(args.td_plant, f"lt{index}")
        try:
            td.query(f"DROP TABLE IF EXISTS {args.td_db}.{table}")
            removed.append(table)
        except Exception as exc:
            failed[table] = str(exc)
    return {"dropped": removed, "failed": failed, "count": len(removed)}


# ==================== 6. 旁听统计模式（--listen-topic） ====================

def listen_topic_mode(args: argparse.Namespace) -> int:
    """只旁听一个主题、统计「唯一报文条数」，不发布不写库。

    为什么要按**唯一 ts** 去重计数：网关的补传会把**同一份报文原样重发**，
    按消息条数会把重发算成新数据；而 `(子表, ts)` 在库里也是唯一键，
    所以"唯一 ts 数"才是与库内条数可比的口径。

    返回 {"messages": 收到的消息总条数, "distinct_ts": 唯一 ts 数, ...}。
    """
    topics = [t.strip() for t in args.listen_topic.split(",") if t.strip()]
    seen_ts: dict[str, dict[str, Any]] = {}
    messages = 0
    lock = threading.Lock()
    ready = threading.Event()
    t_start = time.time()

    def on_connect(client: mqtt.Client, _u: Any, _f: Any, reason_code: Any, _p: Any) -> None:
        if reason_code != 0:
            print(f"[listen] 连接失败 reason_code={reason_code}", file=sys.stderr)
            return
        for topic in topics:
            client.subscribe(topic, qos=args.qos)

    def on_subscribe(_c: mqtt.Client, _u: Any, _mid: int, _rcs: Any, _p: Any) -> None:
        ready.set()

    def on_message(_c: mqtt.Client, _u: Any, msg: mqtt.MQTTMessage) -> None:
        nonlocal messages
        text = msg.payload.decode("utf-8", errors="replace")
        parts = text.split()
        if len(parts) < 3:
            return
        ts = f"{parts[0]} {parts[1]}"
        with lock:
            messages += 1
            item = seen_ts.setdefault(ts, {"first_host_epoch": time.time(), "count": 0,
                                           "topic": msg.topic, "payload": text})
            item["count"] += 1

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"cems-listen-{os.getpid()}", clean_session=True)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    client.loop_start()
    if not ready.wait(timeout=10.0):
        print("[listen] 10 秒内未收到 SUBACK", file=sys.stderr)
    print(f"[listen] 已订阅 {topics}，聆听 {args.listen_seconds or '∞'} 秒", flush=True)

    try:
        while args.listen_seconds <= 0 or time.time() - t_start < args.listen_seconds:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    time.sleep(3.0)          # 收尾：把在途的投递收干净
    client.disconnect()
    client.loop_stop()

    with lock:
        summary = {
            "topics": topics,
            "seconds": round(time.time() - t_start, 2),
            "messages_received": messages,
            "distinct_ts": len(seen_ts),
            "duplicate_deliveries": messages - len(seen_ts),
            "first_ts": min(seen_ts) if seen_ts else "",
            "last_ts": max(seen_ts) if seen_ts else "",
            "sample_payload": (sorted(seen_ts.items())[0][1]["payload"] if seen_ts else ""),
        }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if args.listen_out:
        path = Path(args.listen_out)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
    return 0


# ==================== 7. 命令行 ====================

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="负载发生器：模拟 N 台设备并发向 EMQX 上送（报文格式与真实网关一致）。"
                    "直发 EMQX、绕过网关 ⇒ 压的是接入层+存储层，不是网关采集层。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--devices", type=int, required=True, help="并发设备台数 N")
    parser.add_argument("--interval", type=float, default=5.0, help="每台设备的上报周期（秒）")
    parser.add_argument("--duration", type=float, default=60.0, help="压测时长（秒）")
    parser.add_argument("--tag", default="", help="本次运行的标签（进输出文件名与 client_id）")
    parser.add_argument("--topic-prefix", default=DEFAULT_TOPIC_PREFIX,
                        help="主题前缀；实际主题 = <前缀>/device<i>/data")
    parser.add_argument("--qos", type=int, default=1, choices=(0, 1))
    parser.add_argument("--mqtt-host", default=DEFAULT_MQTT_HOST)
    parser.add_argument("--mqtt-port", type=int, default=DEFAULT_MQTT_PORT)
    parser.add_argument("--ack-timeout", type=float, default=5.0,
                        help="等 PUBACK 的秒数；0 = 不等（只测发送侧吞吐）")
    parser.add_argument("--settle", type=float, default=5.0,
                        help="停止发布后，'本轮无新行入库'持续多少秒就算收尾完成")
    parser.add_argument("--visibility-timeout", type=float, default=180.0,
                        help="收尾阶段最长等多少秒等'库内可见 = 已发布'（硬上限）")
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL_S,
                        help="库内可见性轮询间隔（秒）：lat_store 的量化误差等于它")
    parser.add_argument("--parallel-publishers", type=int, default=16,
                        help="发布线程数上限（N 台共用 1 条 MQTT 连接，线程轮流发）")
    parser.add_argument("--ingest-channels", type=int, default=1,
                        help="入库通道数 k：N 台按【连续】划成 k 段，每段一个独立 device 标签"
                             "（= 一个独立接入实例）。k=1 表示单写入者口径")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="曲线种子基数")
    parser.add_argument("--td-url", default=DEFAULT_TD_URL)
    parser.add_argument("--td-user", default=DEFAULT_TD_USER)
    parser.add_argument("--td-pass", default=DEFAULT_TD_PASS)
    parser.add_argument("--td-db", default=DEFAULT_TD_DB)
    parser.add_argument("--td-stable", default=DEFAULT_TD_STABLE)
    parser.add_argument("--td-plant", default=DEFAULT_TD_PLANT,
                        help="写入用的 plant 标签（隔离用，默认 loadtest）")
    parser.add_argument("--out-dir", default="docs/evidence/perf")
    parser.add_argument("--no-verify", action="store_true", help="跳过逐条对账")
    parser.add_argument("--cleanup", action="store_true",
                        help="只做清理：删掉 <td_plant>_lt<i> 子表，然后退出")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印将要发布的报文，不连 MQTT、不写库")
    parser.add_argument("--debug-probe", action="store_true",
                        help="把旁听探针的 connect/suback/message 逐个打到 stderr（排查旁听不到时用）")
    parser.add_argument("--listen-topic", default="",
                        help="只当旁听器：订阅该主题（逗号分隔多个）并统计『唯一报文条数』，"
                             "不发布、不写库、不做对账。用于量另一条链路（如隔离网关实例）"
                             "实际发布了多少条，见 --listen-out")
    parser.add_argument("--listen-out", default="",
                        help="--listen-topic 的汇总 JSON 输出路径（留空则只打印）")
    parser.add_argument("--listen-seconds", type=float, default=0.0,
                        help="--listen-topic 的聆听时长（秒）；0 = 一直听到 Ctrl+C")
    return parser.parse_args(argv)


def dry_run(args: argparse.Namespace) -> None:
    """打印前两台设备各一条报文，用于人工核对格式。"""
    channels = max(1, min(args.ingest_channels, args.devices))
    offsets, _groups = build_channel_offsets(args.devices, channels)
    curves = make_curves(args.devices, args.seed)
    for curve in curves:
        curve.ts_offset = float(offsets[curve.index])
    now = datetime.now(tz=LOCAL_TZ)
    for curve in curves:
        payload, ts = curve.payload_at(now)
        print(f"[{curve.name}] topic={args.topic_prefix}/device{curve.index}/data ts_offset={curve.ts_offset}")
        print(f"  {payload}")
        if curve.index >= 2:
            break
    print(f"[digest] 台1={curves[0].digest_at(now)} 台2={curves[1].digest_at(now)}")


def write_outputs(args: argparse.Namespace, summary: dict[str, Any],
                  rows: list[Sent]) -> tuple[Path, Path]:
    """把逐条原始数据与汇总写到 --out-dir（覆盖同名文件，不累积垃圾）。

    对账还没跑完时文件名带 `_partial` 后缀，跑完再写最终名 —— 这样**不会出现**
    一个"已存在但少了对账结果"的最终文件被误当成完整证据。
    """
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = PROJECT_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = args.tag or f"n{args.devices}"
    partial = "" if args.stage == "final" else "_partial"
    csv_path = out_dir / f"loadgen_{tag}_raw{partial}.csv"
    json_path = out_dir / f"loadgen_{tag}_summary{partial}.json"

    fieldnames = ["seq", "device", "topic_device_index", "payload_ts", "rc",
                  "lat_gen_ms", "lat_broker_ms", "lat_store_ms", "lat_e2e_ms"]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in rows:
            writer.writerow(item.row())

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
    return csv_path, json_path


def print_report(summary: dict[str, Any], csv_path: Path, json_path: Path) -> None:
    """把关键数字打到控制台（设备数多时只打前 5 台 + 末 1 台）。"""
    config = summary["config"]
    counts = summary["counts"]
    throughput = summary["throughput"]
    print("\n===== 负载发生器结果 =====")
    print(f"N={config['devices']}  间隔={config['interval_seconds']}s  "
          f"时长={config['duration_seconds']}s  入库通道 k={config['ingest_channels']}  "
          f"名义速率={throughput['nominal_per_second']} 条/秒")
    print(f"发布窗口={throughput['publish_window_seconds']}s  "
          f"实际发布速率={throughput['published_per_second']} 条/秒  "
          f"库内可见速率={throughput['visible_in_db_per_second']} 条/秒")
    print(f"端到端排空={throughput['drain_seconds']}s（发布结束到全部可见）  "
          f"端到端吞吐={throughput['e2e_rows_per_second']} 条/秒  "
          f"排空完成={throughput['drain_complete']}")
    print(f"入队={counts['published_enqueued']}  成功={counts['published_ok']}  "
          f"入队失败={counts['enqueue_failures']}  无PUBACK={counts['ack_failures']}  "
          f"旁听到达={counts['arrived_at_broker']}  库内可见={counts['visible_in_db']}")
    print(f"旁听订阅成功={config['probe_subscribed']}  SUBACK={config['probe_suback_rc']}")
    for label, values in summary["latency_ms"].items():
        if isinstance(values, dict) and values:
            print(f"  {label}: {values}")
    print(f"{'设备':<5}{'主题':<34}{'子表':<18}{'发':>5}{'到':>5}{'入库':>6}  曲线指纹")
    devices = list(summary["per_device"].items())
    shown = devices if len(devices) <= 8 else devices[:5] + [("...", None)] + devices[-1:]
    for name, info in shown:
        if info is None:
            print(f"{'...':<5}")
            continue
        print(f"{name:<5}{info['publish_topic']:<34}{info['ingest_table']:<18}"
              f"{info['published']:>5}{info['arrived_at_broker']:>5}{info['visible_in_db']:>6}"
              f"  {info['curve_digest']}")
    if "reconciliation" in summary:
        totals = summary["reconciliation"]["totals"]
        verdict = "✅ 无丢数" if totals["loss_free"] else "❌ 有丢数"
        print(f"对账: 发布={totals['published']}  库内行={totals['in_db_rows']}  "
              f"缺失={totals['missing']}  多余={totals['extra']}  重复ts={totals['duplicate_ts']}  {verdict}")
    print(f"原始数据: {csv_path}")
    print(f"汇总: {json_path}")


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    args.stage = "partial"
    if args.cleanup:
        result = cleanup(args)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if result["failed"]:
            return 1
        return 0
    if args.dry_run:
        dry_run(args)
        return 0
    if args.listen_topic:
        return listen_topic_mode(args)

    generator = LoadGen(args)
    print(f"[loadgen] {args.devices} 台设备，间隔 {args.interval}s，时长 {args.duration}s，"
          f"主题 {args.topic_prefix}/device<i>/data，QoS={args.qos}，"
          f"入库通道 k={generator.channels}（每通道 {generator.devices_per_channel} 台）")
    print(f"[loadgen] 发布端 client_id={generator.client_id}  旁听端 client_id={generator.probe_id}")

    summary = generator.run()
    with generator._lock:
        rows = list(generator.sent)
    published: dict[str, set[str]] = {}
    for item in rows:
        published.setdefault(item.device_name, set()).add(item.payload_ts)

    csv_path, json_path = write_outputs(args, summary, rows)   # 阶段 1：partial（防半成品被误用）

    if not args.no_verify:
        print("[loadgen] 逐条对账（实际发布集合 vs 库内 ts）...")
        summary["reconciliation"] = verify(args, published)

    args.stage = "final"
    csv_path, json_path = write_outputs(args, summary, rows)
    for stale in (csv_path.with_name(csv_path.name.replace(".csv", "_partial.csv")),
                  json_path.with_name(json_path.name.replace(".json", "_partial.json"))):
        try:
            stale.unlink(missing_ok=True)
        except OSError:
            pass

    print_report(summary, csv_path, json_path)
    recon = summary.get("reconciliation", {}).get("totals", {})
    return 0 if (not recon or recon.get("loss_free")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
