# -*- coding: utf-8 -*-
"""平台接入：订阅 MQTT 上送报文，解析 9 个烟气测点后写入 TDengine 时序库 cems.cems_data 超级表。

★ 逐点质量位（2026-10-11 接入，依据 docs/adr/0009-接入层逐点质量位.md）：
  把"**任一测点不合格 → 整条报文拒收**"改成"**逐点处理**"，两种"坏"分开走：

    | 坏法 | 判定 | 处理 |
    |---|---|---|
    | **语法坏** | 时间戳不可解析 / 报文长度不足 / **一个测点都没有** / **同一个测点出现两次** / 测点值不是数 | `PayloadFormatError` → **整条拒收** |
    | **语义坏** | 单点 `NaN`/`inf`/超量程、**报文里没给这个点** | `PointInvalid`（值对象，**不抛**）→ **只丢该点**，其余照常入库 |

  ⚠️ 逐点丢之后那一列在库里是 `NULL` —— 与"这一分钟没采到"**不可区分**，
    所以必须同时写"**证据层**"：无效点事件表 `cems_point_invalid`（表名 env 可覆盖，
    见 `POINT_INVALID_STABLE`；列 `ts/point/kind/reason/raw`，`TAGS(plant, device)`）。
    它是本方案能成立的前提，不是可选增强（ADR-0009 §4/§6.2）。
  ⚠️ 计数也跟着拆（§10.3）：`rejected_messages`（语法坏丢的**报文条数**）
    + `invalid_points`（语义坏丢的**测点个数**）—— 一个指标看不出"整条不通"还是"偶尔一个点坏"。

★ 排放超标告警（2026-10-02 接入，依据 docs/adr/0002-告警判据选型.md 方案乙）：
  入库成功 → 算折算值（全链路唯一一处）→ **接入层逐条判折算值超限** → 落告警事件表/推送记录。
  判定不改拒收逻辑、不改契约、不加服务、不加依赖；
  折算值仍然不给 cems_data 建列，事件表里存的是**判定快照**（判定证据，不是数据冗余列）。
  ⚠️ 折算与判定都要求 O2 + 三个污染物**同帧齐全**；逐点化后某一帧可能不齐（见
  `supports_reference`），那种帧**跳过折算与判定但仍照常入库**。
"""

from __future__ import annotations

import logging
import math
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final, Iterable, Mapping, Optional, Sequence

import paho.mqtt.client as mqtt
import taosrest

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.alarm_judge import (   # noqa: E402
    AlarmConfig,
    AlarmEvent,
    AlarmJudge,
    AlarmTables,
    PUSH_STATUS_FAILED,
    PUSH_STATUS_RECORDED,
    PUSH_STATUS_SENT,
    hour_start_of,
    normalize_ts,
    # SQL 字面量拼装（含"非有限值必须写 NULL"的口径）：唯一真源在 alarm_judge，
    # 无效点事件表的 DML 直接复用，免得"nan 拼进 SQL"这类坑在这里再踩一次
    sql_float,
    sql_text,
    sql_timestamp,
)
# 异常分类（唯一真源 src/common/errors.py）：
#   语法坏 → PayloadFormatError（整条拒收）；语义坏 → PointInvalid（值对象，**不抛**）
from src.common.errors import (   # noqa: E402
    PayloadFormatError,
    PointInvalid,
    PointInvalidKind,
)
from src.common.logging import setup_logging   # noqa: E402
from src.common.points import (   # noqa: E402
    COLUMNS,
    NAMES,
    O2_COLUMN,
    POINTS,
    RANGES,
    ZS_TARGETS,
    ZS_UNAVAILABLE_SENTINEL,
    to_reference_o2,
    to_transmit_value,
)
# 会被拼进 SQL 的标识符/tag 值白名单：**唯一真源**在 src/common/sql_safety.py，
# 展示层读路径（src/web/cache.py 的 device_tag_parts）共用同一条正则。
from src.common.sql_safety import SQL_NAME_RE, SQL_NAME_RULE   # noqa: E402
# 接入层落盘队列（写库失败时的本地持久副本）：机制与崩溃安全见该模块头部说明
from src.platform.ingest_spool import IngestSpool   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================
# 连接参数支持环境变量覆盖，默认值与本机直接运行一致；
# 容器化部署时由 docker-compose.yml 注入服务名（如 MQTT_HOST=emqx、TD_URL=http://tdengine:6041）。

# ---- MQTT（对应 EMQX）----
BROKER: Final[str] = os.getenv("MQTT_HOST", "localhost")
PORT: Final[int] = int(os.getenv("MQTT_PORT", "1883"))
TOPIC: Final[str] = os.getenv("MQTT_TOPIC", "cems/plant1/data")
MQTT_QOS: Final[int] = int(os.getenv("MQTT_QOS", "1"))
MQTT_KEEPALIVE: Final[int] = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_CLIENT_ID: Final[str] = os.getenv("MQTT_CLIENT_ID", "cems-subscriber-plant1")
# ★ 会话持久化：默认 False = 非干净会话（持久会话）。
#   订阅端离线期间，broker 会为该 client_id 排队 QoS≥1 的消息，重连后自动补投；
#   若置 True 变成干净会话，broker 不保留任何状态，离线期间的报文会被直接丢弃。
#   持久会话依赖固定的 client_id，所以 MQTT_CLIENT_ID 不能改成随机值。
MQTT_CLEAN_SESSION: Final[bool] = os.getenv("MQTT_CLEAN_SESSION", "0").strip().lower() in (
    "1", "true", "yes", "on",
)

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = os.getenv("TD_URL", "http://localhost:6041")
TD_USER: Final[str] = os.getenv("TD_USER", "root")
TD_PASS: Final[str] = os.getenv("TD_PASS", "taosdata")
TD_DB: Final[str] = os.getenv("TD_DB", "cems")
TD_STABLE: Final[str] = os.getenv("TD_STABLE", "cems_data")   # 超级表（模板）
TD_PLANT: Final[str] = os.getenv("TD_PLANT", "plant1")        # 标签：厂区（同时也用作子表名）
TD_DEVICE: Final[str] = os.getenv("TD_DEVICE", "device1")     # 标签：设备号
TD_KEEP_DAYS: Final[int] = 365             # 数据保留 1 年
TD_DURATION_DAYS: Final[int] = 30          # 每 30 天一个分片

# ---- 子表名：厂区_设备 ----
# TDengine 对**已存在的子表**会沿用第一次写入时的 TAGS 且不报任何错，
# 所以子表名必须能区分设备。若只用厂区名（plant1），接入第二台设备时
# 数据会被静默挂到第一台设备的标签下。这里改成 plant1_device1 这种形式。
# 统一转小写：TDengine 的表名不区分大小写，若配置写成 Device1，
# 实际建出来仍是 device1，而一致性检查按原样去查就会查不到行、静默跳过。
CHILD_TABLE: Final[str] = f"{TD_PLANT}_{TD_DEVICE}".lower()

# ---- 会被拼进 SQL 的标识符白名单（防注入）----
# ⚠️ 正则本体**不再定义在这里**：唯一真源是 `src.common.sql_safety.SQL_NAME_RE`
#    （本文件顶部 import 进来，名字保持 `SQL_NAME_RE` 不变，供既有调用点与外部引用）。
#    原因：展示层读路径也要用它（`src/web/cache.py` 的 `device_tag_parts()`），
#    常量放在平台层会把 paho.mqtt 这类重依赖拖进 Web 容器，所以下沉到 src/common。
#    规则文案 `SQL_NAME_RULE` 同源，保证两边的错误提示逐字一致。

# ---- 主题 ↔ 标签 一致性自检（多设备多实例部署的护栏）----
# 推荐形态是"**每台设备一个订阅端实例**"：实例只订阅一个主题、只写一张子表，
# 于是本模块里所有带状态的东西（判据滑窗、最近折算值快照、持久会话 client_id）
# 天然是"每设备一份"。多实例的代价只是多一个容器，换来的是单设备代码路径**零改动**。
# 代价对应的风险是配置写错：订阅了 B 厂区的主题，却写 TD_PLANT=A 厂区，
# 数据会**静默**挂到 A 的标签下（入库成功、报表挂错名）。
#
# ★ 这一条为什么**默认拒绝启动**（2026-10-02 升级，原来是只打 WARNING）：
#   典型误配是"只改了 MQTT_TOPIC2 没改 PLANT2"：主题已经是 cems/plant2/data，
#   TD_PLANT 还是 plant1、TD_DEVICE 被改成 device2 ⇒ 数据写进子表 `plant1_device2`。
#   字面上 (plant1, device2) 是自洽的，所以**入库、报表、导出都不会报错**，只是归属错了 ——
#   这类"静默张冠李戴"事后无法从数据里恢复，只能拒之门外。WARNING 在 10 个容器的日志里
#   太容易被淹掉，所以在启动阶段就把它变成一条 CRITICAL + 非零退出（容器会重启并持续可见）。
#   ⚠️ 放行开关：确实要用自定义厂区前缀的部署，显式设 TD_ALLOW_TOPIC_TAG_MISMATCH=1。
#
# ★ 关于"把 TD_DEVICE 也纳入这条校验"：**做不到，也不该假装做到**。
#   主题契约是 `cems/<厂区>/<用途>`（如 cems/plant1/data），**没有设备段**——
#   第三段是用途（data），不是设备号，拿它当设备比只会误报。设备维度的护栏在别处：
#   ① 本模块 `_check_tags()` 在连库时核对已存在子表 `{plant}_{device}` 的真实 TAGS；
#   ② 启动日志直接打印**最终子表名**，让"哪台设备写哪张表"一眼可查（见 main()）。
TOPIC_PLANT_RE: Final[re.Pattern[str]] = re.compile(
    r"^cems/(?P<plant>[A-Za-z_][A-Za-z0-9_]*)/"
)
#: 主题厂区段与 TD_PLANT 不一致时是否还允许启动。默认 0（拒绝）。
ALLOW_TOPIC_TAG_MISMATCH: Final[bool] = os.getenv(
    "TD_ALLOW_TOPIC_TAG_MISMATCH", "0"
).strip().lower() in ("1", "true", "yes", "on")


def topic_plant_of(topic: str) -> Optional[str]:
    """取主题里的厂区段（`cems/<plant>/...`，统一小写）；不是这个形态时返回 None。"""
    match = TOPIC_PLANT_RE.match(topic.strip())
    return match.group("plant").lower() if match else None


def check_topic_matches_plant(topic: str, plant: str, device: str = "") -> bool:
    """主题的厂区段与 TD_PLANT 是否一致；一致/无法判断返回 True，不一致记 ERROR 并返回 False。

    只做判断与记录，不决定"要不要拒绝启动"——那个决策在 `enforce_topic_scope()` 里，
    这样既有的调用方（如果只想拿一个 bool）行为不变。
    """
    topic_plant = topic_plant_of(topic)
    if topic_plant is None or topic_plant == plant.strip().lower():
        return True
    LOGGER.error(
        "主题 %s 的厂区段是 %r，但 TD_PLANT=%s、TD_DEVICE=%s：本实例会把数据写进子表 %s，"
        "归属与主题不一致（入库不报错，报表按错的厂区/设备出数）。"
        "改法：主题的厂区段必须与 TD_PLANT 相同（或者反过来改 TD_PLANT/TD_DEVICE）",
        topic, topic_plant, plant, device or TD_DEVICE, CHILD_TABLE,
    )
    return False


def enforce_topic_scope(topic: str, plant: str, device: str = "") -> bool:
    """启动期强制检查主题 ↔ 标签一致性：不一致**默认拒绝启动**（返回 True 才继续）。

    - 一致、或主题不是 `cems/<厂区>/...` 形态（自定义主题）→ True，照常启动；
    - 不一致 → 记 CRITICAL 并把最终子表名打出来，然后 `SystemExit`（非零退出，
      由 compose 的 restart 策略放大成持续可见的故障，而不是被日志淹没的一条 WARNING）；
    - `TD_ALLOW_TOPIC_TAG_MISMATCH=1` → 只记 CRITICAL 放行（给确实要自定义前缀的部署留口子）。
    """
    if check_topic_matches_plant(topic, plant, device):
        return True
    detail = (
        f"主题 {topic} 的厂区段与 TD_PLANT={plant} 不一致：数据会静默写入子表 "
        f"{CHILD_TABLE}（plant={plant}, device={device or TD_DEVICE}）"
    )
    if ALLOW_TOPIC_TAG_MISMATCH:
        LOGGER.critical(
            "%s。TD_ALLOW_TOPIC_TAG_MISMATCH=1：按配置放行，请自行确认这是有意为之", detail
        )
        return True
    raise SystemExit(
        f"拒绝启动：{detail}。"
        f"要么让主题的厂区段与 TD_PLANT 一致，要么显式设 TD_ALLOW_TOPIC_TAG_MISMATCH=1 放行"
    )


# ---- 测点契约 ----
# 测点定义（MQTT 字段名 / TDengine 列名 / 量程）统一来自 src/common/points.py：
#   NAMES   报文里应该出现的字段名集合
#   COLUMNS 入库列名，顺序即入库列顺序
#   RANGES  量程白名单：超出量程、负数一律判该点无效（ADR-0009 §3"语义坏"）
#           ⚠️ 逐点化之前这里是"整条拒收"；现在是"只丢该点"，见 parse_payload 说明。
#           NaN/inf 会让 TDengine 报 syntax error，所以它**也不进主表**，
#           但在归类上同样是"该点无效"（不是整条拒收）。
TD_COLUMNS: Final[tuple[str, ...]] = COLUMNS
POINT_RANGES: Final[dict[str, tuple[float, float]]] = RANGES
TS_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"

# ---- 折算值（导出量）：只算、不进库 ----
# ⚠️ 折算值**不给 TDengine 建列**。它是从"标干浓度 + 氧含量"算出来的导出量，
#    存一份就必然有一天跟算出来的对不上（冗余必然漂）；库里保持 9 个物理量列。
# ⚠️ 判定口径：**超标判的是折算值，不是标干值**。实测反例：SO2 标干 30.0 在限值
#    35 以内（按标干判 = 达标），但 O2=9% 时折算 37.5（> 35）已超标 —— 同一份数据
#    两种判法结论相反，所以折算值必须有一个能取到的出口（见 reference_values()）。
# 公式、基准氧含量（6%）、参与折算的污染物、氧含量测点名全部来自 src/common/points.py
# 契约，这里**不重复推导公式**，也不硬编码列名字符串。
COLUMN_TO_NAME: Final[dict[str, str]] = {point.column: point.name for point in POINTS}
O2_FIELD_NAME: Final[str] = COLUMN_TO_NAME[O2_COLUMN]
REFERENCE_LOG_EVERY: Final[int] = 20       # 折算值抽样日志周期（条），避免每条都刷屏

# ---- 收发/拒收/落盘计数（把上游数据质量与"落盘队列"状态量化出来）----
# ★ 逐点化（ADR-0009 §10.3）后"拒收"有**两个粒度**，必须拆成两个指标：
#   只有一个按"条"计的指标时，**看不出一台设备是"整条不通"还是"偶尔一个点坏"**——
#   又是一种静默（1 个坏点算 1 条拒收，9 个坏点也算 1 条）。
STATS: Final[dict[str, int]] = {
    "received": 0,
    # 语法坏丢掉的**报文条数**（时间戳坏/长度不足/一个测点都没有/重复测点/值不是数）
    "rejected_messages": 0,
    # 语义坏丢掉的**测点个数**（超量程/NaN/inf → invalid；报文没给 → missing）
    "invalid_points": 0,
    "accepted": 0,
    "spooled": 0,        # 写库失败 → 已落本地队列（有持久副本，等补传）
    "spool_lost": 0,     # 写库失败且落盘也失败 → 真丢（伴随 CRITICAL 日志）
    "drained": 0,        # 由落盘队列补传成功入库的条数
}

# ---- 排放超标告警（ADR-0002 方案乙：接入层逐条判折算值）----
# 判据参数（窗口 M/N、恢复系数、覆盖率门限…）全部在 src/common/alarm_judge.py 里定义，
# 这里只负责：绑定表名/标签、开关、以及"落库 + 推送记录 + 日志"这三件事。
# ⚠️ 判定不改拒收逻辑、不改契约、不加服务、不加依赖；折算值仍然不给 cems_data 建列。
try:
    ALARM_CONFIG: Final[AlarmConfig] = AlarmConfig.from_env()
except (ValueError, TypeError) as exc:      # 环境变量写错就拒绝启动，别带病运行
    raise SystemExit(f"ALARM_* 配置无法解析，拒绝启动: {exc}") from exc

ALARM_TABLES: Final[AlarmTables] = AlarmTables(
    db=TD_DB,
    plant=TD_PLANT,
    device=TD_DEVICE,
    event_stable=os.getenv("ALARM_EVENT_STABLE", "cems_alarm_event"),
    verdict_stable=os.getenv("ALARM_VERDICT_STABLE", "cems_hourly_verdict"),
    push_stable=os.getenv("ALARM_PUSH_STABLE", "cems_alarm_push"),
    data_stable=TD_STABLE,
)
ALARM_RESTORE_LIMIT: Final[int] = 400            # 启动时折叠最近多少条事件行来恢复状态
# ---- 无效点事件表（ADR-0009 §4.1：逐点化之后"证据层"不是可选增强，是本方案成立的前提）----
# ★ 为什么必须有它：逐点丢之后，那一列在库里是 NULL —— 而"这一分钟没采到"也是 NULL。
#   若到此为止，就是把**整条静默**换成了**单点静默**（分不清"断线/设备死/被判无效"，
#   甚至会把"超标数据因超量程被丢掉"看成"缺数"）。所以必须**逐点留痕**。
# ⚠️ 表名照 ALARM_EVENT_STABLE 的既有写法：env 覆盖，默认 cems_point_invalid。
# ⚠️ 它**不参与达标判定**、**不加列到主表**，只是证据层（主表列布局保持不动）。
#    DDL/DML 文本与写入器在下面的 §4d，实例化也在那里。
POINT_INVALID_STABLE: Final[str] = os.getenv("POINT_INVALID_STABLE", "cems_point_invalid")
# 判据统计日志周期（条）：打印 已判/重投跳过/事件/无效样本 计数，
# 让人一眼看出"判据是活的"（ADR-0002 §2.2 2) 可观测性）。
ALARM_STATS_LOG_SAMPLES: Final[int] = int(os.getenv("ALARM_STATS_LOG_SAMPLES", "60"))
# 整点后等多久再结算上一个小时：等网关补传收尾（补传实测最大滞后 32.1s，
# 取 60s 与 web 侧 REPORT_INFLIGHT_GRACE_SECONDS 同一依据）。
ALARM_SETTLE_DELAY_SECONDS: Final[float] = float(
    os.getenv("ALARM_SETTLE_DELAY_SECONDS", "60")
)

# ---- 日志 ----
# 级别/格式/时间格式统一在 src/common/logging.py（唯一真源）；
# 这里只留本进程的 LOGGER —— 4 个进程的日志靠 getLogger 的名字区分来源。
LOGGER: Final[logging.Logger] = logging.getLogger("platform.subscriber_to_td")

# ---- 接入层落盘队列（写库失败时的本地副本）----
# ★ 为什么必须有它：订阅端拿到 QoS1 报文后，paho 在 on_message **正常返回**时发 PUBACK，
#   broker 随即删掉队列里唯一的持久副本；网关那边也早在 broker PUBACK 时就删了 cache.jsonl。
#   若写库失败后直接返回 → 这条数据在整条链路上**没有任何副本** = 永久丢失（旧「P1 未修」）。
#   现在的口径：写库失败 → 先落本地队列（fsync）→ 后台补传 → **入库成功才删**。
#   ⚠️ 代价：写库失败时多一次本地写；队列积压超上限会丢最旧（CRITICAL），容量默认 256 MB。
# ★ 队列目录必须**每实例一份**（多设备部署的关键约束，与网关缓存目录同源）：两个订阅端
#   实例若共用同一个目录，各自的补传线程会把对方的报文拿自己的标签写进自己的子表 →
#   数据张冠李戴，且两边都以为成功。
SUBSCRIBER_DATA_DIR_ENV: Final[str] = os.getenv("SUBSCRIBER_DATA_DIR", "").strip()


def resolve_spool_dir(raw: str) -> Path:
    """把 SUBSCRIBER_DATA_DIR 解析成绝对路径；空值回落 PROJECT_ROOT/data/subscriber。

    - 未设置 → `PROJECT_ROOT/data/subscriber`（容器内即 /app/data/subscriber，配合 compose 的卷）
    - 绝对路径 → 原样使用（容器里的推荐写法，如 /app/data-plant2/subscriber）
    - 相对路径 → 相对**项目根**解析（不是 CWD），本机运行与容器运行不分叉
    """
    if not raw:
        return PROJECT_ROOT / "data" / "subscriber"
    candidate = Path(raw).expanduser()
    return candidate if candidate.is_absolute() else (PROJECT_ROOT / candidate)


SPOOL_DIR: Final[Path] = resolve_spool_dir(SUBSCRIBER_DATA_DIR_ENV)
# 容量上限/裁剪比例：与网关 cache.jsonl 同一套"丢最旧、保最新"口径（见 ingest_spool 说明）
SPOOL_MAX_BYTES: Final[int] = int(
    os.getenv("SUBSCRIBER_SPOOL_MAX_BYTES", str(256 * 1024 * 1024))
)
SPOOL_TRIM_RATIO: Final[float] = float(os.getenv("SUBSCRIBER_SPOOL_TRIM_RATIO", "0.5"))
# 补传节拍（秒）：TDengine 恢复后，积压最多在一个节拍内被取走
SPOOL_RETRY_INTERVAL: Final[float] = float(os.getenv("SUBSCRIBER_SPOOL_RETRY_INTERVAL", "15"))
# 单轮最多补传多少条：既限制一轮的内存/耗时，也避免长时间占着补传线程
SPOOL_BATCH_LIMIT: Final[int] = int(os.getenv("SUBSCRIBER_SPOOL_BATCH_LIMIT", "500"))

SPOOL: Final[IngestSpool] = IngestSpool(
    SPOOL_DIR,
    max_bytes=SPOOL_MAX_BYTES,
    trim_ratio=SPOOL_TRIM_RATIO,
    logger=LOGGER,
)
# 退出信号：main 的 finally 里置位，补传线程在下一个节拍醒来后自然结束
SPOOL_STOP: Final[threading.Event] = threading.Event()


# ==================== 2. 日志 ====================
# setup_logging() 已提取到 src/common/logging.py，4 个入口共用同一份实现。


# ==================== 3. 报文解析 ====================

def validate_config() -> None:
    """校验会被拼进 SQL 的配置值；不合法抛 ValueError，启动阶段就拦下来。

    库名/表名/标签值都是直接拼进 SQL 的，只允许字母数字下划线（防注入）。
    """
    for label, value in (
        ("TD_DB", TD_DB),
        ("TD_STABLE", TD_STABLE),
        ("TD_PLANT", TD_PLANT),
        ("TD_DEVICE", TD_DEVICE),
    ):
        if not SQL_NAME_RE.match(value):
            raise ValueError(f"{label} {SQL_NAME_RULE}: {value!r}")
    # 告警侧：判据参数自检 + 表名同样要能安全拼进 SQL
    ALARM_CONFIG.validate()
    for label, value in (
        ("ALARM_EVENT_STABLE", ALARM_TABLES.event_stable),
        ("ALARM_VERDICT_STABLE", ALARM_TABLES.verdict_stable),
        ("ALARM_PUSH_STABLE", ALARM_TABLES.push_stable),
        ("POINT_INVALID_STABLE", POINT_INVALID_STABLE),
    ):
        if not SQL_NAME_RE.match(value):
            raise ValueError(f"{label} {SQL_NAME_RULE}: {value!r}")


def parse_timestamp(text: str) -> str:
    """校验时间戳格式（顺带挡掉拼进 SQL 的非法字符串），返回原字符串。

    ⚠️ 本函数仍按**自己的**契约抛 `ValueError`；把它升级成"整条报文语法坏"（`PayloadFormatError`）
    是 `parse_payload` 的职责 —— 这样单独用它的调用方行为不变。
    """
    try:
        datetime.strptime(text, TS_FORMAT)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"时间戳格式非法: {text!r}") from exc
    return text


def parse_payload(
    payload: str,
) -> tuple[str, dict[str, float], dict[str, PointInvalid]]:
    """把 MQTT 报文解析成 `(时间戳, 有效测点, 无效测点)`。

    报文格式: "2026-09-10 12:00:00 SO2=35.2 NOx=18.5 ... Pressure=101.3 Flag=N"

    ★★ **两种"坏"分开处理**（ADR-0009 §3 + §10.1，这是本函数存在的全部理由）：

    | 坏法 | 判定 | 处理 |
    |---|---|---|
    | **语法坏** | 时间戳不可解析 / 报文长度不足 / **一个测点都没有** / **同一个测点出现两次** / 测点值根本不是数 | 抛 `PayloadFormatError` → 调用方**整条拒收** |
    | **语义坏** | 单点 `NaN`/`inf`/超量程（`kind=invalid`）、报文里没给这个点（`kind=missing`） | 收进第三个返回值，**不抛**；其余测点照常入库 |

    ⚠️ **为什么"重复测点"算语法坏**（§10.1）：同一个点有两个值，是**报文自相矛盾** ——
      采信哪个都是猜。这不是数据质量问题，是格式问题，所以整条拒收。
    ⚠️ **为什么"值不是数"（`SO2=abc`）算语法坏**：它连"一个合法的数"都不是，
      值对象 `PointInvalid.raw`（`float | None`）表达不了它；而且这类报文通常是
      **编码/配置错**（网关把文本当数发），不是单点仪表故障 —— 宁可整条拦住，别把
      8 个本来正确的点一起放行到"看起来正常"的库里。
    ⚠️ **语义坏一律不抛异常**：它是**正常的数据分类结果**（`src/common/errors.py` 的口径），
      一条报文里 3 个点超量程不该走 3 次异常。
    ⚠️ **缺测点现在是"语义坏"**（§10.1）：与"超量程"一样是单点问题；
      判整条坏就把单点问题又放大成整条。
    """
    parts = payload.split()
    if len(parts) < 3:
        raise PayloadFormatError(f"报文长度不足: {payload!r}")

    try:
        ts = parse_timestamp(f"{parts[0]} {parts[1]}")
    except ValueError as exc:
        raise PayloadFormatError(str(exc)) from exc

    names = set(NAMES)
    values: dict[str, float] = {}
    invalid: dict[str, PointInvalid] = {}
    for item in parts[2:]:
        key, sep, value = item.partition("=")
        if not sep or key not in names:
            continue                    # 跳过 Flag 等非测点字段
        if key in values or key in invalid:
            raise PayloadFormatError(
                f"测点 {key} 在报文里出现两次（同一测点两个值，报文自相矛盾）: {payload!r}"
            )
        try:
            number = float(value)
        except ValueError as exc:
            raise PayloadFormatError(f"测点 {key} 数值非法: {value!r}") from exc
        if not math.isfinite(number):
            # 语法上解析得出来、数学上是个数，但 TDengine 写不进去也不能拿去算 —— 该点无效
            invalid[key] = PointInvalid(
                key, PointInvalidKind.INVALID,
                f"测点 {key} 不是有限数值（NaN/inf）: {value!r}", number,
            )
            continue
        low, high = POINT_RANGES[key]
        if not low <= number <= high:
            invalid[key] = PointInvalid(
                key, PointInvalidKind.INVALID,
                f"测点 {key} 超出量程 [{low}, {high}]: {number}", number,
            )
            continue
        values[key] = number

    # "一个测点都没有" = 语法坏：没有可写的列，报文无意义（§3）
    if not values and not invalid:
        raise PayloadFormatError(f"报文里一个测点都没有（没有可写的列）: {payload!r}")

    # 报文里根本没给的测点 → kind=missing（与"给了但不可信"分开，§10.1）
    for name in NAMES:
        if name not in values and name not in invalid:
            invalid[name] = PointInvalid(
                name, PointInvalidKind.MISSING, "报文未提供", None,
            )

    return ts, values, invalid


#: 算折算值 / 跑告警判据所需的那几个测点（**必须同帧齐全**）。
#: 折算公式要 O2；`AlarmJudge.on_sample()` 还要三个污染物的标干值
#: （它按 `values[NAME]` 直接取键，缺一个就 KeyError）。
REFERENCE_FRAME_POINTS: Final[tuple[str, ...]] = (
    O2_FIELD_NAME,
    *(COLUMN_TO_NAME[column] for column in ZS_TARGETS),
)


def supports_reference(values: Mapping[str, float]) -> bool:
    """该条报文能不能算折算值（= 判据能不能跑）：要求 O2 + 三个污染物**同帧齐全**。

    ⚠️ 逐点化（ADR-0009）之后这是个**必然会出现**的新情况：某个点被判无效/缺失时，
    帧就不完整了。此时**跳过折算与告警判定**（该条仍照常入库），
    而不是猜一个值、也不是把整条报文丢掉 ——
    "折算用的 O2 必须与浓度同帧"是 ADR-0002 §3.1 的既有口径，本次不动判据。
    """
    return all(name in values for name in REFERENCE_FRAME_POINTS)


# ==================== 4. TDengine 写入器 ====================

class TdWriter:
    """TDengine 写入器：负责建库建表、写数据，连接断了自动重建。"""

    def __init__(self) -> None:
        self._conn: Optional[Any] = None
        self._cur: Optional[Any] = None

    @property
    def ready(self) -> bool:
        """当前是否持有可用的数据库游标。"""
        return self._cur is not None

    def connect(self) -> bool:
        """连接 TDengine 并保证库与超级表存在；成功返回 True，失败只记 error 不抛异常。"""
        conn: Optional[Any] = None
        try:
            conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
            cur = conn.cursor()
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS {TD_DB} "
                f"KEEP {TD_KEEP_DAYS} DURATION {TD_DURATION_DAYS}"
            )
            # ★ REST 接口无状态：USE 不生效，每条 SQL 必须显式带库名前缀 cems.xxx
            cur.execute(
                f"CREATE STABLE IF NOT EXISTS {TD_DB}.{TD_STABLE} "
                f"(ts TIMESTAMP, so2 FLOAT, nox FLOAT, flow FLOAT, "
                f"dust FLOAT, o2 FLOAT, temp FLOAT, humidity FLOAT, pressure FLOAT) "
                f"TAGS (plant NCHAR(20), device NCHAR(20))"
            )
            self._ensure_columns(cur)
            self._check_tags(cur)
        except Exception as exc:
            LOGGER.error("TDengine 连接或建表失败: %s", exc)
            self.close()
            if conn is not None:
                self._safe_close(conn)
            return False

        self.close()
        self._conn, self._cur = conn, cur
        LOGGER.info("TDengine 就绪: 库=%s, 超级表=%s", TD_DB, TD_STABLE)
        return True

    @staticmethod
    def _ensure_columns(cur: Any) -> None:
        """老库平滑升级：对比超级表实际列，缺哪列补哪列（幂等，可重复执行）。

        CREATE STABLE IF NOT EXISTS 不会改动已存在的表，所以老版本建的超级表（测点列不全）
        必须靠 ALTER STABLE 把新测点列补上；历史数据不丢，新列的旧值为 NULL。
        """
        cur.execute(f"DESCRIBE {TD_DB}.{TD_STABLE}")
        existing = {str(row[0]).lower() for row in cur.fetchall()}
        for column in TD_COLUMNS:
            if column not in existing:
                cur.execute(f"ALTER STABLE {TD_DB}.{TD_STABLE} ADD COLUMN {column} FLOAT")
                LOGGER.info("超级表新增测点列: %s FLOAT", column)

    @staticmethod
    def _check_tags(cur: Any) -> None:
        """核对子表已有标签是否与配置一致。

        TDengine 对已存在的子表会沿用第一次写入的 TAGS 且不报错，
        所以改标签/换设备时必须主动比对，否则数据会静默挂到旧标签上。
        """
        try:
            cur.execute(
                f"SELECT DISTINCT plant, device FROM {TD_DB}.{TD_STABLE} "
                f"WHERE tbname = '{CHILD_TABLE}'"
            )
            rows = list(cur.fetchall())
        except Exception as exc:
            LOGGER.warning("子表标签一致性检查跳过（%s）", exc)
            return

        if not rows:
            return                      # 子表还没建，首次写入时按当前配置创建
        mismatched = [
            (str(row[0]), str(row[1])) for row in rows
            if str(row[0]) != TD_PLANT or str(row[1]) != TD_DEVICE
        ]
        if mismatched:
            LOGGER.error(
                "子表 %s 的标签与配置不一致：库中 %s，配置为 plant=%s device=%s。"
                "新数据会挂到旧标签上，请改 TD_PLANT/TD_DEVICE 或清理该子表",
                CHILD_TABLE, mismatched, TD_PLANT, TD_DEVICE,
            )
        else:
            LOGGER.info("子表标签校验通过: %s -> (plant=%s, device=%s)", CHILD_TABLE, TD_PLANT, TD_DEVICE)

    def write(self, ts: str, values: dict[str, float]) -> bool:
        """写入一条数据；首次失败自动重连重试一次，仍失败返回 False。"""
        if not self.ready and not self.connect():
            return False

        if self._insert(ts, values):
            return True

        LOGGER.error("入库失败，重连 TDengine 后重试一次")
        if self.connect() and self._insert(ts, values):
            return True
        return False

    def _insert(self, ts: str, values: dict[str, float]) -> bool:
        """执行一次插入：自动建子表（USING 超级表模板 + TAGS 标签）。

        ★ **逐点化（ADR-0009 §10.2 第 1 步）**：**传入哪几列就写哪几列** ——
          只按 `values` 里实际有的测点拼列，缺失的列**整列不出现**，
          由 TDengine 按"省略的列就是 NULL"处理（部分列 INSERT 与 USING/TAGS 共存
          已实测，见 ADR-0009 §6.2）。

        ⚠️ **9 列齐全时，拼出来的 SQL 与逐点化之前逐字节相同**（既有路径零变化）：
          列名仍按 `POINTS` 契约顺序展开（不是 `values` 的键序），
          所以报文顺序/建表顺序漂移都不会写错列。
        ⚠️ 一个可写的列都没有（全列判无效）时返回 False 且**不执行 SQL**：
          这类报文 TDengine 写不进去（没有 ts 列之外的内容），
          调用方（`on_message`）会提前拦住，不让它进落盘队列空转。
        """
        if self._cur is None:
            return False
        points = [point for point in POINTS if point.name in values]
        if not points:
            LOGGER.error("没有可写入的测点列，跳过插入（ts=%s）", ts)
            return False
        # 显式列出列名时，主时间戳列 ts 必须一起列出，否则 TDengine 报
        # "Primary timestamp column should not be null"
        columns = ", ".join(("ts", *(point.column for point in points)))
        numbers = ", ".join(f"{values[point.name]}" for point in points)   # 与 columns 同序
        sql = (
            f"INSERT INTO {TD_DB}.{CHILD_TABLE} USING {TD_DB}.{TD_STABLE} "
            f"TAGS ('{TD_PLANT}', '{TD_DEVICE}') "
            f"({columns}) VALUES ('{ts}', {numbers})"
        )
        try:
            self._cur.execute(sql)
            return True
        except Exception as exc:
            LOGGER.error("TDengine 插入失败: %s | SQL: %s", exc, sql)
            return False

    @staticmethod
    def _safe_close(conn: Any) -> None:
        """安全关闭连接，忽略关闭过程中的异常。"""
        try:
            conn.close()
        except Exception as exc:
            LOGGER.debug("关闭 TDengine 连接时出错（忽略）: %s", exc)

    def close(self) -> None:
        """释放当前连接（游标置空，下次写入会自动重连）。"""
        conn, self._conn, self._cur = self._conn, None, None
        if conn is not None:
            self._safe_close(conn)


# ==================== 4b. 告警表写入器 ====================

class AlarmWriter:
    """告警表读写器：建三张告警超级表，幂等写入事件行 / 推送记录 / 小时结论行。

    为什么要独立一个写入器（而不是复用 TdWriter）：
        - 小时结算跑在**独立线程**里，不能和 MQTT 回调线程抢同一个游标；
        - 告警表的写失败绝不该影响入库，两条链路各自重连、各自重试。

    幂等语义 = **`(子表, ts)` 覆盖**：子表名 = `plant_device_point_judgetype`，
    同一子表写同一个 ts，TDengine 覆盖原行而不是新增（本机 3.3.6.13 实测：
    `COUNT(*)` 不变、值被覆盖）。所以 QoS1 重投同一条报文只会覆盖同一行。
    """

    def __init__(self, tables: AlarmTables) -> None:
        self._tables = tables
        self._conn: Optional[Any] = None
        self._cur: Optional[Any] = None

    @property
    def ready(self) -> bool:
        """当前是否持有可用的数据库游标。"""
        return self._cur is not None

    def connect(self) -> bool:
        """连接并保证三张告警超级表存在；成功返回 True（失败只记 error 不抛）。"""
        conn: Optional[Any] = None
        try:
            conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
            cur = conn.cursor()
            for sql in self._tables.create_sql():
                cur.execute(sql)
        except Exception as exc:
            LOGGER.error("告警表连接或建表失败: %s", exc)
            self.close()
            if conn is not None:
                TdWriter._safe_close(conn)
            return False

        self.close()
        self._conn, self._cur = conn, cur
        LOGGER.info(
            "告警表就绪: %s / %s / %s",
            self._tables.event_stable, self._tables.verdict_stable, self._tables.push_stable,
        )
        return True

    def execute(self, sql: str) -> bool:
        """执行一条写语句；失败自动重连重试一次，仍失败返回 False。"""
        if not self.ready and not self.connect():
            return False
        if self._execute(sql):
            return True
        LOGGER.error("告警写库失败，重连后重试一次")
        if self.connect() and self._execute(sql):
            return True
        return False

    def _execute(self, sql: str) -> bool:
        if self._cur is None:
            return False
        try:
            self._cur.execute(sql)
            return True
        except Exception as exc:
            LOGGER.error("告警写库失败: %s | SQL: %s", exc, sql)
            return False

    def write(self, sqls: Iterable[str]) -> int:
        """逐条写；返回成功条数（一条失败不影响其余，告警不能因为一行写坏就全丢）。"""
        return sum(1 for sql in sqls if self.execute(sql))

    def query(self, sql: str) -> list[list[Any]]:
        """执行只读查询，返回行列表；失败返回空列表（调用方按"查不到"处理）。"""
        if not self.ready and not self.connect():
            return []
        try:
            self._cur.execute(sql)
            return [list(row) for row in self._cur.fetchall()]
        except Exception as exc:
            LOGGER.error("告警查库失败: %s | SQL: %s", exc, sql)
            return []

    def close(self) -> None:
        """释放当前连接（游标置空，下次操作会自动重连）。"""
        conn, self._conn, self._cur = self._conn, None, None
        if conn is not None:
            TdWriter._safe_close(conn)


# ==================== 4d. 无效点事件表（ADR-0009 §4.1） ====================
#
# ★ 这张表是**证据层**，不是数据层：
#     - 不进主表、不给主表加列（主表列布局保持不动，既有报表/导出不受影响）
#     - 不参与达标判定（覆盖率怎么算、小时门限怎么定，本次都不动）
#     - 只回答一个问题：**哪一分钟、哪个点、为什么没进主表**
#
# ⚠️ 为什么 `point` 不写成 TAG（照 §4.1 的字面：TAGS 只有 plant/device），
#    却把点名列进**子表名**（`plant_device_invalid_<point>`）：
#    TDengine 的幂等/覆盖语义是 **(子表, ts)** —— 若所有点共用一张子表，
#    同一条报文里 2 个点同时无效时会**互相覆盖**，只剩一条证据（静默丢证据）。
#    按点分子表既保住了"同一报文重投不重复"（V5），也不丢多点的行。
@dataclass(frozen=True)
class PointInvalidTables:
    """无效点事件表在 TDengine 里的位置与命名，以及 DDL/DML 文本（纯字符串）。"""

    db: str
    plant: str
    device: str
    stable: str = "cems_point_invalid"

    def child(self, point: str) -> str:
        """该测点的子表名（幂等键 = `(子表, ts)`，同一报文重投只覆盖不新增）。"""
        return f"{self.plant}_{self.device}_invalid_{point}".lower()

    def create_sql(self) -> str:
        """建超级表（`IF NOT EXISTS`，可重复执行；照 AlarmTables 的写法）。"""
        return (
            f"CREATE STABLE IF NOT EXISTS {self.db}.{self.stable} "
            f"(ts TIMESTAMP, point NCHAR(16), kind NCHAR(10), "
            f"reason NCHAR(200), raw FLOAT) "
            f"TAGS (plant NCHAR(20), device NCHAR(20))"
        )

    def insert_sql(self, ts: str, item: PointInvalid) -> str:
        """一条无效点证据行。

        - `kind`：`invalid`（有值但不可信）/ `missing`（报文里根本没这个点）
        - `raw`：原始值（**保留证据**）；`missing` 与非有限值（NaN/inf）写 `NULL`
          —— `sql_float()` 与非有限值必须写 NULL 的口径同源（NaN 进 SQL 会让 TDengine
          报 syntax error，原始文本已经写在 `reason` 里，证据不丢）
        """
        columns = "ts, point, kind, reason, raw"
        values = ", ".join((
            sql_timestamp(ts),
            sql_text(item.point),
            sql_text(PointInvalidKind(item.kind).value),
            sql_text(str(item.reason)[:200]),
            sql_float(item.raw),
        ))
        return (
            f"INSERT INTO {self.db}.{self.child(item.point)} USING {self.db}.{self.stable} "
            f"TAGS ({sql_text(self.plant)}, {sql_text(self.device)}) "
            f"({columns}) VALUES ({values})"
        )


#: 本实例的无效点事件表（表名由 POINT_INVALID_STABLE 覆盖，默认 cems_point_invalid）
POINT_INVALID_TABLES: Final[PointInvalidTables] = PointInvalidTables(
    db=TD_DB, plant=TD_PLANT, device=TD_DEVICE, stable=POINT_INVALID_STABLE,
)


class PointInvalidWriter:
    """无效点事件表写入器：建表 + 逐条写证据行。

    为什么要独立一个写入器（而不是复用 TdWriter / AlarmWriter）：
        - **失败域要分清**：它与主表写入是两件事。主表写入失败要落盘队列补传，
          证据行写失败只是**少一条证据**，不能反过来把主链路的落盘逻辑搅乱；
        - `AlarmWriter.connect()` 会连带建三张**告警**表，而告警开关（ALARM_ENABLE=0）
          与本表无关，不该被它绑定；
        - 连接、重连、重试的口径与 `AlarmWriter` 完全一致（照抄既有写法，不另造一套）。
    """

    def __init__(self, tables: PointInvalidTables) -> None:
        self._tables = tables
        self._conn: Optional[Any] = None
        self._cur: Optional[Any] = None

    @property
    def ready(self) -> bool:
        """当前是否持有可用的数据库游标。"""
        return self._cur is not None

    def connect(self) -> bool:
        """连接并保证无效点事件超级表存在；成功返回 True（失败只记 error 不抛）。"""
        conn: Optional[Any] = None
        try:
            conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
            cur = conn.cursor()
            cur.execute(self._tables.create_sql())
        except Exception as exc:
            LOGGER.error("无效点事件表连接或建表失败: %s", exc)
            self.close()
            if conn is not None:
                TdWriter._safe_close(conn)
            return False

        self.close()
        self._conn, self._cur = conn, cur
        LOGGER.info("无效点事件表就绪: %s.%s", self._tables.db, self._tables.stable)
        return True

    def execute(self, sql: str) -> bool:
        """执行一条写语句；失败自动重连重试一次，仍失败返回 False。"""
        if not self.ready and not self.connect():
            return False
        if self._execute(sql):
            return True
        LOGGER.error("无效点事件写库失败，重连后重试一次")
        if self.connect() and self._execute(sql):
            return True
        return False

    def _execute(self, sql: str) -> bool:
        if self._cur is None:
            return False
        try:
            self._cur.execute(sql)
            return True
        except Exception as exc:
            LOGGER.error("无效点事件写库失败: %s | SQL: %s", exc, sql)
            return False

    def write(self, ts: str, items: Sequence[PointInvalid]) -> int:
        """逐条写本次的无效点证据行；返回成功条数（一条失败不影响其余）。"""
        return sum(1 for item in items if self.execute(self._tables.insert_sql(ts, item)))

    def close(self) -> None:
        """释放当前连接（游标置空，下次操作会自动重连）。"""
        conn, self._conn, self._cur = self._conn, None, None
        if conn is not None:
            TdWriter._safe_close(conn)


def record_invalid_points(
    writer: PointInvalidWriter,
    ts: str,
    invalid: Mapping[str, PointInvalid],
) -> int:
    """逐点留痕：把本条报文的无效测点写进事件表，返回成功条数。

    ⚠️ **绝不抛异常、也绝不阻断入库**：证据写失败只记 CRITICAL ——
      反过来把 8 个本来正确的点丢掉，是这个改造最不该发生的事
      （而且事件表与主表在同一个 TDengine 实例上，它单独坏掉的概率极低）。
    ⚠️ 调用点**在写主表之前**（见 `on_message`）：先留痕再入库，
      这样"主表有 NULL 列"与"事件表有证据"不会因为顺序而错开。
    """
    items = list(invalid.values())
    if not items:
        return 0
    for item in items:
        LOGGER.warning(
            "[无效点] %s %s kind=%s raw=%s: %s",
            ts, item.point, PointInvalidKind(item.kind).value,
            "NULL" if item.raw is None else repr(item.raw), item.reason,
        )
    written = writer.write(ts, items)
    if written != len(items):
        LOGGER.critical(
            "[无效点] 事件表写入不完整: %d/%d 条 —— 主表对应列会是 NULL 且没有留痕"
            "（分不清「没采到」还是「被判无效」），请查事件表连接: ts=%s points=%s",
            written, len(items), ts, [item.point for item in items],
        )
    return written


def _post_webhook(url: str, payload_json: str, timeout: float) -> bool:
    """把一条告警 POST 到 Webhook；返回是否送达（2xx）。

    ⚠️ **绝不抛异常**：推送失败只记 warning + 把 status 记成 `failed`，
    事件行与推送记录**照常落库**。理由与网关的补传判定同源 ——
    "通知"是附加动作，**不能让它的失败影响数据主链路**。
    """
    import urllib.error
    import urllib.request

    request = urllib.request.Request(
        url, data=payload_json.encode("utf-8"), method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = int(getattr(response, "status", 0) or 0)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        LOGGER.warning("[告警推送] Webhook 失败（不影响告警落库）: %s: %s",
                       type(exc).__name__, exc)
        return False
    if 200 <= status < 300:
        return True
    LOGGER.warning("[告警推送] Webhook 返回非 2xx: %s", status)
    return False


def publish_alarm_events(
    writer: AlarmWriter,
    tables: AlarmTables,
    events: Sequence[AlarmEvent],
    channel: str,
) -> tuple[int, int]:
    """把告警事件落成**可验证的动作**：事件行 + 推送记录 + 结构化日志（+ 可选真实 Webhook）。

    推送被实现成三件可查的事：
        1. `cems_alarm_push` 里一条推送记录（`channel`/`status`/`payload`，与事件行同 ts，
           因此同样满足 `(子表, ts)` 幂等）；
        2. 一行 `ALARM_PUSH ...` 结构化日志（可 grep、可进日志采集）；
        3. ⭐ `channel == "webhook"` 时：真的 POST 到 `ALARM_PUSH_URL`，
           成功记 `status=sent`、失败记 `status=failed`（**都不影响事件落库**）。

    ⚠️ 边界：默认 channel 是 `log`（只落记录，无外部通道）。真实 Webhook 需对方提供
    接收地址（`ALARM_PUSH_URL`）；短信 / 环保平台仍未接入（ADR-0002 §6 未决 5）。

    返回 (成功的事件行数, 成功的推送记录数)。
    """
    if not events:
        return 0, 0

    # ---- 1) 先定推送状态：webhook 失败只降级 status，不阻断落库 ----
    statuses: list[str] = [PUSH_STATUS_RECORDED] * len(events)
    if channel == "webhook":
        url = os.getenv("ALARM_PUSH_URL", "").strip()
        if not url:
            LOGGER.warning(
                "[告警推送] ALARM_PUSH_CHANNEL=webhook 但未配置 ALARM_PUSH_URL，"
                "降级为只落记录（status=recorded）"
            )
        else:
            timeout = float(os.getenv("ALARM_PUSH_TIMEOUT", "3.0"))
            for index, event in enumerate(events):
                ok = _post_webhook(url, event.payload_json(), timeout)
                statuses[index] = PUSH_STATUS_SENT if ok else PUSH_STATUS_FAILED

    event_sqls = [tables.event_insert_sql(event) for event in events]
    push_sqls = [
        tables.push_insert_sql(event, channel, status)
        for event, status in zip(events, statuses)
    ]
    for event, status in zip(events, statuses):
        LOGGER.info(event.log_text(channel, status))
    written_events = writer.write(event_sqls)
    written_push = writer.write(push_sqls)
    if written_events != len(event_sqls) or written_push != len(push_sqls):
        LOGGER.error(
            "告警落库不完整: 事件行 %d/%d，推送记录 %d/%d",
            written_events, len(event_sqls), written_push, len(push_sqls),
        )
    return written_events, written_push


def restore_judge_state(judge: AlarmJudge, writer: AlarmWriter, tables: AlarmTables) -> int:
    """从事件表折叠出判据状态（§3.3「重启恢复」）。

    恢复不成功**不阻止启动**：只是该测点的 OPEN 事件会从"当前"重新开始计（可能重复一次
    START，但 `(子表, ts)` 覆盖语义保证不会多出事件行）。所以失败只记 warning。
    """
    rows = writer.query(tables.restore_select_sql(ALARM_RESTORE_LIMIT))
    if not rows:
        return 0
    folded = judge.restore(rows)
    LOGGER.info(
        "判据状态已从事件表恢复: 折叠 %d 行；仍 OPEN 的测点 = %s（不会重发 START）",
        folded, judge.open_points() or "无",
    )
    return folded


# ==================== 4c. 小时结算（§3.4 覆盖率三态结论） ====================

def settle_hour(
    writer: AlarmWriter,
    tables: AlarmTables,
    judge: AlarmJudge,
    hour_start: datetime,
) -> tuple[int, int]:
    """结算一个整点小时，返回 (写入的结论行数, 写入的事件行数)。

    ⚠️ 折算值不落库，所以这里**回读原始行、用契约函数重算**（`points.to_reference_o2()`，
    仍然是唯一一份公式）；覆盖率分母 = 3600 / ALARM_POLL_INTERVAL = 720 条/小时。
    ⚠️ 覆盖率不足判 `insufficient`，**绝不判达标**（§3.4：最危险的漏报形态是"缺数据被当达标"）。
    """
    start_text = hour_start.strftime(TS_FORMAT)
    end_text = (hour_start + timedelta(hours=1)).strftime(TS_FORMAT)
    rows = writer.query(tables.hour_rows_sql(start_text, end_text))

    samples: list[tuple[str, float, dict[str, float]]] = []
    for row in rows:
        # 列序由 AlarmTables.hour_rows_sql 固定：ts, o2, dust, so2, nox
        o2 = row[1]
        values = {column: row[2 + index] for index, column in enumerate(ZS_TARGETS)}
        # 缺列（老数据/老库补列留下的 NULL）按"折算不出来"处理：不该混进有效样本
        if o2 is None or any(value is None for value in values.values()):
            LOGGER.warning("小时结算遇到缺列的行，按无效样本计入: %s", row[0])
            samples.append((str(row[0]), float("nan"), {k: float("nan") for k in ZS_TARGETS}))
            continue
        samples.append((
            str(row[0]), float(o2), {key: float(value) for key, value in values.items()},
        ))

    verdicts, events = judge.judge_hour(start_text, samples)
    written_verdicts = writer.write(
        [tables.verdict_insert_sql(verdict) for verdict in verdicts]
    )
    written_events = 0
    if events:
        written_events, _push = publish_alarm_events(
            writer, tables, events, ALARM_CONFIG.push_channel,
        )
    for verdict in verdicts:
        LOGGER.info(
            "小时结论 %s %s: verdict=%s n_total=%d n_valid=%d n_invalid=%d "
            "coverage=%.4f conv_mean=%s limit=%.1f judge_version=%s",
            verdict.ts, verdict.point, verdict.verdict, verdict.n_total, verdict.n_valid,
            verdict.n_invalid, verdict.coverage,
            "nan" if not math.isfinite(verdict.conv_mean) else f"{verdict.conv_mean:.4f}",
            verdict.limit_value, verdict.judge_version,
        )
    return written_verdicts, written_events


def _settle_hours(
    writer: AlarmWriter,
    tables: AlarmTables,
    judge: AlarmJudge,
    hours: Sequence[datetime],
) -> None:
    """结算若干个小时，单个小时失败不影响其余（异常只记 error）。"""
    for hour_start in hours:
        try:
            n_verdicts, n_events = settle_hour(writer, tables, judge, hour_start)
            LOGGER.info(
                "小时结算完成 %s: 结论 %d 行，事件 %d 行",
                hour_start.strftime(TS_FORMAT), n_verdicts, n_events,
            )
        except Exception:
            LOGGER.exception("小时结算异常 %s（继续下一个）", hour_start.strftime(TS_FORMAT))


def hourly_settlement_loop(
    writer: AlarmWriter,
    tables: AlarmTables,
    judge: AlarmJudge,
    backfill_hours: int,
    settle_delay_seconds: float,
) -> None:
    """小时结算线程主体：启动补结算 + 之后每个整点后结算上一个小时。

    - 启动时补结算最近 `backfill_hours` 个**已闭合**小时（进程重启/首次上线不用等整点）
    - 之后每次醒来的时刻 = 下一个整点 + `settle_delay_seconds`（等补传收尾）
    - 重复结算同一个小时是安全的：结论表与事件行都按 `(子表, ts)` 覆盖
    - 本线程是 daemon：主进程退出即结束；任何异常都吞掉并记录，绝不拖垮订阅
    """
    now = datetime.now()
    current_hour = hour_start_of(now)
    if backfill_hours > 0:
        closed_hours = [
            current_hour - timedelta(hours=offset)
            for offset in range(backfill_hours, 0, -1)
        ]
        LOGGER.info("启动补结算 %d 个已闭合小时: %s", len(closed_hours), [
            hour.strftime(TS_FORMAT) for hour in closed_hours
        ])
        _settle_hours(writer, tables, judge, closed_hours)

    while True:
        now = datetime.now()
        next_hour = hour_start_of(now) + timedelta(hours=1)
        sleep_seconds = (next_hour - now).total_seconds() + settle_delay_seconds
        LOGGER.info(
            "下次小时结算: %s（%s 起算，等待 %.0fs = 到整点 + %.0fs 补传余量）",
            (next_hour + timedelta(seconds=settle_delay_seconds)).strftime(TS_FORMAT),
            next_hour.strftime(TS_FORMAT), sleep_seconds, settle_delay_seconds,
        )
        time.sleep(max(1.0, sleep_seconds))
        target = hour_start_of(datetime.now()) - timedelta(hours=1)
        _settle_hours(writer, tables, judge, [target])


# ==================== 5. 折算值（标干 → 基准氧含量） ====================
#
# 折算值分**两层**，职责不同，别混：
#   ① 数学层 reference_values()：调 points.to_reference_o2() 算折算浓度。
#      O2 >= 21% 时分母 <= 0，折算无物理意义 → 返回 nan（数学语义，也是判定口径）。
#   ② 协议层 transmit_reference_values()：调 points.to_transmit_value()，把 nan 编码成
#      HJ 212-2025 §8.1.1 d) 要求的哨兵值（+9999.99），有限折算值原样透出。
#   record_reference() 记录的是②（往外报的那一份）；抽样日志也打②。
#   ⚠️ 判定/统计必须用①：哨兵值 9999.99 大于任何限值，拿它比限值必然误报。
#
# 全仓库**唯一**调用 points.to_reference_o2() 的地方就是 reference_values()：
#   - 只有它算折算值，别处要折算就调它 / 取最近值快照，不许再推一遍公式
#   - 折算值不写库、不进寄存器（导出量，存了会漂）
#   - O2 >= 21% 时 to_reference_o2() 返回 nan（分母 <= 0，折算无物理意义），
#     这里原样透出，不绕过、不当 0；要出站再走②编码

# 最近一条**已成功入库**数据的传输值快照（含时间戳）；读接口返回副本
LAST_REFERENCE: Final[dict[str, Any]] = {"ts": "", "values": {}}


def reference_values(values: dict[str, float]) -> dict[str, float]:
    """纯函数：把一条报文的标干浓度折算到基准氧含量下（不碰库、不改状态）。

    入参 values 的键 = MQTT 报文字段名（Dust / SO2 / NOx / O2，即 points.NAMES），
    返回值的键 = TDengine 列名（dust / so2 / nox，即 points.ZS_TARGETS），
    这样既能直接打日志给下游看，也能与库里存的标干值按列名对齐比较。

    公式（出自契约 points.to_reference_o2，本函数只做调用，不重写）：
        折算浓度 = 标干浓度 × (21 - 基准氧含量) / (21 - 实测氧含量)

    ⚠️ 返回 nan 的情形：实测 O2 >= 21%（契约行为，别绕过、别当 0）。
       要**往外传输**时必须经 transmit_reference_values() 编码，不能直接把 nan 发出去。
    ⚠️ 判定口径：**超标判的是折算值，不是标干值**（本函数不做告警判定）。
    """
    o2 = values[O2_FIELD_NAME]
    return {
        column: to_reference_o2(values[COLUMN_TO_NAME[column]], o2)
        for column in ZS_TARGETS
    }


def transmit_reference_values(values: dict[str, float]) -> dict[str, float]:
    """纯函数：把一条报文的折算值编码成**可传输值**（协议层，不碰库、不改状态）。

    这是本工程**唯一**的折算值协议编码点：
      - 数学层算不出来的（nan/±inf）→ 哨兵值 +9999.99（HJ 212-2025 §8.1.1 d)：
        无法计算折算浓度时按缺省数据类型的最大值传输，不是 0、不是实测值、不是报无效）
      - 算得出来的有限折算值 → 原样透出（不缩放、不舍入）

    编码规则的真源在契约 points.to_transmit_value() / ZS_UNAVAILABLE_SENTINEL，
    本函数只逐列调用，不自己写 if/else —— 免得协议细节在接入层再抄一份。

    ⚠️ 出参键与 reference_values() 相同（TDengine 列名）；**只用于出站/日志/展示**，
    不许拿去做达标判定或统计（哨兵值一定大于限值）。
    """
    return {
        column: to_transmit_value(value)
        for column, value in reference_values(values).items()
    }


def record_reference(
    ts: str,
    values: dict[str, float],
    math_references: Optional[dict[str, float]] = None,
) -> dict[str, float]:
    """算一次折算值、编码成传输值，并记成"最近一条"快照；返回本次结果。

    只在写入 TDengine **成功之后**调用：进不了库的数据不配当"最近一条"。
    每条报文只调 reference_values() 一次（折算值全链路只在一处算），
    快照存的是**传输编码后**的值（下游/验证脚本看到的应与上报出去的一致）。

    `math_references`：调用方已经算好的**数学层**折算值（`reference_values()` 的结果）。
    告警判定要的正是这一份（`O2 >= 21%` 时是 nan），接入层算一次后同时喂给
    "协议编码"和"判据"两处 —— 既有"公式只算一次"，也避免判定拿到哨兵值。
    """
    references = reference_values(values) if math_references is None else math_references
    snapshot = {
        column: to_transmit_value(value) for column, value in references.items()
    }
    LAST_REFERENCE["values"] = snapshot
    LAST_REFERENCE["ts"] = ts
    return snapshot


def latest_reference_values() -> dict[str, float]:
    """取最近一条已入库数据的**传输值**（副本，键为 TDengine 列名 dust/so2/nox）。

    与上报出去的那一份一致：算不出来时是哨兵值 +9999.99，不是 nan。
    验证脚本或下游可以直接：
        from src.platform.subscriber_to_td import latest_reference_values
    返回副本，避免调用方改到进程内的快照。
    ⚠️ 别拿它做达标判定/统计（哨兵值大于限值），判定请按条调用 reference_values()。
    """
    return dict(LAST_REFERENCE["values"])


def latest_reference_timestamp() -> str:
    """取最近一条已入库折算值对应的时间戳（没有则为空字符串）。"""
    return str(LAST_REFERENCE["ts"])


# ==================== 6. MQTT 回调 ====================

def on_connect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    """连接/重连成功 → 订阅数据主题。

    持久会话下 broker 会先补投离线期间排队的消息，再走实时消息，
    所以这里不需要额外做什么；重新 SUBSCRIBE 是幂等的，顺便兜住
    "会话已过期、broker 上已经没有订阅关系"的情况。
    """
    if reason_code != 0:
        LOGGER.error("MQTT 连接失败 reason_code=%s", reason_code)
        return

    session_present = getattr(flags, "session_present", None)
    LOGGER.info("MQTT 已连接: %s:%d（会话恢复=%s）", BROKER, PORT, session_present)

    state: dict[str, Any] = userdata if isinstance(userdata, dict) else {}
    is_reconnect = bool(state.get("connected_before"))
    state["connected_before"] = True

    if MQTT_CLEAN_SESSION:
        LOGGER.warning("当前是干净会话，订阅端离线期间的消息不会被 broker 保留")
    elif is_reconnect and session_present is False:
        # 重连时 broker 说没有旧会话：会话已过期或被清理，这段时间的报文拿不回来了
        LOGGER.warning("broker 未恢复旧会话，离线超过会话有效期期间的报文已丢失")

    try:
        client.subscribe(TOPIC, qos=MQTT_QOS)
        LOGGER.info("已订阅主题: %s (QoS=%d)", TOPIC, MQTT_QOS)
    except Exception as exc:
        LOGGER.error("订阅主题失败: %s", exc)


def on_disconnect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    """连接断开 → 记录日志（loop_forever 会自动重连）。"""
    if reason_code != 0:
        LOGGER.error("MQTT 连接断开 reason_code=%s，等待自动重连", reason_code)


def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    """收到消息 → 解析 → 逐点留痕 → 入库；任何失败只记日志，绝不抛异常打断订阅。

    ★ 逐点化（ADR-0009）后的四条路径：
      1. **语法坏** → `PayloadFormatError` → `STATS["rejected_messages"] += 1`，整条拒收（不写库）；
      2. **语义坏** → 该点进事件表（`kind=invalid/missing`）+ `STATS["invalid_points"] += n`，
         其余点**照常入库**（主表该列是 NULL，证据在事件表里）；
      3. 全部测点都语义坏 → 没有可写的列：只留痕、不写主表、**也不落盘**
         （落盘会永远补不进去，只会把队列堵死）；
      4. 入库失败（TDengine 不可用）→ 与以前一样落本地队列，等补传。
    """
    payload = msg.payload.decode("utf-8", errors="replace")
    STATS["received"] += 1

    try:
        ts, values, invalid = parse_payload(payload)
    except PayloadFormatError as exc:
        # 语法坏（格式错/自相矛盾）：整条拒收，计数并记下原因，方便看上游数据质量
        STATS["rejected_messages"] += 1
        LOGGER.warning(
            "报文已整条拒收（语法坏，累计 %d 条）: %s | 原始报文: %s",
            STATS["rejected_messages"], exc, payload,
        )
        return

    if invalid:
        # ⚠️ 顺序：**先把证据写下来，再写主表** —— 免得"主表已经有 NULL 列"而事件表还没落。
        STATS["invalid_points"] += len(invalid)
        try:
            record_invalid_points(userdata["point_invalid_writer"], ts, invalid)
        # 证据层失败绝不打断数据主链路（record_invalid_points 内部已记 CRITICAL）
        except Exception:
            LOGGER.exception("[无效点] 留痕异常（入库与订阅不受影响）")

    if not values:
        # 9 个点全判坏/全缺：没有可写的列（ADR-0009 §3 的"没有可写的列"）。
        # ⚠️ 这里**不能**走落盘队列：补传时同样一列都写不进去，只会永远堵在队首。
        LOGGER.warning(
            "本条报文的测点全部无效（无可写列），只留痕、不写主表也不落盘: %s",
            sorted(invalid),
        )
        return

    STATS["accepted"] += 1
    writer: TdWriter = userdata["writer"]
    spool: IngestSpool = userdata["spool"]
    if writer.write(ts, values):
        # ★ 折算值 / 告警判定要 O2 与三个污染物**同帧齐全**（ADR-0002 §3.1）。
        #   逐点化后可能出现"整帧不齐"：这时**跳过折算与判定**，但该条仍然已经入库。
        if not supports_reference(values):
            LOGGER.warning(
                "本条报文缺折算/判定所需的测点（O2 或 dust/so2/nox 之一被判无效或缺失），"
                "折算值与告警判定本次跳过（该条已按有效点入库）: 缺 %s",
                sorted(name for name in REFERENCE_FRAME_POINTS if name not in values),
            )
        else:
            # 折算值全链路只在这里算一次：数学层（nan 语义）留给判定，协议层（哨兵值）留给出站
            math_references = reference_values(values)
            references = record_reference(ts, values, math_references)
            if STATS["accepted"] == 1 or STATS["accepted"] % REFERENCE_LOG_EVERY == 0:
                # 抽样打 INFO 而不是每条都打（首条也打：重启后立刻能看到折算出口是活的）
                # 打的是**传输值**：O2 >= 21% 时应当看到哨兵值，而不是 nan
                LOGGER.info(
                    "折算值抽样（第 %d 条，已按 HJ 212-2025 §8.1.1 d) 的规则处理："
                    "无法折算时传缺省类型最大值哨兵 %.2f；"
                    "⚠️ 注意这是【规则对齐】，本项目出站报文体仍是内部简化格式，不是 HJ212 报文）: "
                    "标干 ts=%s O2=%.4f -> %s",
                    STATS["accepted"],
                    ZS_UNAVAILABLE_SENTINEL,
                    ts,
                    values[O2_FIELD_NAME],
                    " ".join(f"{column}={references[column]:.4f}" for column in ZS_TARGETS),
                )
            # ★ 排放超标告警：逐条判**折算值**（吃上面算好的数学层结果，不同帧不判、不重算公式）
            #   O2 >= 21% 时数学层是 nan → 判"数据无效"，绝不拿哨兵值去比限值
            if ALARM_CONFIG.enable:
                try:
                    events = userdata["judge"].on_sample(ts, values, math_references)
                    if events:
                        publish_alarm_events(
                            userdata["alarm_writer"], ALARM_TABLES, events,
                            ALARM_CONFIG.push_channel,
                        )
                except Exception:
                    # 告警链路出问题绝不影响入库与订阅（判定只是入库后的附加动作）
                    LOGGER.exception("告警判定/落库异常（入库与订阅不受影响）")
                if STATS["accepted"] % ALARM_STATS_LOG_SAMPLES == 0:
                    LOGGER.info("告警判据统计: %s", userdata["judge"].stats)
        # 高频成功降到 debug；只在排查数据问题时才需要开
        LOGGER.debug("已入库: %s %s", ts, " ".join(f"{k}={v}" for k, v in values.items()))
    else:
        # ★ 入库失败：先把**原始报文**落到本地队列，再正常返回。
        #   ⚠️ 顺序不能反：必须"落盘成功"之后才允许返回 —— 返回即代表 paho 会发 PUBACK、
        #      broker 会删掉唯一副本；落盘失败还返回，就等于让这条数据凭空消失。
        #   落盘成功 → 这条有持久副本，补传线程会重试入库（顺序/幂等/崩溃安全见 ingest_spool）。
        if spool.append(payload):
            STATS["spooled"] += 1
            LOGGER.warning(
                "[落盘队列] 入库失败，报文已落本地待补传（累计 %d 条）: %s",
                STATS["spooled"], payload,
            )
        else:
            # 落盘也失败（磁盘满/无权限）：这条真的丢了。append() 内部已打过 CRITICAL。
            STATS["spool_lost"] += 1


# ==================== 6b. 落盘队列补传 ====================

def drain_spool_once(
    writer: TdWriter,
    spool: IngestSpool,
    *,
    batch_limit: int = SPOOL_BATCH_LIMIT,
    point_invalid_writer: Optional[PointInvalidWriter] = None,
) -> int:
    """把落盘队列里的一批报文补传进 TDengine，返回本轮成功条数。

    语义（顺序与崩溃安全见 ingest_spool 头部）：
      - 取批（原子改名）→ 逐条 parse + 留痕 + `writer.write` → 收尾（未成功的放回队首、删段）
      - **第一条写失败就停**：说明 TDengine 整体不可用，没必要把整批都试一遍（每次失败都带重连）；
        本条及之后（含未处理的尾部）原样放回，下一个节拍再试。
      - 解析不了的行走 CRITICAL 后**丢弃**：入队前已校验过，落盘内容本不该解析失败；真遇到
        说明队列文件被外部改坏，留着它只会永远卡住整个队列。
      - ⚠️ 补传**不喂告警判据**（`AlarmJudge.on_sample` 有时间水位保护，迟到旧样本会被当重复丢掉）；
        小时结算是从库里回读的，补传进去的行照样参与小时三态结论。
      - ⚠️ 补传**不更新 `LAST_REFERENCE` 快照**：那是"最近一条已入库数据的折算出口"，
        让迟到的旧样本把它往回拨没有意义。
      - ★ **补传也要补留痕**（ADR-0009 §4.1）：报文入队时若含无效点，那条证据同样得落库，
        否则"补传成功"的数据会出现 NULL 列而没有证据行。留痕写失败**也算本条没补成功**
        （放回队首，下一个节拍重试），避免留下"NULL 无证据"的行。
      - `point_invalid_writer=None`（单测里只关心主表路径时）跳过留痕。
    """
    if not spool.has_backlog():
        return 0
    batch = spool.take_batch()
    if not batch:
        return 0

    head = batch[:batch_limit]
    tail = batch[batch_limit:]
    confirmed = 0
    remaining: list[str] = []
    for index, line in enumerate(head):
        try:
            ts, values, invalid = parse_payload(line)
        except PayloadFormatError as exc:
            LOGGER.critical(
                "[落盘队列] 补传报文无法解析，丢弃这一条（不影响其余）: %s | %s", exc, line,
            )
            continue
        if invalid and point_invalid_writer is not None:
            written = record_invalid_points(point_invalid_writer, ts, invalid)
            if written != len(invalid):
                # 证据没写全 → 本条不算补成功，放回队首下个节拍重试（别留下"NULL 无证据"的行）
                remaining = head[index:] + tail
                break
        if not values:
            # 全列无效：没有可写的列（与 on_message 同一处置，别放回队列空转）
            LOGGER.warning("[落盘队列] 本条测点全部无效，无可写列，丢弃: %s", line)
            continue
        if writer.write(ts, values):
            confirmed += 1
        else:
            remaining = head[index:] + tail
            break
    spool.finish(remaining)
    if confirmed:
        STATS["drained"] += confirmed
    if confirmed or remaining:
        LOGGER.info(
            "[落盘队列] 补传：成功 %d 条，放回 %d 条；剩余积压 %s",
            confirmed, len(remaining), spool.backlog_stats(),
        )
    return confirmed


def spool_retry_loop(
    writer: TdWriter,
    spool: IngestSpool,
    point_invalid_writer: Optional[PointInvalidWriter] = None,
) -> None:
    """补传线程主体：启动先补一轮（清上次残留），之后每个节拍补一轮。

    任何异常都只记日志、绝不退出线程 —— 它守护的是"不丢数据"，比它自己活着更重要。
    用 `Event.wait` 而不是 `sleep`：main 退出时置位就能立刻结束，不必等满一个节拍。
    """
    while not SPOOL_STOP.is_set():
        try:
            drain_spool_once(writer, spool, point_invalid_writer=point_invalid_writer)
        # 线程边界：任何异常都不能让补传线程静默死掉（下一轮继续）
        except Exception:
            LOGGER.exception("[落盘队列] 补传线程异常（下一轮继续）")
        SPOOL_STOP.wait(SPOOL_RETRY_INTERVAL)


# ==================== 7. 主流程 ====================

def main() -> None:
    """连 TDengine → 连 MQTT → 订阅主题并持续入库。"""
    setup_logging()

    # 0. 先校验配置：库名/表名/标签会被拼进 SQL，不合法就别带病运行
    try:
        validate_config()
    except ValueError as exc:
        LOGGER.critical("配置非法，拒绝启动: %s", exc)
        return

    # 0b. 主题 ↔ 标签 一致性自检（默认拒绝启动，见 enforce_topic_scope）
    #     + 把"本实例负责哪台设备、最终写哪张子表"打进日志：
    #     多实例部署时，这行日志是判断"哪个容器管哪台设备"的第一现场。
    enforce_topic_scope(TOPIC, TD_PLANT, TD_DEVICE)
    LOGGER.info(
        "本实例负责: 主题 %s → 最终子表名 = %s（plant=%s, device=%s）",
        TOPIC, CHILD_TABLE, TD_PLANT, TD_DEVICE,
    )

    # 1. 初始化 TDengine（失败不退出：收到数据时会自动重连重试）
    writer = TdWriter()
    if not writer.connect():
        LOGGER.error("首次连接 TDengine 失败，将在收到数据时自动重试")

    # 1a. 无效点事件表（ADR-0009 §4.1：逐点化之后的"证据层"）
    #     失败**不阻止启动**：第一次要写证据时会自动重连重试；
    #     它坏了只影响"留痕"，绝不该把主链路（入库）一起拖住。
    point_invalid_writer = PointInvalidWriter(POINT_INVALID_TABLES)
    if not point_invalid_writer.connect():
        LOGGER.error("首次连接无效点事件表失败，将在需要写入时自动重试")

    # 1b. 落盘队列：目录可写检查 + 补传线程
    #     ⚠️ 目录必须**每实例一份**（多设备部署的关键约束，见配置区）。这一行是排查
    #     "两个实例串味"的第一现场：它必须每实例互不相同。
    if not SPOOL.check_writable():
        LOGGER.critical(
            "落盘队列目录不可写：写库失败时将没有副本可落（会丢数据）。目录: %s", SPOOL_DIR,
        )
    LOGGER.info(
        "落盘队列已启用: 目录=%s（SUBSCRIBER_DATA_DIR=%s）；"
        "写库失败先落盘、每 %.0fs 补传一轮，入库成功才删",
        SPOOL_DIR,
        SUBSCRIBER_DATA_DIR_ENV or "未设置，用默认 PROJECT_ROOT/data/subscriber",
        SPOOL_RETRY_INTERVAL,
    )
    threading.Thread(
        target=spool_retry_loop,
        args=(writer, SPOOL, point_invalid_writer),
        name="ingest-spool-retry",
        daemon=True,
    ).start()

    # 1c. 初始化告警链路：判据状态从事件表折叠恢复 + 独立的告警表写入器
    alarm_writer = AlarmWriter(ALARM_TABLES)
    if not alarm_writer.connect():
        LOGGER.error("首次连接告警表失败，将在需要写入时自动重试")
    judge = AlarmJudge(ALARM_CONFIG)
    if ALARM_CONFIG.enable:
        if alarm_writer.ready:
            restore_judge_state(judge, alarm_writer, ALARM_TABLES)
        else:
            LOGGER.warning(
                "告警表不可用，判据状态未能恢复：该测点若原本 OPEN，重启后会重新计一次 START"
                "（(子表, ts) 覆盖语义保证不会多出事件行）"
            )
        LOGGER.info(
            "告警判据已启用: 判折算值；滑窗 %d 条里 >= %d 条越限触发；"
            "连续 %d 条 <= 限值×%.2f 结束；小时覆盖率门限 %.2f（低于则判数据不足）",
            ALARM_CONFIG.window_samples, ALARM_CONFIG.min_over_samples,
            ALARM_CONFIG.recover_samples, ALARM_CONFIG.recover_ratio,
            ALARM_CONFIG.coverage_min,
        )
    else:
        LOGGER.warning("ALARM_ENABLE=0：本次不判超标（只入库、只算折算值）")

    # 1c. 小时结算线程（独立连接，daemon，异常不影响订阅）
    if ALARM_CONFIG.hourly_enable and ALARM_CONFIG.enable:
        settlement_writer = AlarmWriter(ALARM_TABLES)
        threading.Thread(
            target=hourly_settlement_loop,
            args=(
                settlement_writer, ALARM_TABLES, judge,
                ALARM_CONFIG.hourly_backfill_hours, ALARM_SETTLE_DELAY_SECONDS,
            ),
            name="alarm-hourly-settlement",
            daemon=True,
        ).start()
        LOGGER.info(
            "小时结算已启用: 每小时整点后 %.0fs 结算上一小时，启动补结算 %d 个小时",
            ALARM_SETTLE_DELAY_SECONDS, ALARM_CONFIG.hourly_backfill_hours,
        )
    elif not ALARM_CONFIG.hourly_enable:
        LOGGER.info("ALARM_HOURLY_ENABLE=0：本次不跑小时结算")

    # 2. 连 MQTT（用 userdata 把写入器传给回调）
    # clean_session=False → 持久会话，broker 为离线期间的消息排队（配合固定 client_id）
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        clean_session=MQTT_CLEAN_SESSION,
    )
    client.user_data_set({
        "writer": writer, "judge": judge, "alarm_writer": alarm_writer, "spool": SPOOL,
        # ★ 逐点化（ADR-0009）：无效测点的证据写入器，按 (子表, ts) 幂等
        "point_invalid_writer": point_invalid_writer,
    })
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    try:
        client.connect_async(BROKER, PORT, keepalive=MQTT_KEEPALIVE)
    except Exception as exc:
        LOGGER.error("MQTT 初始化失败: %s", exc)

    LOGGER.info("等待 MQTT 数据...（Ctrl+C 停止）")
    try:
        # retry_first_connection=True：EMQX 没起来时也会持续重试，不会直接退出
        client.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        LOGGER.info("已停止（Ctrl+C）")
    except Exception:
        LOGGER.exception("MQTT 事件循环异常退出")
    finally:
        SPOOL_STOP.set()          # 让补传线程在下一个节拍醒来后自然结束
        try:
            client.disconnect()
        except Exception as exc:
            LOGGER.debug("断开 MQTT 时出错（忽略）: %s", exc)
        writer.close()
        alarm_writer.close()
        point_invalid_writer.close()


if __name__ == "__main__":
    main()
