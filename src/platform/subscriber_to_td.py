# -*- coding: utf-8 -*-
"""平台接入：订阅 MQTT 上送报文，解析 9 个烟气测点后写入 TDengine 时序库 cems.cems_data 超级表。

逐点质量位（2026-10-11 接入，依据 docs/adr/0009-接入层逐点质量位.md）：
  把"任一测点不合格 → 整条报文拒收"改成"逐点处理"，两种"坏"分开走：

    | 坏法 | 判定 | 处理 |
    |---|---|---|
    | 语法坏 | 时间戳不可解析 / 报文长度不足 / 一个测点都没有 / 同一个测点出现两次 / 测点值不是数 | PayloadFormatError → 整条拒收 |
    | 语义坏 | 单点 NaN/inf/超量程、报文里没给这个点 | PointInvalid（值对象，不抛）→ 只丢该点，其余照常入库 |

  逐点丢之后那一列在库里是 NULL —— 与"这一分钟没采到"不可区分，
  所以必须同时写"证据层"：无效点事件表 cems_point_invalid（表名 env 可覆盖，
  见 POINT_INVALID_STABLE；列 ts/point/kind/reason/raw，TAGS(plant, device)）。
  它是本方案能成立的前提，不是可选增强（ADR-0009 §4/§6.2）。
  计数也跟着拆（§10.3）：rejected_messages（语法坏丢的报文条数）
  + invalid_points（语义坏丢的测点个数）—— 一个指标看不出"整条不通"还是"偶尔一个点坏"。

数据合理性判据（2026-10-11 接入，依据 docs/adr/0010-数据合理性判据.md）：
  核心是「可疑」≠「无效」：

    | 判据 | kind | stored | 处置 |
    |---|---|---|---|
    | 超量程 / NaN / inf | invalid | 0 | 不写该列（ADR-0009 既有，未改） |
    | 报文没有该点 | missing | 0 | 不写（ADR-0009 既有，未改） |
    | 跳变可疑 | jump | 1 | 照常入库 + 留痕 |
    | 冻结可疑 | frozen | 1 | 照常入库 + 留痕 |

  为什么可疑不能丢：跳变可能是真实的工况突变（设备启停），
  冻结可能是真实的停机/检修恒值 —— 丢掉等于掩盖真实事件。
  判据用"量程宽度的百分比"表达（免逐点配置），且冻结是「容差 + 持续」
  而非「值相等」：CEMS 稳态本来就极稳，用相等会大量误报，而误报比漏报更糟。
  事件表新增 stored 列：老库升级靠 ALTER STABLE ... ADD COLUMN
  （CREATE STABLE IF NOT EXISTS 不会给已存在的表加列，见 ensure_stored_column）。

排放超标告警（2026-10-02 接入，依据 docs/adr/0002-告警判据选型.md 方案乙）：
  入库成功 → 算折算值（全链路唯一一处）→ 接入层逐条判折算值超限 → 落告警事件表/推送记录。
  判定不改拒收逻辑、不改契约、不加服务、不加依赖；
  折算值仍然不给 cems_data 建列，事件表里存的是判定快照（判定证据，不是数据冗余列）。
  折算与判定都要求 O2 + 三个污染物同帧齐全；逐点化后某一帧可能不齐（见
  supports_reference），那种帧跳过折算与判定但仍照常入库。
"""

from __future__ import annotations

import logging
import math
import os
import re
import sys
import threading
import time
from dataclasses import dataclass, fields as dataclass_fields
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
#   语法坏 → PayloadFormatError（整条拒收）；语义坏 → PointInvalid（值对象，不抛）
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
    contract_version,
    to_reference_o2,
    to_transmit_value,
)
# 会被拼进 SQL 的标识符/tag 值白名单：唯一真源在 src/common/sql_safety.py，
# 展示层读路径（src/web/cache.py 的 device_tag_parts）共用同一条正则。
from src.common.sql_safety import SQL_NAME_RE, SQL_NAME_RULE   # noqa: E402
# 接入层落盘队列（写库失败时的本地持久副本）：机制与崩溃安全见该模块头部说明
from src.platform.ingest_spool import IngestSpool   # noqa: E402

# ==================== 配置区（要改参数只动这里） ====================
# 连接参数支持环境变量覆盖，默认值与本机直接运行一致；
# 容器化部署时由 docker-compose.yml 注入服务名（如 MQTT_HOST=emqx、TD_URL=http://tdengine:6041）。

# ---- MQTT（对应 EMQX）----
BROKER: Final[str] = os.getenv("MQTT_HOST", "localhost")
PORT: Final[int] = int(os.getenv("MQTT_PORT", "1883"))
TOPIC: Final[str] = os.getenv("MQTT_TOPIC", "cems/plant1/data")
MQTT_QOS: Final[int] = int(os.getenv("MQTT_QOS", "1"))
MQTT_KEEPALIVE: Final[int] = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_CLIENT_ID: Final[str] = os.getenv("MQTT_CLIENT_ID", "cems-subscriber-plant1")
# 会话持久化：默认 False = 非干净会话（持久会话）。
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
# TDengine 对已存在的子表会沿用第一次写入时的 TAGS 且不报任何错，
# 所以子表名必须能区分设备。若只用厂区名（plant1），接入第二台设备时
# 数据会被静默挂到第一台设备的标签下。这里改成 plant1_device1 这种形式。
# 统一转小写：TDengine 的表名不区分大小写，若配置写成 Device1，
# 实际建出来仍是 device1，而一致性检查按原样去查就会查不到行、静默跳过。
CHILD_TABLE: Final[str] = f"{TD_PLANT}_{TD_DEVICE}".lower()

# ---- 会被拼进 SQL 的标识符白名单（防注入）----
# 正则本体不再定义在这里：唯一真源是 src.common.sql_safety.SQL_NAME_RE
#    （本文件顶部 import 进来，名字保持 SQL_NAME_RE 不变，供既有调用点与外部引用）。
#    原因：展示层读路径也要用它（src/web/cache.py 的 device_tag_parts()），
#    常量放在平台层会把 paho.mqtt 这类重依赖拖进 Web 容器，所以下沉到 src/common。
#    规则文案 SQL_NAME_RULE 同源，保证两边的错误提示逐字一致。

# ---- 主题 ↔ 标签 一致性自检（多设备多实例部署的护栏）----
# 推荐形态是"每台设备一个订阅端实例"：实例只订阅一个主题、只写一张子表，
# 于是本模块里所有带状态的东西（判据滑窗、最近折算值快照、持久会话 client_id）
# 天然是"每设备一份"。多实例的代价只是多一个容器，换来的是单设备代码路径零改动。
# 代价对应的风险是配置写错：订阅了 B 厂区的主题，却写 TD_PLANT=A 厂区，
# 数据会静默挂到 A 的标签下（入库成功、报表挂错名）。
#
# 这一条为什么默认拒绝启动（2026-10-02 升级，原来是只打 WARNING）：
#   典型误配是"只改了 MQTT_TOPIC2 没改 PLANT2"：主题已经是 cems/plant2/data，
#   TD_PLANT 还是 plant1、TD_DEVICE 被改成 device2 ⇒ 数据写进子表 plant1_device2。
#   字面上 (plant1, device2) 是自洽的，所以入库、报表、导出都不会报错，只是归属错了 ——
#   这类"静默张冠李戴"事后无法从数据里恢复，只能拒之门外。WARNING 在 10 个容器的日志里
#   太容易被淹掉，所以在启动阶段就把它变成一条 CRITICAL + 非零退出（容器会重启并持续可见）。
#   放行开关：确实要用自定义厂区前缀的部署，显式设 TD_ALLOW_TOPIC_TAG_MISMATCH=1。
#
# 关于"把 TD_DEVICE 也纳入这条校验"：做不到，也不该假装做到。
#   主题契约是 cems/<厂区>/<用途>（如 cems/plant1/data），没有设备段——
#   第三段是用途（data），不是设备号，拿它当设备比只会误报。设备维度的护栏在别处：
#   ① 本模块 _check_tags() 在连库时核对已存在子表 {plant}_{device} 的真实 TAGS；
#   ② 启动日志直接打印最终子表名，让"哪台设备写哪张表"一眼可查（见 main()）。
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
    """启动期强制检查主题 ↔ 标签一致性：不一致默认拒绝启动（返回 True 才继续）。

    - 一致、或主题不是 cems/<厂区>/... 形态（自定义主题）→ True，照常启动；
    - 不一致 → 记 CRITICAL 并把最终子表名打出来，然后 SystemExit（非零退出，
      由 compose 的 restart 策略放大成持续可见的故障，而不是被日志淹没的一条 WARNING）；
    - TD_ALLOW_TOPIC_TAG_MISMATCH=1 → 只记 CRITICAL 放行（给确实要自定义前缀的部署留口子）。
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
#           逐点化之前这里是"整条拒收"；现在是"只丢该点"，见 parse_payload 说明。
#           NaN/inf 会让 TDengine 报 syntax error，所以它也不进主表，
#           但在归类上同样是"该点无效"（不是整条拒收）。
TD_COLUMNS: Final[tuple[str, ...]] = COLUMNS
POINT_RANGES: Final[dict[str, tuple[float, float]]] = RANGES
TS_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"

# ---- 折算值（导出量）：只算、不进库 ----
# 折算值不给 TDengine 建列。它是从"标干浓度 + 氧含量"算出来的导出量，
#    存一份就必然有一天跟算出来的对不上（冗余必然漂）；库里保持 9 个物理量列。
# 判定口径：超标判的是折算值，不是标干值。实测反例：SO2 标干 30.0 在限值
#    35 以内（按标干判 = 达标），但 O2=9% 时折算 37.5（> 35）已超标 —— 同一份数据
#    两种判法结论相反，所以折算值必须有一个能取到的出口（见 reference_values()）。
# 公式、基准氧含量（6%）、参与折算的污染物、氧含量测点名全部来自 src/common/points.py
# 契约，这里不重复推导公式，也不硬编码列名字符串。
COLUMN_TO_NAME: Final[dict[str, str]] = {point.column: point.name for point in POINTS}
O2_FIELD_NAME: Final[str] = COLUMN_TO_NAME[O2_COLUMN]
REFERENCE_LOG_EVERY: Final[int] = 20       # 折算值抽样日志周期（条），避免每条都刷屏

# ---- 收发/拒收/落盘计数（把上游数据质量与"落盘队列"状态量化出来）----
# 逐点化（ADR-0009 §10.3）后"拒收"有两个粒度，必须拆成两个指标：
#   只有一个按"条"计的指标时，看不出一台设备是"整条不通"还是"偶尔一个点坏"——
#   又是一种静默（1 个坏点算 1 条拒收，9 个坏点也算 1 条）。
STATS: Final[dict[str, int]] = {
    "received": 0,
    # 语法坏丢掉的报文条数（时间戳坏/长度不足/一个测点都没有/重复测点/值不是数）
    "rejected_messages": 0,
    # 语义坏丢掉的测点个数（超量程/NaN/inf → invalid；报文没给 → missing）
    "invalid_points": 0,
    # ADR-0010：被标为可疑的测点个数（跳变/冻结，stored=1）——
    #   它与 invalid_points 不是一回事：可疑点照样入库，
    #   这个计数涨起来说明"数据在动/在停"，不代表丢了数（漏看它会把真实工况当噪声）。
    "suspect_points": 0,
    "accepted": 0,
    "spooled": 0,        # 写库失败 → 已落本地队列（有持久副本，等补传）
    "spool_lost": 0,     # 写库失败且落盘也失败 → 真丢（伴随 CRITICAL 日志）
    "drained": 0,        # 由落盘队列补传成功入库的条数
    # 契约指纹与网关不一致的报文条数（见 check_contract_version）——
    #   它涨起来不代表数据坏了，代表"网关和接入层不是同一版契约"，
    #   同一列的数据含义可能已经不同。这条必须有人看，否则会静默混库。
    "cfg_mismatch": 0,
}

# ---- 排放超标告警（ADR-0002 方案乙：接入层逐条判折算值）----
# 判据参数（窗口 M/N、恢复系数、覆盖率门限…）全部在 src/common/alarm_judge.py 里定义，
# 这里只负责：绑定表名/标签、开关、以及"落库 + 推送记录 + 日志"这三件事。
# 判定不改拒收逻辑、不改契约、不加服务、不加依赖；折算值仍然不给 cems_data 建列。
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
# 为什么必须有它：逐点丢之后，那一列在库里是 NULL —— 而"这一分钟没采到"也是 NULL。
#   若到此为止，就是把整条静默换成了单点静默（分不清"断线/设备死/被判无效"，
#   甚至会把"超标数据因超量程被丢掉"看成"缺数"）。所以必须逐点留痕。
# 表名照 ALARM_EVENT_STABLE 的既有写法：env 覆盖，默认 cems_point_invalid。
# 它不参与达标判定、不加列到主表，只是证据层（主表列布局保持不动）。
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

# ---- 数据合理性判据（跳变 / 冻结）：ADR-0010 ----
# 核心：「可疑」≠「无效」 ——
#     超量程/NaN/inf（invalid）物理上不可能 → 不写该列（丢）；
#     跳变/冻结（jump/frozen）可疑但可能是真实工况（设备启停、停机检修恒值）→
#     照常入库 + 留痕（stored=1）。丢掉等于掩盖真实事件。
# 判据都用"量程宽度的百分比"表达 → 不需要逐点配置
#    （9 个测点量纲差 4 个数量级：Flow ≈ 34000 vs O2 ≈ 6.8）。
# 冻结不能用"值完全相等"：CEMS 稳态本来就极稳（实测 Temp 在 26.0~26.2
#    稳了 40 分钟），用相等会大量误报 —— 而误报比漏报更糟（运维会直接关掉它）。
#    所以判据是「容差 + 持续」，不是「相等」（ADR-0010 §2）。
# 默认开：判据只留痕不丢数，"开着"的代价远小于"把真实数据丢掉"；
#    标定手段就是 ADR-0010 §8 的 W4。
# 实测（2026-10-11 · W4）：ADR-0010 §5 的默认阈值会在真机数据上误报 ——
#    连续重放 3.2 小时真实数据（2262 条）：
#      (容差 0.005, N=12 ) → 238 条 frozen，任意 10 分钟窗口最多 18 条（在线实测 10.4 分钟 39 条）
#      (容差 0.005, N=180) →  12 条，10 分钟最多 2 条（在线实测 20.5 分钟 3 条）
#      (容差 1e-4 , N=360) →   0 条，10 分钟最多 0 条（在线实测 13.7 分钟 0 条）
#    在 ADR 的容差（0.5% 量程）下加大 N 也到不了 0：真机数据里本来就有跨越十几分钟、
#       始终落在带内的慢漂移段（ADR §2 自己观测的"Temp 26.0~26.2 稳了 40 分钟"就是它）。
#    零误报要同时收紧容差（≈寄存器分辨率）并拉长 N（≥360 ≈ 30 分钟 @5s）。
#    本文件的默认值仍是 ADR 的 12 / 0.005（未擅自改 ADR），标定值用 env 覆盖
#      （compose 注释里有完整数据与一行命令）。
# 附带实测：即使把容差设成 0（"值完全相等"）也躲不掉误报（Dust 量化后连续 12 个样本
#    读数完全相同，3.2 小时里 16 次）—— 这反过来印证了 ADR §2"不能用相等"的判断。
# env 的解析在下面的 QualityConfig.from_env()（照 AlarmConfig 的既有写法）：
#    QUALITY_CHECK_ENABLE / QUALITY_JUMP_RATIO / QUALITY_FREEZE_SAMPLES /
#    QUALITY_FREEZE_TOLERANCE_RATIO —— 加在 compose 的 x-subscriber-env 锚点，
#    两个 subscriber 实例都拿到（照 ADR-0009 的教训：漏一个实例就有一半数据不判）。

# ---- 日志 ----
# 级别/格式/时间格式统一在 src/common/logging.py（唯一真源）；
# 这里只留本进程的 LOGGER —— 4 个进程的日志靠 getLogger 的名字区分来源。
LOGGER: Final[logging.Logger] = logging.getLogger("platform.subscriber_to_td")

# ---- 接入层落盘队列（写库失败时的本地副本）----
# 为什么必须有它：订阅端拿到 QoS1 报文后，paho 在 on_message 正常返回时发 PUBACK，
#   broker 随即删掉队列里唯一的持久副本；网关那边也早在 broker PUBACK 时就删了 cache.jsonl。
#   若写库失败后直接返回 → 这条数据在整条链路上没有任何副本 = 永久丢失（旧「P1 未修」）。
#   现在的口径：写库失败 → 先落本地队列（fsync）→ 后台补传 → 入库成功才删。
#   代价：写库失败时多一次本地写；队列积压超上限会丢最旧（CRITICAL），容量默认 256 MB。
# 队列目录必须每实例一份（多设备部署的关键约束，与网关缓存目录同源）：两个订阅端
#   实例若共用同一个目录，各自的补传线程会把对方的报文拿自己的标签写进自己的子表 →
#   数据张冠李戴，且两边都以为成功。
SUBSCRIBER_DATA_DIR_ENV: Final[str] = os.getenv("SUBSCRIBER_DATA_DIR", "").strip()


def resolve_spool_dir(raw: str) -> Path:
    """把 SUBSCRIBER_DATA_DIR 解析成绝对路径；空值回落 PROJECT_ROOT/data/subscriber。

    - 未设置 → PROJECT_ROOT/data/subscriber（容器内即 /app/data/subscriber，配合 compose 的卷）
    - 绝对路径 → 原样使用（容器里的推荐写法，如 /app/data-plant2/subscriber）
    - 相对路径 → 相对项目根解析（不是 CWD），本机运行与容器运行不分叉
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


# ==================== 日志 ====================
# setup_logging() 已提取到 src/common/logging.py，4 个入口共用同一份实现。


# ==================== 报文解析 ====================

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
    # 合理性判据（ADR-0010）：阈值自检（写错的阈值比不开更糟 —— 会静默误报或静默失能）
    QUALITY_CONFIG.validate()
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

    本函数仍按自己的契约抛 ValueError；把它升级成"整条报文语法坏"（PayloadFormatError）
    是 parse_payload 的职责 —— 这样单独用它的调用方行为不变。
    """
    try:
        datetime.strptime(text, TS_FORMAT)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"时间戳格式非法: {text!r}") from exc
    return text


def parse_payload(
    payload: str,
) -> tuple[str, dict[str, float], dict[str, PointInvalid]]:
    """把 MQTT 报文解析成 (时间戳, 有效测点, 无效测点)。

    报文格式: "2026-09-10 12:00:00 SO2=35.2 NOx=18.5 ... Pressure=101.3 Flag=N"

    两种"坏"分开处理（ADR-0009 §3 + §10.1，这是本函数存在的全部理由）：

    | 坏法 | 判定 | 处理 |
    |---|---|---|
    | 语法坏 | 时间戳不可解析 / 报文长度不足 / 一个测点都没有 / 同一个测点出现两次 / 测点值根本不是数 | 抛 PayloadFormatError → 调用方整条拒收 |
    | 语义坏 | 单点 NaN/inf/超量程（kind=invalid）、报文里没给这个点（kind=missing） | 收进第三个返回值，不抛；其余测点照常入库 |

    为什么"重复测点"算语法坏（§10.1）：同一个点有两个值，是报文自相矛盾 ——
      采信哪个都是猜。这不是数据质量问题，是格式问题，所以整条拒收。
    为什么"值不是数"（SO2=abc）算语法坏：它连"一个合法的数"都不是，
      值对象 PointInvalid.raw（float | None）表达不了它；而且这类报文通常是
      编码/配置错（网关把文本当数发），不是单点仪表故障 —— 宁可整条拦住，别把
      8 个本来正确的点一起放行到"看起来正常"的库里。
    语义坏一律不抛异常：它是正常的数据分类结果（src/common/errors.py 的口径），
      一条报文里 3 个点超量程不该走 3 次异常。
    缺测点现在是"语义坏"（§10.1）：与"超量程"一样是单点问题；
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


# ---- 契约指纹比对：网关与接入层必须"按同一版契约"解读同一列 ----
# 报文末尾带 `Cfg=<8 位十六进制>`（见 gateway.build_payload）。这里取出来跟自己的比。
#
# 为什么值得单独做一步：网关和接入层是两个镜像、两个发布节奏，完全可能
# "一边改了测点契约、另一边没改"。这种漂移**两边都不报错** —— 网关发的值仍落在
# （旧的）量程内，接入层照收照存，但同一个列名的物理含义已经变了。等发现的时候，
# 库里已经混了两版含义的数据，而且分不出哪段是哪版。指纹把这个静默故障变成一行日志。
#
# 只记不拦：指纹不一致不等于这条数据不能用（比如只改了 unit），而且拦掉会把
# "配置没对齐"升级成"数据断了"—— 那比漂移本身更糟。喊出来，让人去查。
CFG_VERSION: Final[str] = contract_version()

#: 不一致告警只打第一次（之后靠 STATS["cfg_mismatch"] 体现规模），免得一次部署错刷满日志
_CFG_MISMATCH_WARNED: bool = False


def check_contract_version(payload: str) -> Optional[str]:
    """取报文里的 `Cfg=` 并与本进程的契约指纹比对；返回报文里的指纹（无则 None）。

    契约不一致时打一条 ERROR 并把 `STATS["cfg_mismatch"]` +1 ——
    运维从计数就能看出漂移规模，不必翻全量日志。
    """
    global _CFG_MISMATCH_WARNED
    found: Optional[str] = None
    for item in payload.split():
        key, sep, value = item.partition("=")
        if sep and key == "Cfg":
            found = value
            break

    if found is None:
        # 老网关（未升级）不发这个字段：不算错，但要说一次，否则"没人发指纹"
        # 会被误读成"契约一致"。
        if not _CFG_MISMATCH_WARNED:
            _CFG_MISMATCH_WARNED = True
            LOGGER.warning(
                "报文里没有契约指纹（Cfg=），无法核对网关与接入层是否同一版契约。"
                "若网关已升级仍出现本提示，说明网关的 build_payload 没带上 Cfg。"
            )
        return None

    if found != CFG_VERSION:
        STATS["cfg_mismatch"] += 1
        if not _CFG_MISMATCH_WARNED:
            _CFG_MISMATCH_WARNED = True
            LOGGER.error(
                "契约指纹不一致：报文=%s 本机=%s —— 网关与接入层的测点契约不是同一版，"
                "同一列的数据含义可能已经不同（先查两边镜像是否为同一次构建）。",
                found, CFG_VERSION,
            )
    return found


#: 算折算值 / 跑告警判据所需的那几个测点（必须同帧齐全）。
#: 折算公式要 O2；AlarmJudge.on_sample() 还要三个污染物的标干值
#: （它按 values[NAME] 直接取键，缺一个就 KeyError）。
REFERENCE_FRAME_POINTS: Final[tuple[str, ...]] = (
    O2_FIELD_NAME,
    *(COLUMN_TO_NAME[column] for column in ZS_TARGETS),
)


def supports_reference(values: Mapping[str, float]) -> bool:
    """该条报文能不能算折算值（= 判据能不能跑）：要求 O2 + 三个污染物同帧齐全。

    逐点化（ADR-0009）之后这是个必然会出现的新情况：某个点被判无效/缺失时，
    帧就不完整了。此时跳过折算与告警判定（该条仍照常入库），
    而不是猜一个值、也不是把整条报文丢掉 ——
    "折算用的 O2 必须与浓度同帧"是 ADR-0002 §3.1 的既有口径，本次不动判据。
    """
    return all(name in values for name in REFERENCE_FRAME_POINTS)


# ==================== 数据合理性判据（跳变 / 冻结，ADR-0010） ====================
#
# 本节的唯一要点：「可疑」≠「无效」。
#     这里产出的 PointInvalid 是可疑（kind=jump/frozen，stored=1）——
#     它的值照常入库，只是额外留一笔痕。判据本身绝不把值从报文里拿掉。
#
# 判据定义（都用"量程宽度的百分比"，免逐点配置）：
#   - 跳变：|当前值 − 该点上一次值| > 量程宽度 × jump_ratio
#   - 冻结：连续 N 个样本都落在 [v0 − 容差, v0 + 容差]，v0 = 该游程的第一个值，
#           容差 = 量程宽度 × freeze_tolerance_ratio
#
# 判据只吃"已过量程检查"的点（parse_payload 的 values）：
#    超量程/NaN 的原始值不是可信测量值，拿它推进"上一次值/游程"会让状态被垃圾带偏。
# 冷启动窗口（ADR-0010 §9）：进程重启后第一个样本没有"上一次值"、
#    也凑不满 N 个样本 → 前 N-1 个样本判不出冻结。这是进程内状态的固有代价，登记为已知边界。

@dataclass(frozen=True)
class QualityConfig:
    """数据合理性判据参数（全部来自 QUALITY_* 环境变量；默认值即 ADR-0010 §5 那一组）。

    默认值已经过标定（2026-10-11）—— 标定方法就是 ADR-0010 §8 的 W4。
    标定证据（实测，不是估计）：
      · 真机数据在线 (容差 0.005, N=12) → 10.4 分钟误报 39 条 frozen；
      · 3.2 小时连续重放：(0.005,12) 238 条 / (0.005,180) 12 条 / (0.005,360) 7 条；
      · 10 天全量 25093 个样本复算最长自然平台期：dust = 404 样本（33.7 分钟，
        2026-10-02 22:07:15~22:41:00），其余测点 ≤186。
    所以 N 取 1200 = 404 × 3 倍裕量；容差取 1e-4（寄存器分辨率量级）。
    实测结论：0.5% 的容差下"加大 N 也到不了零误报"（真机数据本就含跨十几分钟、
    始终落在 0.5% 带内的近恒值段）—— 必须同时收紧容差并拉长 N。
    全部参数都可在部署层覆盖（compose 里同名 env）。
    """

    enable: bool = True                          # QUALITY_CHECK_ENABLE（默认开，见 ADR-0010 §7）
    jump_ratio: float = 0.25                     # QUALITY_JUMP_RATIO（量程宽度的 25%）
    #: 1200 ≈ 100 分钟 @5s。由 10 天实测标定（最长自然平台期 dust=404，取 3 倍裕量）
    freeze_samples: int = 1200                   # QUALITY_FREEZE_SAMPLES
    #: 1e-4 × 量程 ≈ 寄存器分辨率量级。实测：0.5% 的容差无论 N 多大都到不了零误报
    freeze_tolerance_ratio: float = 0.0001       # QUALITY_FREEZE_TOLERANCE_RATIO
    #: 用哪个门限算"跳变/容差"：默认取契约 points.RANGES（唯一真源，不重复抄量程）
    ranges: Mapping[str, tuple[float, float]] = None      # type: ignore[assignment]

    def resolve_ranges(self) -> Mapping[str, tuple[float, float]]:
        """取判据用的量程表（默认就是契约 points.RANGES，允许测试注入自定义量程）。"""
        return POINT_RANGES if self.ranges is None else self.ranges

    def validate(self) -> None:
        """参数自检；不合法抛 ValueError（由接入层在启动阶段拦下，别带病运行）。"""
        if self.jump_ratio <= 0:
            raise ValueError(f"QUALITY_JUMP_RATIO 必须 > 0: {self.jump_ratio}")
        if self.freeze_tolerance_ratio < 0:
            raise ValueError(
                f"QUALITY_FREEZE_TOLERANCE_RATIO 不能为负: {self.freeze_tolerance_ratio}"
            )
        # 冻结至少要 2 个样本才有意义（1 个样本"不变"是恒真的，会把每条报文都标成冻结）
        if self.freeze_samples < 2:
            raise ValueError(f"QUALITY_FREEZE_SAMPLES 必须 >= 2: {self.freeze_samples}")

    @classmethod
    def from_env(cls) -> "QualityConfig":
        """从 QUALITY_* 环境变量构造（未设置时取本类字段的默认值）。

        默认值不在这里重打一遍 —— 这里曾经把默认值抄了第二份（"12" / "0.005"），
        与 dataclass 字段各存一份。2026-10-11 改标定值时只改了一份，
        于是 from_env() 与 QualityConfig() 悄悄不一致（freeze_samples 12 vs 1200）——
        被 test_quality_config_from_env_matches_the_dataclass_defaults 抓到。
        现在统一从 dataclasses.fields() 取唯一真源：改默认值只需改字段那一处。

        保留一条铁律：新增 QUALITY_* 参数时必须同时加进下面的 cls(...) 显式列表 ——
        from_env 是"显式列出"而不是"遍历字段"，所以漏加不会自动生效（会被 validate() 或
        该一致性测试暴露，而不是静默用默认值）。
        """
        defaults = {f.name: f.default for f in dataclass_fields(cls)}

        def flag(key: str, default: str) -> bool:
            return os.getenv(key, default).strip().lower() not in ("0", "false", "no", "off", "")

        return cls(
            enable=flag("QUALITY_CHECK_ENABLE", str(defaults["enable"])),
            jump_ratio=float(os.getenv("QUALITY_JUMP_RATIO", str(defaults["jump_ratio"]))),
            freeze_samples=int(os.getenv("QUALITY_FREEZE_SAMPLES", str(defaults["freeze_samples"]))),
            freeze_tolerance_ratio=float(
                os.getenv("QUALITY_FREEZE_TOLERANCE_RATIO", str(defaults["freeze_tolerance_ratio"]))
            ),
        )


def _fmt_number(value: float) -> str:
    """把数值写进 `reason` 时用的紧凑格式（`%g`：34000.0 → `34000`，26.18 → `26.18`）。"""
    return f"{value:g}"


class QualityChecker:
    """跳变 / 冻结判据：每设备实例一份的进程内状态（ADR-0010 §9 的冷启动窗口见类注释）。

    它只产出"可疑"证据，从不删改 values —— 「可疑照常入库」这条纪律
    由调用方（on_message）保证：把本函数的返回值加进事件表，值本身仍写主表。
    """

    def __init__(self, config: QualityConfig) -> None:
        self._config = config
        #: 该点上一次通过量程检查的值（跳变判据的基准）
        self._last: dict[str, float] = {}
        #: 该点当前冻结游程的基准值 v0（不随游程漂移，严格照 ADR-0010 §2 的定义）
        self._run_base: dict[str, float] = {}
        #: 该点当前游程里已累计的样本数（含基准样本自身）
        self._run_len: dict[str, int] = {}
        #: 该点当前游程是否已经报过冻结（一个游程只报一次，别每来一个样本报一次）
        self._run_reported: set[str] = set()

    @property
    def config(self) -> QualityConfig:
        return self._config

    def check(self, values: Mapping[str, float]) -> list[PointInvalid]:
        """对一条报文的有效测点跑两类判据，返回可疑证据（可能为空）。

        - 只读 values（已过量程/有限性检查的点）；返回的项不属于 values 的删改，
          调用方必须继续把 values 照常入库；
        - 按契约 NAMES 顺序遍历（不用 values 的键序）→ 同一条报文的事件顺序确定；
        - enable=False（W5）→ 直接返回空表，且不推进任何状态。
        """
        if not self._config.enable:
            return []
        ranges = self._config.resolve_ranges()
        suspects: list[PointInvalid] = []
        for name in NAMES:
            if name not in values:
                continue
            value = values[name]
            low, high = ranges[name]
            width = high - low
            jump = self._check_jump(name, value, width)
            if jump is not None:
                suspects.append(jump)
            frozen = self._check_frozen(name, value, width)
            if frozen is not None:
                suspects.append(frozen)
            self._last[name] = value
        return suspects

    def _check_jump(self, name: str, value: float, width: float) -> Optional[PointInvalid]:
        """跳变判据：与该点上一次值比；首次见到该点（冷启动）不判。"""
        previous = self._last.get(name)
        if previous is None:
            return None
        threshold = width * self._config.jump_ratio
        delta = abs(value - previous)
        if delta <= threshold:
            return None
        return PointInvalid(
            name, PointInvalidKind.JUMP,
            f"测点 {name} 跳变可疑（值已入库）: 上次 {_fmt_number(previous)} → "
            f"本次 {_fmt_number(value)}，|Δ|={_fmt_number(delta)} > "
            f"量程宽度 {_fmt_number(width)}×{_fmt_number(self._config.jump_ratio)}"
            f"={_fmt_number(threshold)}",
            value,
        )

    def _check_frozen(self, name: str, value: float, width: float) -> Optional[PointInvalid]:
        """冻结判据：连续 N 个样本落在 [v0 ± 容差] 内；一个游程只报一次。

        容差不是 0（"值完全相等"会大量误报）：CEMS 稳态本来就极稳，见 ADR-0010 §2。
        """
        tolerance = width * self._config.freeze_tolerance_ratio
        base = self._run_base.get(name)
        if base is not None and abs(value - base) <= tolerance:
            self._run_len[name] = self._run_len.get(name, 1) + 1
        else:
            # 换了游程（冷启动 / 超出容差）：重新起算，并复位"已报过"标记
            self._run_base[name] = value
            self._run_len[name] = 1
            self._run_reported.discard(name)
        if self._run_len[name] < self._config.freeze_samples:
            return None
        if name in self._run_reported:
            return None
        self._run_reported.add(name)
        return PointInvalid(
            name, PointInvalidKind.FROZEN,
            f"测点 {name} 冻结可疑（值已入库）: 连续 {self._run_len[name]} 个样本落在 "
            f"[{_fmt_number(base if base is not None else value)}±{_fmt_number(tolerance)}] 内",
            value,
        )


#: 本实例的合理性判据配置（env 解析失败 → 拒绝启动，照 ALARM_* 的既有纪律）
try:
    QUALITY_CONFIG: Final[QualityConfig] = QualityConfig.from_env()
except (ValueError, TypeError) as exc:
    raise SystemExit(f"QUALITY_* 配置无法解析，拒绝启动: {exc}") from exc

#: 本实例的判据状态（进程内、每设备一份；模块级是为了让 MQTT 回调线程拿到同一份状态）
QUALITY_CHECKER = QualityChecker(QUALITY_CONFIG)


# ==================== TDengine 写入器 ====================

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
            # REST 接口无状态：USE 不生效，每条 SQL 必须显式带库名前缀 cems.xxx
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

        逐点化（ADR-0009 §10.2 第 1 步）：传入哪几列就写哪几列 ——
          只按 values 里实际有的测点拼列，缺失的列整列不出现，
          由 TDengine 按"省略的列就是 NULL"处理（部分列 INSERT 与 USING/TAGS 共存
          已实测，见 ADR-0009 §6.2）。

        9 列齐全时，拼出来的 SQL 与逐点化之前逐字节相同（既有路径零变化）：
          列名仍按 POINTS 契约顺序展开（不是 values 的键序），
          所以报文顺序/建表顺序漂移都不会写错列。
        一个可写的列都没有（全列判无效）时返回 False 且不执行 SQL：
          这类报文 TDengine 写不进去（没有 ts 列之外的内容），
          调用方（on_message）会提前拦住，不让它进落盘队列空转。
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


# ==================== 告警表写入器 ====================

class AlarmWriter:
    """告警表读写器：建三张告警超级表，幂等写入事件行 / 推送记录 / 小时结论行。

    为什么要独立一个写入器（而不是复用 TdWriter）：
        - 小时结算跑在独立线程里，不能和 MQTT 回调线程抢同一个游标；
        - 告警表的写失败绝不该影响入库，两条链路各自重连、各自重试。

    幂等语义 = (子表, ts) 覆盖：子表名 = plant_device_point_judgetype，
    同一子表写同一个 ts，TDengine 覆盖原行而不是新增（本机 3.3.6.13 实测：
    COUNT(*) 不变、值被覆盖）。所以 QoS1 重投同一条报文只会覆盖同一行。
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


# ==================== 无效点 / 可疑点事件表（ADR-0009 §4.1 + ADR-0010 §4） ====================
#
# 这张表是证据层，不是数据层：
#     - 不进主表、不给主表加列（主表列布局保持不动，既有报表/导出不受影响）
#     - 不参与达标判定（覆盖率怎么算、小时门限怎么定，本次都不动）
#     - 只回答一个问题：哪一分钟、哪个点、为什么
#
# ADR-0010 之后它同时装两类证据（同一张表、同一套 kind，靠 stored 列分开）：
#     - stored=0：丢了什么（invalid 超量程/NaN/inf、missing 报文没给）
#     - stored=1：标了什么但留着（jump 跳变可疑、frozen 冻结可疑，值已入库）
#
# 为什么 point 不写成 TAG（照 §4.1 的字面：TAGS 只有 plant/device），
#    却把点名列进子表名（plant_device_invalid_<point>）：
#    TDengine 的幂等/覆盖语义是 (子表, ts) —— 若所有点共用一张子表，
#    同一条报文里 2 个点同时无效时会互相覆盖，只剩一条证据（静默丢证据）。
#    按点分子表既保住了"同一报文重投不重复"（V5），也不丢多点的行。
#    ADR-0010 的跳变/冻结同样受益：一条报文里多点同时跳变也各留一条证据。

#: ADR-0010 新增列：该 kind 的值有没有进主表（0=丢了 / 1=标了可疑但留着）
STORED_COLUMN: Final[str] = "stored"


@dataclass(frozen=True)
class PointInvalidTables:
    """无效点事件表在 TDengine 里的位置与命名，以及 DDL/DML 文本（纯字符串）。"""

    db: str
    plant: str
    device: str
    stable: str = "cems_point_invalid"

    def child(self, point: str) -> str:
        """该测点的子表名（幂等键 = (子表, ts)，同一报文重投只覆盖不新增）。"""
        return f"{self.plant}_{self.device}_invalid_{point}".lower()

    def create_sql(self) -> str:
        """建超级表（IF NOT EXISTS，可重复执行；照 AlarmTables 的写法）。

        CREATE STABLE IF NOT EXISTS 不会给已存在的表加列 —— 所以
        stored（ADR-0010 新增）不能只靠这一条：老库升级必须显式 ALTER
        （见 alter_stored_column_sql() 与 PointInvalidWriter.connect()）。
        """
        return (
            f"CREATE STABLE IF NOT EXISTS {self.db}.{self.stable} "
            f"(ts TIMESTAMP, point NCHAR(16), kind NCHAR(10), "
            f"reason NCHAR(200), raw FLOAT, stored TINYINT) "
            f"TAGS (plant NCHAR(20), device NCHAR(20))"
        )

    def alter_stored_column_sql(self) -> str:
        """老库平滑升级：给已存在的事件超级表补 stored TINYINT（ADR-0010 §6①）。

        已实测 ALTER STABLE xxx ADD COLUMN stored TINYINT; 在 TDengine 3.3.6.13 上可用，
        且旧行读回 stored = NULL（向后兼容）——
        所以"老行没有 stored"不等于数据坏了，查"标了什么但留着"时用
        WHERE stored = 1（NULL 不满足该条件，老行自然不被算进去）。
        列已存在时这条会报错 —— 调用方按 DESCRIBE 判断后再执行（幂等，见 ensure_stored_column）。
        """
        return f"ALTER STABLE {self.db}.{self.stable} ADD COLUMN stored TINYINT"

    def insert_sql(self, ts: str, item: PointInvalid) -> str:
        """一条证据行。

        - kind：invalid（有值但不可信）/ missing（报文里根本没这个点）
          / jump / frozen（可疑，值已入库）—— 短名，受 NCHAR(10) 限制（ADR-0010 §6②）
        - stored：该 kind 的值有没有进主表（0=丢了 / 1=留着但标了可疑）。
          这是"「丢了什么」与「标了什么但留着」各是一句 SQL"的落点（ADR-0010 §4）。
        - raw：原始值（保留证据）；missing 与非有限值（NaN/inf）写 NULL
          —— sql_float() 与非有限值必须写 NULL 的口径同源（NaN 进 SQL 会让 TDengine
          报 syntax error，原始文本已经写在 reason 里，证据不丢）
        """
        columns = "ts, point, kind, reason, raw, stored"
        values = ", ".join((
            sql_timestamp(ts),
            sql_text(item.point),
            sql_text(PointInvalidKind(item.kind).value),
            sql_text(str(item.reason)[:200]),
            sql_float(item.raw),
            str(PointInvalidKind(item.kind).stored),
        ))
        return (
            f"INSERT INTO {self.db}.{self.child(item.point)} USING {self.db}.{self.stable} "
            f"TAGS ({sql_text(self.plant)}, {sql_text(self.device)}) "
            f"({columns}) VALUES ({values})"
        )


def ensure_stored_column(cur: Any, tables: PointInvalidTables) -> bool:
    """确保事件超级表有 stored 列（幂等，可重复执行）；返回"本次是否真的 ALTER 了"。

    为什么必须显式做这一步：CREATE STABLE IF NOT EXISTS 不会给已存在的表加列 ——
    只改建表语句，老库（ADR-0009 建的、没有 stored）会一直缺列，
    于是新写入的行也没有 stored，WHERE stored = 1 永远查不到东西（静默失能）。

    ALTER 必须容忍"列已存在"（不能因此崩，也不能因此拒绝启动）：
    先 DESCRIBE 判断（正常路径），ALTER 报错时再 DESCRIBE 复核一次 ——
    只有"复核后确实还是没有这一列"才算真失败。这样并发/重复 ALTER 也安全。
    """
    def columns() -> set[str]:
        cur.execute(f"DESCRIBE {tables.db}.{tables.stable}")
        return {str(row[0]).lower() for row in cur.fetchall()}

    if STORED_COLUMN in columns():
        return False
    try:
        cur.execute(tables.alter_stored_column_sql())
    except Exception as exc:
        # 列已存在 / 并发 ALTER 都会走到这里：以 DESCRIBE 为准复核，别凭异常文本猜
        if STORED_COLUMN not in columns():
            raise
        LOGGER.info("事件表 %s.%s 的 %s 列已存在（ALTER 被拒后复核通过，忽略）: %s",
                    tables.db, tables.stable, STORED_COLUMN, exc)
        return False
    LOGGER.info("事件表新增列（老库平滑升级）: %s.%s %s TINYINT",
                tables.db, tables.stable, STORED_COLUMN)
    return True


#: 本实例的无效点事件表（表名由 POINT_INVALID_STABLE 覆盖，默认 cems_point_invalid）
POINT_INVALID_TABLES: Final[PointInvalidTables] = PointInvalidTables(
    db=TD_DB, plant=TD_PLANT, device=TD_DEVICE, stable=POINT_INVALID_STABLE,
)


class PointInvalidWriter:
    """无效点事件表写入器：建表 + 逐条写证据行。

    为什么要独立一个写入器（而不是复用 TdWriter / AlarmWriter）：
        - 失败域要分清：它与主表写入是两件事。主表写入失败要落盘队列补传，
          证据行写失败只是少一条证据，不能反过来把主链路的落盘逻辑搅乱；
        - AlarmWriter.connect() 会连带建三张告警表，而告警开关（ALARM_ENABLE=0）
          与本表无关，不该被它绑定；
        - 连接、重连、重试的口径与 AlarmWriter 完全一致（照抄既有写法，不另造一套）。
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
        """连接并保证无效点事件超级表存在（含 ADR-0010 的 stored 列）；成功返回 True。

        建表两条都要做（ADR-0010 §6①）：CREATE STABLE IF NOT EXISTS（新建时带列）
        + ensure_stored_column()（已存在时 ALTER 补列）——
        只做前者的话，老库永远缺 stored，新写入的行也带上 NULL（静默失能）。
        失败只记 error 不抛。
        """
        conn: Optional[Any] = None
        try:
            conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
            cur = conn.cursor()
            cur.execute(self._tables.create_sql())
            ensure_stored_column(cur, self._tables)
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
    """逐点留痕：把本条报文的证据行写进事件表，返回成功条数。

    名字里的 "invalid" 是 ADR-0009 的历史包袱：ADR-0010 之后这里同时装
    可疑（kind=jump/frozen，stored=1，值已入库）与无效
    （kind=invalid/missing，stored=0，值没入库）—— 两者的落点完全一样，
    靠事件表的 stored 列区分。调用方不要在这里做区分。

    绝不抛异常、也绝不阻断入库：证据写失败只记 CRITICAL ——
      反过来把 8 个本来正确的点丢掉，是这个改造最不该发生的事
      （而且事件表与主表在同一个 TDengine 实例上，它单独坏掉的概率极低）。
    调用点在写主表之前（见 on_message）：先留痕再入库，
      这样"主表有 NULL 列"与"事件表有证据"不会因为顺序而错开。
    """
    items = list(invalid.values())
    if not items:
        return 0
    for item in items:
        LOGGER.warning(
            "[事件表] %s %s kind=%s stored=%d raw=%s: %s",
            ts, item.point, PointInvalidKind(item.kind).value,
            PointInvalidKind(item.kind).stored,
            "NULL" if item.raw is None else repr(item.raw), item.reason,
        )
    written = writer.write(ts, items)
    if written != len(items):
        LOGGER.critical(
            "[事件表] 证据写入不完整: %d/%d 条 —— 对应的留痕会缺"
            "（无效点会分不清「没采到」还是「被判无效」；可疑点会看不出标过），"
            "请查事件表连接: ts=%s points=%s",
            written, len(items), ts, [item.point for item in items],
        )
    return written


def _post_webhook(url: str, payload_json: str, timeout: float) -> bool:
    """把一条告警 POST 到 Webhook；返回是否送达（2xx）。

    绝不抛异常：推送失败只记 warning + 把 status 记成 failed，
    事件行与推送记录照常落库。理由与网关的补传判定同源 ——
    "通知"是附加动作，不能让它的失败影响数据主链路。
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
    """把告警事件落成可验证的动作：事件行 + 推送记录 + 结构化日志（+ 可选真实 Webhook）。

    推送被实现成三件可查的事：
        1. cems_alarm_push 里一条推送记录（channel/status/payload，与事件行同 ts，
           因此同样满足 (子表, ts) 幂等）；
        2. 一行 ALARM_PUSH ... 结构化日志（可 grep、可进日志采集）；
        3. channel == "webhook" 时：真的 POST 到 ALARM_PUSH_URL，
           成功记 status=sent、失败记 status=failed（都不影响事件落库）。

    边界：默认 channel 是 log（只落记录，无外部通道）。真实 Webhook 需对方提供
    接收地址（ALARM_PUSH_URL）；短信 / 环保平台仍未接入（ADR-0002 §6 未决 5）。

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

    恢复不成功不阻止启动：只是该测点的 OPEN 事件会从"当前"重新开始计（可能重复一次
    START，但 (子表, ts) 覆盖语义保证不会多出事件行）。所以失败只记 warning。
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


# ==================== 小时结算（§3.4 覆盖率三态结论） ====================

def settle_hour(
    writer: AlarmWriter,
    tables: AlarmTables,
    judge: AlarmJudge,
    hour_start: datetime,
) -> tuple[int, int]:
    """结算一个整点小时，返回 (写入的结论行数, 写入的事件行数)。

    折算值不落库，所以这里回读原始行、用契约函数重算（points.to_reference_o2()，
    仍然是唯一一份公式）；覆盖率分母 = 3600 / ALARM_POLL_INTERVAL = 720 条/小时。
    覆盖率不足判 insufficient，绝不判达标（§3.4：最危险的漏报形态是"缺数据被当达标"）。
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

    - 启动时补结算最近 backfill_hours 个已闭合小时（进程重启/首次上线不用等整点）
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


# ==================== 折算值（标干 → 基准氧含量） ====================
#
# 折算值分两层，职责不同，别混：
#   ① 数学层 reference_values()：调 points.to_reference_o2() 算折算浓度。
#      O2 >= 21% 时分母 <= 0，折算无物理意义 → 返回 nan（数学语义，也是判定口径）。
#   ② 协议层 transmit_reference_values()：调 points.to_transmit_value()，把 nan 编码成
#      HJ 212-2025 §8.1.1 d) 要求的哨兵值（+9999.99），有限折算值原样透出。
#   record_reference() 记录的是②（往外报的那一份）；抽样日志也打②。
#   判定/统计必须用①：哨兵值 9999.99 大于任何限值，拿它比限值必然误报。
#
# 折算公式只有一份实现（points.to_reference_o2），别处不许再推一遍：
#   - 调它的地方：本函数的 reference_values()、alarm_judge 的小时结算重算、
#     report_export 的导出 —— 都是调用，不是各自写一份公式
#   - 折算值不写库、不进寄存器（导出量，存了会漂）
#   - O2 >= 21% 时 to_reference_o2() 返回 nan（分母 <= 0，折算无物理意义），
#     这里原样透出，不绕过、不当 0；要出站再走②编码
#   - 展示层要用的限值与基准氧也是注入的（web_dashboard 注入给前端），前端同样不手抄

# 最近一条已成功入库数据的传输值快照（含时间戳）；读接口返回副本
LAST_REFERENCE: Final[dict[str, Any]] = {"ts": "", "values": {}}


def reference_values(values: dict[str, float]) -> dict[str, float]:
    """纯函数：把一条报文的标干浓度折算到基准氧含量下（不碰库、不改状态）。

    入参 values 的键 = MQTT 报文字段名（Dust / SO2 / NOx / O2，即 points.NAMES），
    返回值的键 = TDengine 列名（dust / so2 / nox，即 points.ZS_TARGETS），
    这样既能直接打日志给下游看，也能与库里存的标干值按列名对齐比较。

    公式（出自契约 points.to_reference_o2，本函数只做调用，不重写）：
        折算浓度 = 标干浓度 × (21 - 基准氧含量) / (21 - 实测氧含量)

    返回 nan 的情形：实测 O2 >= 21%（契约行为，别绕过、别当 0）。
       要往外传输时必须经 transmit_reference_values() 编码，不能直接把 nan 发出去。
    判定口径：超标判的是折算值，不是标干值（本函数不做告警判定）。
    """
    o2 = values[O2_FIELD_NAME]
    return {
        column: to_reference_o2(values[COLUMN_TO_NAME[column]], o2)
        for column in ZS_TARGETS
    }


def transmit_reference_values(values: dict[str, float]) -> dict[str, float]:
    """纯函数：把一条报文的折算值编码成可传输值（协议层，不碰库、不改状态）。

    这是本工程唯一的折算值协议编码点：
      - 数学层算不出来的（nan/±inf）→ 哨兵值 +9999.99（HJ 212-2025 §8.1.1 d)：
        无法计算折算浓度时按缺省数据类型的最大值传输，不是 0、不是实测值、不是报无效）
      - 算得出来的有限折算值 → 原样透出（不缩放、不舍入）

    编码规则的真源在契约 points.to_transmit_value() / ZS_UNAVAILABLE_SENTINEL，
    本函数只逐列调用，不自己写 if/else —— 免得协议细节在接入层再抄一份。

    出参键与 reference_values() 相同（TDengine 列名）；只用于出站/日志/展示，
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

    只在写入 TDengine 成功之后调用：进不了库的数据不配当"最近一条"。
    每条报文只调 reference_values() 一次（折算值全链路只在一处算），
    快照存的是传输编码后的值（下游/验证脚本看到的应与上报出去的一致）。

    math_references：调用方已经算好的数学层折算值（reference_values() 的结果）。
    告警判定要的正是这一份（O2 >= 21% 时是 nan），接入层算一次后同时喂给
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
    """取最近一条已入库数据的传输值（副本，键为 TDengine 列名 dust/so2/nox）。

    与上报出去的那一份一致：算不出来时是哨兵值 +9999.99，不是 nan。
    验证脚本或下游可以直接：
        from src.platform.subscriber_to_td import latest_reference_values
    返回副本，避免调用方改到进程内的快照。
    别拿它做达标判定/统计（哨兵值大于限值），判定请按条调用 reference_values()。
    """
    return dict(LAST_REFERENCE["values"])


def latest_reference_timestamp() -> str:
    """取最近一条已入库折算值对应的时间戳（没有则为空字符串）。"""
    return str(LAST_REFERENCE["ts"])


# ==================== MQTT 回调 ====================

def _run_quality_check(userdata: Any, values: Mapping[str, float]) -> list[PointInvalid]:
    """跑一次合理性判据（跳变/冻结），返回可疑证据；绝不抛异常、绝不改 values。

    - 判据实例优先取 userdata["quality_checker"]（每个订阅实例一份状态）；
      单测/未接线时退回模块级的 QUALITY_CHECKER —— "忘了接线"只会少判可疑，
      绝不会把数据判丢（这是本 ADR 里唯一允许的失败方向）。
    - 判据异常只记日志：它只是加一笔痕，绝不能拖垮入库与订阅。
    """
    checker = userdata.get("quality_checker") if isinstance(userdata, dict) else None
    if checker is None:
        checker = QUALITY_CHECKER
    try:
        return checker.check(values)
    except Exception:
        LOGGER.exception("[可疑点] 合理性判据异常（入库与订阅不受影响）")
        return []


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

    逐点化（ADR-0009）后的四条路径：
      1. 语法坏 → PayloadFormatError → STATS["rejected_messages"] += 1，整条拒收（不写库）；
      2. 语义坏 → 该点进事件表（kind=invalid/missing，stored=0）+ STATS["invalid_points"] += n，
         其余点照常入库（主表该列是 NULL，证据在事件表里）；
      3. 全部测点都语义坏 → 没有可写的列：只留痕、不写主表、也不落盘
         （落盘会永远补不进去，只会把队列堵死）；
      4. 入库失败（TDengine 不可用）→ 与以前一样落本地队列，等补传。

    数据合理性判据（ADR-0010）插在 2 与 3 之间，只加证据、不动数据：
      5. 可疑（kind=jump/frozen，stored=1）→ 该点照常入库 + 留一笔痕。
         这里绝不能像 invalid 那样把值从 values 里拿掉 ——
         跳变可能是真实的工况突变（设备启停），冻结可能是真实的停机恒值，
         丢掉等于掩盖真实事件（ADR-0010 §3）。
    """
    payload = msg.payload.decode("utf-8", errors="replace")
    STATS["received"] += 1

    # 先核对契约指纹（只记不拦）：网关与接入层不是同一版契约时，
    # 这条日志是唯一的可见信号 —— 两边都不报错，但同一列的含义可能已经不同。
    check_contract_version(payload)

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

    # ADR-0010：数据合理性判据（跳变 / 冻结）——
    #    它只产出"可疑"证据，values 一个点都不删（可疑照常入库）；
    #    只对过了量程检查的点判（超量程的原始值不是可信测量值）。
    #    判据状态在 userdata 里（每实例一份）；单测没接线时退回模块级的 QUALITY_CHECKER，
    #    这样"忘了接线"只会少判可疑，绝不会把数据判丢。
    suspects = _run_quality_check(userdata, values)
    if suspects:
        STATS["suspect_points"] += len(suspects)

    # 无效（stored=0）与可疑（stored=1）共用同一张事件表与同一次写入：
    # 区别只在 kind（⇒ stored 列）。逐点一次性写，避免"两条路径各写一次"造成计数与顺序分叉。
    # 键不会冲突：parse_payload 把每个点要么放进 values 要么放进 invalid，
    #    而可疑点只可能来自 values；万一真撞上，保留先到的那条（setdefault），不静默吞掉证据。
    evidence: dict[str, PointInvalid] = dict(invalid)
    for item in suspects:
        evidence.setdefault(item.point, item)
    if evidence:
        # 顺序：先把证据写下来，再写主表 —— 免得"主表已经有 NULL 列"而事件表还没落。
        STATS["invalid_points"] += len(invalid)
        try:
            record_invalid_points(userdata["point_invalid_writer"], ts, evidence)
        # 证据层失败绝不打断数据主链路（record_invalid_points 内部已记 CRITICAL）
        except Exception:
            LOGGER.exception("[事件表] 留痕异常（入库与订阅不受影响）")

    if not values:
        # 9 个点全判坏/全缺：没有可写的列（ADR-0009 §3 的"没有可写的列"）。
        # 这里不能走落盘队列：补传时同样一列都写不进去，只会永远堵在队首。
        LOGGER.warning(
            "本条报文的测点全部无效（无可写列），只留痕、不写主表也不落盘: %s",
            sorted(invalid),
        )
        return

    STATS["accepted"] += 1
    writer: TdWriter = userdata["writer"]
    spool: IngestSpool = userdata["spool"]
    if writer.write(ts, values):
        # 折算值 / 告警判定要 O2 与三个污染物同帧齐全（ADR-0002 §3.1）。
        #   逐点化后可能出现"整帧不齐"：这时跳过折算与判定，但该条仍然已经入库。
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
                # 打的是传输值：O2 >= 21% 时应当看到哨兵值，而不是 nan
                LOGGER.info(
                    "折算值抽样（第 %d 条，已按 HJ 212-2025 §8.1.1 d) 的规则处理："
                    "无法折算时传缺省类型最大值哨兵 %.2f；"
                    "注意这是【规则对齐】，本项目出站报文体仍是内部简化格式，不是 HJ212 报文）: "
                    "标干 ts=%s O2=%.4f -> %s",
                    STATS["accepted"],
                    ZS_UNAVAILABLE_SENTINEL,
                    ts,
                    values[O2_FIELD_NAME],
                    " ".join(f"{column}={references[column]:.4f}" for column in ZS_TARGETS),
                )
            # 排放超标告警：逐条判折算值（吃上面算好的数学层结果，不同帧不判、不重算公式）
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
        # 入库失败：先把原始报文落到本地队列，再正常返回。
        #   顺序不能反：必须"落盘成功"之后才允许返回 —— 返回即代表 paho 会发 PUBACK、
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


# ==================== 落盘队列补传 ====================

def drain_spool_once(
    writer: TdWriter,
    spool: IngestSpool,
    *,
    batch_limit: int = SPOOL_BATCH_LIMIT,
    point_invalid_writer: Optional[PointInvalidWriter] = None,
) -> int:
    """把落盘队列里的一批报文补传进 TDengine，返回本轮成功条数。

    语义（顺序与崩溃安全见 ingest_spool 头部）：
      - 取批（原子改名）→ 逐条 parse + 留痕 + writer.write → 收尾（未成功的放回队首、删段）
      - 第一条写失败就停：说明 TDengine 整体不可用，没必要把整批都试一遍（每次失败都带重连）；
        本条及之后（含未处理的尾部）原样放回，下一个节拍再试。
      - 解析不了的行走 CRITICAL 后丢弃：入队前已校验过，落盘内容本不该解析失败；真遇到
        说明队列文件被外部改坏，留着它只会永远卡住整个队列。
      - 补传不喂告警判据（AlarmJudge.on_sample 有时间水位保护，迟到旧样本会被当重复丢掉）；
        小时结算是从库里回读的，补传进去的行照样参与小时三态结论。
      - 补传不更新 LAST_REFERENCE 快照：那是"最近一条已入库数据的折算出口"，
        让迟到的旧样本把它往回拨没有意义。
      - 补传也要补留痕（ADR-0009 §4.1）：报文入队时若含无效点，那条证据同样得落库，
        否则"补传成功"的数据会出现 NULL 列而没有证据行。留痕写失败也算本条没补成功
        （放回队首，下一个节拍重试），避免留下"NULL 无证据"的行。
      - 补传不重跑合理性判据（ADR-0010）：判据吃的是"相邻样本"，
        而补传是迟到的旧样本（还可能与实时流交错），重跑会让进程内状态被带偏；
        而且可疑证据在收报文那一刻（on_message 写主表之前）就已经落库了，
        同一条报文重投也只是 (子表, ts) 覆盖 —— 不会漏，也不需要重判。
      - point_invalid_writer=None（单测里只关心主表路径时）跳过留痕。
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
    用 Event.wait 而不是 sleep：main 退出时置位就能立刻结束，不必等满一个节拍。
    """
    while not SPOOL_STOP.is_set():
        try:
            drain_spool_once(writer, spool, point_invalid_writer=point_invalid_writer)
        # 线程边界：任何异常都不能让补传线程静默死掉（下一轮继续）
        except Exception:
            LOGGER.exception("[落盘队列] 补传线程异常（下一轮继续）")
        SPOOL_STOP.wait(SPOOL_RETRY_INTERVAL)


# ==================== 主流程 ====================

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
    # 0c. 合理性判据（ADR-0010）：把生效的那一组阈值打进启动日志 ——
    #     "判据到底开没开、阈值是多少"必须一眼可查，否则事后没法解释事件表里的行。
    if QUALITY_CONFIG.enable:
        LOGGER.info(
            "数据合理性判据已启用（ADR-0010）: 跳变阈值 = 量程宽度×%.4g；"
            "冻结 = 连续 %d 个样本落在 量程宽度×%.4g 的容差带内；"
            "两者都只留痕不丢数（可疑点照常入库，事件表 stored=1）",
            QUALITY_CONFIG.jump_ratio, QUALITY_CONFIG.freeze_samples,
            QUALITY_CONFIG.freeze_tolerance_ratio,
        )
    else:
        LOGGER.warning(
            "QUALITY_CHECK_ENABLE=0：本次不判跳变/冻结（行为与不实现 ADR-0010 一致，"
            "事件表不会再出现 kind=jump/frozen）"
        )

    # 1. 初始化 TDengine（失败不退出：收到数据时会自动重连重试）
    writer = TdWriter()
    if not writer.connect():
        LOGGER.error("首次连接 TDengine 失败，将在收到数据时自动重试")

    # 1a. 无效点事件表（ADR-0009 §4.1：逐点化之后的"证据层"）
    #     失败不阻止启动：第一次要写证据时会自动重连重试；
    #     它坏了只影响"留痕"，绝不该把主链路（入库）一起拖住。
    point_invalid_writer = PointInvalidWriter(POINT_INVALID_TABLES)
    if not point_invalid_writer.connect():
        LOGGER.error("首次连接无效点事件表失败，将在需要写入时自动重试")

    # 1b. 落盘队列：目录可写检查 + 补传线程
    #     目录必须每实例一份（多设备部署的关键约束，见配置区）。这一行是排查
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
        # 逐点化（ADR-0009）：无效测点的证据写入器，按 (子表, ts) 幂等
        "point_invalid_writer": point_invalid_writer,
        # 合理性判据（ADR-0010）：跳变/冻结的进程内状态（每实例一份）
        "quality_checker": QUALITY_CHECKER,
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
