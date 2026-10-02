# -*- coding: utf-8 -*-
"""平台接入：订阅 MQTT 上送报文，解析 9 个烟气测点后写入 TDengine 时序库 cems.cems_data 超级表。

★ 排放超标告警（2026-10-02 接入，依据 docs/adr/0002-告警判据选型.md 方案乙）：
  入库成功 → 算折算值（全链路唯一一处）→ **接入层逐条判折算值超限** → 落告警事件表/推送记录。
  判定不改拒收逻辑、不改契约、不加服务、不加依赖；
  折算值仍然不给 cems_data 建列，事件表里存的是**判定快照**（判定证据，不是数据冗余列）。
"""

from __future__ import annotations

import logging
import math
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final, Iterable, Optional, Sequence

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
    hour_start_of,
    normalize_ts,
)
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
SQL_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

# ---- 测点契约 ----
# 测点定义（MQTT 字段名 / TDengine 列名 / 量程）统一来自 src/common/points.py：
#   NAMES   报文里应该出现的字段名集合
#   COLUMNS 入库列名，顺序即入库列顺序
#   RANGES  量程白名单：超出量程、负数、NaN、inf 一律拒收
#           （NaN/inf 会让 TDengine 报 syntax error，整行连其余正常测点一起被拒）
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

# ---- 收发/拒收计数（把上游数据质量问题量化出来）----
STATS: Final[dict[str, int]] = {"received": 0, "accepted": 0, "rejected": 0}

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
# 判据统计日志周期（条）：打印 已判/重投跳过/事件/无效样本 计数，
# 让人一眼看出"判据是活的"（ADR-0002 §2.2 2) 可观测性）。
ALARM_STATS_LOG_SAMPLES: Final[int] = int(os.getenv("ALARM_STATS_LOG_SAMPLES", "60"))
# 整点后等多久再结算上一个小时：等网关补传收尾（补传实测最大滞后 32.1s，
# 取 60s 与 web 侧 REPORT_INFLIGHT_GRACE_SECONDS 同一依据）。
ALARM_SETTLE_DELAY_SECONDS: Final[float] = float(
    os.getenv("ALARM_SETTLE_DELAY_SECONDS", "60")
)

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("platform.subscriber_to_td")


# ==================== 2. 日志 ====================

def setup_logging() -> None:
    """初始化日志：控制台输出，级别由 LOG_LEVEL 统一控制。"""
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        force=True,
    )


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
            raise ValueError(
                f"{label} 只能由字母、数字、下划线组成且以字母或下划线开头: {value!r}"
            )
    # 告警侧：判据参数自检 + 表名同样要能安全拼进 SQL
    ALARM_CONFIG.validate()
    for label, value in (
        ("ALARM_EVENT_STABLE", ALARM_TABLES.event_stable),
        ("ALARM_VERDICT_STABLE", ALARM_TABLES.verdict_stable),
        ("ALARM_PUSH_STABLE", ALARM_TABLES.push_stable),
    ):
        if not SQL_NAME_RE.match(value):
            raise ValueError(
                f"{label} 只能由字母、数字、下划线组成且以字母或下划线开头: {value!r}"
            )


def parse_timestamp(text: str) -> str:
    """校验时间戳格式（顺带挡掉拼进 SQL 的非法字符串），返回原字符串。"""
    try:
        datetime.strptime(text, TS_FORMAT)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"时间戳格式非法: {text!r}") from exc
    return text


def parse_payload(payload: str) -> tuple[str, dict[str, float]]:
    """把 MQTT 报文解析成 (时间戳, {MQTT字段名: 数值})；格式不符抛 ValueError。

    报文格式: "2026-09-10 12:00:00 SO2=35.2 NOx=18.5 ... Pressure=101.3 Flag=N"

    每个测点都要过两关：先 math.isfinite（挡 NaN/inf），再比量程白名单
    （挡负数、超量程）。任一测点不合格就整条拒收 —— 因为 NaN/inf 会让
    TDengine 报 syntax error，整行连其余正常测点一起丢，不如提前拦下。
    """
    parts = payload.split()
    if len(parts) < 3:
        raise ValueError(f"报文长度不足: {payload!r}")

    ts = parse_timestamp(f"{parts[0]} {parts[1]}")

    names = set(NAMES)
    data: dict[str, float] = {}
    for item in parts[2:]:
        key, sep, value = item.partition("=")
        if not sep or key not in names:
            continue                    # 跳过 Flag 等非测点字段
        try:
            number = float(value)
        except ValueError as exc:
            raise ValueError(f"测点 {key} 数值非法: {value!r}") from exc
        if not math.isfinite(number):
            raise ValueError(f"测点 {key} 不是有限数值（NaN/inf）: {value!r}")
        low, high = POINT_RANGES[key]
        if not low <= number <= high:
            raise ValueError(f"测点 {key} 超出量程 [{low}, {high}]: {number}")
        data[key] = number

    missing = [name for name in NAMES if name not in data]
    if missing:
        raise ValueError(f"缺少测点 {missing}: {payload!r}")

    return ts, data


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

        显式列出列名、列序由 POINTS 统一决定，避免报文顺序与建表顺序漂移导致写错列。
        """
        if self._cur is None:
            return False
        # 显式列出列名时，主时间戳列 ts 必须一起列出，否则 TDengine 报
        # "Primary timestamp column should not be null"
        columns = ", ".join(("ts", *TD_COLUMNS))
        numbers = ", ".join(f"{values[point.name]}" for point in POINTS)  # 与 TD_COLUMNS 同序
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


def publish_alarm_events(
    writer: AlarmWriter,
    tables: AlarmTables,
    events: Sequence[AlarmEvent],
    channel: str,
) -> tuple[int, int]:
    """把告警事件落成**可验证的动作**：事件行 + 推送记录 + 结构化日志。

    一期没有外部推送通道（没有邮件/Webhook 服务），所以"推送"被实现成两件可查的事：
        1. `cems_alarm_push` 里一条推送记录（`channel`/`status`/`payload`，与事件行同 ts，
           因此同样满足 `(子表, ts)` 幂等）；
        2. 一行 `ALARM_PUSH ...` 结构化日志（可 grep、可进日志采集）。
    ⚠️ 真实外部推送（Webhook / 短信 / 环保平台）**未接入**，属待接项（ADR-0002 §6 未决 5）。

    返回 (成功的事件行数, 成功的推送记录数)。
    """
    if not events:
        return 0, 0
    event_sqls = [tables.event_insert_sql(event) for event in events]
    push_sqls = [tables.push_insert_sql(event, channel) for event in events]
    for event in events:
        LOGGER.info(event.log_text(channel))
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
    """收到消息 → 解析 → 入库；解析或入库失败只记日志，绝不抛异常打断订阅。"""
    payload = msg.payload.decode("utf-8", errors="replace")
    STATS["received"] += 1

    try:
        ts, values = parse_payload(payload)
    except ValueError as exc:
        # 故意拒收（格式错、非有限值、超量程）：计数并记下原因，方便看上游数据质量
        STATS["rejected"] += 1
        LOGGER.warning(
            "报文已拒收（累计 %d 条）: %s | 原始报文: %s",
            STATS["rejected"], exc, payload,
        )
        return

    STATS["accepted"] += 1
    writer: TdWriter = userdata["writer"]
    if writer.write(ts, values):
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

    # 1. 初始化 TDengine（失败不退出：收到数据时会自动重连重试）
    writer = TdWriter()
    if not writer.connect():
        LOGGER.error("首次连接 TDengine 失败，将在收到数据时自动重试")

    # 1b. 初始化告警链路：判据状态从事件表折叠恢复 + 独立的告警表写入器
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
        "writer": writer, "judge": judge, "alarm_writer": alarm_writer,
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
        try:
            client.disconnect()
        except Exception as exc:
            LOGGER.debug("断开 MQTT 时出错（忽略）: %s", exc)
        writer.close()
        alarm_writer.close()


if __name__ == "__main__":
    main()
