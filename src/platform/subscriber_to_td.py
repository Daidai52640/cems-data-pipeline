# -*- coding: utf-8 -*-
"""平台接入：订阅 MQTT 上送报文，解析 9 个烟气测点后写入 TDengine 时序库 cems.cems_data 超级表。"""

from __future__ import annotations

import logging
import math
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Optional

import paho.mqtt.client as mqtt
import taosrest

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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


def record_reference(ts: str, values: dict[str, float]) -> dict[str, float]:
    """算一次折算值、编码成传输值，并记成"最近一条"快照；返回本次结果。

    只在写入 TDengine **成功之后**调用：进不了库的数据不配当"最近一条"。
    每条报文只调 reference_values() 一次（折算值全链路只在一处算），
    快照存的是**传输编码后**的值（下游/验证脚本看到的应与上报出去的一致）。
    """
    snapshot = transmit_reference_values(values)
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
        # 写入成功后算折算值（全链路唯一一处），并留一份"最近值"给下游/验证脚本
        references = record_reference(ts, values)
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

    # 2. 连 MQTT（用 userdata 把写入器传给回调）
    # clean_session=False → 持久会话，broker 为离线期间的消息排队（配合固定 client_id）
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        clean_session=MQTT_CLEAN_SESSION,
    )
    client.user_data_set({"writer": writer})
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


if __name__ == "__main__":
    main()
