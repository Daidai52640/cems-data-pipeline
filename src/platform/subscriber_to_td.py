# -*- coding: utf-8 -*-
"""平台接入：订阅 MQTT 上送报文，解析 8 个烟气测点后写入 TDengine 时序库 cems.cems_data 超级表。"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Final, Optional

import paho.mqtt.client as mqtt
import taosrest

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

# ---- 测点定义表：(MQTT 字段名, TDengine 列名)，顺序即入库列顺序 ----
# 只入库这张表里的数值测点，Flag 等标记字段自动跳过。
POINTS: Final[tuple[tuple[str, str], ...]] = (
    ("SO2",      "so2"),        # 二氧化硫
    ("NOx",      "nox"),        # 氮氧化物
    ("Flow",     "flow"),       # 烟气流量
    ("Dust",     "dust"),       # 颗粒物
    ("O2",       "o2"),         # 氧含量
    ("Temp",     "temp"),       # 烟气温度
    ("Humidity", "humidity"),   # 烟气湿度
    ("Pressure", "pressure"),   # 烟气压力
)
TD_COLUMNS: Final[tuple[str, ...]] = tuple(column for _, column in POINTS)
TS_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"

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
    """
    parts = payload.split()
    if len(parts) < 3:
        raise ValueError(f"报文长度不足: {payload!r}")

    ts = parse_timestamp(f"{parts[0]} {parts[1]}")

    names = {name for name, _ in POINTS}
    data: dict[str, float] = {}
    for item in parts[2:]:
        key, sep, value = item.partition("=")
        if not sep or key not in names:
            continue                    # 跳过 Flag 等非测点字段
        try:
            data[key] = float(value)
        except ValueError as exc:
            raise ValueError(f"测点 {key} 数值非法: {value!r}") from exc

    missing = [name for name, _ in POINTS if name not in data]
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

        CREATE STABLE IF NOT EXISTS 不会改动已存在的表，所以老版本建的 4 列超级表
        必须靠 ALTER STABLE 把新测点列补上；历史数据不丢，新列的旧值为 NULL。
        """
        cur.execute(f"DESCRIBE {TD_DB}.{TD_STABLE}")
        existing = {str(row[0]).lower() for row in cur.fetchall()}
        for column in TD_COLUMNS:
            if column not in existing:
                cur.execute(f"ALTER STABLE {TD_DB}.{TD_STABLE} ADD COLUMN {column} FLOAT")
                LOGGER.info("超级表新增测点列: %s FLOAT", column)

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
        numbers = ", ".join(f"{values[name]}" for name, _ in POINTS)   # 与 TD_COLUMNS 同序
        sql = (
            f"INSERT INTO {TD_DB}.{TD_PLANT} USING {TD_DB}.{TD_STABLE} "
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


# ==================== 5. MQTT 回调 ====================

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

    try:
        ts, values = parse_payload(payload)
    except ValueError as exc:
        LOGGER.error("报文解析失败: %s | 原始报文: %s", exc, payload)
        return

    writer: TdWriter = userdata["writer"]
    if writer.write(ts, values):
        # 高频成功降到 debug；只在排查数据问题时才需要开
        LOGGER.debug("已入库: %s %s", ts, " ".join(f"{k}={v}" for k, v in values.items()))


# ==================== 6. 主流程 ====================

def main() -> None:
    """连 TDengine → 连 MQTT → 订阅主题并持续入库。"""
    setup_logging()

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
