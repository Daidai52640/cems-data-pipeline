# -*- coding: utf-8 -*-
"""平台接入：订阅 MQTT 上送报文，解析测点后写入 TDengine 时序库 cems.cems_data 超级表。"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Final, Optional

import paho.mqtt.client as mqtt
import taosrest

# ==================== 1. 配置区（要改参数只动这里） ====================

# ---- MQTT（对应 EMQX）----
BROKER: Final[str] = "localhost"
PORT: Final[int] = 1883
TOPIC: Final[str] = "cems/plant1/data"
MQTT_QOS: Final[int] = 1
MQTT_KEEPALIVE: Final[int] = 60
MQTT_CLIENT_ID: Final[str] = "cems-subscriber-plant1"

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = "http://localhost:6041"
TD_USER: Final[str] = "root"
TD_PASS: Final[str] = "taosdata"
TD_DB: Final[str] = "cems"
TD_STABLE: Final[str] = "cems_data"        # 超级表（模板）
TD_PLANT: Final[str] = "plant1"            # 标签：厂区（同时也用作子表名）
TD_DEVICE: Final[str] = "device1"          # 标签：设备号
TD_KEEP_DAYS: Final[int] = 365             # 数据保留 1 年
TD_DURATION_DAYS: Final[int] = 30          # 每 30 天一个分片

# ---- 只入库约定的数值测点，Flag 等标记字段跳过 ----
MEASURE_POINTS: Final[tuple[str, ...]] = ("SO2", "NOx", "Flow")
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


def parse_payload(payload: str) -> tuple[str, float, float, float]:
    """把 MQTT 报文解析成 (时间戳, so2, nox, flow)；格式不符抛 ValueError。

    报文格式: "2026-09-10 12:00:00 SO2=35.2 NOx=18.5 Flow=95.3 Flag=N"
    """
    parts = payload.split()
    if len(parts) < 3:
        raise ValueError(f"报文长度不足: {payload!r}")

    ts = parse_timestamp(f"{parts[0]} {parts[1]}")

    data: dict[str, float] = {}
    for item in parts[2:]:
        key, sep, value = item.partition("=")
        if not sep or key not in MEASURE_POINTS:
            continue                    # 跳过 Flag 等非测点字段
        try:
            data[key] = float(value)
        except ValueError as exc:
            raise ValueError(f"测点 {key} 数值非法: {value!r}") from exc

    missing = [key for key in MEASURE_POINTS if key not in data]
    if missing:
        raise ValueError(f"缺少测点 {missing}: {payload!r}")

    return ts, data["SO2"], data["NOx"], data["Flow"]


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
                f"(ts TIMESTAMP, so2 FLOAT, nox FLOAT, flow FLOAT) "
                f"TAGS (plant NCHAR(20), device NCHAR(20))"
            )
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

    def write(self, ts: str, so2: float, nox: float, flow: float) -> bool:
        """写入一条数据；首次失败自动重连重试一次，仍失败返回 False。"""
        if not self.ready and not self.connect():
            return False

        if self._insert(ts, so2, nox, flow):
            return True

        LOGGER.error("入库失败，重连 TDengine 后重试一次")
        if self.connect() and self._insert(ts, so2, nox, flow):
            return True
        return False

    def _insert(self, ts: str, so2: float, nox: float, flow: float) -> bool:
        """执行一次插入：自动建子表（USING 超级表模板 + TAGS 标签）。"""
        if self._cur is None:
            return False
        sql = (
            f"INSERT INTO {TD_DB}.{TD_PLANT} USING {TD_DB}.{TD_STABLE} "
            f"TAGS ('{TD_PLANT}', '{TD_DEVICE}') "
            f"VALUES ('{ts}', {so2}, {nox}, {flow})"
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
    """连接/重连成功 → 订阅数据主题。"""
    if reason_code != 0:
        LOGGER.error("MQTT 连接失败 reason_code=%s", reason_code)
        return

    LOGGER.info("MQTT 已连接: %s:%d", BROKER, PORT)
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
        ts, so2, nox, flow = parse_payload(payload)
    except ValueError as exc:
        LOGGER.error("报文解析失败: %s | 原始报文: %s", exc, payload)
        return

    writer: TdWriter = userdata["writer"]
    if writer.write(ts, so2, nox, flow):
        LOGGER.debug("已入库: %s SO2=%s NOx=%s Flow=%s", ts, so2, nox, flow)   # 高频成功降到 debug


# ==================== 6. 主流程 ====================

def main() -> None:
    """连 TDengine → 连 MQTT → 订阅主题并持续入库。"""
    setup_logging()

    # 1. 初始化 TDengine（失败不退出：收到数据时会自动重连重试）
    writer = TdWriter()
    if not writer.connect():
        LOGGER.error("首次连接 TDengine 失败，将在收到数据时自动重试")

    # 2. 连 MQTT（用 userdata 把写入器传给回调）
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
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
