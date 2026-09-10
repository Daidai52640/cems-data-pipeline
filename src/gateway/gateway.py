# -*- coding: utf-8 -*-
"""数采网关：轮询 Modbus 设备读数并加时间戳经 MQTT 上送，断网时写本地缓存、恢复后自动补传。"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Final, Optional

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusTcpClient

# ==================== 1. 配置区（要改参数只动这里） ====================

# ---- Modbus 设备（对应 modbus_server.py）----
MODBUS_HOST: Final[str] = "localhost"
MODBUS_PORT: Final[int] = 5020
MODBUS_UNIT: Final[int] = 1            # 从站地址

# ---- 寄存器地图（必须与 modbus_server.py 保持一致）----
REG_SO2: Final[int] = 0
REG_NOX: Final[int] = 1
REG_FLOW: Final[int] = 2
REG_COUNT: Final[int] = 3              # 一次读 3 个寄存器
SCALE: Final[int] = 10                 # 还原系数：352 → 35.2

# ---- MQTT（对应 EMQX）----
MQTT_HOST: Final[str] = "localhost"
MQTT_PORT: Final[int] = 1883
MQTT_TOPIC: Final[str] = "cems/plant1/data"
MQTT_QOS: Final[int] = 1
MQTT_KEEPALIVE: Final[int] = 60
MQTT_CLIENT_ID: Final[str] = "cems-gateway-plant1"

# ---- 采集节奏与重连 ----
POLL_INTERVAL: Final[float] = 5.0           # 轮询周期（秒）
MODBUS_RETRY_INTERVAL: Final[float] = 5.0   # Modbus 断线后的重连间隔（秒）

# ---- 断网缓存文件：相对项目根目录推导，换机器/换路径都不用改 ----
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
CACHE_FILE: Final[Path] = PROJECT_ROOT / "data" / "cache.jsonl"

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("gateway")


# ==================== 2. 日志 ====================

def setup_logging() -> None:
    """初始化日志：控制台输出，级别由 LOG_LEVEL 统一控制。"""
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        force=True,
    )


# ==================== 3. 设备读取 ====================

def _read_holding_registers(client: ModbusTcpClient, address: int, count: int) -> Any:
    """读保持寄存器；兼容 pymodbus 新老版本（老版用 slave=，新版改名 device_id=）。"""
    try:
        return client.read_holding_registers(address=address, count=count, slave=MODBUS_UNIT)
    except TypeError:
        return client.read_holding_registers(address=address, count=count, device_id=MODBUS_UNIT)


def read_device(client: ModbusTcpClient) -> Optional[tuple[float, float, float]]:
    """读设备 3 个保持寄存器并换算成真实值，任何失败都返回 None（不抛异常）。"""
    try:
        response = _read_holding_registers(client, REG_SO2, REG_COUNT)
    except Exception as exc:
        LOGGER.error("读 Modbus 设备异常: %s", exc)
        return None

    if response is None or response.isError():
        LOGGER.error("读 Modbus 设备失败（设备无响应或返回异常码）: %s", response)
        return None

    try:
        return (
            response.registers[REG_SO2] / SCALE,
            response.registers[REG_NOX] / SCALE,
            response.registers[REG_FLOW] / SCALE,
        )
    except (IndexError, TypeError) as exc:
        LOGGER.error("Modbus 返回数据不完整: %s", exc)
        return None


def close_modbus(client: Optional[ModbusTcpClient]) -> None:
    """安全关闭 Modbus 连接，忽略关闭过程中的异常。"""
    if client is None:
        return
    try:
        client.close()
    except Exception as exc:
        LOGGER.debug("关闭 Modbus 连接时出错（忽略）: %s", exc)


def connect_modbus() -> Optional[ModbusTcpClient]:
    """连接 Modbus 设备；失败返回 None，由主循环稍后重试。"""
    client = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    try:
        if client.connect():
            LOGGER.info("Modbus 已连接: %s:%d 从站%d", MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT)
            return client
        LOGGER.error(
            "连不上 Modbus 设备 %s:%d（确认 modbus_server.py 在跑），%.0f 秒后重试",
            MODBUS_HOST, MODBUS_PORT, MODBUS_RETRY_INTERVAL,
        )
    except Exception as exc:
        LOGGER.error("Modbus 连接异常: %s", exc)
    close_modbus(client)
    return None


# ==================== 4. 断网续传（★ 核心逻辑，勿改动） ====================

def save_to_cache(payload: str) -> bool:
    """断网时把一条数据追加写入本地缓存文件；写入失败返回 False 并记 error。"""
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with CACHE_FILE.open("a", encoding="utf-8") as fp:
            fp.write(payload + "\n")
        LOGGER.info("[缓存] 断网，数据已存本地: %s", payload)
        return True
    except OSError as exc:
        LOGGER.error("[缓存] 写入本地失败，本条数据丢失: %s | %s", exc, payload)
        return False


def publish(client: mqtt.Client, payload: str, tag: str) -> bool:
    """按 QoS=1 发布一条消息，返回是否成功；失败只记日志不抛异常。"""
    try:
        info = client.publish(MQTT_TOPIC, payload, qos=MQTT_QOS)
    except Exception as exc:
        LOGGER.error("[%s] 发布异常: %s | %s", tag, exc, payload)
        return False

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        LOGGER.error("[%s] 发布失败 rc=%s | %s", tag, info.rc, payload)
        return False

    LOGGER.debug("[%s] %s", tag, payload)   # 高频成功日志降到 debug
    return True


def resend_cache(client: mqtt.Client) -> int:
    """重连成功后按顺序补传缓存；全部成功才清空缓存，失败的行留在文件里下次再补。"""
    if not CACHE_FILE.exists():
        return 0

    try:
        pending = [line.strip() for line in CACHE_FILE.read_text(encoding="utf-8").splitlines()]
    except OSError as exc:
        LOGGER.error("[补传] 读取缓存文件失败: %s", exc)
        return 0

    pending = [line for line in pending if line]
    if not pending:
        return 0

    failed: list[str] = []
    for payload in pending:
        if not publish(client, payload, tag="补传"):
            failed.append(payload)

    sent = len(pending) - len(failed)
    if sent:
        LOGGER.info("[补传完成] 共补传 %d 条", sent)

    try:
        if failed:
            # 有失败的行：保留在缓存里等下次重连继续补，避免数据丢失
            CACHE_FILE.write_text("\n".join(failed) + "\n", encoding="utf-8")
            LOGGER.error("[补传未完成] %d 条仍留在缓存等待重试", len(failed))
        else:
            # 补传完清空缓存（教学版简化：生产上要等 broker 确认后再清）
            CACHE_FILE.write_text("", encoding="utf-8")
    except OSError as exc:
        LOGGER.error("[补传] 回写缓存文件失败: %s", exc)

    return sent


# ==================== 5. MQTT 回调 ====================

def on_connect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    """连接/重连成功 → 先补传断网期间缓存的数据，再继续实时发送。"""
    if reason_code == 0:
        LOGGER.info("MQTT 已连接: %s:%d", MQTT_HOST, MQTT_PORT)
        resend_cache(client)          # ★ 重连成功 → 补传缓存
    else:
        LOGGER.error("MQTT 连接失败 reason_code=%s，数据将写入本地缓存", reason_code)


def on_disconnect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    """连接断开 → 提示进入断网缓存模式（后台 loop 会自动重连）。"""
    if reason_code != 0:
        LOGGER.error("MQTT 连接断开 reason_code=%s，后续数据写入本地缓存", reason_code)


# ==================== 6. 主流程 ====================

def build_payload(so2: float, nox: float, flow: float) -> str:
    """把三个测点值组装成上送报文：时间戳 + 测点 + 数据标记。"""
    return (
        f"{time.strftime('%Y-%m-%d %H:%M:%S')} "
        f"SO2={so2} NOx={nox} Flow={flow} Flag=N"
    )


def main() -> None:
    """网关主流程：连 MQTT → 连设备 → 循环"读→换算→加时间戳→（直发/缓存）"。"""
    setup_logging()
    LOGGER.info(
        "网关启动: 设备 %s:%d → MQTT %s:%d 主题 %s (QoS=%d)",
        MODBUS_HOST, MODBUS_PORT, MQTT_HOST, MQTT_PORT, MQTT_TOPIC, MQTT_QOS,
    )
    LOGGER.info("断网续传已启用，缓存文件: %s", CACHE_FILE)

    # ---- 连 MQTT：connect_async + loop_start，broker 暂时不可用也会后台自动重连 ----
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    try:
        mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
        mqtt_client.loop_start()
    except Exception as exc:
        LOGGER.error("MQTT 初始化失败: %s，数据将先写本地缓存", exc)

    # ---- 连 Modbus：连不上不退出进程，进主循环后持续重试 ----
    modbus_client = connect_modbus()

    count = 0
    try:
        while True:
            if modbus_client is None:
                time.sleep(MODBUS_RETRY_INTERVAL)
                modbus_client = connect_modbus()
                continue

            data = read_device(modbus_client)
            if data is None:
                # 读失败：重建连接后重试，不退出进程
                LOGGER.error("本轮采集失败，重建 Modbus 连接")
                close_modbus(modbus_client)
                modbus_client = None
                time.sleep(MODBUS_RETRY_INTERVAL)
                continue

            so2, nox, flow = data
            count += 1
            payload = build_payload(so2, nox, flow)

            if mqtt_client.is_connected():
                # ★ 在线：直接发；发布失败也按断网处理落盘续传
                if not publish(mqtt_client, payload, tag=f"第{count}条 在线直发"):
                    save_to_cache(payload)
            else:
                # ★ 断网：写缓存，数据不丢
                save_to_cache(payload)

            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        LOGGER.info("网关已停止（Ctrl+C）")
    except Exception:
        LOGGER.exception("网关异常退出")
    finally:
        close_modbus(modbus_client)
        try:
            mqtt_client.disconnect()
        except Exception as exc:
            LOGGER.debug("断开 MQTT 时出错（忽略）: %s", exc)
        mqtt_client.loop_stop()


if __name__ == "__main__":
    main()
