# -*- coding: utf-8 -*-
"""
MQTT 网关 v2（gateway）—— 加了断网续传
功能：复刻数采仪采集逻辑——
      Modbus 读仿真设备寄存器 → 换算（÷10）→ 加时间戳 → 发 MQTT
      ★ 新增：断网时数据写本地缓存文件，网络恢复后按顺序补传（数据一条不丢）
运行：python gateway.py
      （需要 modbus_server.py 在跑 + subscriber.py 在收）
"""

import os
import time
import paho.mqtt.client as mqtt
from pymodbus.client import ModbusTcpClient

# ========== 配置 ==========
MODBUS_HOST = "localhost"
MODBUS_PORT = 5020
MODBUS_UNIT = 1          # 从站地址

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "cems/plant1/data"

SCALE = 10               # 放大系数

REG_SO2 = 0
REG_NOX = 1
REG_FLOW = 2

CACHE_FILE = r"F:\deepseek学习助手\cems-data-pipeline\data\cache.jsonl"   # ★ 断网缓存文件


def read_device(modbus_client):
    """读 Modbus 设备：读保持寄存器 0~2，返回换算后的实际值"""
    rr = modbus_client.read_holding_registers(address=REG_SO2, count=3, slave=MODBUS_UNIT)
    if rr.isError():          # 读取失败（设备没响应）
        return None
    so2 = rr.registers[REG_SO2] / SCALE
    nox = rr.registers[REG_NOX] / SCALE
    flow = rr.registers[REG_FLOW] / SCALE
    return so2, nox, flow


# ★★★ 断网续传：两个新函数 ★★★

def save_to_cache(payload):
    """断网时：数据写本地缓存文件（一行一条，追加写）"""
    with open(CACHE_FILE, "a", encoding="utf-8") as f:
        f.write(payload + "\n")
    print(f"  [缓存] 断网，数据已存本地: {payload}")


def resend_cache(mqtt_client):
    """恢复连接时：把缓存按时间顺序补传，传完清空文件"""
    if not os.path.exists(CACHE_FILE):
        return 0
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()
    if not lines:
        return 0

    for line in lines:
        payload = line.strip()
        if payload:
            mqtt_client.publish(MQTT_TOPIC, payload, qos=1)
            print(f"  [补传] {payload}")
    # 补传完清空缓存（生产上要等确认收到再清，教学版简化）
    open(CACHE_FILE, "w", encoding="utf-8").close()
    print(f"  [补传完成] 共补传 {len(lines)} 条，缓存已清空")
    return len(lines)


def on_connect(client, userdata, flags, reason_code, properties):
    """连接/重连成功时触发 → 先补传缓存，再继续实时发"""
    if reason_code == 0:
        print(f"[MQTT已连接] {MQTT_HOST}:{MQTT_PORT}")
        resend_cache(client)          # ★ 重连成功 → 补传断网期间缓存的数据
    else:
        print(f"[MQTT连接失败] reason_code={reason_code}")


def main():
    # 1. 连 Modbus 设备
    modbus_client = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    if not modbus_client.connect():
        print(f"[错误] 连不上 Modbus 设备 {MODBUS_HOST}:{MODBUS_PORT}，请确认 modbus_server.py 在跑")
        return

    # 2. 连 MQTT Broker
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)# type: ignore
    mqtt_client.on_connect = on_connect
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()   # 后台跑消息循环（★ 断线自动重连靠它）
    print(f"[Modbus已连接] {MODBUS_HOST}:{MODBUS_PORT} 从站{MODBUS_UNIT}")
    print(f"[断网续传已启用] 缓存文件: {CACHE_FILE}")

    # 3. 主循环：读 → 换算 → 加时间戳 →（在线直发 / 断网缓存）
    count = 0
    try:
        while True:
            data = read_device(modbus_client)
            if data is None:
                print("[告警] 读Modbus失败（设备无响应），跳过本轮")
            else:
                so2, nox, flow = data
                count += 1
                payload = f"{time.strftime('%Y-%m-%d %H:%M:%S')} SO2={so2} NOx={nox} Flow={flow} Flag=N"

                if mqtt_client.is_connected():
                    # ★ 在线：直接发
                    mqtt_client.publish(MQTT_TOPIC, payload, qos=1)
                    print(f"[第{count}条] 在线直发: {payload}")
                else:
                    # ★ 断网：写缓存，数据不丢
                    save_to_cache(payload)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n网关已停止")
        modbus_client.close()
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
