# -*- coding: utf-8 -*-
"""
MQTT 发布端（发送消息）
功能：连接 EMQX，往主题发一条消息，然后退出
运行：python publisher.py
"""

import paho.mqtt.client as mqtt
import random,time

# ========== 配置 ==========
BROKER = "localhost"   # EMQX 地址（本机）
PORT = 1883            # MQTT 端口
TOPIC = "cems/test"    # 发布到哪个主题

# ========== 回调函数 ==========

def on_connect(client, userdata, flags, reason_code, properties):
    """连接成功时被调用"""
    if reason_code == 0:
        print(f"[连接成功] 已连上 {BROKER}:{PORT}")
        # 连接成功后发消息
        payload = "2026-09-09 21:30:00 SO2=35.2mg/m3 NOx=18.5mg/m3 Flag=N"
        result = client.publish(TOPIC, payload, qos=1)
        print(f"[已发布] 主题: {TOPIC}")
        print(f"  内容: {payload}")
        print(f"  发布结果: {result.rc}（0=成功）")
        # 发完断开
        client.disconnect()
    else:
        print(f"[连接失败] reason_code={reason_code}")

# ========== 主流程 ==========
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect

print("正在连接 EMQX...")
client.connect(BROKER, PORT, keepalive=60)

# 事件循环（阻塞，等 on_connect 执行完发布后断开）
client.loop_forever()
print("发布端已退出")
