# -*- coding: utf-8 -*-
"""
MQTT 订阅端（接收消息）
功能：连接 EMQX，订阅主题，收到消息就打印
运行：python subscriber.py
"""

import paho.mqtt.client as mqtt

# ========== 配置 ==========
BROKER = "localhost"   # EMQX 地址（本机）
PORT = 1883            # MQTT 端口（EMQX 映射的）
TOPIC = "cems/test"    # 订阅的主题

# ========== 回调函数 ==========

def on_connect(client, userdata, flags, reason_code, properties):
    """连接成功时被调用"""
    if reason_code == 0:
        print(f"[连接成功] 已连上 {BROKER}:{PORT}")
        # 连接成功后订阅主题
        client.subscribe(TOPIC, qos=1)
        print(f"[已订阅] 主题: {TOPIC}")
    else:
        print(f"[连接失败] reason_code={reason_code}")

def on_message(client, userdata, msg):
    """收到消息时被调用"""
    print(f"[收到消息] 主题: {msg.topic}, QoS: {msg.qos}")
    print(f"  内容: {msg.payload.decode('utf-8')}")

def on_subscribe(client, userdata, mid, reason_code_list, properties):
    """订阅成功时被调用"""
    print(f"[订阅成功] mid={mid}, 结果={reason_code_list}")

# ========== 主流程 ==========
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.on_subscribe = on_subscribe

print("正在连接 EMQX...")
client.connect(BROKER, PORT, keepalive=60)

# 进入事件循环（阻塞，持续监听消息）
client.loop_forever()
