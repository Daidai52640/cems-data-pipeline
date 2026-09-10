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
    else:
        print(f"[连接失败] reason_code={reason_code}")

def gen_data():
    so2 = round(random.uniform(20, 50), 1)   # SO2 实测值 mg/m3
    nox = round(random.uniform(10, 30), 1)   # NOx 实测值 mg/m3
    o2 = round(random.uniform(5, 25), 1)    # O2 实测值 %
    dust = round(random.uniform(0, 10), 1)   # 烟尘实测值 mg/m3
    flow = round(random.uniform(80, 120), 1) # 流速 m3/s
    return f"{time.strftime('%Y-%m-%d %H:%M:%S')} SO2={so2} NOx={nox} O2={o2} Dust={dust} Flow={flow} Flag=N"


# ========== 主流程 ==========
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect

print("正在连接 EMQX...")
client.connect(BROKER, PORT, keepalive=60)
client.loop_start()

try:
    count=0
    while True:
        count+=1
        data = gen_data()
        client.publish(TOPIC, data, qos=1)
        print(f"第 {count} 条数据已发布：{data}")
        time.sleep(5)

except KeyboardInterrupt:
    print("\n发布端已退出")
    client.disconnect()




