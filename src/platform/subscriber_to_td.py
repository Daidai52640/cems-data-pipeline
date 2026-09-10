# -*- coding: utf-8 -*-
"""
MQTT 平台接收端 → TDengine 入库（Day 14）
功能：订阅 cems/plant1/data → 解析数据 → 写入 TDengine 时序库
      （替代 subscriber.py 的打印，数据从此可查可画曲线）
运行：python subscriber_to_td.py
      （需要 modbus_server.py + gateway.py 在跑）
"""

import paho.mqtt.client as mqtt
import taosrest

# ========== 配置 ==========
BROKER = "localhost"
PORT = 1883
TOPIC = "cems/plant1/data"

TD_URL = "http://localhost:6041"
TD_USER = "root"
TD_PASS = "taosdata"
TD_DB = "cems"
STABLE = "cems_data"        # 超级表名（模板）
PLANT = "plant1"            # 标签：厂区
DEVICE = "device1"          # 标签：设备号


def init_td():
    """连接 TDengine + 建库 + 建超级表（模板）"""
    conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
    cur = conn.cursor()
    # 建库：KEEP 365 = 数据保留1年, DURATION 30 = 每30天一个分片
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {TD_DB} KEEP 365 DURATION 30")
    # ★ REST接口无状态：USE不生效，每条SQL必须显式带库名前缀 cems.xxx
    cur.execute(
        f"CREATE STABLE IF NOT EXISTS {TD_DB}.{STABLE} "
        f"(ts TIMESTAMP, so2 FLOAT, nox FLOAT, flow FLOAT) "
        f"TAGS (plant NCHAR(20), device NCHAR(20))"
    )
    print(f"[TDengine就绪] 库={TD_DB}, 超级表={STABLE}")
    return conn, cur


def parse_payload(payload):
    """解析 MQTT 消息 → (时间戳, so2, nox, flow)
    格式: "2026-09-10 12:00:00 SO2=35.2 NOx=18.5 Flow=95.3 Flag=N"
    """
    parts = payload.split()
    ts = parts[0] + " " + parts[1]      # 前两个是时间戳
    data = {}
    for p in parts[2:]:
        if "=" in p:
            k, v = p.split("=")
            # ★ 只取约定的数值测点（SO2/NOx/Flow），跳过 Flag 等标记字段
            if k in ("SO2", "NOx", "Flow"):
                data[k] = float(v)      # 值转成数字
    return ts, data["SO2"], data["NOx"], data["Flow"]


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[MQTT已连接] 订阅主题: {TOPIC}")
        client.subscribe(TOPIC, qos=1)
    else:
        print(f"[MQTT连接失败] reason_code={reason_code}")


def on_message(client, userdata, msg):
    """收到消息 → 解析 → 入库"""
    cur = userdata["cur"]
    payload = msg.payload.decode("utf-8")
    try:
        ts, so2, nox, flow = parse_payload(payload)
        # TDengine 插入：自动创建子表 plant1（USING 超级表模板 + TAGS 标签）
        sql = (
            f"INSERT INTO {TD_DB}.{PLANT} USING {TD_DB}.{STABLE} "
            f"TAGS ('{PLANT}', '{DEVICE}') "
            f"VALUES ('{ts}', {so2}, {nox}, {flow})"
        )
        cur.execute(sql)
        print(f"[已入库] {ts} SO2={so2} NOx={nox} Flow={flow}")
    except Exception as e:
        print(f"[入库失败] {e} | 原始消息: {payload}")


def main():
    # 1. 初始化 TDengine
    conn, cur = init_td()

    # 2. 连 MQTT（用 userdata 把 cursor 传给回调）
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.user_data_set({"cur": cur})   # ★ 把数据库游标传给回调
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, keepalive=60)

    print(f"等待 MQTT 数据...（Ctrl+C 停止）")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n已停止")
        conn.close()


if __name__ == "__main__":
    main()
