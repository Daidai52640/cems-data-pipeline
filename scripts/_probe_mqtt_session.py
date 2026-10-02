# -*- coding: utf-8 -*-
"""只做一次 MQTT 连接/订阅/发布的最小探针，用于定位 broker 是否在"新客户端接入"时出问题。

不写库、不建表、不改代码；只连 emqx:1883、订阅一个专属主题、发 3 条、等 5 秒、退出。
配合 `docker inspect emqx --format '{{.State.Status}}'` 前后对照使用。
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid

import paho.mqtt.client as mqtt

HOST = os.getenv("MQTT_HOST", "127.0.0.1")
PORT = int(os.getenv("MQTT_PORT", "1883"))
CLIENT_ID = os.getenv("PROBE_CLIENT_ID", f"cems-emqx-probe-{uuid.uuid4().hex[:8]}")
TOPIC = os.getenv("PROBE_TOPIC", "cems/loadtest/probe/data")

events: list[dict] = []
received: list[str] = []


def on_connect(client, _u, _f, reason_code, _p=None):
    events.append({"event": "connect", "rc": str(reason_code), "t": time.time()})
    client.subscribe(TOPIC, qos=1)


def on_subscribe(_c, _u, mid, rcs, _p=None):
    events.append({"event": "suback", "rcs": [str(x) for x in rcs], "t": time.time()})


def on_message(_c, _u, msg):
    received.append(msg.payload.decode("utf-8", "replace"))
    events.append({"event": "message", "t": time.time()})


def on_disconnect(_c, _u, *args):
    events.append({"event": "disconnect", "args": [str(a) for a in args], "t": time.time()})


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID, clean_session=False)
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message
client.on_disconnect = on_disconnect

print(f"connecting {HOST}:{PORT} client_id={CLIENT_ID} clean_session=False", flush=True)
client.connect(HOST, PORT, keepalive=30)
client.loop_start()
time.sleep(3)
for i in range(3):
    body = ("Flow=1000.0 Dust=1.0 SO2=1.0 NOx=1.0 O2=5.0 Velocity=1.0 "
            "Temp=1.0 Humidity=1.0 Pressure=1.0 Flag=N")
    payload = f"2026-10-03 00:00:{i:02d} {body}"
    info = client.publish(TOPIC, payload, qos=1)
    info.wait_for_publish(timeout=5)
    print(f"published #{i} rc={info.rc} is_published={info.is_published()}", flush=True)
time.sleep(5)
client.loop_stop()
client.disconnect()
print(json.dumps({"client_id": CLIENT_ID, "topic": TOPIC, "received": len(received),
                  "events": events}, ensure_ascii=False, indent=2))
sys.exit(0)
