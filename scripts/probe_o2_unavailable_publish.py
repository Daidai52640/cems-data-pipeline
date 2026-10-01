# -*- coding: utf-8 -*-
"""合规验证探针：向实时 MQTT 主题发布 O2 >= 21% 的报文（HJ 212-2025 §8.1.1 d) 验收用）。

用途：在**真实链路**上验证"无法计算折算浓度"的场景确实传出哨兵值。
    仿真器把 O2 锚在实测区间 4.5%~7.5%（见 src/device/simulator.py 的 O2 基线注释），
    正常运行**永远不会**出现 O2 >= 21%，所以这个场景只能手工构造一条报文。

⚠️ 这是一次性验收探针，不是链路的一部分：
    - 报文里 9 个测点全部在量程内（会被接入层正常接收并入库），只把 O2 取 22.0%
    - 会在 TDengine cems.cems_data 留下真实数据行（时间戳倒推 2 小时，不覆盖实时数据）
    - 用完即弃；不要在正式环境运行

用法：
    python scripts/probe_o2_unavailable_publish.py              # 默认发 20 条（确保命中抽样日志）
    python scripts/probe_o2_unavailable_publish.py --count 40
    python scripts/probe_o2_unavailable_publish.py --o2 21.0

MQTT 参数默认取与环境变量同名的本机默认值（MQTT_HOST/MQTT_PORT/MQTT_TOPIC/MQTT_QOS），
与接入层一致；本脚本**不改**主题、QoS 和 client_id 约定。
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import paho.mqtt.client as mqtt   # noqa: E402

from src.common.points import NAMES, RANGES   # noqa: E402

HOST: Final[str] = os.getenv("MQTT_HOST", "localhost")
PORT: Final[int] = int(os.getenv("MQTT_PORT", "1883"))
TOPIC: Final[str] = os.getenv("MQTT_TOPIC", "cems/plant1/data")
QOS: Final[int] = int(os.getenv("MQTT_QOS", "1"))
PROBE_CLIENT_ID: Final[str] = "cems-o2-probe"      # 独立 client_id，不占用订阅端会话

# 探针读数：除 O2 外全部取量程中部，保证"只有 O2 触发无法折算"这一条差异
PROBE_VALUES: Final[dict[str, float]] = {
    "Flow": 30000.0,
    "Dust": 4.0,
    "SO2": 30.0,
    "NOx": 40.0,
    "O2": 22.0,
    "Velocity": 12.0,
    "Temp": 140.0,
    "Humidity": 8.0,
    "Pressure": 101.0,
}


def build_payload(ts: datetime, o2: float) -> str:
    """构造一条与网关同格式的报文（9 个测点齐全，Flag=N）。"""
    values = dict(PROBE_VALUES)
    values["O2"] = o2
    fields = " ".join(f"{name}={values[name]}" for name in NAMES)
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')} {fields} Flag=N"


def main() -> int:
    parser = argparse.ArgumentParser(description="发布 O2 >= 21% 的合规验证探针报文")
    parser.add_argument("--count", type=int, default=20, help="发布条数（默认 20）")
    parser.add_argument("--o2", type=float, default=22.0, help="探针氧含量（默认 22.0）")
    parser.add_argument("--start", default="", help="起始时间戳，默认=当前 UTC 倒推 2 小时")
    args = parser.parse_args()

    if args.o2 < 21.0:
        print(f"O2={args.o2} 不会触发'无法折算'，请传 >= 21.0", file=sys.stderr)
        return 2
    low, high = RANGES["O2"]
    if not low <= args.o2 <= high:
        print(f"O2={args.o2} 超出量程 [{low}, {high}]，接入层会整条拒收", file=sys.stderr)
        return 2

    if args.start:
        base = datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
    else:
        now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
        base = now - timedelta(hours=2)

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=PROBE_CLIENT_ID,
        clean_session=True,
    )
    client.connect(HOST, PORT, keepalive=30)
    client.loop_start()
    try:
        for index in range(args.count):
            ts = base + timedelta(seconds=index)
            payload = build_payload(ts, args.o2)
            info = client.publish(TOPIC, payload, qos=QOS)
            info.wait_for_publish(timeout=5.0)
            if not info.is_published():
                print(f"发布失败（PUBACK 超时）: {payload}", file=sys.stderr)
                return 1
            print(f"[{index + 1}/{args.count}] -> {payload}")
            time.sleep(0.05)
    finally:
        client.loop_stop()
        client.disconnect()

    print(f"\n已向 {HOST}:{PORT} 主题 {TOPIC} 发布 {args.count} 条 O2={args.o2} 的探针报文。")
    print("接下来看接入层抽样日志（每 20 条打一条，O2>=21% 时应打出哨兵值）：")
    print("    docker logs --since 5m cems-subscriber 2>&1 | findstr 折算值抽样")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
