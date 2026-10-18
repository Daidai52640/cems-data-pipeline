#!/usr/bin/env python3
"""One-shot evidence probe: read a Modbus TCP register block and print it per point.

Retained under docs/evidence/ because the numbers in
docs/runbooks/直读串口.md come from this exact output: it is the readback side
of the device-layer mixed flow (2 real points + 7 simulated points) and shows
whether Temp/Humidity are inside the CEMS contract range or are the
out-of-range sentinels (301.0 degC / 101.0 %).

Not production code; not imported by src/. Pure ASCII on purpose.

Usage:
    python3 read_registers.py --port 15021 --unit 1 [--count 9] [--label initial]
"""
from __future__ import annotations

import argparse
import pathlib
import sys
import time

_HERE = pathlib.Path(__file__).resolve()
# 从脚本所在目录向上找仓库根；证据脚本可能被复制到 /tmp 跑，此时按当前目录（仓库根）执行
REPO_ROOT = next(
    (parent for parent in _HERE.parents if (parent / "src" / "common" / "points.py").is_file()),
    pathlib.Path.cwd(),
)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pymodbus.client import ModbusTcpClient  # noqa: E402

from src.common.points import POINTS, REG_BASE, SCALE  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=15021)
    parser.add_argument("--unit", type=int, default=1)
    parser.add_argument("--count", type=int, default=9)
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    client = ModbusTcpClient(args.host, port=args.port, timeout=2)
    try:
        if not client.connect():
            print(f"[{time.strftime('%F %T')}] {args.label} CONNECT FAIL "
                  f"{args.host}:{args.port}")
            return 1
        response = client.read_holding_registers(
            address=REG_BASE, count=args.count, slave=args.unit
        )
        if response.isError():
            print(f"[{time.strftime('%F %T')}] {args.label} MODBUS ERROR: {response}")
            return 1
        registers = [int(value) for value in response.registers]
    finally:
        client.close()

    print(f"[{time.strftime('%F %T')}] {args.label} unit={args.unit} "
          f"raw_registers={registers}")
    for point in POINTS:
        index = point.address - REG_BASE
        if index >= len(registers):
            continue
        raw = registers[index]
        signed = raw - 0x10000 if raw > 0x7FFF else raw
        value = signed / point.scale
        in_range = point.low <= value <= point.high
        flag = "ok" if in_range else "OUT_OF_RANGE"
        print(f"    {point.name:<9} raw={raw:<6} value={value:>8.1f} {point.unit:<6} "
              f"range=[{point.low:g},{point.high:g}] {flag}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
