#!/usr/bin/env python3
"""One-shot fault injector: write garbage bytes onto a real RS485 (Modbus RTU) tty.

Purpose (evidence only): with the real transmitter answering on /dev/ttyUSB0,
this injects bus noise / frame collisions from a second process, so the device
layer sees what it sees on a physically disturbed line: truncated responses,
CRC errors and timeouts. Stopping the injector must let the device layer
recover without a restart (next successful read clears the stale state).

It does NOT reconfigure the transmitter, does NOT write registers (the device is
read-only anyway) and does NOT touch the tty settings beyond opening it at the
same 4800/8/N/1 so the bytes land on the same bus.

Not production code; not imported by src/. Pure ASCII on purpose.

Usage:
    python3 noise_on_rtu_bus.py --port /dev/ttyUSB0 --seconds 6
"""
from __future__ import annotations

import argparse
import sys
import time

import serial


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", default="/dev/ttyUSB0")
    parser.add_argument("--seconds", type=float, default=6.0)
    parser.add_argument("--period", type=float, default=0.02)
    args = parser.parse_args()

    try:
        bus = serial.Serial(
            port=args.port, baudrate=4800, bytesize=8, parity="N", stopbits=1, timeout=0.2
        )
    except Exception as exc:  # noqa: BLE001 - evidence probe: report and fail loudly
        print(f"[noise] cannot open {args.port}: {type(exc).__name__}: {exc}", flush=True)
        return 1

    deadline = time.monotonic() + args.seconds
    sent = 0
    try:
        print(f"[noise] start {args.port} seconds={args.seconds} period={args.period}s",
              flush=True)
        while time.monotonic() < deadline:
            bus.write(b"\xff\x00\x55\xaa\xff\x00\x55\xaa")
            sent += 1
            time.sleep(args.period)
    finally:
        bus.close()
    print(f"[noise] stop {args.port} frames={sent}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
