#!/usr/bin/env python3
"""One-shot harness: a SIMULATED Modbus RTU slave on a pty pair (NOT the real device).

Why it exists: the real transmitter answers on /dev/ttyUSB0, but the real hardware
cannot be unplugged from software. To prove the device layer's fault semantics on
a *disappearing serial link* (open failure / read timeout) and its recovery, this
harness plays a peer that speaks the same FC03 exchange and can be told to stop
answering or to disappear entirely.

⚠️ Everything produced with this peer is "模拟对端（pty）", never "真机". The real
hardware path is documented separately (probe + mixed flow + bus-noise fault).

Control signals:
    SIGUSR1 -> pause: accept/parse requests but send no response (read timeout)
    SIGUSR2 -> resume
    SIGTERM -> exit (the pty pair goes away: device sees an open failure)

Not production code; not imported by src/. Pure ASCII on purpose.

Usage:
    python3 sim_peer_rtu_slave.py --link /tmp/ttyV0 [--humidity 561 --temperature 273]
"""
from __future__ import annotations

import argparse
import os
import pty
import select
import signal
import sys
import time
import tty

ANSWER = {"on": True}
RUNNING = {"on": True}


def crc16_modbus(data: bytes) -> int:
    """Independent standard Modbus CRC16 (0xA001 reflected, init 0xFFFF)."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def with_crc(payload: bytes) -> bytes:
    crc = crc16_modbus(payload)
    return payload + bytes([crc & 0xFF, (crc >> 8) & 0xFF])


def stamp() -> str:
    return time.strftime("%F %T")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--link", default="/tmp/ttyV0")
    parser.add_argument("--slave", type=int, default=1)
    parser.add_argument("--humidity", type=int, default=561)
    parser.add_argument("--temperature", type=int, default=273)
    args = parser.parse_args()

    master_fd, slave_fd = pty.openpty()
    tty.setraw(slave_fd)
    slave_name = os.ttyname(slave_fd)
    try:
        os.unlink(args.link)
    except FileNotFoundError:
        pass
    os.symlink(slave_name, args.link)

    def on_pause(_signum, _frame):
        ANSWER["on"] = False
        print(f"[{stamp()}] peer: SIGUSR1 -> PAUSED (no answers)", flush=True)

    def on_resume(_signum, _frame):
        ANSWER["on"] = True
        print(f"[{stamp()}] peer: SIGUSR2 -> RESUMED", flush=True)

    def on_term(_signum, _frame):
        RUNNING["on"] = False
        print(f"[{stamp()}] peer: SIGTERM -> exit", flush=True)

    signal.signal(signal.SIGUSR1, on_pause)
    signal.signal(signal.SIGUSR2, on_resume)
    signal.signal(signal.SIGTERM, on_term)

    response = with_crc(
        bytes([args.slave, 3, 4])
        + (args.humidity & 0xFFFF).to_bytes(2, "big")
        + (args.temperature & 0xFFFF).to_bytes(2, "big")
    )
    print(
        f"[{stamp()}] peer up: simulated RTU slave link={args.link} -> {slave_name} "
        f"slave={args.slave} humidity={args.humidity} temperature={args.temperature} "
        f"response={response.hex(' ')}",
        flush=True,
    )

    buffer = b""
    while RUNNING["on"]:
        readable, _, _ = select.select([master_fd], [], [], 0.2)
        if not readable:
            continue
        data = os.read(master_fd, 256)
        if not data:
            break
        buffer += data
        while len(buffer) >= 8:
            request, buffer = buffer[:8], buffer[8:]
            if crc16_modbus(request[:-2]) != (request[-2] | (request[-1] << 8)):
                print(f"[{stamp()}] peer: bad-CRC request ignored {request.hex(' ')}",
                      flush=True)
                continue
            if request[0] != args.slave or request[1] != 3:
                print(f"[{stamp()}] peer: unexpected request ignored {request.hex(' ')}",
                      flush=True)
                continue
            if not ANSWER["on"]:
                print(f"[{stamp()}] peer: request {request.hex(' ')} -> SILENT (paused)",
                      flush=True)
                continue
            os.write(master_fd, response)
            print(f"[{stamp()}] peer: request {request.hex(' ')} -> response "
                  f"{response.hex(' ')}", flush=True)

    os.close(master_fd)
    os.close(slave_fd)
    print(f"[{stamp()}] peer down", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
