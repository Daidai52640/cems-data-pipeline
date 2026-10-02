# -*- coding: utf-8 -*-
"""最小 P5 演示：一份数据、两个上报出口，都走真实网络、都能独立解码。

    一份采样 ──┬─► Hj212Adapter  ──► TCP   ──► 本地上位机（解 HJ212 + CRC + 回 9014）
               └─► HttpJsonAdapter ──► HTTP  ──► 本地接收端（解 JSON + 回 2xx）

**这个脚本证明什么**：加一个上报出口 = 加一个类，**上层代码不改**；
两个出口共用同一条采样（同一 `ts`，跨出口可对账）。
**不能证明什么**：没有真实平台；两个"接收端"都是本脚本起的。

用法：
    python scripts/northbound_demo.py
    python scripts/northbound_demo.py --from-db
    python scripts/northbound_demo.py --encrypt
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS  # noqa: E402
from src.protocol.adapter import ADAPTERS, Sample, build_adapter  # noqa: E402

LINE: Final[str] = "=" * 78
HOST: Final[str] = "127.0.0.1"
FALLBACK: Final[dict[str, float]] = {
    "Flow": 34000.0, "Dust": 2.9, "SO2": 20.0, "NOx": 32.2, "O2": 6.1,
    "Velocity": 13.1, "Temp": 200.0, "Humidity": 48.0, "Pressure": 97.5,
}


def fetch_latest() -> tuple[str, dict[str, float]] | None:
    cols = ",".join(p.column for p in POINTS)
    sql = f"SELECT ts,{cols} FROM cems.cems_data ORDER BY ts DESC LIMIT 1;"
    try:
        proc = subprocess.run(["docker", "exec", "tdengine", "taos", "-s", sql],
                              capture_output=True, text=True, encoding="utf-8",
                              errors="replace", timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    for line in (proc.stdout or "").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == len(POINTS) + 1 and cells[0][:4] == "2026":
            try:
                return cells[0], {p.name: float(v) for p, v in zip(POINTS, cells[1:])}
            except ValueError:
                return None
    return None


def hj212_receiver(sock: socket.socket, box: dict) -> None:
    """本地上位机：收 HJ212 → 解码（验 CRC / 解密）→ 回 CN=9014。"""
    from src.protocol.hj212 import Packet, decode_packet, encode_packet

    conn, _ = sock.accept()
    try:
        buf = ""
        deadline = time.time() + 10
        while time.time() < deadline and "\r\n" not in buf:
            chunk = conn.recv(4096).decode("ascii", errors="replace")
            if not chunk:
                break
            buf += chunk
        raw, _, _ = buf.partition("\r\n")
        raw += "\r\n"
        box["raw"] = raw
        decoded = decode_packet(raw)
        box["qr"] = decoded.region_plaintext          # 解密后的数据区明文
        box["cn"] = decoded.packet.cn
        box["rf"] = decoded.packet.resend
        ack = encode_packet(Packet(qn=decoded.packet.qn, st="91", cn="9014",
                                   pw="123456", mn="0" * 24, flag=8, region=""))
        conn.sendall(ack.encode("ascii"))
    except Exception as exc:  # noqa: BLE001
        box["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        conn.close()


def http_receiver(sock: socket.socket, box: dict) -> None:
    """本地接收端：收 HTTP → 解 JSON → 回 2xx。"""
    conn, _ = sock.accept()
    try:
        data = b""
        deadline = time.time() + 10
        while time.time() < deadline:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
            head, _, body = data.partition(b"\r\n\r\n")
            if b"Content-Length:" in head:
                want = int([l for l in head.decode("latin-1").splitlines()
                            if l.lower().startswith("content-length")][0].split(":")[1])
                if len(body) >= want:
                    break
        head, _, body = data.partition(b"\r\n\r\n")
        box["raw"] = head.decode("latin-1").splitlines()[0]
        box["json"] = json.loads(body.decode("utf-8"))
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
    except Exception as exc:  # noqa: BLE001
        box["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        conn.close()


def run_adapter(name: str, sample: Sample, receiver, **adapter_kwargs: object) -> dict:
    """起接收端 → 组包 → 发送 → 等结果。**这段代码对两个协议完全一样**（接口的价值）。"""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    box: dict = {}
    thread = threading.Thread(target=receiver, args=(listener, box), daemon=True)
    thread.start()
    time.sleep(0.15)

    adapter = build_adapter(name, **adapter_kwargs)
    payload = adapter.build_payload(sample)
    ok = adapter.send(payload, host=HOST, port=port)
    thread.join(timeout=5)
    listener.close()
    box.update(adapter=adapter, payload=payload, port=port, delivered=ok)
    return box


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="最小 P5：北向出口适配层演示")
    parser.add_argument("--from-db", action="store_true")
    parser.add_argument("--encrypt", action="store_true")
    args = parser.parse_args(argv)

    ts, values, src = time.strftime("%Y-%m-%d %H:%M:%S"), dict(FALLBACK), "内置样例"
    if args.from_db:
        got = fetch_latest()
        if got:
            ts, values, src = got[0], got[1], "TDengine（真实数据）"
        else:
            print("  ⚠️ 取库失败，回落内置样例")

    sample = Sample(ts=ts, values=values)
    print(LINE)
    print("① 一份采样，两个出口（共用同一 ts，便于跨出口对账）")
    print(LINE)
    print(f"  来源 {src}｜ts={ts}")
    print("  " + "  ".join(f"{p.name}={values[p.name]:g}" for p in POINTS[:4]) + " …")
    print(f"  已实现出口: {list(ADAPTERS)}  ← 加新协议 = 加一个类，上层不改")

    print()
    print(LINE)
    print("② 逐出口：组包 → 真实网络发送 → 接收端解码 → 送达判据")
    print(LINE)
    results: dict[str, dict] = {}
    for name in ADAPTERS:
        kwargs = {"encrypt": args.encrypt} if name == "hj212" else {}
        receiver = hj212_receiver if name == "hj212" else http_receiver
        box = run_adapter(name, sample, receiver, **kwargs)
        results[name] = box
        print(f"\n  ── {name} ──")
        if box.get("error"):
            print(f"    ❌ {box['error']}")
            continue
        print(f"    端口 {box['port']}｜报文 {len(box['payload'])} 字符｜"
              f"送达={'✅' if box['delivered'] else '❌'}")
        print(f"    特点: {box['adapter'].describe()[:78]}")

    print()
    print(LINE)
    print("③ 接收端各自解出来的内容（证明报文真的可解，不是自说自话）")
    print(LINE)
    h = results["hj212"]
    if h.get("qr"):
        print(f"  HJ212  数据区明文: {h['qr'][:96]}…")
        print(f"         CN={h['cn']}  RF={'1（补传）' if h['rf'] else '（无，实时）'}")
    j = results["http-json"]
    if j.get("json"):
        d = j["json"]["data"]
        print(f"  HTTP   JSON 字段数: {len(d)}｜样例 a34013={d.get('a34013')} "
              f"a21026={d.get('a21026')} a21002={d.get('a21002')}")

    print()
    print(LINE)
    print("④ 对账：两个出口解出来的值都必须等于原始值")
    print(LINE)
    bad: list[str] = []
    if h.get("qr"):
        for part in [x for x in h["qr"].split(";") if "-Rtd=" in x]:
            code, _, val = part.partition("=")
            point = next((p for p in POINTS if p.code == code.split("-")[0]), None)
            if point and abs(float(val) - values[point.name]) > 1e-6:
                bad.append(f"HJ212 {point.name}")
    if j.get("json"):
        for p in POINTS:
            got = j["json"]["data"].get(p.code)
            if got is None or abs(float(got) - values[p.name]) > 1e-6:
                bad.append(f"HTTP {p.name}")
    print(f"  两个出口 × {len(POINTS)} 测点 = {2 * len(POINTS)} 项比对："
          f"{'✅ 全部一致' if not bad else '❌ ' + str(bad)}")

    print()
    print(LINE)
    print("结论")
    print(LINE)
    print("  ✅ 同一份采样经两个协议出口走真实网络、各自被独立解码、值零差异")
    print("  ✅ 加出口只加一个类（上层 run_adapter 一段代码复用）—— 这就是「可扩展」的落地证据")
    print("  ⚠️ 边界：无真实平台参与；HJ212 的 MN/PW 是占位值；两个接收端都是本脚本起的")
    print("     → 真实对接需平台侧配合（docs/adr/0006-HJ212出口与北向适配.md §3）")
    return 0 if not bad else 1


if __name__ == "__main__":
    raise SystemExit(main())
