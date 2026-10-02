# -*- coding: utf-8 -*-
"""最小 P4 演示：本地上位机回环（真实 TCP，不是纸上谈兵）。

流程（全部在本机跑，不依赖真实环保平台）：

    取一条真实数据（TDengine 最新一行，回落到内置样例）
        ↓  assemble_packet：组装 HJ212 数据段
        ↓  encode_packet：算长度 + ANSI CRC16 + 组包（可选 SM4 加密数据区）
        ↓  通过【真实 TCP socket】发到 127.0.0.1
    本地上位机（本脚本内起线程监听）
        ↓  按 \\r\\n 分帧（演示半包/粘包处理）
        ↓  decode_packet：拆包 + CRC 校验 + SM4 解密
        ↓  解析数据区，取出各测点
        ↓  回一条 CN=9014 数据应答（标准表 12）

**这个脚本证明什么**：报文能被真实 socket 传输、能被独立解码器还原、CRC/加密都能验通过。
**它不能证明什么（诚实边界）**：没有真实环保平台参与；`MN`/`PW` 是占位值；
应答是我们自己写的上位机回的。真实对接仍需平台侧配合（见 ADR-0006 §3）。

用法：
    python scripts/hj212_loopback_demo.py              # 不加密
    python scripts/hj212_loopback_demo.py --encrypt    # 数据区 SM4 加密（附录 A.2 的公开密钥）
    python scripts/hj212_loopback_demo.py --from-db    # 从 TDengine 取真实最新一行
"""

from __future__ import annotations

import argparse
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS, O2_REFERENCE  # noqa: E402
from src.protocol.hj212 import (  # noqa: E402
    CN_UPLOAD_REALTIME,
    OFFICIAL_TEST_KEY,
    Packet,
    ST_ATMOSPHERIC_SOURCE,
    code_of,
    decode_packet,
    encode_packet,
    looks_encrypted,
    make_qn,
)

HOST: Final[str] = "127.0.0.1"
PORT: Final[int] = 0          # 0 = 由 OS 分配空闲端口（避免占用真实端口）
ACK_CN: Final[str] = "9014"   # 标准表 12：数据应答
LINE: Final[str] = "=" * 78

# 内置样例（--from-db 不可用时回落；数值取自真实运行区间）
FALLBACK: Final[dict[str, float]] = {
    "Flow": 34000.0, "Dust": 2.9, "SO2": 20.0, "NOx": 32.2, "O2": 6.1,
    "Velocity": 13.1, "Temp": 200.0, "Humidity": 48.0, "Pressure": 97.5,
}


def fetch_latest_from_db() -> tuple[str, dict[str, float]] | None:
    """取 TDengine 最新一行（失败返回 None，由调用方回落样例）。"""
    cols = ",".join(point.column for point in POINTS)
    sql = f"SELECT ts,{cols} FROM cems.cems_data ORDER BY ts DESC LIMIT 1;"
    try:
        proc = subprocess.run(
            ["docker", "exec", "tdengine", "taos", "-s", sql],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    for line in (proc.stdout or "").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == len(POINTS) + 1 and cells[0][:4] == "2026":
            ts = cells[0]
            try:
                values = {p.name: float(v) for p, v in zip(POINTS, cells[1:])}
            except ValueError:
                return None
            return ts, values
    return None


def build_packet(ts: str, values: dict[str, float], *, resend: bool) -> Packet:
    """把一条采样组装成 HJ212 数据段。

    ⚠️ 分隔符按原文 §6.3.4.1：**同一项目的不同类别用 `,`、不同项目之间用 `;`**。
    本演示每个测点只上一个类别（`-Rtd`），故项目之间必须用 `;`。
    """
    fields = [f"DataTime={ts.replace('-', '').replace(':', '').replace(' ', '')}"]
    fields.extend(f"{code_of(p.name)}-Rtd={values[p.name]:g}" for p in POINTS)
    return Packet(
        qn=make_qn(),
        st=ST_ATMOSPHERIC_SOURCE,      # 31 = 大气污染源（本项目是烟气）
        cn=CN_UPLOAD_REALTIME,         # 2011 = 上传实时数据
        pw="123456",                   # ⚠️ 占位：真实密码由平台下发
        mn="0" * 24,                   # ⚠️ 占位：真实 MN 由平台按 CPUID/MAC 赋码
        flag=8 | 1,                    # 位掩码：本次修订版(版本=2) + D=1（需应答）
        region=";".join(fields),
        resend=resend,
    )


def run_upper_machine(listener: socket.socket, results: dict) -> None:
    """本地上位机：收报文 → 分帧 → 解码 → 回 9014 应答。"""
    conn, _addr = listener.accept()
    conn.settimeout(10)
    try:
        buf = ""
        deadline = time.time() + 10
        while time.time() < deadline:
            chunk = conn.recv(4096).decode("ascii", errors="replace")
            if not chunk:
                break
            buf += chunk
            # ⚠️ 真实上位机必须处理半包/粘包：这里以 \r\n 为分帧符
            while "\r\n" in buf:
                raw, buf = buf.split("\r\n", 1)
                raw += "\r\n"
                results["raw"] = raw
                try:
                    decoded = decode_packet(raw, key=results.get("key"))
                    results["decoded"] = decoded
                    results["error"] = None
                except Exception as exc:  # noqa: BLE001
                    results["error"] = f"{type(exc).__name__}: {exc}"
                # 回数据应答（标准表 12：CN=9014）
                ack = encode_packet(
                    Packet(qn=results["qn"], st="91", cn=ACK_CN, pw="123456",
                           mn="0" * 24, flag=8, region="")
                )
                conn.sendall(ack.encode("ascii"))
                results["ack_sent"] = True
                return
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="最小 P4：HJ212 本地上位机回环演示")
    parser.add_argument("--encrypt", action="store_true", help="数据区 SM4 加密（用附录 A.2 公开密钥）")
    parser.add_argument("--from-db", action="store_true", help="从 TDengine 取真实最新一行")
    parser.add_argument("--resend", action="store_true", help="标记为补传报文（写 RF=1）")
    args = parser.parse_args(argv)

    # ---- 1) 数据来源 ----
    print(LINE)
    print("① 取数据")
    print(LINE)
    src = "内置样例"
    ts, values = time.strftime("%Y-%m-%d %H:%M:%S"), dict(FALLBACK)
    if args.from_db:
        got = fetch_latest_from_db()
        if got:
            ts, values, src = got[0], got[1], "TDengine 最新一行（真实数据）"
        else:
            print("  ⚠️ 取库失败（容器未运行？），回落到内置样例")
    print(f"  来源: {src}")
    print(f"  时间: {ts}")
    print("  " + "  ".join(f"{p.name}={values[p.name]:g}{p.unit}" for p in POINTS[:4]) + " …")

    # ---- 2) 组包 + 编码 ----
    print()
    print(LINE)
    print("② 组包并编码（长度 + ANSI CRC16 + 组包" + ("，数据区 SM4 加密" if args.encrypt else "") + "）")
    print(LINE)
    packet = build_packet(ts, values, resend=args.resend)
    key = OFFICIAL_TEST_KEY if args.encrypt else None
    raw = encode_packet(packet, key=key)
    print(f"  报文长度: {len(raw)} 字符")
    print(f"  长度字段: {raw[2:6]}  （= 数据段字符数）")
    print(f"  补传标志: {'RF=1（补传报文）' if args.resend else '不写 RF（实时报文，标准要求非补传时无本字段）'}")
    if args.encrypt:
        print(f"  加密: 数据区已 SM4/ECB/Nopadding 加密（密钥为附录 A.2 公开值）")
    print(f"  报文头: {raw[:80]}…")
    print(f"  报文尾: …{raw[-24:]}")

    # ---- 3) 真实 TCP 传输 ----
    print()
    print(LINE)
    print("③ 经真实 TCP socket 发送（不是函数直调）")
    print(LINE)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(1)
    actual_port = listener.getsockname()[1]
    print(f"  本地上位机监听 {HOST}:{actual_port}")

    results: dict = {"qn": packet.qn, "key": key}
    server = threading.Thread(target=run_upper_machine, args=(listener, results), daemon=True)
    server.start()

    time.sleep(0.2)
    with socket.create_connection((HOST, actual_port), timeout=10) as client:
        client.sendall(raw.encode("ascii"))
        ack = client.recv(4096).decode("ascii", errors="replace")
    server.join(timeout=5)
    listener.close()
    print(f"  已发送 {len(raw)} 字符；收到应答 {len(ack)} 字符")

    # ---- 4) 解码结果 ----
    print()
    print(LINE)
    print("④ 上位机解码（拆包 + CRC 校验 + 解密 + 解析数据区）")
    print(LINE)
    if results.get("error"):
        print(f"  ❌ 解码失败: {results['error']}")
        return 1
    decoded = results["decoded"]
    seg = decoded.packet.to_data_segment()
    print(f"  CRC 校验: ✅ 通过")
    print(f"  QN={decoded.packet.qn}  ST={decoded.packet.st}  CN={decoded.packet.cn}  "
          f"Flag={decoded.packet.flag}  RF={'1' if decoded.packet.resend else '（无）'}")
    print(f"  数据区形态: {'加密' if looks_encrypted(decoded.packet.region) else '明文'}")
    region = decoded.region_plaintext
    print(f"  数据区明文: {region[:110]}{'…' if len(region) > 110 else ''}")
    parts = [x for x in region.split(";") if x]
    print(f"  解析出 {len(parts)} 个字段（1 个 DataTime + {len(parts) - 1} 个测点）")

    # ---- 5) 与原始值对账 ----
    print()
    print(LINE)
    print("⑤ 对账：解码出来的值与原始值逐一比对")
    print(LINE)
    mismatches: list[str] = []
    for part in parts[1:]:
        name, _, val = part.partition("=")
        code = name.split("-")[0]
        point = next((p for p in POINTS if code_of(p.name) == code), None)
        if point is None:
            mismatches.append(f"未知编码 {code}")
            continue
        if abs(float(val) - values[point.name]) > 1e-6:
            mismatches.append(f"{point.name}: {val} != {values[point.name]}")
    if mismatches:
        print(f"  ❌ 不一致: {mismatches}")
        return 1
    print(f"  ✅ {len(parts) - 1} 个测点全部一致（编码 {code_of('Dust')} / {code_of('SO2')} / {code_of('NOx')} …）")

    # ---- 6) 应答 ----
    print()
    print(LINE)
    print("⑥ 上位机应答（标准表 12：CN=9014 数据应答）")
    print(LINE)
    print(f"  已回: {ack.strip()}")
    print(f"  （真实平台还会按 QN 匹配请求；本演示验证的是机制自洽，不是平台验收）")

    print()
    print(LINE)
    print("结论")
    print(LINE)
    print("  ✅ 报文经真实 TCP 往返、独立解码器还原、CRC/加密均验通过、9 测点零差异")
    print("  ⚠️ 边界：无真实环保平台参与；MN/PW 是占位值；应答由本脚本自己的上位机回")
    print("     → 真实对接需平台侧配合（见 docs/adr/0006-HJ212出口与北向适配.md §3）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
