# -*- coding: utf-8 -*-
"""出口适配层：把"一份数据、多个上报出口"抽成统一接口（ADR-0006 · P5 最小实现）。

## 为什么现在才抽接口

ADR-0006 §6 的原则是「**先有实现，再抽象接口**」——
只有一个实现时抽象出来的接口一定是错的。现在 HJ212 与 HTTP 两个实现都在，
才从它们的**共同点**里抽出这层。

## 这层统一了什么（两个实现真正共有的）

| 共有概念 | HJ212 | HTTP/REST |
|---|---|---|
| 有哪些测点、编码是什么 | `a34013` 等 | 同左（同一份 `points.py` + `factors.py`） |
| 一条数据长什么样 | `DataTime=…;code-Rtd=v;…` | JSON body |
| 怎么算"送达" | 等 `CN=9014` 应答 | HTTP 2xx |
| 补传时怎么标记 | 写 `RF=1` | 自定义头/字段 |

**⚠️ 不该统一什么（抽错接口的典型）**：
- ❌ **传输方式**（TCP 长连接 vs HTTP 请求）——**不抽**，各有各的连接语义
- ❌ **加密**（HJ212 规定 SM4；HTTP 用 TLS）——**不抽**，各协议自己的规范
- ❌ **报文格式**——不抽，那是各协议的本质差别

**→ 所以这层只统一三件事**：`build_payload(ts, values, resend)` / `send(...)` / `describe()`。

边界（诚实）：**本项目没有真实对接方**。这里的 `send()` 走本机 socket/HTTP，
证明的是"机制自洽 + 接口可扩展"，**不是"已对接某平台"**。
"""

from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from .hj212 import (
    CN_UPLOAD_REALTIME,
    OFFICIAL_TEST_KEY,
    Packet,
    ST_ATMOSPHERIC_SOURCE,
    code_of,
    decode_packet,
    encode_packet,
    make_qn,
)
from .hj212.framing import StreamFramer
from ..common.points import POINTS


@dataclass(frozen=True)
class Sample:
    """一份待上报的采样（时间戳 + 各测点值）。时间戳由调用方给，保证多出口同一时刻。"""

    ts: str          # "YYYY-MM-DD HH:MM:SS"
    values: dict[str, float]
    resend: bool = False   # 是否补传报文


@runtime_checkable
class ProtocolAdapter(Protocol):
    """上报出口适配器：**只统一"组包 → 发送 → 描述"三件事**。"""

    name: str

    def build_payload(self, sample: Sample) -> str:
        """把采样编成该协议的报文字符串。"""
        ...

    def send(self, payload: str, *, host: str, port: int) -> bool:
        """发出去；返回是否送达（各协议自己的送达判据）。"""
        ...

    def describe(self) -> str:
        """一句话说明这个出口的特点与边界（写进报告/文档用）。"""
        ...


# --------------------------------------------------------------------------- #
# 连接管理：心跳 / 超时重发 / 重连退避（P4 缺口补齐）
#
# 标准依据（HJ 212-2025 表 12，PDF 第 22 页）：
#   心跳包 9015  用于判断网络连接在线状态
#   通知应答 9013 回应通知命令
# 表 1 给出超时/重发次数（按通信方式取值），本项目**全部外部化为配置项**，
# 并给出保守默认值 —— 对接时填配置即可，无需改代码。
# --------------------------------------------------------------------------- #

#: 心跳命令编码（现场机 → 上位机）
CN_HEARTBEAT: Final[str] = "9015"
#: 心跳应答（上位机 → 现场机）
CN_HEARTBEAT_ACK: Final[str] = "9013"


@dataclass
class RetryPolicy:
    """超时重发与重连退避策略（全部可配；默认值只是保守起点，不是标准规定值）。

    ⚠️ 标准表 1 按通信方式给超时/重发次数（ADSL/GPRS 不同），取值随现场通信条件而异，
    因此本项目**把这几项全部外部化为配置**；`build_adapter()` 从环境变量读取，
    对接时按目标平台给出的取值填入即可，不需改动代码。
    """

    ack_timeout_s: float = 5.0      # 等一次应答的超时
    send_retries: int = 3           # 单条报文重发次数（含首次共 N 次）
    backoff_base_s: float = 1.0     # 重连退避基数
    backoff_max_s: float = 60.0     # 重连退避上限（参照 paho 实测最坏 84s 取更小值）
    heartbeat_interval_s: float = 30.0   # 心跳间隔；0 = 关闭心跳

    def backoff_of(self, attempt: int) -> float:
        """第 attempt 次重连的等待秒数（指数退避，封顶）。attempt 从 1 起。"""
        if attempt < 1:
            return 0.0
        return min(self.backoff_base_s * (2 ** (attempt - 1)), self.backoff_max_s)


class Hj212Connection:
    """HJ212 的 TCP 长连接：分帧读、心跳、超时重发、指数退避重连。

    **为什么单列一个类**（而不是把 socket 塞进 `Hj212Adapter.send()`）：
    P3 要把它挂到网关的**独立线程**上长期持有连接，需要「连接状态 + 重连计数 + 心跳计时」
    这些跨调用的状态；一次性 `send()` 无法承载。

    ⚠️ 本类**不做任何阻塞式重试等待**：退避等待由调用方（网关出口线程）`sleep`，
    这样"连不上时不要拖住采集"的原则在调用方可见、可测。
    """

    def __init__(self, *, policy: RetryPolicy | None = None,
                 encrypt: bool = False) -> None:
        self.policy = policy or RetryPolicy()
        self._key = OFFICIAL_TEST_KEY if encrypt else None
        self._sock: socket.socket | None = None
        self._framer = StreamFramer()
        self.reconnect_attempts = 0      # 连续失败次数（成功一次即清零）
        self.last_error: str = ""
        self.heartbeats_sent = 0
        self._last_heartbeat = 0.0

    # ---- 连接状态 ----
    @property
    def connected(self) -> bool:
        return self._sock is not None

    def connect(self, host: str, port: int) -> bool:
        """建立连接（失败不抛异常，只记 last_error 并返回 False）。"""
        self.close()
        try:
            sock = socket.create_connection((host, port), timeout=self.policy.ack_timeout_s)
            sock.settimeout(self.policy.ack_timeout_s)
        except OSError as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.reconnect_attempts += 1
            return False
        self._sock = sock
        self._framer.reset()             # ⚠️ 新连接的半包不能拼到旧连接上
        self.reconnect_attempts = 0
        self.last_error = ""
        self._last_heartbeat = time.monotonic()
        return True

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock = None
        self._framer.reset()

    def next_backoff(self) -> float:
        """下次重连前该等多久（调用方负责 sleep）。"""
        return self.policy.backoff_of(max(1, self.reconnect_attempts))

    # ---- 收发 ----
    def send_raw(self, payload: str) -> bool:
        """发一条报文（不等待应答）。"""
        if self._sock is None:
            self.last_error = "未连接"
            return False
        try:
            self._sock.sendall(payload.encode("ascii"))
            return True
        except OSError as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.close()
            return False

    def recv_packets(self) -> list[str]:
        """读一次 socket，返回这一轮拿到的**完整**报文（可能为空；半包留在分帧器里）。

        ⚠️ 返回空列表**不等于失败**（可能只是还没有完整报文）；调用方应结合超时判断。
        """
        if self._sock is None:
            return []
        try:
            chunk = self._sock.recv(4096)
        except (socket.timeout, TimeoutError):
            return []
        except OSError as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.close()
            return []
        if not chunk:
            # 对端关闭：不是异常，但要标记为断开
            self.last_error = "对端关闭连接"
            self.close()
            return []
        return self._framer.feed(chunk.decode("ascii", errors="replace"))

    def wait_ack(self, qn: str, *, cn: str = "9014") -> bool:
        """等一条**同 QN** 的应答（标准：应答按 QN 匹配请求）。超时返回 False。"""
        deadline = time.monotonic() + self.policy.ack_timeout_s
        while time.monotonic() < deadline:
            for raw in self.recv_packets():
                try:
                    packet = decode_packet(raw, key=self._key).packet
                except Exception:  # noqa: BLE001 — 坏报文不该打断等待
                    continue
                if packet.cn == cn and packet.qn == qn:
                    return True
        return False

    def send_with_retry(self, payload: str, qn: str, *, cn: str = "9014") -> bool:
        """发一条并等应答；失败按 `send_retries` 重发。返回是否最终送达。

        ⚠️ 重发**不改变报文内容**（QN 相同）——接收端可按 (QN, 命令) 幂等去重。
        """
        for _ in range(max(1, self.policy.send_retries)):
            if not self.send_raw(payload):
                return False
            if self.wait_ack(qn, cn=cn):
                return True
        return False

    def maybe_heartbeat(self, *, now: float | None = None) -> bool:
        """到点就发一次心跳（`CN=9015`）；返回是否发了。

        心跳的作用（标准表 12）：**判断网络连接在线状态**。
        本项目在出口线程按 `heartbeat_interval_s` 调用它。
        """
        interval = self.policy.heartbeat_interval_s
        if interval <= 0 or not self.connected:
            return False
        moment = time.monotonic() if now is None else now
        if moment - self._last_heartbeat < interval:
            return False
        payload = encode_packet(
            Packet(qn=make_qn(), st="91", cn=CN_HEARTBEAT, pw="123456",
                   mn="0" * 24, flag=8, region=""),
            key=self._key,
        )
        self._last_heartbeat = moment
        if self.send_raw(payload):
            self.heartbeats_sent += 1
            return True
        return False


# --------------------------------------------------------------------------- #
# 实现 1：HJ212（本项目主用；对接环保平台）
# --------------------------------------------------------------------------- #


class Hj212Adapter:
    """HJ 212-2025 出口：报文走 TCP，等 `CN=9014` 数据应答算送达。

    ⭐ 现在支持**长连接 + 心跳 + 超时重发 + 重连退避**（P4 缺口补齐）：
    传入 `connection=Hj212Connection(...)` 即走长连接路径；
    不传则退化为一次性请求（演示/单次调用用）。
    """

    name = "hj212"

    def __init__(self, *, encrypt: bool = False,
                 connection: Hj212Connection | None = None) -> None:
        self._key = OFFICIAL_TEST_KEY if encrypt else None
        self.connection = connection

    def build_payload(self, sample: Sample) -> str:
        ts_compact = sample.ts.replace("-", "").replace(":", "").replace(" ", "")
        # ⚠️ 原文 §6.3.4.1：同一项目不同类别用 `,`、不同项目之间用 `;`
        fields = [f"DataTime={ts_compact}"]
        fields.extend(f"{code_of(p.name)}-Rtd={sample.values[p.name]:g}" for p in POINTS)
        packet = Packet(
            qn=make_qn(), st=ST_ATMOSPHERIC_SOURCE, cn=CN_UPLOAD_REALTIME,
            pw="123456",                 # ⚠️ 占位：真实密码由平台下发
            mn="0" * 24,                 # ⚠️ 占位：真实 MN 由平台按 CPUID/MAC 赋码
            flag=8 | 1,
            region=";".join(fields),
            resend=sample.resend,        # True → 写 RF=1
        )
        return encode_packet(packet, key=self._key)

    def send(self, payload: str, *, host: str, port: int) -> bool:
        """发一条并等应答。有连接对象时走长连接+重发；否则一次性请求。"""
        if self.connection is not None:
            conn = self.connection
            if not conn.connected and not conn.connect(host, port):
                return False
            qn = Packet.from_data_segment(_segment_of(payload)).qn
            return conn.send_with_retry(payload, qn)
        with socket.create_connection((host, port), timeout=10) as sock:
            sock.sendall(payload.encode("ascii"))
            ack = sock.recv(4096).decode("ascii", errors="replace")
        # 送达判据：收到 CN=9014（标准表 12 数据应答）
        try:
            return decode_packet(ack).packet.cn == "9014"
        except Exception:  # noqa: BLE001
            return False

    def describe(self) -> str:
        return ("HJ212/TCP：报文 + ANSI CRC16" + ("+ SM4 数据区加密" if self._key else "")
                + "；送达判据=CN=9014 应答；补传写 RF=1。⭐ 参数全可配，对接时填入平台连接信息即可")


# --------------------------------------------------------------------------- #
# 实现 2：HTTP/JSON（第二个实现——抽接口的依据）
# --------------------------------------------------------------------------- #


class HttpJsonAdapter:
    """HTTP/REST 出口：JSON body，送达判据 = HTTP 2xx。

    用途定位（ADR-0006 §2 路线 B）：**面向企业上层系统 / 中台的统计类出口**，
    不是"实时上报环保平台"。加密走 TLS，不用 SM4（各协议自己的规范）。
    """

    name = "http-json"

    def build_payload(self, sample: Sample) -> str:
        return json.dumps(
            {
                "ts": sample.ts,
                "resend": sample.resend,
                # 用标准参数编码而不是项目字段名：让两种出口共用同一套语义
                "data": {code_of(p.name): sample.values[p.name] for p in POINTS},
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def send(self, payload: str, *, host: str, port: int) -> bool:
        body = payload.encode("utf-8")
        req = (
            f"POST /api/ingest HTTP/1.1\r\nHost: {host}:{port}\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("ascii") + body
        with socket.create_connection((host, port), timeout=10) as sock:
            sock.sendall(req)
            head = sock.recv(4096).decode("latin-1", errors="replace")
        # 送达判据：HTTP 2xx（对应 HJ212 的"等应答"）
        return head.startswith("HTTP/1.1 2") or head.startswith("HTTP/1.0 2")

    def describe(self) -> str:
        return "HTTP/JSON：送达判据=HTTP 2xx；加密走 TLS（不用 SM4）。定位=企业上层系统统计出口"


# --------------------------------------------------------------------------- #
# 实现 3：Modbus-TCP 出口（第三个实现 —— 用来检验 P5 抽的接口够不够用）
#
# 定位：把测点写成**厂级 DCS / PLC 的保持寄存器**（ADR-0006 §2 路线 B 的一种）。
# 现实中"数据给到 DCS"确实常走 Modbus，而不是 HTTP。
# 用它检验抽象的好处：它与前两个实现的**送达判据完全不同** ——
#   HJ212  = 等 CN=9014 应答（应用层确认）
#   HTTP   = 等 2xx（应用层确认）
#   Modbus = **没有应用层应答**，连接成功 + 写成功即认为送达（协议本身如此）
# → 正好暴露"送达判据"是否被抽象对了。
# --------------------------------------------------------------------------- #

#: 写寄存器时的放大系数：Modbus 保持寄存器是 16 位整数，测点值有小数。
#: 取 100（两位小数）；量程上限 < 655.35 才不溢出 —— 契约里的 m3/h 流量（65000）会溢出，
#: 所以这里**只在能安全放下的测点上用**，流量单独用系数 1（整数部分够用）。
MODBUS_OUT_SCALE_DEFAULT: Final[int] = 100


class ModbusTcpAdapter:
    """Modbus-TCP 出口：把 9 个测点按系数写成保持寄存器（起始地址 = 契约的 address）。"""

    name = "modbus-tcp"

    def __init__(self, *, scale: int = MODBUS_OUT_SCALE_DEFAULT) -> None:
        self._scale = scale

    def _scale_of(self, point: object) -> int:
        """每个测点的写法系数：大数（流量/压力）用 1，其余用 self._scale。

        ⚠️ 这是**出口侧的取舍**，不是契约变更：契约里的 `Point.scale` 是"设备寄存器→真实值"
        的系数，与"真实值→DCS 寄存器"无关（方向相反、接收方也不同）。
        """
        high = float(getattr(point, "high"))
        return 1 if high * self._scale > 65535 else self._scale

    def build_payload(self, sample: Sample) -> str:
        """这里"报文"= 要写的寄存器值（十六进制），便于打印与对账。"""
        parts = []
        for point in POINTS:
            factor = self._scale_of(point)
            raw = int(round(sample.values[point.name] * factor))
            parts.append(f"{point.address}={raw}")
        return "modbus://hr[" + ",".join(parts) + "]"

    def _registers(self, sample: Sample) -> list[int]:
        regs: list[int] = []
        for point in POINTS:
            factor = self._scale_of(point)
            regs.append(int(round(sample.values[point.name] * factor)))
        return regs

    def send(self, payload: str, *, host: str, port: int) -> bool:
        """用 pymodbus 客户端写保持寄存器；送达判据 = 连接成功 + 写成功（协议无应用层应答）。"""
        from pymodbus.client import ModbusTcpClient

        # payload 只是给人看的；真正的寄存器值由调用方通过 build 时的 sample 决定。
        # 这里为了保持 ProtocolAdapter 的签名，从 payload 里回解出寄存器值。
        regs = [int(item.split("=")[1]) for item in payload.split("[", 1)[1].rstrip("]").split(",")]
        client = ModbusTcpClient(host, port=port, timeout=5)
        try:
            if not client.connect():
                return False
            result = client.write_registers(0, regs)
            return not (result is None or result.isError())
        except Exception:  # noqa: BLE001 — 出口失败不能让调用方崩
            return False
        finally:
            client.close()

    def describe(self) -> str:
        return ("Modbus-TCP：写保持寄存器（9 测点，大数用系数1）；"
                "送达判据=连接+写成功（协议无应用层应答）。定位=DCS/PLC")


#: 已实现的出口（加新协议 = 在这里加一个类，**不改上层**）
ADAPTERS: dict[str, type] = {
    "hj212": Hj212Adapter,
    "http-json": HttpJsonAdapter,
    "modbus-tcp": ModbusTcpAdapter,
}


def build_adapter(name: str, **kwargs: object) -> ProtocolAdapter:
    """按名字取适配器；名字不认识时报错并列出可用的。"""
    try:
        cls = ADAPTERS[name]
    except KeyError as exc:
        raise KeyError(f"未知出口 {name!r}；已实现: {sorted(ADAPTERS)}") from exc
    return cls(**kwargs)  # type: ignore[call-arg]


__all__ = ["Sample", "ProtocolAdapter", "Hj212Adapter", "HttpJsonAdapter",
           "ADAPTERS", "build_adapter", "Hj212Adapter"]
