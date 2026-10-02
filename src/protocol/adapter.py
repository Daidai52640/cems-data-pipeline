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
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

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
# 实现 1：HJ212（本项目主用；对接环保平台）
# --------------------------------------------------------------------------- #


class Hj212Adapter:
    """HJ 212-2025 出口：报文走 TCP，等 `CN=9014` 数据应答算送达。"""

    name = "hj212"

    def __init__(self, *, encrypt: bool = False) -> None:
        self._key = OFFICIAL_TEST_KEY if encrypt else None

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
                + "；送达判据=CN=9014 应答；补传写 RF=1。⚠️ MN/PW 占位，无真实平台")


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


#: 已实现的出口（加新协议 = 在这里加一个类，**不改上层**）
ADAPTERS: dict[str, type] = {"hj212": Hj212Adapter, "http-json": HttpJsonAdapter}


def build_adapter(name: str, **kwargs: object) -> ProtocolAdapter:
    """按名字取适配器；名字不认识时报错并列出可用的。"""
    try:
        cls = ADAPTERS[name]
    except KeyError as exc:
        raise KeyError(f"未知出口 {name!r}；已实现: {sorted(ADAPTERS)}") from exc
    return cls(**kwargs)  # type: ignore[call-arg]


__all__ = ["Sample", "ProtocolAdapter", "Hj212Adapter", "HttpJsonAdapter",
           "ADAPTERS", "build_adapter", "Hj212Adapter"]
