# -*- coding: utf-8 -*-
"""HJ 212 的 TCP 分帧器：把字节流切成一条条完整报文。

TCP 是字节流，不保留消息边界。半包（报文没到齐）直接解析会 CRC 对不上；
粘包（一次收到多条）只取第一条会丢掉后面的。分帧器按 ``##`` 开头、``\\r\\n``
结尾（§6.3.2）切出完整报文。

只做分帧不做校验：CRC 校验由 codec.decode_packet() 负责，
"校验失败就丢"是调用方的策略。
"""

from __future__ import annotations

from typing import Final

#: 报文前缀与后缀（与 codec 保持一致）
PACKET_PREFIX: Final[str] = "##"
PACKET_SUFFIX: Final[str] = "\r\n"

#: 缓冲上限（字符）。超过它说明对面在灌垃圾或没按标准分帧 → 抛错而不是无限吃内存。
#: 取 64 KB：远超单条报文（典型 200~1200 字符，加密后也不会到这个量级）。
MAX_BUFFER_CHARS: Final[int] = 64 * 1024


class FramingError(ValueError):
    """分帧层面的错误（缓冲超限等），与报文内容无关。"""


class StreamFramer:
    """把 TCP 字节流切成完整报文。

    内部按字符缓冲（HJ 212 是 ASCII 文本协议）。若将来支持二进制/GBK 扩展，
    需要改成按字节缓冲。
    """

    def __init__(self, *, max_buffer_chars: int = MAX_BUFFER_CHARS) -> None:
        self._buffer: str = ""
        self._max = max_buffer_chars
        #: 已丢弃的前导垃圾字符数（用于可观测性/告警）
        self.skipped_chars = 0

    # ---- 状态查询 ----
    @property
    def pending(self) -> int:
        """缓冲里还没凑成完整报文的字符数（>0 表示有半包待续）。"""
        return len(self._buffer)

    def reset(self) -> None:
        """丢弃缓冲（重连/换连接时调用——旧连接的半包不能拼到新连接上）。"""
        self._buffer = ""

    # ---- 核心 ----
    def feed(self, chunk: str) -> list[str]:
        """喂入一段新收到的文本，返回这一轮完整的报文列表（可能为空）。"""
        if chunk:
            self._buffer += chunk

        packets: list[str] = []
        while True:
            end = self._buffer.find(PACKET_SUFFIX)
            if end < 0:
                break
            head = self._buffer[: end + len(PACKET_SUFFIX)]
            self._buffer = self._buffer[end + len(PACKET_SUFFIX):]

            # 前导垃圾在这里清：`\r\n` 之前若不含 `##`，那是噪声，不能交给上层。
            start = head.find(PACKET_PREFIX)
            if start < 0:
                self.skipped_chars += len(head)
                continue
            if start > 0:
                self.skipped_chars += start
                head = head[start:]
            packets.append(head)

        # 半包侧也可能被垃圾污染：`\r\n` 还没到，但前缀 `##` 之前全是噪声。
        # 只在"确实看到 ## 之后还有内容"时才裁，避免把正常半包误判成垃圾。
        first = self._buffer.find(PACKET_PREFIX)
        if first > 0:
            self.skipped_chars += first
            self._buffer = self._buffer[first:]

        if len(self._buffer) > self._max:
            raise FramingError(
                f"缓冲 {len(self._buffer)} 字符超过上限 {self._max}："
                f"对面可能没按 '##…\\r\\n' 分帧。已收到 {len(packets)} 条完整报文。"
            )
        return packets

    def decode_chunks(self, chunks: list[str]) -> tuple[list[str], int]:
        """便捷方法：按顺序喂多段，返回 (全部完整报文, 剩余缓冲字符数)。

        给测试/回放用：可以模拟任意切分方式（逐字节、随机切分），断言结果一致。
        """
        packets: list[str] = []
        for chunk in chunks:
            packets.extend(self.feed(chunk))
        return packets, self.pending


def split_packet(raw: str) -> tuple[str, str]:
    """把一条完整报文切成 (数据段, 包尾)；用于校验"结尾是不是 \\r\\n"。

    返回的数据段不含包头 `##` 与长度字段（那部分由 codec 处理）。
    """
    if not raw.startswith(PACKET_PREFIX):
        raise FramingError(f"报文没有以 {PACKET_PREFIX!r} 开头: {raw[:16]!r}")
    if not raw.endswith(PACKET_SUFFIX):
        raise FramingError(f"报文没有以 {PACKET_SUFFIX!r} 结尾: {raw[-8:]!r}")
    body = raw[len(PACKET_PREFIX): -len(PACKET_SUFFIX)]
    return body, PACKET_SUFFIX


__all__ = ["StreamFramer", "FramingError", "split_packet",
           "PACKET_PREFIX", "PACKET_SUFFIX", "MAX_BUFFER_CHARS"]
