# -*- coding: utf-8 -*-
"""接入层落盘队列：写库失败时把原始报文落到本地，等 TDengine 恢复后再补传。

====================== 为什么需要它（要修的那个洞） ======================
订阅端收到 QoS1 报文后，paho 在 `on_message` **正常返回**时发送 PUBACK，broker 随即
删掉队列里那条唯一的持久副本；而网关那边早在收到 broker PUBACK 时就把 `cache.jsonl`
删了。于是"**写库失败、但回调正常返回**" = 这条数据在整条链路上**没有任何副本**，
永久丢失（README 曾把它列为「接入层无副本 · P1 未修」）。

修法是**把提交点从"broker 确认"挪到"下游真的持久化"**：写库失败时先把**原始报文**
追加进本地队列（fsync 落盘）再返回；后台线程定期补传，只有入库成功才把它从队列里删掉。
这样任一时刻，"尚未入库的报文"至少有一份本地持久副本。

====================== 崩溃安全（照搬网关已验证的段方案） ======================
    spool.jsonl           只由 on_message 追加（单写者，永不整文件回写）
    inflight-<seq>.jsonl  只由补传线程持有（段名唯一，永不覆盖已有文件）
两个写者各写各的文件，靠"原子改名"交接：
    1) 取批：把 spool.jsonl 原子改名成一个**新**段名（目标名唯一，不覆盖任何文件）
    2) 补传：只读段文件（此时新采的数据继续追加进全新的 spool.jsonl）
    3) 收尾：把未成功的行写回 spool.jsonl 队首，**写成功之后**才删段文件
任何一步被杀，段文件都还在盘上 → 下次取批接着处理 → **不丢**；最坏是重复投递一次，
而 TDengine 按 `(子表, ts)` 覆盖，重复是幂等的、无害的。

⚠️ 与网关 `data/cache.jsonl` 是**两回事**：网关卡在"网关→broker"，本队列补的是
   "接入层→TDengine"。多设备时每台设备各用一份目录（`SUBSCRIBER_DATA_DIR`），
   理由与网关缓存目录同源（见 src/gateway/gateway.py 配置区）。
⚠️ 只保证**进程被杀/容器重启**不丢（依赖 fsync 落盘）；掉电等更极端场景未做演练，
   与项目其它部分的口径一致（见 README「已知边界」）。
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Final, Optional, Sequence

# ---- 文件名约定（改这里要同步改测试与运维手册的排障路径）----
SPOOL_FILE_NAME: Final[str] = "spool.jsonl"
SEGMENT_PREFIX: Final[str] = "inflight-"
SEGMENT_SUFFIX: Final[str] = ".jsonl"
SEGMENT_GLOB: Final[str] = f"{SEGMENT_PREFIX}*{SEGMENT_SUFFIX}"


class IngestSpool:
    """写库失败时的本地持久队列（append-only 队尾 + 唯一段名交接）。

    线程模型：`append()` 由 MQTT 回调线程调用；`take_batch()` / `finish()` 由**唯一**
    一个补传线程调用。两者用同一把锁串行化文件操作，但**绝不在锁内做网络 I/O** ——
    补传的写库动作发生在锁外（`take_batch` 返回之后），否则会卡住接收。
    """

    def __init__(
        self,
        data_dir: Path,
        *,
        max_bytes: int,
        trim_ratio: float,
        logger: logging.Logger,
    ) -> None:
        self.dir: Final[Path] = Path(data_dir)
        self.spool_file: Final[Path] = self.dir / SPOOL_FILE_NAME
        self.max_bytes: Final[int] = max(1, int(max_bytes))
        # 裁剪后保留的比例；夹在 (0, 1]
        self.trim_ratio: Final[float] = min(1.0, max(0.01, float(trim_ratio)))
        self.log: Final[logging.Logger] = logger
        self._lock: Final[threading.Lock] = threading.Lock()
        #: 本批涉及的段文件（`take_batch` 维护；收尾成功后清空）
        self._batch: list[Path] = []

    # ==================== 基础文件操作（调用方自持锁的除外） ====================

    def _read_lines(self, path: Path) -> Optional[list[str]]:
        """读文件的非空行；**返回 None 表示读失败**（与"文件不存在/为空"的 [] 严格区分）。

        ⚠️ 这个区分是安全关键：若把"读失败"当成"空"，`finish()` 就会把一个读不出来的段
        当成"已并入"而删掉 = 抹掉唯一副本。读失败一律返回 None，由调用方保守处理
        （不删任何文件、本轮不推进）。
        """
        try:
            if not path.exists():
                return []
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            self.log.error("[落盘队列] 读取 %s 失败（本轮不推进，保留文件）: %s", path.name, exc)
            return None
        return [line.strip() for line in text.splitlines() if line.strip()]

    @staticmethod
    def _size(path: Path) -> int:
        """文件字节数；不存在/读不到按 0（"没有待处理数据"）处理。"""
        try:
            return path.stat().st_size
        except OSError:
            return 0

    def _write_lines(self, path: Path, lines: Sequence[str]) -> bool:
        """整文件写入：先写 .tmp 再原子替换；tmp 写完 fsync，避免只停在页缓存里。

        ⚠️ 只有补传线程会调它（`finish`）；`append` 走的是"追加 + fsync"，不整文件回写，
        所以"追加中被回写抹掉"在结构上不可能发生。
        """
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + ".tmp")
            with tmp.open("w", encoding="utf-8") as fp:
                fp.write("".join(f"{line}\n" for line in lines))
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp, path)
            return True
        except OSError as exc:
            self.log.error("[落盘队列] 写入 %s 失败: %s", path.name, exc)
            return False

    def _segments(self) -> list[Path]:
        """全部在途段，按 seq 升序（= 时间序，最老的先发）。目录不存在时返回空列表。"""
        if not self.dir.is_dir():
            return []
        return sorted(self.dir.glob(SEGMENT_GLOB), key=self._segment_seq)

    @staticmethod
    def _segment_seq(path: Path) -> int:
        """段文件名里的序号；名字不合规范返回 -1（新段号从 max+1 起，不受歪名字影响）。"""
        name = path.name
        if not (name.startswith(SEGMENT_PREFIX) and name.endswith(SEGMENT_SUFFIX)):
            return -1
        digits = name[len(SEGMENT_PREFIX):-len(SEGMENT_SUFFIX)]
        return int(digits) if digits.isdigit() else -1

    def _next_segment(self) -> Path:
        """下一个段名 = 现有最大 seq + 1（只增不减；不覆盖任何已有文件）。"""
        seqs = [self._segment_seq(path) for path in self._segments()]
        return self.dir / f"{SEGMENT_PREFIX}{max(seqs, default=0) + 1:06d}{SEGMENT_SUFFIX}"

    # ==================== 对外接口 ====================

    def check_writable(self) -> bool:
        """启动时确认队列目录可写；不可写要立刻喊出来，而不是等第一条数据丢的时候才发现。"""
        probe = self.dir / ".write_probe"
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return True
        except OSError as exc:
            self.log.critical(
                "[落盘队列] 目录不可写，写库失败时将**无处落盘**（会丢数据）: %s (%s)",
                self.dir, exc,
            )
            return False

    def append(self, payload: str) -> bool:
        """把一条原始报文追加到队尾并 fsync 落盘；失败返回 False（调用方据此记 CRITICAL）。"""
        try:
            with self._lock:
                self.dir.mkdir(parents=True, exist_ok=True)
                with self.spool_file.open("a", encoding="utf-8") as fp:
                    fp.write(payload + "\n")
                    fp.flush()
                    os.fsync(fp.fileno())
                self._trim_if_oversize()
            return True
        except OSError as exc:
            self.log.critical("[落盘队列] 追加失败，本条数据将丢失: %s | %s", exc, payload)
            return False

    def _trim_if_oversize(self) -> None:
        """队列超过容量上限时，按"丢最旧、保最新"裁剪（调用方需已持有锁）。

        ⚠️ 这一步**会丢数据**（丢的是最旧的一批），所以只在"再不裁就要把磁盘写满、
        之后每条都写不进去"时才做，并且升 CRITICAL。容量因此必须给得比"一次正常
        断库窗口的积压"大得多（默认 256 MB ≈ 按 12 条/分钟算可扛数月）。
        """
        size = self._size(self.spool_file)
        if size <= self.max_bytes:
            return
        lines = self._read_lines(self.spool_file)
        if lines is None:                 # 读不出来就别写（否则可能把读失败当成"空文件"覆盖掉）
            return
        keep_count = max(1, int(len(lines) * self.trim_ratio))
        dropped = len(lines) - keep_count
        self.log.critical(
            "[落盘队列] 已积压 %.1f MB 超过上限 %.1f MB，丢弃最旧的 %d 条（保留最新 %d 条）"
            "—— 请尽快恢复 TDengine，否则会继续丢最旧的数据",
            size / 1024 / 1024, self.max_bytes / 1024 / 1024, dropped, keep_count,
        )
        self._write_lines(self.spool_file, lines[dropped:])

    def has_backlog(self) -> bool:
        """是否还有待补传数据（空文件与空段都不算）。

        ⚠️ 必须把**在途段**也算进来：硬杀发生在取批过程中时，数据只落在段文件里，
        此时 spool.jsonl 可能为空；只看它就永远不会触发补传 = 残留段永久滞留 = 真丢。
        """
        try:
            if self._size(self.spool_file) > 0:
                return True
            return any(self._size(path) > 0 for path in self._segments())
        except OSError as exc:
            self.log.error("[落盘队列] 检查积压失败: %s", exc)
            return False

    def backlog_stats(self) -> dict[str, int]:
        """积压统计（条数 / 字节），供补传日志使用。读失败按 0 处理。"""
        paths = [self.spool_file, *self._segments()]
        lines = 0
        total_bytes = 0
        for path in paths:
            total_bytes += self._size(path)
            read = self._read_lines(path)
            if read is not None:
                lines += len(read)
        return {"lines": lines, "bytes": total_bytes}

    def take_batch(self) -> list[str]:
        """接管待补传数据：把 spool.jsonl 原子改名成唯一段名，返回按时序的批次。

        改名之后，接收线程新采的数据继续追加到全新的 spool.jsonl，补传线程只动段文件，
        两个写者各写各的文件。上一轮中断残留的段排在本批队首，保证旧数据先于新数据补传。
        """
        with self._lock:
            leftover = [path for path in self._segments() if self._size(path) > 0]
            current: Optional[Path] = None
            if self._size(self.spool_file) > 0:
                target = self._next_segment()
                try:
                    os.replace(self.spool_file, target)   # ★ 原子交接（目标名唯一）
                    current = target
                except OSError as exc:
                    # 改名失败：本轮只补残留段；spool.jsonl 原样留着，下轮再试
                    self.log.error("[落盘队列] 取批改名失败: %s", exc)
            # 顺序 = 最老的先发：中断残留的段（更老）排在本批新段（刚改名）之前
            self._batch = leftover + ([current] if current is not None else [])
            lines: list[str] = []
            for path in self._batch:
                read = self._read_lines(path)
                if read is None:
                    # 有一段读不出来：本轮不推进（返回空批次），段文件原样留在盘上。
                    # 调用方拿到空批次会直接返回，不会调 finish，因此不会删任何文件。
                    self.log.error("[落盘队列] 取批中途读失败，本轮跳过（数据仍在盘上）")
                    return []
                lines.extend(read)
            return lines

    def finish(self, remaining: Sequence[str]) -> bool:
        """补传收尾：未确认的行放回 spool.jsonl 队首，写成功后才删本批的段文件。

        ⚠️ 回写失败时**绝不能删段文件** —— 它是这批数据当下唯一的副本，删掉就是真丢。
        """
        with self._lock:
            existing = self._read_lines(self.spool_file)
            if existing is None:
                # 读不出 spool.jsonl：保守处理 —— 不写、不删段，等下一轮
                self.log.critical(
                    "[落盘队列] 读取 %s 失败，保留段文件（本批 %d 条未确认）等待下次重试",
                    self.spool_file.name, len(remaining),
                )
                return False
            merged = list(remaining) + existing
            if not self._write_lines(self.spool_file, merged):
                self.log.critical(
                    "[落盘队列] 回写 %s 失败，保留段文件（本批 %d 条未确认）等待下次重试",
                    self.spool_file.name, len(remaining),
                )
                return False
            segments, self._batch = self._batch, []
            self._drop_segments(segments)
            return True

    def _drop_segments(self, segments: Sequence[Path]) -> None:
        """数据已并入 spool.jsonl 之后删掉残留段（删晚了只是重复补传一次，无害）。"""
        for segment in segments:
            try:
                segment.unlink(missing_ok=True)
            except OSError as exc:
                self.log.error("[落盘队列] 删除段 %s 失败: %s", segment.name, exc)
