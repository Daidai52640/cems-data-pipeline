# -*- coding: utf-8 -*-
"""接入层落盘队列的单元测试（纯逻辑，不连 MQTT / TDengine）。

覆盖三类必须守住的纪律：
  1. 基本收发：append → take → finish 的顺序与去留
  2. 崩溃安全：残留段被接回（老数据先补）、段名不覆盖、回写失败绝不删段
  3. 与接入层接线：`drain_spool_once` 的"第一条失败就停""坏行只丢那一条"
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import NAMES, RANGES   # noqa: E402
from src.platform import subscriber_to_td as sub   # noqa: E402
from src.platform.ingest_spool import IngestSpool   # noqa: E402

LOGGER = logging.getLogger("test.ingest_spool")


def make_spool(tmp_path: Path, *, max_bytes: int = 1 << 20, trim_ratio: float = 0.5) -> IngestSpool:
    return IngestSpool(tmp_path, max_bytes=max_bytes, trim_ratio=trim_ratio, logger=LOGGER)


def make_payload(ts: str = "2026-10-03 10:00:00") -> str:
    """造一条 `parse_payload` 能通过的合法报文（各测点取量程中点）。"""
    body = " ".join(f"{name}={(RANGES[name][0] + RANGES[name][1]) / 2}" for name in NAMES)
    return f"{ts} {body} Flag=N"


# ==================== 一、基本收发 ====================

def test_append_take_finish_roundtrip(tmp_path):
    spool = make_spool(tmp_path)
    assert spool.append("a") and spool.append("b")
    assert spool.has_backlog() is True
    assert spool.take_batch() == ["a", "b"]      # 队尾追加 → 取批保持顺序
    assert spool.finish([]) is True
    assert spool.has_backlog() is False
    assert spool.take_batch() == []


def test_finish_keeps_unconfirmed_at_front(tmp_path):
    spool = make_spool(tmp_path)
    for line in ("a", "b", "c"):
        assert spool.append(line)
    assert spool.take_batch() == ["a", "b", "c"]
    assert spool.finish(["b", "c"]) is True       # a 已入库，b/c 未确认
    assert spool.has_backlog() is True
    assert spool.take_batch() == ["b", "c"]        # 未确认的原样留着、下次接着补


def test_check_writable(tmp_path):
    spool = make_spool(tmp_path / "not-yet-created")
    assert spool.check_writable() is True


# ==================== 二、崩溃安全 ====================

def test_leftover_segment_recovered_oldest_first(tmp_path):
    """硬杀发生在取批之后：残留段必须在下次取批时被接回，且排在新数据之前。"""
    (tmp_path / "inflight-000001.jsonl").write_text("old\n", encoding="utf-8")
    spool = make_spool(tmp_path)
    assert spool.append("new")
    assert spool.take_batch() == ["old", "new"]    # 老数据先补
    assert spool.finish([]) is True
    assert spool.has_backlog() is False
    assert not (tmp_path / "inflight-000001.jsonl").exists()


def test_take_without_finish_is_recoverable(tmp_path):
    """模拟"取批之后进程被杀"：不调 finish，换一个新实例重读目录仍能拿到同一批。"""
    spool = make_spool(tmp_path)
    assert spool.append("a")
    assert spool.take_batch() == ["a"]
    again = make_spool(tmp_path)                   # 不 finish → 模拟硬杀后重启
    assert again.has_backlog() is True
    assert again.take_batch() == ["a"]


def test_next_segment_never_overwrites_existing(tmp_path):
    (tmp_path / "inflight-000001.jsonl").write_text("old\n", encoding="utf-8")
    spool = make_spool(tmp_path)
    assert spool.append("new")
    spool.take_batch()
    # 新段必须是 000002 —— 000001 原封不动（"没有任何一次改名会覆盖已有文件"）
    assert (tmp_path / "inflight-000001.jsonl").read_text(encoding="utf-8") == "old\n"
    assert (tmp_path / "inflight-000002.jsonl").read_text(encoding="utf-8") == "new\n"


def test_finish_write_failure_keeps_segments(tmp_path, monkeypatch):
    """回写 spool.jsonl 失败时，绝不能删段文件 —— 那是本批数据当下唯一的副本。"""
    spool = make_spool(tmp_path)
    assert spool.append("a")
    spool.take_batch()
    monkeypatch.setattr(spool, "_write_lines", lambda *a, **k: False)
    assert spool.finish([]) is False
    segs = list(tmp_path.glob("inflight-*.jsonl"))
    assert len(segs) == 1
    assert segs[0].read_text(encoding="utf-8") == "a\n"


def test_oversize_trims_oldest(tmp_path):
    """容量超限时丢最旧、保最新（正常路径不该触发；这里只是把边界钉住）。"""
    spool = make_spool(tmp_path, max_bytes=1)      # 让每次 append 都触发一次裁剪
    for line in ("a", "b", "c"):
        assert spool.append(line)
    assert spool._read_lines(spool.spool_file) == ["c"]


# ==================== 三、与接入层接线（drain_spool_once） ====================

class FakeWriter:
    """按脚本返回成败的假写入器；记录收到的 (ts, values)。"""

    def __init__(self, results: list[bool]) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, dict[str, float]]] = []

    def write(self, ts: str, values: dict[str, float]) -> bool:
        self.calls.append((ts, values))
        return self.results.pop(0) if self.results else True


def test_drain_first_failure_stops_and_keeps_all(tmp_path):
    spool = make_spool(tmp_path)
    for i in range(3):
        assert spool.append(make_payload(f"2026-10-03 10:00:0{i}"))
    writer = FakeWriter([False, True, True])
    assert sub.drain_spool_once(writer, spool) == 0
    assert len(writer.calls) == 1                  # 第一条失败就停，不把整批都试一遍
    assert spool.backlog_stats()["lines"] == 3     # 三条都还在队列里


def test_drain_partial_then_success(tmp_path):
    spool = make_spool(tmp_path)
    for i in range(3):
        assert spool.append(make_payload(f"2026-10-03 10:00:0{i}"))
    assert sub.drain_spool_once(FakeWriter([True, False, True]), spool) == 1
    assert spool.backlog_stats()["lines"] == 2     # 第 2、3 条放回队首
    assert sub.drain_spool_once(FakeWriter([True, True]), spool) == 2
    assert spool.has_backlog() is False


def test_drain_drops_unparsable_line_only(tmp_path):
    """坏行只丢它自己、不影响其余（落盘内容本不该解析失败，真遇到不能让它卡住队列）。"""
    spool = make_spool(tmp_path)
    assert spool.append("this-is-not-a-valid-payload")
    assert spool.append(make_payload("2026-10-03 11:00:00"))
    assert sub.drain_spool_once(FakeWriter([True, True]), spool) == 1
    assert spool.has_backlog() is False
