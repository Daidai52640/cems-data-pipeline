# -*- coding: utf-8 -*-
"""原子交接回归测试：`cache.jsonl` ↔ 在途批次文件（离线 / 确定性 / 不碰生产 `data/`）。

规格：`docs/adr/0007-P3出口与传输方案.md` §2（P1~P7 断言、N1 负向对照、D1 已知窗口、
排序关/开两遍、退出码约定、CLI）。设计期取证：`docs/evidence/cache/atomic_handover_probe.json`。

用法::

    $env:PYTHONIOENCODING="utf-8"
    python scripts/verify_atomic_handover.py                 # 排序关/开各跑一遍
    python scripts/verify_atomic_handover.py --sort-mode off
    python scripts/verify_atomic_handover.py --verbose
    python scripts/verify_atomic_handover.py --keep-tmp      # 调试用，不放松 < 1 MB 断言

退出码：

| 码 | 含义 |
|---|---|
| 0 | 全部门禁断言通过（`[KNOWN]` 的 D1 不计入判定） |
| 1 | 有门禁断言 FAIL（含 P7 生产文件被改动） |
| 2 | **测试自身**的问题：语料缺失/行数不符/时间戳格式非法；或不匹配的坏实现没被负向对照抓住 |
| 3 | **安全守卫拒绝运行**：路径补丁未生效（防止污染生产 `data/`）——在任何写操作之前退出 |

⚠️ 本脚本完全离线：stub MQTT 客户端（不连 broker）、`TemporaryDirectory`（不碰生产 `data/`）、
失败与注入全部由"第 k 次 publish"下标决定（不 sleep、不起线程、不用随机）。

⚠️ 在途批次文件的读法（测试侧纪律）：本脚本**不硬编码**某一个文件名，而是把临时目录里
"除 `cache.jsonl` 以外的全部 `.jsonl` 产物"当作在途批次视图（`_in_flight_files()`）。
这样"固定名 `.sending`"与"唯一段名 `inflight-<seq>.jsonl`"两种命名都能被如实观测，
而 §2 的 P2/P5/P6/D1 断言**逐条强度不变**（不因为改名而放松）。
"""

from __future__ import annotations

import argparse
import logging
import os
import pathlib
import re
import shutil
import sys
from typing import Any, Callable, Optional, Sequence

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
for extra in (str(PROJECT_ROOT), str(PROJECT_ROOT / "scripts")):
    if extra not in sys.path:
        sys.path.insert(0, extra)

import src.gateway.gateway as gw  # noqa: E402
from _atomic_handover import (    # noqa: E402
    Checks,
    Sandbox,
    StubMqttClient,
    canonical,
    directory_fingerprint,
    fingerprint,
    fingerprint_text,
    multiset_equal,
    no_duplicates,
    read_all_lines,
    read_lines,
    STATUS_FAIL,
    STATUS_PASS,
)

# ==================== 常量（对应 ADR-0007 §2.4） ====================
SNAPSHOT = PROJECT_ROOT / "docs" / "evidence" / "drill" / "resend_snapshot_drill2.txt"
CORPUS_LINES = 35            # 快照行数（不足/超出都是测试自身的问题 → 退出码 2）
CURRENT_SLICE = slice(0, 33)
LEFTOVER_SLICE = slice(33, 35)
RESEND_WINDOW = 3            # 与沙箱里的 gw.RESEND_WINDOW 一致（造多窗口）
HOOK_PUBLISH_INDEX = 4       # 第 2 窗第 2 条：注入"补传中主循环同时写"
FAIL_INDICES = {4}           # 只让第 5 次 publish 未被确认 → 恰好 1 条未确认
NEW_LINE_TS = "2026-10-01 15:00:00"     # "补传期间主循环新采的数据"的显式时间戳
TS_PREFIX_LEN = 19                      # YYYY-MM-DD HH:MM:SS 定长
TS_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
TMP_BYTE_LIMIT = 1024 * 1024            # P7：临时目录 < 1 MB
TMP_FILE_LIMIT = 8                      # P7：临时目录文件数上限

PRODUCTION_FILES = (gw.CACHE_FILE, gw.SENDING_FILE)
PRODUCTION_DIR = gw.DATA_DIR

VERBOSE = False
KEEP_TMP = False


class CorpusError(Exception):
    """语料不可用（缺失/行数不符/时间戳格式非法）——测试自身的问题，退出码 2。"""


# ==================== 输入 ====================

def load_corpus(path: pathlib.Path = SNAPSHOT) -> tuple[list[str], list[str]]:
    """读真实报文快照并**逐字**校验，返回 `(leftover, current)`。

    - 逐字：只 `splitlines()` + 去首尾空白，不改大小写、不动时间戳、不解析字段。
    - 校验：必须恰好 35 行，且每行行首 19 字符是定长时间戳——否则测试的前提就不成立。
    """
    if not path.exists():
        raise CorpusError(f"语料缺失: {path}")
    raw = path.read_text(encoding="utf-8")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(lines) != CORPUS_LINES:
        raise CorpusError(f"语料行数不符: 期望 {CORPUS_LINES} 行，实际 {len(lines)} 行（{path}）")
    bad = [
        index + 1 for index, line in enumerate(lines)
        if len(line) < TS_PREFIX_LEN or not TS_PATTERN.match(line[:TS_PREFIX_LEN])
    ]
    if bad:
        raise CorpusError(f"语料行首时间戳格式非法（行号）: {bad[:5]}（{path}）")
    if not no_duplicates(lines):
        raise CorpusError("语料本身有重复行——'重复 + 丢失互相抵消'的判据会失效")
    return lines[LEFTOVER_SLICE], lines[CURRENT_SLICE]


def permute(batch: Sequence[str], sort_mode: str) -> list[str]:
    """测试自己的排列函数：排序关 = 原序；排序开 = 按行首时间戳稳定排序。

    ⚠️ 它代表"将来有人给取批加按时间序排序"这件事，期望值也由同一个函数算出，
    所以断言的是"**交接对排列不敏感**"，不是"必须保持某个具体顺序"。
    """
    if sort_mode == "off":
        return list(batch)
    return sorted(batch, key=lambda line: line[:TS_PREFIX_LEN])


def install_takeover_permutation(sandbox: Sandbox, sort_mode: str) -> None:
    """安装"按时间戳排序"的取批视图（只改返回**值**与它在盘上的顺序，不碰交接动作本身）。

    ⚠️ 必须把排列后的批次写回**本批自己的那个在途产物**（真实文件操作一行不改）：
    否则"盘上的在途批次"与调用方拿到的批次**顺序不一致**，P5/P6 拿"盘上内容 == 批次"
    作判据时会因为**测试自己造出来的不一致**而误报 FAIL（ADR-0007 §2.7 要求期望值与
    返回值同源，故盘上顺序必须与返回值同源）。

    写回目标是"实现自己声明的本批在途产物"：固定名实现 = `.sending`；
    段实现 = `gw.BATCH_SEGMENTS[0]`（本批那个段）。
    """
    if sort_mode == "off":
        return
    original = gw._take_over_pending

    def permuted() -> list[str]:
        ordered = permute(original(), sort_mode)
        if ordered:
            gw._write_lines(_current_in_flight_path(), ordered)
        return ordered

    gw._take_over_pending = permuted        # type: ignore[assignment]
    sandbox.original["_take_over_pending"] = original


# ==================== 磁盘观测（不硬编码文件名） ====================

def _cache_file() -> pathlib.Path:
    return gw.CACHE_FILE


def _in_flight_candidates(sandbox: Sandbox) -> list[pathlib.Path]:
    """候选在途文件：临时目录里"待发队列之外"的全部产物。

    实现侧两种命名都要覆盖（ADR-0007 §3.3）：
    - 固定名 `.sending`（`cache.jsonl.sending`）
    - 唯一段名 `inflight-<seq>.jsonl`

    判据 = 临时目录里的每个文件，去掉：
    - 待发队列本体 `cache.jsonl`（那是主循环的落点，不属于在途批次）
    - `_write_lines()` 的中间产物 `*.tmp`
    - 网关心跳 `.gateway_alive`（与批次无关）
    """
    cache_name = _cache_file().name
    result: list[pathlib.Path] = []
    for item in sorted(sandbox.tmp.rglob("*")):
        if not item.is_file():
            continue
        if item.name == cache_name or item.name.endswith(".tmp"):
            continue
        if item.name == ".gateway_alive":
            continue
        result.append(item)
    return result


def _in_flight_files(sandbox: Sandbox) -> list[pathlib.Path]:
    """在途批次视图 = 全部存在且在途产物（改名/换段都不影响这条读法）。"""
    return [item for item in _in_flight_candidates(sandbox) if item.exists()]


def _in_flight_lines(sandbox: Sandbox) -> list[str]:
    """在途批次视图的内容（按文件路径排序后拼接；单文件时就是文件原序）。"""
    return read_all_lines(_in_flight_files(sandbox))


def _in_flight_path(sandbox: Sandbox) -> pathlib.Path:
    """当前实现"正在持有本批数据"的那个文件路径。"""
    return _current_in_flight_path(sandbox)


def _current_in_flight_path(sandbox: Sandbox | None = None) -> pathlib.Path:
    """本批数据当下落在哪个文件上。

    - 旧版固定名实现：`gw.SENDING_FILE`（`cache.jsonl.sending`）
    - 唯一段名实现：`gw.BATCH_SEGMENTS[0]`（本批那个段）
    - 两者都取不到时：目录里最新的在途产物（兜底）
    """
    segments = getattr(gw, "BATCH_SEGMENTS", None)
    if segments:
        return pathlib.Path(segments[0])
    if sandbox is not None:
        files = _in_flight_files(sandbox)
        if files:
            return max(files, key=lambda item: (item.stat().st_mtime_ns, item.name))
    return pathlib.Path(gw.SENDING_FILE)


def _in_flight_bytes(sandbox: Sandbox) -> dict[str, bytes]:
    """在途批次产物的逐字节快照，键 = 文件名（P2/P5 的"逐字不变"判据用它）。"""
    return {item.name: item.read_bytes() for item in _in_flight_files(sandbox)}


def _next_segment_path() -> pathlib.Path:
    """实现自己算出的"下一个在途产物的路径"。

    固定名实现（`.sending`）返回 `DATA_DIR/cache.jsonl.sending`；
    唯一段名实现（`inflight-<seq>.jsonl`）返回尚未使用的下一个段名。
    测试用它来构造"上一轮补传残留"，从而**同时适配两种命名**。
    """
    helper = getattr(gw, "_next_segment_path", None)
    if callable(helper):
        return pathlib.Path(helper())
    return pathlib.Path(gw.SENDING_FILE)


def _setup_leftover(sandbox: Sandbox, lines: Sequence[str]) -> pathlib.Path:
    """把 `lines` 写成"上一轮补传的残留"（路径由实现自己决定，不覆盖已有产物）。"""
    path = _next_segment_path()
    assert gw._write_lines(path, list(lines)), f"构造残留失败: {path}"
    assert path.exists(), f"残留未落盘: {path}"
    return path


def _snapshot_in_flight(sandbox: Sandbox) -> dict[str, Optional[bytes]]:
    """在途产物的逐字节快照；**不预先创建**文件——被杀时该产物本来就没写出来。"""
    snapshot: dict[str, Optional[bytes]] = {}
    for item in _in_flight_candidates(sandbox):
        snapshot[item.name] = item.read_bytes() if item.exists() else None
    return snapshot


def _changed_after(snapshot: dict[str, Optional[bytes]], sandbox: Sandbox) -> list[str]:
    """快照里被改动或消失的在途产物名（"逐字不变"判据：空列表 = 全部逐字未变）。"""
    changed: list[str] = []
    for name, before in snapshot.items():
        path = sandbox.tmp / name
        after = path.read_bytes() if path.exists() else None
        if after != before:
            changed.append(name)
    return changed


def _expected_in_flight(window_index: int, batch: Sequence[str], fails: set[int]) -> list[str]:
    """第 `window_index` 窗结束、下一窗开始前的在途批次期望内容。

    此刻已完成前 `window_index` 窗的滚动回写，故 = 前 `window_index` 窗内未确认的行
    + 其后尚未发送的尾部（与 `resend_cache()` 的 `remaining + batch[offset+len(chunk):]` 同口径）。
    """
    boundary = window_index * RESEND_WINDOW
    remaining = [line for index, line in enumerate(batch) if index < boundary and index in fails]
    return remaining + list(batch[boundary:])


def _new_line_from(payload: str) -> str:
    """由批内报文的 K=V 段生成"补传期间主循环新采的数据"（逐字保留真实报文形态）。"""
    return f"{NEW_LINE_TS} {payload.split(' ', 2)[2]}"


def _section_a(sandbox: Sandbox, leftover: list[str], current: list[str]) -> None:
    print("\nA. 沙箱守卫与语料")
    print(f"     临时目录: {sandbox.tmp}")
    print(f"     CACHE_FILE: {gw.CACHE_FILE}")
    print(f"     SENDING_FILE: {gw.SENDING_FILE}")
    print(f"     DATA_DIR: {gw.DATA_DIR}")
    print(f"     RESEND_WINDOW={gw.RESEND_WINDOW}  HOOK=publish#{HOOK_PUBLISH_INDEX}  "
          f"FAIL=publish#{sorted(FAIL_INDICES)}")
    print(f"     语料: {SNAPSHOT.relative_to(PROJECT_ROOT)}（{CORPUS_LINES} 行，逐字）"
          f"  leftover={len(leftover)}  current={len(current)}")


# ==================== P1：取批只是排列 ====================

def case_p1_takeover_is_permutation(sandbox: Sandbox, mode: str, checks: Checks) -> None:
    """P1：取批只允许"排列"，不许改内容 / 丢 / 重。"""
    leftover, current = load_corpus()
    _setup_leftover(sandbox, leftover)
    gw._write_lines(gw.CACHE_FILE, current)

    expected_batch = permute(leftover + current, mode)
    batch = gw._take_over_pending()

    ok_multiset = multiset_equal(batch, expected_batch)
    ok_count = len(batch) == CORPUS_LINES
    ok_distinct = no_duplicates(batch)
    ok_cache_moved = not gw.CACHE_FILE.exists()
    ok_sending_count = len(_in_flight_lines(sandbox)) == CORPUS_LINES

    checks.check(
        ok_multiset and ok_count and ok_distinct and ok_cache_moved and ok_sending_count,
        f"P1 取批只是排列：多重集相等、{len(batch)} 条、无重复、cache.jsonl 已搬走",
    )
    print(f"         多重集={ok_multiset} 条数={len(batch)}/{CORPUS_LINES} "
          f"无重复={ok_distinct} cache已搬走={ok_cache_moved} "
          f"在途条数={len(_in_flight_lines(sandbox))}")
    if mode == "off":
        checks.check(
            batch == leftover + current,
            "P1⑥ 排序关时逐元素 == leftover + current（旧数据排在本批队首）",
        )


# ==================== P2：交接后两写者各写各的 ====================

def case_p2_two_writers_coexist(sandbox: Sandbox, mode: str, checks: Checks) -> None:
    """P2：单写者所有权模型的核心承诺——新采数据只进 cache.jsonl，在途产物逐字不变。"""
    leftover, current = load_corpus()
    _setup_leftover(sandbox, leftover)
    gw._write_lines(gw.CACHE_FILE, current)
    gw._take_over_pending()

    in_flight_before = _in_flight_bytes(sandbox)
    x1 = _new_line_from(current[0])
    x2 = _new_line_from(current[1])
    ok_save = gw.save_to_cache(x1) and gw.save_to_cache(x2)

    cache_lines = read_lines(gw.CACHE_FILE)
    in_flight_after = _in_flight_bytes(sandbox)

    checks.check(
        ok_save and cache_lines == [x1, x2],
        "P2 交接后新采数据只进 cache.jsonl（2 次追加逐元素 == [x1, x2]）",
    )
    checks.check(
        in_flight_after == in_flight_before,
        f"P2 在途产物逐字节未变（{sorted(in_flight_before)}）",
    )
    if VERBOSE:
        print(f"         cache.jsonl = {cache_lines}")


# ==================== P3 + P4 + P6：补传中主循环同时写 ====================

def case_p3_interleaved_write(sandbox: Sandbox, mode: str, checks: Checks) -> int:
    """P3/P4/P6：hook 在第 4 次 publish 时调 `save_to_cache()`，等价于"补传正发第 2 窗时主循环追加一条"。

    返回本轮的 `confirmed` 条数（供排序关/开交叉判据使用）。
    """
    leftover, current = load_corpus()
    _setup_leftover(sandbox, leftover)
    gw._write_lines(gw.CACHE_FILE, current)

    expected_batch = permute(leftover + current, mode)
    batch = gw._take_over_pending()
    assert multiset_equal(batch, expected_batch), "P3 前置失败：取批结果与期望排列不符"

    injected: list[str] = []
    observations: list[bool] = []
    obs_detail: list[str] = []

    def hook(index: int, payload: str) -> None:
        if index == HOOK_PUBLISH_INDEX:
            new_line = _new_line_from(payload)
            injected.append(new_line)
            gw.save_to_cache(new_line)          # ★ 全测试最关键的一次注入
        if index > 0 and index % RESEND_WINDOW == 0:
            window_index = index // RESEND_WINDOW
            on_disk = _in_flight_lines(sandbox)
            expected = _expected_in_flight(window_index, batch, FAIL_INDICES)
            matched = on_disk == expected
            observations.append(matched)
            obs_detail.append(f"#{index}={len(on_disk)}{'' if matched else '✗'}")
            if VERBOSE and not matched:
                print(f"         [窗口#{index}] 盘上 {len(on_disk)} 条 != 期望 {len(expected)} 条")

    stub = StubMqttClient(fail_indices=FAIL_INDICES, hook=hook)
    confirmed = gw.resend_cache(stub)

    unconfirmed = [line for index, line in enumerate(batch) if index in FAIL_INDICES]
    cache_lines = read_lines(gw.CACHE_FILE)

    # P3：收尾 == 未确认（前缀）+ 期间新增（后缀原序）
    ok_multiset = multiset_equal(cache_lines, unconfirmed + injected)
    ok_suffix = cache_lines[len(unconfirmed):] == injected
    prefix = cache_lines[:len(unconfirmed)]
    ok_prefix = (
        multiset_equal(prefix, unconfirmed) if mode == "on" else prefix == unconfirmed
    )
    checks.check(
        ok_multiset and ok_suffix and ok_prefix and no_duplicates(cache_lines),
        f"P3 收尾 cache.jsonl 前缀==未确认({len(unconfirmed)})、后缀==期间新增({len(injected)}，原序)",
    )
    if VERBOSE:
        print(f"         cache.jsonl 条数={len(cache_lines)} 未确认={len(unconfirmed)} 新增={len(injected)}")

    # P4：条数守恒
    checks.check(
        len(expected_batch) + len(injected) == confirmed + len(cache_lines)
        and confirmed == len(expected_batch) - len(unconfirmed),
        f"P4 条数守恒 {len(expected_batch)} + {len(injected)} == {confirmed} + {len(cache_lines)}",
    )

    # P6：多窗口滚动回写（观测点读真实盘）
    checks.check(
        len(observations) > 0 and all(observations),
        f"P6 {len(observations)} 个窗口观测点全部 == remaining + 未发尾部",
    )
    print(f"         窗口观测: {' '.join(obs_detail)}")
    return confirmed


# ==================== P5：回写失败 ====================

def case_p5_writeback_failure(sandbox: Sandbox, mode: str, checks: Checks) -> None:
    """P5：回写失败时在途产物仍在且逐字不变、一条不丢；解除阻塞后能补齐。

    ⚠️ 失败是**真实 OSError**：把 `cache.jsonl.tmp` 建成目录 → `_write_lines()` 里
    `tmp.write_text(...)` 抛 `PermissionError`（`OSError` 子类），走真实异常分支，
    不 patch `_write_lines`。
    """
    leftover, current = load_corpus()
    _setup_leftover(sandbox, leftover)
    gw._write_lines(gw.CACHE_FILE, current[:3])
    batch = gw._take_over_pending()

    in_flight_before = _in_flight_bytes(sandbox)
    cache_before = fingerprint(gw.CACHE_FILE)
    blocker = sandbox.tmp / "cache.jsonl.tmp"
    blocker.mkdir()

    stub = StubMqttClient(fail_indices=range(0, len(batch)))
    confirmed = gw.resend_cache(stub)

    in_flight_after = _in_flight_bytes(sandbox)
    in_flight_lines = _in_flight_lines(sandbox)
    cache_after = fingerprint(gw.CACHE_FILE)

    exists_ok = bool(_in_flight_files(sandbox))
    bytes_ok = in_flight_after == in_flight_before
    cache_ok = cache_after == cache_before
    nothing_dropped = multiset_equal(in_flight_lines, batch)
    checks.check(
        exists_ok and bytes_ok and cache_ok and nothing_dropped,
        f"P5 回写失败：在途产物仍在且逐字节不变；cache.jsonl 未被部分写入；{len(batch)} 条一条没丢",
    )
    if VERBOSE:
        print(f"         回写失败后 confirmed={confirmed} 在途条数={len(in_flight_lines)} "
              f"cache={fingerprint_text(cache_after)}")

    # 解除阻塞：保留确实保住了数据
    shutil.rmtree(blocker)
    stub2 = StubMqttClient()
    confirmed2 = gw.resend_cache(stub2)
    cache_lines = read_lines(gw.CACHE_FILE)
    checks.check(
        confirmed2 == len(batch) and not _in_flight_files(sandbox) and cache_lines == [],
        f"P5 解除阻塞后 {confirmed2}/{len(batch)} 全部确认、条数守恒、在途产物已清",
    )


# ==================== D1：已知窗口（不计门禁） ====================

class _ProcessKilled(BaseException):
    """模拟进程在"合并批写回"这一步被硬杀。

    用 `BaseException`（不是 `Exception`）：硬杀不会被 `except OSError` / `except Exception`
    接住，数据确实留在内存里再也回不来——这正是要复现的语义。
    ⚠️ 必须**抛出**而不是"跳过写回并返回成功"：返回成功会让调用方误以为已落盘，
    从而继续执行后面的清理动作，模拟出真实的硬杀里根本不会发生的状态。
    """


def case_d1_kill_window(sandbox: Sandbox, mode: str, checks: Checks) -> None:
    """D1：接管途中硬杀（`os.replace` 完成、合并批写回之前）→ leftover 是否还在盘上。

    模拟方式 = **在"合并批写回"那一步把控制流打断**（ADR-0007 §2.9），等价于进程在该步骤前
    被 SIGKILL：`.sending` 已被覆盖/新段已建好，但合并批还没落盘。

    判据（两种在途命名都可用，且**与排列无关**）：

    1. 残留产物**逐字节未被改动**（`read_bytes()` 相等）——覆盖动作会改它；
    2. leftover 的每一行都仍在盘上（多重集包含，不看顺序）；
    3. 落盘总数 == leftover + current（一条不少）。
    """
    leftover, current = load_corpus()
    leftover_path = _setup_leftover(sandbox, leftover)
    gw._write_lines(gw.CACHE_FILE, current)
    leftover_name = leftover_path.name
    leftover_bytes_before = leftover_path.read_bytes()
    expected_total = permute(leftover + current, mode)

    original_write = gw._write_lines
    state = {"killed": False}

    def killing_write(path: Any, lines: Any) -> bool:
        state["killed"] = True
        raise _ProcessKilled("模拟硬杀：合并批尚未写回")

    gw._write_lines = killing_write          # type: ignore[assignment]
    try:
        gw._take_over_pending()
    except _ProcessKilled:
        pass
    finally:
        gw._write_lines = original_write     # type: ignore[assignment]

    if not state["killed"]:
        checks.check(False, "D1 模拟未生效：接管路径没有走到'合并批写回'那一步"
                            "（说明被测实现的写法已变，本节结论需要重做）")
        return

    leftover_survived = (
        leftover_path.exists() and leftover_path.read_bytes() == leftover_bytes_before
    )
    on_disk_lines = _in_flight_lines(sandbox)
    leftover_keys = {canonical(line) for line in leftover}
    leftover_lines_on_disk = [line for line in on_disk_lines if canonical(line) in leftover_keys]
    nothing_lost = multiset_equal(leftover_lines_on_disk, leftover)
    total_ok = multiset_equal(on_disk_lines, expected_total)

    if leftover_survived and nothing_lost and total_ok:
        detail = (f"{len(leftover)} 条 leftover 逐字保留在 {leftover_name} "
                  f"（在途共 {len(on_disk_lines)} 条 == leftover + current）")
    else:
        parts = []
        if not leftover_survived:
            parts.append(f"{leftover_name} 已被覆盖/替换（逐字节不再相同，"
                         f"存在={leftover_path.exists()}）")
        if not nothing_lost:
            parts.append(f"leftover {len(leftover)} 条中仅 {len(leftover_lines_on_disk)} 条在盘上")
        if not total_ok:
            parts.append(f"落盘总数 {len(on_disk_lines)} != 期望 {len(expected_total)}")
        detail = "**leftover 丢失**：" + "；".join(parts)
    checks.known(f"D1 接管途中硬杀：{detail}（既有窗口，见 ADR-0007 §2.9；不计门禁）")
    if VERBOSE:
        print(f"         盘上在途产物 = {[p.name for p in _in_flight_files(sandbox)]}")


# ==================== N1：负向对照（证明检测器有鉴别力） ====================

def _install_finish_resend_always_unlink() -> Callable[[], None]:
    """N1a 的坏实现：回写失败**仍**删在途文件。

    对照的语义是"回写失败还删唯一副本"，所以这里删的是**当前实现真正的在途产物**：
    固定名实现 = `.sending`；段实现 = `BATCH_SEGMENTS` 里的段。测试不写死某一个文件名，
    否则换了命名之后这条对照会变成"删一个不存在的文件"，白白失去鉴别力。
    """
    original = gw._finish_resend

    def broken(remaining: list[str], confirmed: int) -> None:
        with gw.CACHE_LOCK:
            gw._write_lines(gw.CACHE_FILE, remaining + gw._read_lines(gw.CACHE_FILE))
            for path in list(getattr(gw, "BATCH_SEGMENTS", [])) or [gw.SENDING_FILE]:
                pathlib.Path(path).unlink(missing_ok=True)   # ★ 注入的缺陷

    gw._finish_resend = broken                            # type: ignore[assignment]

    def restore() -> None:
        gw._finish_resend = original                      # type: ignore[assignment]

    return restore


def _install_take_over_copy(sandbox: Sandbox) -> Callable[[], None]:
    """N1b 的坏实现：用"读完再写一份"代替 `os.replace`（`cache.jsonl` 仍在）。"""
    original = gw._take_over_pending
    next_segment = getattr(gw, "_next_segment_path", None)

    def broken() -> list[str]:
        with gw.CACHE_LOCK:
            leftover = gw._read_lines(gw.SENDING_FILE) if gw.SENDING_FILE.exists() else []
            current = gw._read_lines(gw.CACHE_FILE)
            batch = leftover + current
            if batch:
                target = pathlib.Path(next_segment()) if callable(next_segment) else gw.SENDING_FILE
                gw._write_lines(target, batch)            # ★ 复制而不是改名
            return batch                              # cache.jsonl 原封不动留在盘上

    gw._take_over_pending = broken                        # type: ignore[assignment]

    def restore() -> None:
        gw._take_over_pending = original                  # type: ignore[assignment]

    return restore


def case_n1a_writeback_failure_unlinks(sandbox: Sandbox) -> tuple[bool, str]:
    """N1a：坏实现（回写失败仍删在途文件）→ P5 的检测器必须报 FAIL。"""
    restore = _install_finish_resend_always_unlink()
    try:
        leftover, current = load_corpus()
        _setup_leftover(sandbox, leftover)
        gw._write_lines(gw.CACHE_FILE, current[:3])
        batch = gw._take_over_pending()
        in_flight_before = _in_flight_bytes(sandbox)
        (sandbox.tmp / "cache.jsonl.tmp").mkdir()
        gw.resend_cache(StubMqttClient(fail_indices=range(0, len(batch))))
        caught = (
            not _in_flight_files(sandbox)                       # 在途产物被删
            or _in_flight_bytes(sandbox) != in_flight_before     # 或字节被改
            or not multiset_equal(_in_flight_lines(sandbox), batch)
        )
        return bool(caught), (
            f"在途产物={[p.name for p in _in_flight_files(sandbox)]} "
            f"盘上条数={len(_in_flight_lines(sandbox))}/{len(batch)}"
        )
    finally:
        restore()


def case_n1b_copy_instead_of_rename(sandbox: Sandbox) -> tuple[bool, str]:
    """N1b：坏实现（复制代替改名）→ P1 的检测器必须报 FAIL。"""
    restore = _install_take_over_copy(sandbox)
    try:
        leftover, current = load_corpus()
        _setup_leftover(sandbox, leftover)
        gw._write_lines(gw.CACHE_FILE, current)
        first = gw._take_over_pending()
        cache_still_there = gw.CACHE_FILE.exists()
        second = gw._take_over_pending() if cache_still_there else []
        duplicated = bool(second) and not no_duplicates(second)
        caught = (
            cache_still_there
            or duplicated
            or not multiset_equal(first, leftover + current)
        )
        return bool(caught), (
            f"取批后 cache.jsonl 仍在={cache_still_there}；"
            f"二次取批 {len(second)} 条、有重复={duplicated}"
        )
    finally:
        restore()


# ==================== P7：磁盘与生产文件 ====================

def guard_p7_production_untouched(
    before_files: dict[str, dict[str, Any]],
    after_files: dict[str, dict[str, Any]],
    before_entries: set[str],
    after_entries: set[str],
    checks: Checks,
) -> None:
    """P7 哨兵断言：生产 `data/` 的两个缓存文件与目录条目集合前后一致。

    ⚠️ 八服务在跑时这条有歧义：变化既可能是"本脚本越界写了"（严重缺陷），
    也可能是**并发运行的网关进程**恰好写了它。脚本遇到变化不猜测，直接 FAIL 并打印
    三种可能的解释 + 判定方法（ADR-0007 §2.10）。
    """
    for name, before in before_files.items():
        after = after_files[name]
        ok = before == after
        checks.check(
            ok,
            f"生产 {name} ({fingerprint_text(before)}) 前后一致",
        )
        if not ok:
            checks.note(f"现在 = {fingerprint_text(after)}")
            checks.note("变化的三种可能：① 本脚本越界写了生产 data/（严重缺陷）；"
                        "② 并发运行的 cems-gateway 恰好写了它；③ 外部进程/人工操作。")
            checks.note("判定方法：a) 看上面的沙箱守卫三项是否全过；"
                        "b) 看临时目录里有没有越界产物；"
                        "c) `docker stop cems-gateway` 后复跑取干净信号。")
    checks.check(
        before_entries == after_entries,
        f"生产 data/ 目录条目集合未增删（{len(before_entries)} 项）",
    )


# ==================== 用例驱动 ====================

def _run_case(
    sort_mode: str,
    checks: Checks,
    label: str,
    body: Callable[[Sandbox, Checks], None],
    *,
    with_permutation: bool = True,
) -> None:
    """在一个全新沙箱里跑一个用例；`__exit__` 无论如何都恢复模块全局并回收临时目录。"""
    with Sandbox(gw, keep=KEEP_TMP) as sandbox:
        if with_permutation:
            install_takeover_permutation(sandbox, sort_mode)
        before = sandbox.temp_footprint()
        body(sandbox, checks)
        files, total_bytes = sandbox.temp_footprint()
        checks.check(
            files <= TMP_FILE_LIMIT and total_bytes < TMP_BYTE_LIMIT,
            f"P7 {label} 临时目录 {files} 个文件 / {total_bytes:,} B "
            f"< 上限 {TMP_FILE_LIMIT} 个 / {TMP_BYTE_LIMIT:,} B",
        )
        if VERBOSE:
            print(f"         （进入时 {before[0]} 个文件 / {before[1]:,} B）")
    # `__exit__` 里的 restore() 已经把 _take_over_pending 等模块全局还原（含排序包装）


def run_sort_mode(sort_mode: str, checks: Checks) -> int:
    """跑一遍完整场景（P1~P6 + D1），返回本轮 `confirmed`（排序关/开交叉判据）。"""
    title = "排序关（原序）" if sort_mode == "off" else "排序开（按行首时间戳稳定排序）"
    print(f"\n{'B' if sort_mode == 'off' else 'C'}. {title}")

    _run_case(sort_mode, checks, "P1", lambda sandbox, ck: case_p1_takeover_is_permutation(sandbox, sort_mode, ck))
    _run_case(sort_mode, checks, "P2", lambda sandbox, ck: case_p2_two_writers_coexist(sandbox, sort_mode, ck))

    confirmed_holder: list[int] = []

    def p3(sandbox: Sandbox, ck: Checks) -> None:
        confirmed_holder.append(case_p3_interleaved_write(sandbox, sort_mode, ck))

    _run_case(sort_mode, checks, "P3", p3)
    _run_case(sort_mode, checks, "P5", lambda sandbox, ck: case_p5_writeback_failure(sandbox, sort_mode, ck))
    _run_case(sort_mode, checks, "D1", lambda sandbox, ck: case_d1_kill_window(sandbox, sort_mode, ck))
    return confirmed_holder[0] if confirmed_holder else -1


def run_negative_controls(checks: Checks) -> tuple[bool, bool]:
    """D 段：N1a / N1b 必须抓住注入的坏实现。返回 `(caught_a, caught_b)`。"""
    print("\nD. 负向对照（注入的坏实现必须被检测器抓住，否则说明检测器无鉴别力）")

    with Sandbox(gw, keep=KEEP_TMP) as sandbox:
        caught_a, detail_a = case_n1a_writeback_failure_unlinks(sandbox)
    checks.check(
        caught_a,
        f"N1a 回写失败仍删在途文件 → 已被 P5 检测器抓住（{detail_a}）",
    )

    with Sandbox(gw, keep=KEEP_TMP) as sandbox:
        caught_b, detail_b = case_n1b_copy_instead_of_rename(sandbox)
    checks.check(
        caught_b,
        f"N1b 复制代替改名 → 已被 P1 检测器抓住（{detail_b}）",
    )
    return caught_a, caught_b


# ==================== 主流程 ====================

def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="verify_atomic_handover.py",
        description="原子交接回归测试：cache.jsonl ↔ 在途批次文件（离线 / 确定性 / 不碰生产 data/）",
        epilog=(
            "可复现的干净跑法（消除并发网关进程对 P7 哨兵的干扰）：\n"
            "  docker stop cems-gateway\n"
            "  $env:PYTHONIOENCODING='utf-8'; python scripts\\verify_atomic_handover.py\n"
            "  docker start cems-gateway"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--sort-mode", choices=("off", "on", "both"), default="both",
                        help="排序关 / 排序开 / 两遍都跑（默认 both）")
    parser.add_argument("--verbose", action="store_true", help="打印每个用例的中间观测")
    parser.add_argument("--keep-tmp", action="store_true",
                        help="调试用：打印临时目录路径并不删除（⚠️ 不放松 < 1 MB 断言）")
    return parser.parse_args(argv)


def _silence_gateway_logger() -> list[logging.Handler]:
    """把网关 logger 的告警/错误收进内存（不改变日志行为，只为输出可读且确定）。"""
    logger = logging.getLogger("gateway")
    logger.setLevel(logging.CRITICAL + 1)
    return logger.handlers


def main(argv: Optional[Sequence[str]] = None) -> int:
    global VERBOSE, KEEP_TMP

    args = _parse_args(argv)
    VERBOSE = bool(args.verbose)
    KEEP_TMP = bool(args.keep_tmp)
    _silence_gateway_logger()

    modes = ("off", "on") if args.sort_mode == "both" else (args.sort_mode,)

    print("=" * 78)
    print("原子交接回归测试：cache.jsonl ↔ 在途批次文件（离线 / 确定性 / 不碰生产 data/）")
    print(f"  语料: {SNAPSHOT.relative_to(PROJECT_ROOT)}（{CORPUS_LINES} 行，逐字）")
    print(f"  沙箱: <TemporaryDirectory>  RESEND_WINDOW={RESEND_WINDOW}  "
          f"HOOK=publish#{HOOK_PUBLISH_INDEX}  FAIL=publish#{sorted(FAIL_INDICES)}")
    print(f"  gateway.py md5: {gw.__file__}")
    print("=" * 78)

    checks = Checks()

    # ---- 前置：语料（测试自身的问题 → 退出码 2） ----
    try:
        leftover, current = load_corpus()
    except CorpusError as exc:
        print(f"\n[测试自身缺陷] {exc}")
        print("退出码 2：语料不可用，无法判定交接行为。")
        return 2

    # ---- 生产哨兵：跑前 ----
    before_files = {path.name: fingerprint(path) for path in PRODUCTION_FILES}
    before_entries = directory_fingerprint(PRODUCTION_DIR)

    # ---- A 段：沙箱守卫与语料 ----
    with Sandbox(gw, keep=KEEP_TMP) as sandbox:
        _section_a(sandbox, leftover, current)
        checks.check(True, "沙箱守卫三项全过（DATA_DIR/CACHE_FILE/SENDING_FILE 都指向临时目录）")
        checks.check(
            len(leftover) == 2 and len(current) == 33,
            f"语料 {CORPUS_LINES} 行，行首时间戳格式合法（leftover={len(leftover)}, current={len(current)}）",
        )

    # ---- B/C 段：排序关 / 排序开 ----
    confirmed_by_mode: dict[str, int] = {}
    for mode in modes:
        confirmed_by_mode[mode] = run_sort_mode(mode, checks)

    if len(confirmed_by_mode) == 2:
        off_n, on_n = confirmed_by_mode["off"], confirmed_by_mode["on"]
        checks.check(
            off_n == on_n >= 0,
            f"交叉判据：排序关/开两遍 confirmed 相同（off={off_n} / on={on_n}）",
        )

    # ---- D 段：负向对照 ----
    caught_a, caught_b = run_negative_controls(checks)

    # ---- E 段：磁盘与生产文件 ----
    print("\nE. 磁盘与生产文件")
    after_files = {path.name: fingerprint(path) for path in PRODUCTION_FILES}
    after_entries = directory_fingerprint(PRODUCTION_DIR)
    guard_p7_production_untouched(before_files, after_files, before_entries, after_entries, checks)

    # ---- 汇总 ----
    print("\n================ 结论 ================")
    for mode in modes:
        label = "排序关" if mode == "off" else "排序开"
        print(f"  {label}: {'通过' if not checks.failed else '见上方 FAIL'}")
    print(f"  D1 为已知窗口，输出 [KNOWN] 不计门禁")
    print(f"  判定统计: PASS {checks.passed} / FAIL {checks.failed} / KNOWN {checks.known_count}")

    # ---- 退出码 ----
    if not (caught_a and caught_b):
        print("\n退出码 2：负向对照没能抓住注入的坏实现 —— 这是**测试自身**的缺陷（检测器无鉴别力）。")
        for text in checks.failures:
            print(f"  - {text}")
        return 2
    if checks.failed:
        print(f"\n退出码 1：门禁断言有 {checks.failed} 项 FAIL。")
        for text in checks.failures:
            print(f"  - {text}")
        return 1
    print("\n全部通过（排序关/开各 1 遍；D1 为已知窗口，不计门禁）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
