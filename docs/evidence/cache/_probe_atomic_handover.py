# -*- coding: utf-8 -*-
"""一次性取证探针：核实 ADR-0007 §2（原子交接回归测试规格）与 §3.3（在途批次命名）里的几条事实。

⚠️ **本脚本是一次性取证用，不是测试，也不进 CI**（按 `docs/README.md` §5 的留档规则放
在 `evidence/cache/` 并在此注明用途）。它只做只读观测 + 临时目录内的小文件写入：

* 不连真 broker（用 stub MQTT 客户端）
* 不碰生产 `data/`（跑前断言 `data/cache.jsonl` / `.sending` 的 size/mtime 未变）
* 所有写入都在 `tempfile.mkdtemp()` 下，跑完 `shutil.rmtree` 删除

用法::

    $env:PYTHONIOENCODING="utf-8"
    python docs/evidence/cache/_probe_atomic_handover.py > docs/evidence/cache/atomic_handover_probe.json

核实四件事（结论写进 ADR-0007 §2/§3.3，本文件只出原始观测）：

A. 模块属性打补丁（`DATA_DIR`/`CACHE_FILE`/`SENDING_FILE`/`RESEND_WINDOW`）能否把缓存重定向到临时目录；
   stub 客户端 + publish hook 里调 `save_to_cache()` 能否确定性地复现"补传中主循环同时写 cache.jsonl"；
   多窗口滚动回写（`RESEND_WINDOW=3`）每窗后的 `.sending` 内容是否符合 `remaining + 未发尾部`。
B. 回写失败（把 `cache.jsonl.tmp` 做成目录 → 真实 OSError）时 `.sending` 是否仍在且**逐字不变**。
C. 接管途中被硬杀（`os.replace` 已覆盖 `.sending`、合并批尚未写回）时 leftover 是否仍在盘上。
D. 生产 `data/` 两个缓存文件的 size/mtime 在本次运行期间是否变化。
"""

from __future__ import annotations

import importlib.metadata
import json
import logging
import pathlib
import shutil
import sys
import tempfile
import time

ROOT = pathlib.Path(r"F:\Project1\cems-data-pipeline")
sys.path.insert(0, str(ROOT))

import paho.mqtt.client as mqtt                      # noqa: E402
import src.gateway.gateway as gw                     # noqa: E402

SNAPSHOT = ROOT / "docs" / "evidence" / "drill" / "resend_snapshot_drill2.txt"
WINDOW = 3
HOOK_INDEX = 4
FAIL_INDICES = {4}
OUT: dict[str, object] = {
    "meta": {
        "purpose": "ADR-0007 §2 原子交接回归测试规格 / §3.3 在途批次命名的设计期取证",
        "one_shot": True,
        "not_a_test": "不属 pytest，不进 CI；保留仅为「结论可追溯到证据」",
        "run_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "python": sys.version.split()[0],
        "paho_mqtt": importlib.metadata.version("paho-mqtt"),
        "gateway_md5": __import__("hashlib").md5(
            (ROOT / "src" / "gateway" / "gateway.py").read_bytes()
        ).hexdigest(),
    }
}


class LogCapture(logging.Handler):
    """把网关 logger 的告警/错误/严重记录收进内存，供证据留档（不改变日志行为）。"""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(f"{record.levelname} {record.getMessage()}")

    def key_lines(self) -> list[str]:
        return [item for item in self.records
                if item.startswith("CRITICAL") or "失败" in item or "保留" in item]


CAPTURE = LogCapture()
logging.getLogger().addHandler(CAPTURE)
logging.getLogger().setLevel(logging.DEBUG)


class Info:
    """stub 消息句柄：只需 rc / wait_for_publish / is_published 三个成员。"""

    def __init__(self, ok: bool) -> None:
        self.rc = mqtt.MQTT_ERR_SUCCESS
        self._ok = ok

    def wait_for_publish(self, timeout: float | None = None) -> None:
        return None

    def is_published(self) -> bool:
        return self._ok


class Stub:
    """stub MQTT 客户端：publish 计数 + 指定下标失败 + publish hook。"""

    def __init__(self, fail=(), hook=None) -> None:
        self.n = 0
        self.fail = set(fail)
        self.hook = hook
        self.sent: list[str] = []

    def publish(self, topic: str, payload: str, qos: int = 0) -> Info:
        index = self.n
        self.n += 1
        self.sent.append(payload)
        if self.hook is not None:
            self.hook(index, payload)
        return Info(index not in self.fail)


def make_env() -> pathlib.Path:
    """把网关模块的缓存路径/在途窗口重定向到临时目录。"""
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="probe_ah_"))
    gw.DATA_DIR = tmp
    gw.CACHE_FILE = tmp / "cache.jsonl"
    gw.SENDING_FILE = tmp / "cache.jsonl.sending"
    gw.RESEND_WINDOW = WINDOW
    gw.PUBLISH_ACK_TIMEOUT = 0.05
    gw.CACHE_MAX_BYTES = 64 * 1024 * 1024
    return tmp


def dir_bytes(path: pathlib.Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def fingerprint(path: pathlib.Path) -> dict[str, object]:
    if not path.exists():
        return {"exists": False}
    stat = path.stat()
    return {"exists": True, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def expected_sending(window_index: int, batch: list[str], fails: set[int]) -> list[str]:
    """第 `window_index` 窗开始时的 `.sending` 期望内容。

    此刻已完成第 `window_index - 1` 窗的回写，故 = 前 `window_index` 窗内未确认的行
    + 其后尚未发送的尾部。
    """
    boundary = window_index * WINDOW
    remaining = [line for i, line in enumerate(batch) if i < boundary and i in fails]
    return remaining + batch[boundary:]


LINES = [line.strip() for line in SNAPSHOT.read_text(encoding="utf-8").splitlines() if line.strip()]
LEFTOVER, CURRENT = LINES[33:35], LINES[0:33]
OUT["input"] = {
    "snapshot": "docs/evidence/drill/resend_snapshot_drill2.txt",
    "snapshot_lines": len(LINES),
    "leftover_lines": len(LEFTOVER),
    "current_lines": len(CURRENT),
    "resend_window": WINDOW,
    "hook_publish_index": HOOK_INDEX,
    "fail_publish_indices": sorted(FAIL_INDICES),
}

PRODUCTION_FILES = [ROOT / "data" / "cache.jsonl", ROOT / "data" / "cache.jsonl.sending"]
before_production = {str(path): fingerprint(path) for path in PRODUCTION_FILES}

# ---- A. 路径补丁 + 竞态复现 + 多窗口滚动回写 ----
tmp_a = make_env()
OUT["A_patch"] = {
    "DATA_DIR": str(gw.DATA_DIR),
    "DATA_DIR_is_temp": gw.DATA_DIR == tmp_a,
    "CACHE_FILE_parent_is_temp": gw.CACHE_FILE.parent == tmp_a,
    "SENDING_FILE_parent_is_temp": gw.SENDING_FILE.parent == tmp_a,
}
gw._write_lines(gw.SENDING_FILE, LEFTOVER)
gw._write_lines(gw.CACHE_FILE, CURRENT)
batch = gw._take_over_pending()
OUT["A_takeover"] = {
    "batch_len": len(batch),
    "cache_exists_after_takeover": gw.CACHE_FILE.exists(),
    "sending_len_after_takeover": len(gw._read_lines(gw.SENDING_FILE)),
    "order_is_leftover_plus_current": batch == LEFTOVER + CURRENT,
    "multiset_equal": sorted(batch) == sorted(LEFTOVER + CURRENT),
    "distinct_lines": len(set(batch)),
}

new_lines: list[str] = []
window_obs: list[dict[str, object]] = []


def hook(index: int, payload: str) -> None:
    """第 HOOK_INDEX 次 publish 时模拟"主循环同时追加一条新采数据"。"""
    if index == HOOK_INDEX:
        new_line = "2026-10-01 15:00:00 " + payload.split(" ", 2)[2]
        new_lines.append(new_line)
        gw.save_to_cache(new_line)
    if index > 0 and index % WINDOW == 0:
        window_index = index // WINDOW
        on_disk = gw._read_lines(gw.SENDING_FILE)
        window_obs.append({
            "k": index,
            "window_index": window_index,
            "sending_len_on_disk": len(on_disk),
            "matches_expected": on_disk == expected_sending(window_index, batch, FAIL_INDICES),
        })


stub_a = Stub(fail=FAIL_INDICES, hook=hook)
confirmed_a = gw.resend_cache(stub_a)
final_cache = gw._read_lines(gw.CACHE_FILE)
unconfirmed_expected = [line for i, line in enumerate(batch) if i in FAIL_INDICES]
OUT["A_resend"] = {
    "publishes": stub_a.n,
    "confirmed": confirmed_a,
    "new_lines_injected": new_lines,
    "final_cache_lines": final_cache,
    "final_sending_exists": gw.SENDING_FILE.exists(),
    "count_conservation": len(LINES) + len(new_lines) == confirmed_a + len(final_cache),
    "final_cache_prefix_is_unconfirmed": final_cache[:len(unconfirmed_expected)] == unconfirmed_expected,
    "final_cache_suffix_is_new_lines_in_order": final_cache[len(unconfirmed_expected):] == new_lines,
    "window_observations": window_obs,
    "all_windows_match_expected": all(item["matches_expected"] for item in window_obs),
}
OUT["A_tmp_bytes"] = dir_bytes(tmp_a)

# ---- B. 回写失败：.sending 是否仍在且逐字不变 ----
tmp_b = make_env()
gw._write_lines(gw.CACHE_FILE, LINES[:3])
gw._write_lines(gw.SENDING_FILE, LINES[3:5])
batch_b = gw._take_over_pending()
sending_before = gw.SENDING_FILE.read_bytes()
cache_before = fingerprint(gw.CACHE_FILE)
(tmp_b / "cache.jsonl.tmp").mkdir()          # 让 _write_lines(CACHE_FILE, …) 真实抛 OSError
stub_b = Stub(fail=range(0, len(batch_b)))
confirmed_b = gw.resend_cache(stub_b)
OUT["B_writeback_failure"] = {
    "batch_len": len(batch_b),
    "publishes": stub_b.n,
    "confirmed": confirmed_b,
    "blocked_by": "cache.jsonl.tmp 被做成目录 → _write_lines 抛 PermissionError(OSError)",
    "cache_before": cache_before,
    "cache_after": fingerprint(gw.CACHE_FILE),
    "sending_exists_after": gw.SENDING_FILE.exists(),
    "sending_bytes_identical": gw.SENDING_FILE.read_bytes() == sending_before,
    "sending_lines_after": len(gw._read_lines(gw.SENDING_FILE)),
    "nothing_dropped": sorted(gw._read_lines(gw.SENDING_FILE)) == sorted(batch_b),
}
shutil.rmtree(tmp_b / "cache.jsonl.tmp")     # 解除阻塞，验证"保留"确实保住了数据
stub_b2 = Stub()
confirmed_b2 = gw.resend_cache(stub_b2)
OUT["B_recovery_after_unblock"] = {
    "publishes": stub_b2.n,
    "confirmed": confirmed_b2,
    "sending_exists": gw.SENDING_FILE.exists(),
    "cache_lines": len(gw._read_lines(gw.CACHE_FILE)),
}
OUT["B_tmp_bytes"] = dir_bytes(tmp_b)

# ---- C. 接管途中被硬杀：leftover 是否还在盘上 ----
tmp_c = make_env()
gw._write_lines(gw.SENDING_FILE, LEFTOVER)
gw._write_lines(gw.CACHE_FILE, CURRENT)
orig_write = gw._write_lines


class Killed(BaseException):
    """模拟进程在"合并批写回"这一步之前被硬杀（该步骤未执行）。"""


def killer(path, lines):
    if pathlib.Path(path) == gw.SENDING_FILE:
        raise Killed()
    return orig_write(path, lines)


gw._write_lines = killer
raised = False
try:
    gw._take_over_pending()
except Killed:
    raised = True
finally:
    gw._write_lines = orig_write

after_kill = gw._read_lines(gw.SENDING_FILE)
OUT["C_kill_between_replace_and_writeback"] = {
    "step_skipped": raised,
    "sending_lines_on_disk": len(after_kill),
    "disk_equals_current_only": after_kill == CURRENT,
    "leftover_preserved": all(line in after_kill for line in LEFTOVER),
    "lost_leftover_lines": [line for line in LEFTOVER if line not in after_kill],
    "window_width_measured": False,
    "note": "窗口 = os.replace(cache→.sending) 之后、合并批 _write_lines(.sending) 完成之前；"
            "此期间 leftover 只存在于内存。窗口宽度的实测未做（需真实 kill 计时）。",
}
OUT["C_tmp_bytes"] = dir_bytes(tmp_c)

# ---- D. 生产 data/ 哨兵 ----
after_production = {str(path): fingerprint(path) for path in PRODUCTION_FILES}
OUT["D_production_sentinel"] = {
    "before": before_production,
    "after": after_production,
    "unchanged": before_production == after_production,
}

OUT["key_log_messages"] = CAPTURE.key_lines()

OUT["verdict"] = {
    "path_patch_works": bool(OUT["A_patch"]["CACHE_FILE_parent_is_temp"]),
    "takeover_is_permutation": bool(OUT["A_takeover"]["multiset_equal"]),
    "interleaved_write_reproduced": len(new_lines) == 1,
    "window_rollback_matches_expected": bool(OUT["A_resend"]["all_windows_match_expected"]),
    "count_conserved": bool(OUT["A_resend"]["count_conservation"]),
    "writeback_failure_keeps_sending_intact": bool(OUT["B_writeback_failure"]["sending_bytes_identical"]),
    "leftover_loss_window_exists": not bool(OUT["C_kill_between_replace_and_writeback"]["leftover_preserved"]),
    "production_data_untouched": bool(OUT["D_production_sentinel"]["unchanged"]),
    "temp_footprint_bytes_max": max(int(OUT["A_tmp_bytes"]), int(OUT["B_tmp_bytes"]), int(OUT["C_tmp_bytes"])),
}

print(json.dumps(OUT, ensure_ascii=False, indent=2))
for path in (tmp_a, tmp_b, tmp_c):
    shutil.rmtree(path, ignore_errors=True)
