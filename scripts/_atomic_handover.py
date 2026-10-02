# -*- coding: utf-8 -*-
"""原子交接回归测试的共用工具（内部共用，沿用 scripts/_td_ops.py 的下划线前缀惯例）。

被 `scripts/verify_atomic_handover.py` 使用；本模块自身不跑任何用例、不产生输出。

⚠️ 纪律（对应 ADR-0007 §2.2 的四条设计约束 C1~C4）：
    - 离线：`StubMqttClient` 不连任何 broker，只实现 `publish` / `wait_for_publish` /
      `is_published` 三个成员（够 `_publish_async` / `_wait_published` 用）。
    - 确定性：失败与注入全部由"第 k 次 publish"下标决定，不 sleep、不起线程、不用随机。
    - 零风险：`Sandbox` 只把网关模块的缓存路径指到 `tempfile.TemporaryDirectory()`；
      `__enter__` 里有**硬守卫**，三个路径不指向临时目录就 `SystemExit(3)`，什么都不做。
    - 不写大量数据：语料 35 行（≈250 B/行），临时目录总字节由调用方断言 < 1 MB。
"""

from __future__ import annotations

import os
import pathlib
import shutil
import tempfile
from typing import Any, Callable, Iterable, Iterator, Sequence

# ---------------------------------------------------------------------------
# 1. 内容比较工具（ADR-0007 §2.6）
# ---------------------------------------------------------------------------

def canonical(line: str) -> str:
    """行内容的规范化键：去首尾空白 + 空白折叠。

    只做空白规范化——不解析字段、不改大小写、不动时间戳格式，
    这样任何"内容被改写"都会在比较里暴露出来。
    """
    return " ".join(line.split())


def multiset_equal(xs: Iterable[str], ys: Iterable[str]) -> bool:
    """多重集比较：规范化后排序逐元素相等（顺序无关，内容相关）。"""
    return sorted(map(canonical, xs)) == sorted(map(canonical, ys))


def no_duplicates(xs: Iterable[str]) -> bool:
    """规范化后无重复。

    单独断言它是必需的：否则"重复一条 + 丢失一条"会在多重集比较里互相抵消。
    """
    keys = [canonical(item) for item in xs]
    return len(keys) == len(set(keys))


# ---------------------------------------------------------------------------
# 2. stub MQTT 客户端（ADR-0007 §2.2 C1）
# ---------------------------------------------------------------------------

class StubMessageInfo:
    """stub 消息句柄：只需 rc / wait_for_publish / is_published 三个成员。

    `is_published()` 直接返回预先算好的结果——即"第 k 次 publish 是否被 broker 确认"，
    由 publish 下标唯一决定（无 sleep、无随机）。
    """

    def __init__(self, rc: int, ok: bool) -> None:
        self.rc = rc
        self._ok = ok

    def wait_for_publish(self, timeout: float | None = None) -> None:
        """对齐 paho：超时是**静默返回**，所以被测代码必须再用 is_published() 复核。"""
        return None

    def is_published(self) -> bool:
        return self._ok


class StubMqttClient:
    """stub MQTT 客户端：publish 计数 + 指定下标失败 + publish hook。

    - `fail_indices`：这些下标的 publish 判为"未确认"（等价于没收到 PUBACK）。
    - `hook(index, payload)`：在每次 publish 返回**之前**调用，用于确定性注入
      （P3 的 `save_to_cache()`）与读盘观测（P6）。
    - 只记录 topic/qos/payload，不产生任何网络行为。
    """

    def __init__(
        self,
        fail_indices: Iterable[int] = (),
        hook: Callable[[int, str], None] | None = None,
    ) -> None:
        self.n = 0
        self.fail_indices = set(fail_indices)
        self.hook = hook
        self.sent: list[str] = []
        self.calls: list[tuple[Any, Any, Any]] = []

    def publish(self, topic: Any, payload: Any = None, qos: Any = 0, **kwargs: Any) -> StubMessageInfo:
        index = self.n
        self.n += 1
        self.calls.append((topic, payload, qos))
        self.sent.append(payload)
        if self.hook is not None:
            self.hook(index, payload)
        return StubMessageInfo(rc=0, ok=index not in self.fail_indices)


# ---------------------------------------------------------------------------
# 3. 沙箱（ADR-0007 §2.3）
# ---------------------------------------------------------------------------

#: 需要在沙箱里被重定向的模块级常量（`DATA_DIR` 是推导常量，**没有 env 出口**）
PATCHED_NAMES: tuple[str, ...] = (
    "DATA_DIR",
    "CACHE_FILE",
    "SENDING_FILE",
    "RESEND_WINDOW",
    "PUBLISH_ACK_TIMEOUT",
    "CACHE_MAX_BYTES",
)

#: 需要在沙箱里被**复位**的模块级可变状态（不是常量，但同样会跨用例泄漏）。
#: `BATCH_SEGMENTS` 记录"本批落在哪些段"：不重置的话，下一个用例会以为上一个用例
#: （临时目录已删）的段仍是自己的，滚动回写与收尾都会写到错的地方。
RESET_STATE: dict[str, Any] = {
    "BATCH_SEGMENTS": [],
}

#: 测试固定用的在途窗口（造多窗口，见 §2.5 P6）
SANDBOX_WINDOW = 3
#: 沙箱里单条等 PUBACK 的超时（stub 立即返回，这个值只为不真等 5 s）
SANDBOX_ACK_TIMEOUT = 0.05
#: 沙箱里放大容量上限，保证 64 MB 裁剪路径不触发（裁剪不在本测试范围）
SANDBOX_MAX_BYTES = 64 * 1024 * 1024


def _temporary_directory() -> pathlib.Path:
    """当前进程的临时目录（`tempfile` 用的那个）。"""
    return pathlib.Path(tempfile.gettempdir()).resolve()


def _norm(path: Any) -> str:
    """把路径归一成可比较的字符串（realpath 展开符号链接/8.3 短名 + normcase 统一大小写）。"""
    return os.path.normcase(os.path.realpath(str(path)))


def same_path(left: Any, right: Any) -> bool:
    """两条路径是否指向同一个位置（归一后相等）。"""
    return _norm(left) == _norm(right)


def path_is_within(path: Any, directory: Any) -> bool:
    """`path` 是否**就在** `directory` 之内（含 `directory` 自身）。

    ⚠️ 方向是单向的：`path_is_within(tmp/"cache.jsonl", tmp)` 为真，
    反之 `path_is_within(tmp, tmp/"cache.jsonl")` 为假。守卫断言必须用单向语义 +
    `same_path()` 判"完全一致"，双向比较会把嵌套路径误判成不通过。
    """
    import fnmatch  # 局部导入：只有这一处用得上

    actual, expected = _norm(path), _norm(directory)
    if actual == expected:
        return True
    return fnmatch.fnmatch(actual, expected + os.sep + "*")


class Sandbox:
    """把 `src.gateway.gateway` 的缓存路径/在途窗口重定向到临时目录的上下文管理器。

    用法::

        with Sandbox(gw) as sandbox:
            sandbox.assert_paths_patched()   # 硬守卫，不通过直接 SystemExit(3)
            ...                              # 用例只往 sandbox.tmp 写

    ⚠️ `Final[...]` 只是类型标注，运行期不设防；上述函数在**调用时**才读模块全局，
    所以改模块属性是生效的（探针实测 `A_patch` 三项全 true）。
    """

    def __init__(self, gw: Any, *, prefix: str = "verify_ah_", keep: bool = False) -> None:
        self.gw = gw
        self.prefix = prefix
        self.keep = keep
        self._tmp_obj: tempfile.TemporaryDirectory[str] | None = None
        self.tmp: pathlib.Path | None = None
        self.cache_file: pathlib.Path | None = None
        self.sending_file: pathlib.Path | None = None
        self.original: dict[str, Any] = {}

    # ---- 路径属性 ----
    @property
    def data_dir(self) -> pathlib.Path:
        assert self.tmp is not None, "沙箱尚未进入"
        return self.tmp

    # ---- 上下文管理 ----
    def __enter__(self) -> "Sandbox":
        self._tmp_obj = tempfile.TemporaryDirectory(prefix=self.prefix)
        self.tmp = pathlib.Path(self._tmp_obj.name).resolve()
        self.cache_file = self.tmp / "cache.jsonl"
        self.sending_file = self.tmp / "cache.jsonl.sending"

        for name in PATCHED_NAMES:
            self.original[name] = getattr(self.gw, name)
        self.gw.DATA_DIR = self.tmp
        self.gw.CACHE_FILE = self.cache_file
        self.gw.SENDING_FILE = self.sending_file
        self.gw.RESEND_WINDOW = SANDBOX_WINDOW
        self.gw.PUBLISH_ACK_TIMEOUT = SANDBOX_ACK_TIMEOUT
        self.gw.CACHE_MAX_BYTES = SANDBOX_MAX_BYTES
        for name, value in RESET_STATE.items():
            # 兼容"还没引入该状态"的旧版实现（本测试要能在修复前后各跑一遍）
            if not hasattr(self.gw, name):
                continue
            self.original[name] = getattr(self.gw, name)
            setattr(self.gw, name, value)

        # ★ 硬守卫必须在任何写操作之前：宁可不测，也不许有污染生产的可能。
        self.assert_paths_patched()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        """无论如何都恢复原值（`try/finally` 语义由上下文管理器本身保证）。"""
        self.restore()
        self.cleanup()
        return False

    # ---- 守卫 ----
    def guard_failures(self) -> list[str]:
        """三项硬检查，返回空列表 = 全过。

        判据（ADR-0007 §2.3 第 3 条）：`gw.CACHE_FILE.parent == tmp` 且
        `gw.SENDING_FILE.parent == tmp` 且 `gw.DATA_DIR == tmp`。
        """
        problems: list[str] = []
        if self.tmp is None:
            return ["沙箱未进入（临时目录不存在）"]
        if not same_path(self.gw.DATA_DIR, self.tmp):
            problems.append(f"gw.DATA_DIR 未指向临时目录: {self.gw.DATA_DIR} vs {self.tmp}")
        if not (same_path(pathlib.Path(self.gw.CACHE_FILE).parent, self.tmp)
                and path_is_within(self.gw.CACHE_FILE, self.tmp)):
            problems.append(f"gw.CACHE_FILE 不在临时目录内: {self.gw.CACHE_FILE}")
        if not (same_path(pathlib.Path(self.gw.SENDING_FILE).parent, self.tmp)
                and path_is_within(self.gw.SENDING_FILE, self.tmp)):
            problems.append(f"gw.SENDING_FILE 不在临时目录内: {self.gw.SENDING_FILE}")
        return problems

    def assert_paths_patched(self) -> None:
        """硬守卫：三条路径任一不指向临时目录 → `SystemExit(3)`，什么都不做。"""
        problems = self.guard_failures()
        if problems:
            for item in problems:
                print(f"  [REFUSE] {item}", flush=True)
            raise SystemExit(3)

    # ---- 恢复与回收 ----
    def restore(self) -> None:
        for name, value in self.original.items():
            setattr(self.gw, name, value)
        self.original.clear()

    def cleanup(self) -> None:
        if self._tmp_obj is None:
            return
        if self.keep:
            print(f"  [KEEP] 临时目录未删除: {self.tmp}", flush=True)
            return
        shutil.rmtree(self.tmp, ignore_errors=True)
        self._tmp_obj.cleanup()

    # ---- 磁盘观测 ----
    def files(self) -> list[pathlib.Path]:
        """临时目录里的全部普通文件（含子目录）。"""
        assert self.tmp is not None
        return sorted(item for item in self.tmp.rglob("*") if item.is_file())

    def file_count(self) -> int:
        return len(self.files())

    def footprint_bytes(self) -> int:
        """临时目录总字节（P7 的 < 1 MB 断言就判它）。"""
        return sum(item.stat().st_size for item in self.files())

    def dir_entries(self) -> set[str]:
        """临时目录下的条目名集合（含目录），用于断言"没有越界产物"。"""
        assert self.tmp is not None
        return {item.name for item in self.tmp.iterdir()}

    def temp_footprint(self, limit_files: int = 8, limit_bytes: int = 1024 * 1024) -> tuple[int, int]:
        """返回 `(文件数, 总字节)`；只观测，判定交给调用方。"""
        return self.file_count(), self.footprint_bytes()


# ---------------------------------------------------------------------------
# 4. 判定收集与输出
# ---------------------------------------------------------------------------

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_KNOWN = "KNOWN"


class Checks:
    """逐条收集 `[PASS]/[FAIL]/[KNOWN]` 判定，最后统一汇总。

    ⚠️ `[KNOWN]` 是**已登记的既有窗口**（ADR-0007 §2.9 D1），只输出、不计门禁；
    否则套件永远红，反而会被人调松。
    """

    def __init__(self) -> None:
        self.results: list[tuple[str, str]] = []

    def check(self, condition: bool, description: str) -> bool:
        status = STATUS_PASS if condition else STATUS_FAIL
        self.results.append((status, description))
        print(f"  [{status}] {description}", flush=True)
        return condition

    def known(self, description: str) -> None:
        self.results.append((STATUS_KNOWN, description))
        print(f"  [KNOWN] {description}", flush=True)

    def note(self, text: str) -> None:
        print(f"         {text}", flush=True)

    # ---- 统计 ----
    def _count(self, status: str) -> int:
        return sum(1 for item, _ in self.results if item == status)

    @property
    def passed(self) -> int:
        return self._count(STATUS_PASS)

    @property
    def failed(self) -> int:
        return self._count(STATUS_FAIL)

    @property
    def known_count(self) -> int:
        return self._count(STATUS_KNOWN)

    @property
    def failures(self) -> list[str]:
        return [text for status, text in self.results if status == STATUS_FAIL]

    def summary(self, title: str) -> None:
        print(f"\n---- {title}: PASS {self.passed} / FAIL {self.failed} / KNOWN {self.known_count} ----")
        for text in self.failures:
            print(f"  [FAIL] {text}")


# ---------------------------------------------------------------------------
# 5. 文件指纹（ADR-0007 §2.10 哨兵断言）
# ---------------------------------------------------------------------------

def fingerprint(path: pathlib.Path) -> dict[str, Any]:
    """`(exists, size, mtime_ns)` 三元组；不存在时只记 `exists=False`。"""
    try:
        if not path.exists():
            return {"exists": False}
        stat = path.stat()
        return {"exists": True, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    except OSError as exc:                      # 权限/瞬时占用：如实登记，不伪装成"未变"
        return {"exists": None, "error": repr(exc)}


def fingerprint_text(fp: dict[str, Any]) -> str:
    """把指纹渲染成一行（输出用）。"""
    if fp.get("exists") is True:
        return f"exists=True,size={fp['size']},mtime_ns={fp['mtime_ns']}"
    if fp.get("exists") is False:
        return "exists=False"
    return f"exists=?,error={fp.get('error')}"


def directory_fingerprint(path: pathlib.Path) -> set[str]:
    """目录条目名集合（含目录本身）；目录不存在返回空集合。"""
    try:
        if not path.is_dir():
            return set()
        return {item.name for item in path.iterdir()}
    except OSError:
        return set()


# ---------------------------------------------------------------------------
# 6. 小工具
# ---------------------------------------------------------------------------

def read_lines(path: pathlib.Path) -> list[str]:
    """读非空行并 strip（与网关 `_read_lines` 同口径，供测试侧独立读盘）。"""
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    return [line.strip() for line in text.splitlines() if line.strip()]


def read_all_lines(paths: Sequence[pathlib.Path]) -> list[str]:
    """按给定顺序把所有文件的行拼起来（段文件/残留文件的合并读法）。"""
    merged: list[str] = []
    for path in paths:
        merged.extend(read_lines(path))
    return merged


def iter_with_index(seq: Sequence[Any]) -> Iterator[tuple[int, Any]]:
    for index, item in enumerate(seq):
        yield index, item
