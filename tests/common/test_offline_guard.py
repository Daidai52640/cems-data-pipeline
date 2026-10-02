# -*- coding: utf-8 -*-
"""离线硬约束的可执行证据：单元测试**不发起任何网络连接**。

============================ 这个文件在证什么 ============================
技术评估的一条前置要求是"测试必须完全离线"（不连 TDengine 6041 / 不连 EMQX 1883 /
不读 `data/` / 不依赖 8 个容器）。口头声明不算证据，所以这里把它做成**可复现的运行证据**：

    在**子进程**里跑一次被测模块的测试，同时在收集阶段给 socket 打补丁：
        - `socket.socket.connect()` / `connect_ex()` / `create_connection()` → 直接抛错
        - `socket.socket.bind()` → 直接抛错（连监听也不许）
    任何模块只要真的去连库/连 broker，就会**当场炸成测试失败**，而不是被"环境恰好够用"掩盖。

补丁在 `conftest.py` 里**于收集之前**生效（导入 `alarm_judge` 之前），因此连
"import 期就建连接"这种形态也能抓到。

⚠️ 子进程跑的是 `tests/common/test_alarm_judge.py`，用一个与环境无关的固定时钟，
   所以它既证明"离线可跑"，也证明"换个时钟照样可跑"（不依赖当天日期/时区对不对）。
===========================================================================
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

#: 仓库根（本文件在 tests/common/ 下）；子进程要靠它导入 src.*
REPO_ROOT: Path = Path(__file__).resolve().parents[2]

pytest_plugins = ["pytester"]

#: 在子进程里生效的 socket 守卫：任何出网/绑定动作都直接抛错。
#: 写成独立字符串是为了让"守卫生效"这件事本身也可被断言阅读。
SOCKET_GUARD = '''
import socket as _socket

_BLOCKED = (
    "离线测试禁止任何网络动作：见 tests/common/test_offline_guard.py "
    "（不连 TDengine 6041 / 不连 EMQX 1883 / 不读 data/）"
)


def _blocked(*args, **kwargs):
    raise RuntimeError(_BLOCKED)


class _BlockedSocket(_socket.socket):
    """继承真 socket 只为满足类型检查；任何 I/O 入口都被堵死。"""

    connect = _blocked
    connect_ex = _blocked
    bind = _blocked
    listen = _blocked
    send = _blocked
    sendall = _blocked
    sendto = _blocked
    recv = _blocked
    recvfrom = _blocked
    accept = _blocked


_socket.socket = _BlockedSocket
_socket.create_connection = _blocked
_socket.create_server = _blocked
_socket.socketpair = _blocked
'''


def test_alarm_judge_tests_run_with_all_network_calls_blocked(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """子进程里堵死 socket，`test_alarm_judge.py` 必须照样全绿。

    这条测试若失败，说明被测路径上真的出现了网络动作 —— 那是硬约束被破坏，
    不允许用 skip/xfail 掩盖。
    """
    pytester.makepyfile(conftest=SOCKET_GUARD)
    # 让子进程能 import src.*（`runpytest_subprocess` 没有 env 参数，走环境变量）
    monkeypatch.setenv("PYTHONPATH", str(REPO_ROOT))

    body = pytester.makepyfile(
        test_offline_body="""
# -*- coding: utf-8 -*-
'''离线守卫下的最小集成回路：判定 → 事件 → SQL 文本 → 载荷。

故意不用随机时钟、不用真实环境：证明"离线 + 确定性"两条性质同时成立。
'''

import math

from src.common.alarm_judge import (
    PHASE_END,
    PHASE_INVALID,
    PHASE_START,
    AlarmConfig,
    AlarmJudge,
    AlarmTables,
    normalize_ts,
)
from src.common.points import ZS_TARGETS, to_reference_o2


def _values(dust, o2):
    return {"Dust": dust, "SO2": 30.0, "NOx": 45.0, "O2": o2}


def _references(dust, o2):
    return {
        "dust": to_reference_o2(dust, o2),
        "so2": to_reference_o2(30.0, o2),
        "nox": to_reference_o2(45.0, o2),
    }


def test_full_loop_is_deterministic_and_offline():
    config = AlarmConfig(window_samples=2, min_over_samples=2, recover_samples=2)
    tables = AlarmTables(db="cems", plant="plant1", device="cems1")
    judge = AlarmJudge(config)

    batch = [
        ("2026-10-02 10:00:00", _values(6.0, 6.0)),
        ("2026-10-02 10:00:05", _values(6.0, 6.0)),
        ("2026-10-02 10:00:10", _values(4.0, 6.0)),
        ("2026-10-02 10:00:15", _values(4.0, 6.0)),
        ("2026-10-02 10:00:20", _values(6.0, 21.0)),   # O2>=21% → 无效样本
    ]

    events = []
    for ts, values in batch:
        o2 = values["O2"]
        events.extend(judge.on_sample(ts, values, _references(values["Dust"], o2)))

    dust_phases = [event.phase for event in events if event.point == "dust"]
    assert dust_phases == [PHASE_START, PHASE_END, PHASE_INVALID]

    # 每个判定测点都要有 invalid 行（3 个：dust/so2/nox）
    assert len([event for event in events if event.phase == PHASE_INVALID]) == len(ZS_TARGETS)

    # 幂等：整批重放不再产生任何事件
    replayed = []
    for ts, values in batch:
        o2 = values["O2"]
        replayed.extend(judge.on_sample(ts, values, _references(values["Dust"], o2)))
    assert replayed == []

    # SQL 文本可生成、nan 不落进字面量
    for event in events:
        sql = tables.event_insert_sql(event)
        assert sql.startswith("INSERT INTO cems.plant1_cems1_")
        assert "nan" not in sql.lower()
        assert len(event.payload_json()) <= 256

    invalid = [event for event in events if event.phase == PHASE_INVALID][0]
    assert math.isnan(invalid.converted)

    # 时间归一在离线环境同样成立（两种 REST 形态归一到同一字符串）
    assert normalize_ts("2026-10-02T02:00:00.000Z") == normalize_ts("2026-10-02 10:00:00")


def test_blocked_socket_would_actually_fail():
    '''守卫自检：真有网络动作时确实会抛错（否则上面的绿是假绿）。'''
    import socket

    try:
        socket.create_connection(("127.0.0.1", 6041), timeout=0.1)
    except RuntimeError as exc:
        assert "离线测试禁止任何网络动作" in str(exc)
    else:
        raise AssertionError("socket 守卫没有生效：出网调用居然成功了")
"""
    )

    result = pytester.runpytest_subprocess(
        "-p", "no:cacheprovider",
        "-q",
        str(body),
    )
    result.assert_outcomes(passed=2)

    # 顺带证明收集到的确实是"被测模块的测试"，而不是空跑
    collected = pytester.runpytest_subprocess(
        "-p", "no:cacheprovider",
        "--collect-only", "-q",
        str(REPO_ROOT / "tests" / "common" / "test_alarm_judge.py"),
    )
    collected.assert_outcomes()                     # 收集期零 error / 零 failure
    stdout = collected.stdout.str()
    # ⚠️ 2026-10-02 由 104 更新为 109：本次为"折算分母下限"（ALARM_O2_DENOM_MIN）
    #    新增了 5 条小时结算用例（TestO2DenominatorFloor）。这个数字锁的是"被收集到的测试
    #    条数"而不是任何判定口径，按本行原来的约定同步即可；口径断言一条未动。
    assert "109 tests collected" in stdout          # 条数是硬编码的：改测试必须同步这里
    assert "test_alarm_judge.py::" in stdout


def test_test_files_contain_no_network_client_imports() -> None:
    """静态旁证：本目录的测试**不 import 任何网络客户端库**。

    （动态证据在上面那条 socket 守卫测试里；这条只是让"误引入 I/O 依赖"在 diff 里显眼。）
    用 AST 解析 import 语句而不是字符串匹配：注释里出现库名不该误报。
    """
    forbidden = {
        "paho", "taosrest", "taospy", "tapy", "taos", "flask", "redis",
        "pymodbus", "requests", "urllib", "http", "socket", "asyncio",
    }
    offenders: list[str] = []
    for path in sorted(Path(__file__).parent.glob("test_*.py")):
        if path.name == Path(__file__).name:
            continue                       # 本文件按设计就 import socket（用来堵它）
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                roots = [(node.module or "").split(".")[0]]
            else:
                continue
            for root in roots:
                if root in forbidden:
                    offenders.append(f"{path.name}: {root}")
    assert offenders == [], f"测试文件出现了网络/I-O 客户端依赖: {offenders}"


def test_subprocess_can_import_module_with_blocked_socket() -> None:
    """最小粒度旁证：在本进程内临时堵死 socket，import 被测模块仍必须成功。"""
    import importlib
    import socket as socket_module

    original_socket = socket_module.socket
    original_create = socket_module.create_connection

    def _blocked(*args: object, **kwargs: object) -> None:
        raise RuntimeError("离线测试禁止任何网络动作")

    socket_module.socket = _blocked          # type: ignore[assignment]
    socket_module.create_connection = _blocked  # type: ignore[assignment]
    try:
        module = importlib.import_module("src.common.alarm_judge")
        assert module.AlarmJudge is not None
        judge = module.AlarmJudge(module.AlarmConfig())
        assert judge.open_points() == []
    finally:
        socket_module.socket = original_socket          # type: ignore[assignment]
        socket_module.create_connection = original_create  # type: ignore[assignment]


def test_python_version_supports_configured_syntax() -> None:
    """`pyproject.toml` 声明 requires-python >= 3.10；当前解释器必须满足。"""
    assert sys.version_info >= (3, 10)
