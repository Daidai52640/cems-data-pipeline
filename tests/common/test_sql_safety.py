# -*- coding: utf-8 -*-
"""sql_safety（会被拼进 SQL 的标识符/tag 值白名单）与展示层启动护栏的测试。

口径（缺陷修复的目标，见 src/common/sql_safety.py 模块头）：
    1. 白名单规则只有一份，平台层（subscriber_to_td）与展示层（web/cache）共用；
    2. 非法值**拒绝启动**（SystemExit），不是 WARNING —— 静默降级正是
       "TD_DEVICE 写错 ⇒ 查询恒 0 行、接口 200、容器 healthy" 的来源。
      ⚠️ 白名单挡的是**注入**；大小写不符（`Device1` vs 库里的 `device1`）是**合法标识符**，
        挡它的是"数据新鲜度"判据（/api/health 503），不在本文件断言范围内。
===========================================================================
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from src.common.sql_safety import SQL_NAME_RE, SQL_NAME_RULE, check_sql_name, is_sql_name

#: 仓库根：子进程按它设置 CWD，保证 `import src.web.cache` 能找到包
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]


def test_valid_names_accepted() -> None:
    """默认部署取值与常见标识符都合法（单设备零回归的前提）。"""
    for value in ("plant1", "device1", "_x", "A_b9", "a" * 63, "P1_D2"):
        assert is_sql_name(value), value
        assert check_sql_name(value, what="x") == value


def test_injection_shaped_values_rejected() -> None:
    """注入形态一律拒绝：单引号 / 分号 / 连字符 / 空格 / 空值 / 非字符串。"""
    for value in (
        "device1' OR '1'='1",
        "device1'; DROP TABLE cems_data; --",
        "device-1",
        "device 1",
        "device1)",
        "",
        "1device",
        "a" * 64,
        "设备1",
        None,
        12,
    ):
        assert not is_sql_name(value), value


def test_check_raises_system_exit_with_actionable_message() -> None:
    """非法值抛 SystemExit（拒绝启动），错误信息带环境变量名与规则文案。"""
    with pytest.raises(SystemExit) as excinfo:
        check_sql_name("device1' OR '1'='1", what="展示层查询 tag device", env_var="TD_DEVICE")
    message = str(excinfo.value)
    assert "TD_DEVICE" in message
    assert SQL_NAME_RULE in message
    assert "拒绝启动" in message


def test_regex_is_single_source_for_both_layers() -> None:
    """平台层与展示层用的是**同一份规则**（平台层不再自己 compile 一份）。

    ⚠️ 用**静态检查**而不是 `import src.platform.subscriber_to_td`：那个模块会 import
    paho.mqtt，测试环境（与 Web 容器）都不该被这个依赖绑架 —— 这也正是把常量下沉到
    src/common 的理由本身。
    """
    from src.web import cache   # 展示层：导入期就会跑一次 tag 自检

    platform_src = (PROJECT_ROOT / "src" / "platform" / "subscriber_to_td.py").read_text("utf-8")
    assert "from src.common.sql_safety import" in platform_src
    # 平台层不得再定义自己的正则（同形常量两份 = 迟早分叉）
    assert "SQL_NAME_RE: Final[re.Pattern[str]] = re.compile" not in platform_src

    # 展示层同样没有私有正则，取值只能来自公共模块，且返回值必然通过白名单
    assert not hasattr(cache, "SQL_NAME_RE")
    plant, device = cache.device_tag_parts()
    assert (plant, device) == cache.DEVICE_SCOPE_PARTS
    assert is_sql_name(plant) and is_sql_name(device)
    assert isinstance(SQL_NAME_RE.pattern, str) and SQL_NAME_RE.pattern.startswith("^[A-Za-z_]")


#: 子进程的导入桩：**先堵死 socket 再 import**，让"导入 src.web.cache"这条路径
#: 与离线约束（tests/common/test_offline_guard.py）不冲突 —— cache 模块导入时会尝试连
#: Redis，堵掉之后走的是它自己的降级分支（连不上 ⇒ 缓存停用），不影响本文件要验的护栏。
_OFFLINE_IMPORT = (
    "import socket as _s\n"
    "def _blocked(*a, **k):\n"
    "    raise RuntimeError('offline test: socket blocked')\n"
    "_s.socket = _blocked\n"
    "_s.create_connection = _blocked\n"
    "import src.web.cache\n"
)


def test_web_cache_refuses_to_start_on_bad_tag() -> None:
    """展示层读路径的取值非法时，**进程起不来**（退出码非 0 + 指名环境变量）。

    用子进程而不是直接 import：这条护栏的语义就是"启动即拒绝"，必须验到进程退出码。
    ⚠️ 用 `device1' OR '1'='1`：这正是拼进 `AND device = '...'` 会短路的形态。
    """
    env = {**os.environ, "TD_DEVICE": "device1' OR '1'='1"}
    proc = subprocess.run(
        [sys.executable, "-c", _OFFLINE_IMPORT],
        cwd=str(PROJECT_ROOT), env=env, capture_output=True, check=False,
        # ⚠️ 必须显式给 utf-8：护栏的报错文案是中文，而 Windows 上 text=True 会用
        # 平台默认编码（本机 GBK）去解码 → UnicodeDecodeError → stderr 变成 None
        # → 下面的断言从"报错信息里有没有 TD_DEVICE"退化成 TypeError（测试自身崩掉，
        # 而不是失败）。这是本机在 Windows 上反复踩到的 GBK 解码陷阱（同一类问题已在别处出现过）。
        encoding="utf-8", errors="replace",
    )
    assert proc.returncode != 0
    assert "TD_DEVICE" in (proc.stderr or "")


def test_web_cache_starts_normally_with_default_tags() -> None:
    """对照：合法 tag 取值下导入成功 —— 护栏不误伤正常部署（单设备零回归）。"""
    env = {**os.environ, "TD_PLANT": "plant1", "TD_DEVICE": "device1"}
    proc = subprocess.run(
        [sys.executable, "-c", _OFFLINE_IMPORT],
        cwd=str(PROJECT_ROOT), env=env, capture_output=True, check=False,
        encoding="utf-8", errors="replace",     # 理由同上一个用例：中文文案 + Windows 默认 GBK
    )
    assert proc.returncode == 0, proc.stderr
