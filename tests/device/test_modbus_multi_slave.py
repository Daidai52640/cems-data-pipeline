# -*- coding: utf-8 -*-
"""多从站分派的服务端语义（离线、不起 TCP 端口、不写库）。

★ 这个文件要挡住的是一条**真实发生过的静默错**（2026-10-02 踩到、2026-10-03 修）：

改动前 `build_server_context([1])`（= 只开 `--profile plant2`、忘开 `SLAVE_IDS=1,2`）
返回 `ModbusServerContext(slaves=<唯一块>, single=True)`，而 pymodbus 在 `single=True` 下
会把**任何**请求的 unit id 改写成内部地址 0 —— 于是"请求一个不存在的从站"会**成功返回**
这份唯一的数据块。实测（pymodbus 3.6.9，临时服务端只建从站 1）：

    read unit=1 → OK   data
    read unit=2 → OK   data     ← 从站 2 不存在，却拿到从站 1 的数据
    read unit=3 → OK   data

后果是 plant2 网关（`MODBUS_UNIT=2`）读到 device1 的数据 → **两台曲线逐值相等、
容器全 healthy、一条报错都没有**。修法：一律 `single=False`，让 pymodbus 走原生的
`NoSuchSlaveException` 分支。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymodbus.exceptions import NoSuchSlaveException   # noqa: E402

from src.common.points import POINTS, REG_BASE, REG_COUNT   # noqa: E402
from src.device.modbus_server import build_server_context   # noqa: E402


def _flow_slot() -> int:
    """Flow 在寄存器块里的下标（用于验证"读到的确实是这一份数据块"）。"""
    return next(p.address - REG_BASE for p in POINTS if p.name == "Flow")


class TestSingleSlaveIsStrict:
    """单从站也不许"有求必应"。"""

    def test_unknown_unit_raises_instead_of_returning_data(self) -> None:
        """⚠️ 本文件的核心断言：请求未登记的从站必须抛异常，不能返回数据。"""
        context = build_server_context([1])
        with pytest.raises(NoSuchSlaveException):
            context[2]

    def test_unknown_unit_is_not_contained(self) -> None:
        """`in` 也必须如实回答（`single=True` 会让它恒为 True，是同一个坑的另一面）。"""
        context = build_server_context([1])
        assert 1 in context
        assert 2 not in context

    def test_registered_unit_returns_the_same_data_block(self) -> None:
        """零回归：单从站时 `context[1]` 仍是刷新循环写的那一份数据块。"""
        context = build_server_context([1])
        block = context[1]
        block.setValues(3, REG_BASE, [7] * REG_COUNT)
        assert block.getValues(3, REG_BASE, REG_COUNT)[0] == 7


class TestMultiSlaveDispatch:
    """多从站按 unit id 各给各的数据块。"""

    def test_each_unit_has_its_own_block(self) -> None:
        """⚠️ 两台设备必须**各写各的**数据块 —— 否则又会退化成"两台数据一样"。"""
        context = build_server_context([1, 2])
        slot = _flow_slot()
        context[1].setValues(3, REG_BASE, [111] * REG_COUNT)
        context[2].setValues(3, REG_BASE, [222] * REG_COUNT)
        assert context[1].getValues(3, REG_BASE + slot, 1)[0] == 111
        assert context[2].getValues(3, REG_BASE + slot, 1)[0] == 222

    def test_unlisted_unit_still_raises(self) -> None:
        """多从站模式下，"第三个从站"同样必须被拒（不能悄悄落到某个已有从站上）。"""
        context = build_server_context([1, 2])
        with pytest.raises(NoSuchSlaveException):
            context[3]

    def test_contains_reflects_the_registered_set(self) -> None:
        context = build_server_context([1, 2])
        assert 1 in context and 2 in context
        assert 3 not in context
