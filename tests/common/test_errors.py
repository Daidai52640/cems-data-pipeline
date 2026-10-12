# -*- coding: utf-8 -*-
"""`src/common/errors.py` 的**分类自检**。

目的不是"测函数返回值"，而是**让这份清单不能悄悄烂掉**：
新增一个异常却忘了登记 → 这里失败。
"""

from __future__ import annotations

import inspect

import pytest

from src.common import errors


def _defined_error_classes() -> set[type[errors.CemsError]]:
    """本模块里定义的**全部** `CemsError` 子类（不含基类自身）。"""
    return {
        obj
        for _, obj in inspect.getmembers(errors, inspect.isclass)
        if issubclass(obj, errors.CemsError)
        and obj is not errors.CemsError
        and obj.__module__ == errors.__name__
    }


def test_all_errors_covers_every_defined_error() -> None:
    """⭐ 漏登记一条 = 失败。

    防的是：有人加了 `NewError` 却忘了加进 `ALL_ERRORS`，
    于是"这个系统里什么东西会失败"这份清单**悄悄不完整** —— 正是债 5 的老毛病。
    """
    assert _defined_error_classes() == set(errors.ALL_ERRORS)


def test_every_registered_error_derives_from_base() -> None:
    for cls in errors.ALL_ERRORS:
        assert issubclass(cls, errors.CemsError)


def test_no_duplicate_registration() -> None:
    assert len(set(errors.ALL_ERRORS)) == len(errors.ALL_ERRORS)


def test_response_modes_partition_all_errors() -> None:
    """⭐ "遇到一种失败该做什么"必须**不重不漏**。

    三种响应模式：致命退出 / 退避重试 / 丢掉这一条。
    若某类异常**不落在任何一组**里，调用方就不知道该怎么处理它 —— 那是个洞。
    """
    groups = [set(group) for group in errors.RESPONSE_MODES]

    # 两两不相交
    for i, left in enumerate(groups):
        for right in groups[i + 1 :]:
            assert left & right == set(), f"响应模式分组重叠: {left & right}"

    # 并集 == 全部
    union: set[type[errors.CemsError]] = set()
    for group in groups:
        union |= group
    assert union == set(errors.ALL_ERRORS)


def test_device_comm_error_retryable_by_default() -> None:
    """现场抖动是常态 —— 一次失败就放弃会丢数，所以默认可重试。"""
    assert errors.DeviceCommError("读超时").retryable is True
    assert errors.DeviceCommError("校验错", retryable=False).retryable is False


def test_payload_format_error_is_not_retryable_category() -> None:
    """语法坏的报文**重试同一条没有意义** —— 它不该落在"可重试"组里。"""
    assert errors.PayloadFormatError not in errors.RETRYABLE
    assert errors.PayloadFormatError in errors.PAYLOAD_LEVEL


def test_point_invalid_defaults_raw_to_none() -> None:
    """`MISSING` 的测点本来就没值 —— `raw` 默认 `None` 是刻意的。"""
    verdict = errors.PointInvalid(
        point="Dust", kind=errors.PointInvalidKind.MISSING, reason="报文未提供"
    )
    assert verdict.raw is None


def test_point_invalid_kinds_are_distinct() -> None:
    """⭐ `invalid` 与 `missing` 必须可区分 —— 两者的根因在不同侧（ADR-0009 §10.1）。"""
    assert errors.PointInvalidKind.INVALID != errors.PointInvalidKind.MISSING
    assert errors.PointInvalidKind.INVALID.value == "invalid"
    assert errors.PointInvalidKind.MISSING.value == "missing"


def test_point_invalid_is_not_an_exception() -> None:
    """⚠️ 它是**数据分类结果**，不是异常 —— 防止有人图省事改成 `raise`。"""
    assert not issubclass(errors.PointInvalid, BaseException)


def test_config_error_and_invariant_error_are_fatal() -> None:
    """配置错与不变量破坏**重试没有意义** —— 必须落在致命组。"""
    assert errors.ConfigError in errors.FATAL_AT_STARTUP
    assert errors.InvariantError in errors.FATAL_AT_STARTUP


@pytest.mark.parametrize("cls", [errors.DeviceCommError, errors.BrokerError, errors.StorageError])
def test_runtime_errors_are_retryable(cls: type[errors.CemsError]) -> None:
    assert cls in errors.RETRYABLE
