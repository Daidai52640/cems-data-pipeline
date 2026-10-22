# -*- coding: utf-8 -*-
"""日志节流规则：第一次必打、到间隔必打、中间抑制。"""

from __future__ import annotations

from src.common.log_throttle import should_emit


def test_first_time_always_emits():
    assert should_emit(0.0, 1234.0, 60.0) is True


def test_within_interval_is_suppressed():
    assert should_emit(1000.0, 1059.9, 60.0) is False


def test_at_or_after_interval_emits():
    assert should_emit(1000.0, 1060.0, 60.0) is True
    assert should_emit(1000.0, 4000.0, 60.0) is True


def test_zero_interval_always_emits():
    assert should_emit(1000.0, 1000.0, 0.0) is True
