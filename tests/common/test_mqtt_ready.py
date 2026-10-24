# -*- coding: utf-8 -*-
"""订阅端就绪判据的单元测试（2026-10-25）。

覆盖 src/common/mqtt_ready.py 的两个纯函数：
- default_ready_topic：从数据主题推导就绪主题
- ready_verdict：放行 / 等待 / 降级等待 的边界
"""

from src.common.mqtt_ready import default_ready_topic, ready_verdict

# 一组固定的时间戳（monotonic 秒），比"现在"更好断言
T0 = 1000.0


def verdict(**kwargs):
    """默认参数：要求就绪、心跳在连接之后 1 秒、未等待、超时 120 秒。"""
    params = dict(
        required=True,
        last_beat=T0 + 1,
        after_connect=T0,
        now=T0 + 2,
        max_age=30.0,
        wait_started=None,
        wait_max=120.0,
    )
    params.update(kwargs)
    return ready_verdict(**params)


def test_ready_topic_from_data_topic() -> None:
    assert default_ready_topic("cems/plant1/data") == "cems/status/subscriber/plant1"
    assert default_ready_topic("cems/plant2/data") == "cems/status/subscriber/plant2"


def test_ready_topic_falls_back_when_topic_too_short() -> None:
    assert default_ready_topic("cems/data") == "cems/status/subscriber/plant1"
    assert default_ready_topic("") == "cems/status/subscriber/plant1"


def test_not_required_always_passes() -> None:
    ok, reason = verdict(required=False, last_beat=None, after_connect=None)
    assert (ok, reason) == (True, "not-required")


def test_no_beat_blocks() -> None:
    ok, reason = verdict(last_beat=None, after_connect=None, wait_started=T0 + 1)
    assert (ok, reason) == (False, "no-beat")


def test_beat_from_before_reconnect_blocks() -> None:
    """broker 重启后：上一轮的心跳不能算数，否则门是假的。"""
    ok, reason = verdict(last_beat=T0 - 5, after_connect=T0, wait_started=T0 + 1)
    assert (ok, reason) == (False, "stale-before-reconnect")


def test_fresh_beat_after_reconnect_passes() -> None:
    assert verdict() == (True, "ok")


def test_beat_too_old_blocks() -> None:
    ok, reason = verdict(last_beat=T0, after_connect=T0 - 10, now=T0 + 60, wait_started=T0 + 1)
    assert (ok, reason) == (False, "beat-stale")


def test_wait_timeout_degrades_to_pass() -> None:
    """等超过上限：放行但给出 degraded 原因（调用方据此打 WARNING）。"""
    ok, reason = verdict(
        last_beat=None, after_connect=T0, now=T0 + 121, wait_started=T0, wait_max=120.0,
    )
    assert (ok, reason) == (True, "degraded-wait-timeout")


def test_wait_timeout_not_reached_keeps_blocking() -> None:
    ok, reason = verdict(
        last_beat=None, after_connect=T0, now=T0 + 119, wait_started=T0, wait_max=120.0,
    )
    assert (ok, reason) == (False, "no-beat")


def test_exactly_at_max_age_passes() -> None:
    """边界：恰好等于 max_age 不算过期（用 > 比较，避免浮点抖动把好数据挡住）。"""
    ok, reason = verdict(last_beat=T0, after_connect=T0 - 1, now=T0 + 30, max_age=30.0)
    assert (ok, reason) == (True, "ok")
