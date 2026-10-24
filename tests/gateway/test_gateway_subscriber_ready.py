# -*- coding: utf-8 -*-
"""网关侧「订阅端就绪」状态机的单元测试（2026-10-25）。

只测状态转移与判据接线，不碰真实 MQTT：全部用显式 now 参数，
避免依赖真实时钟。
"""

import src.gateway.gateway as gw


def reset_state() -> None:
    with gw._READY_LOCK:
        gw.READY_STATE["last_beat"] = None
        gw.READY_STATE["after_connect"] = None
        gw.READY_STATE["wait_started"] = None
    gw._READY_DEGRADE_WARNED[0] = 0.0


def test_blocks_until_first_beat() -> None:
    reset_state()
    gw.note_mqtt_connected(now=100.0)
    ok, reason = gw.subscriber_ready(now=101.0)
    assert ok is False
    assert reason == "no-beat"


def test_beat_after_connect_passes() -> None:
    reset_state()
    gw.note_mqtt_connected(now=100.0)
    gw.note_subscriber_beat(now=101.0)
    ok, reason = gw.subscriber_ready(now=102.0)
    assert (ok, reason) == (True, "ok")


def test_old_beat_does_not_survive_reconnect() -> None:
    """关键回归：broker 重启后上一轮心跳失效，必须先等到新心跳。"""
    reset_state()
    gw.note_mqtt_connected(now=100.0)
    gw.note_subscriber_beat(now=101.0)
    assert gw.subscriber_ready(now=102.0)[0] is True

    gw.note_mqtt_connected(now=200.0)          # 模拟重连（broker 重启）
    ok, reason = gw.subscriber_ready(now=201.0)
    assert ok is False
    assert reason == "stale-before-reconnect"

    gw.note_subscriber_beat(now=202.0)         # 订阅端重连后发来新心跳
    assert gw.subscriber_ready(now=203.0) == (True, "ok")


def test_degrades_to_pass_after_wait_limit() -> None:
    reset_state()
    gw.note_mqtt_connected(now=300.0)
    # 第一次判定会记下 wait_started
    assert gw.subscriber_ready(now=301.0)[0] is False
    # 超过 SUBSCRIBER_READY_WAIT_MAX（默认 120 s）后降级放行
    ok, reason = gw.subscriber_ready(now=301.0 + gw.SUBSCRIBER_READY_WAIT_MAX + 1.0)
    assert ok is True
    assert reason == "degraded-wait-timeout"
