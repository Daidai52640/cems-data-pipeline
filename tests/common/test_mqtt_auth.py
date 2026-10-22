# -*- coding: utf-8 -*-
"""MQTT 凭据下发：默认空时行为必须完全不变（不碰 client）。"""

from __future__ import annotations

from types import SimpleNamespace

from src.common.mqtt_auth import apply_mqtt_credentials


class _FakeClient:
    def __init__(self):
        self.calls: list[tuple] = []

    def username_pw_set(self, username, password=None):
        self.calls.append((username, password))


def test_empty_username_is_a_noop():
    client = _FakeClient()
    assert apply_mqtt_credentials(client, "", "") is False
    assert apply_mqtt_credentials(client, "   ", "secret") is False
    assert client.calls == []


def test_username_and_password_are_applied():
    client = _FakeClient()
    assert apply_mqtt_credentials(client, "cems-edge", "s3cret") is True
    assert client.calls == [("cems-edge", "s3cret")]


def test_empty_password_becomes_none():
    """只校验用户名的 broker：password 传 None 而不是空串。"""
    client = _FakeClient()
    assert apply_mqtt_credentials(client, "cems-edge", "") is True
    assert client.calls == [("cems-edge", None)]


def test_client_without_password_is_tolerated_for_store():
    """凭据来自环境变量/文件，不是 paho 对象；这里只保证不依赖 paho 的类型。"""
    holder = SimpleNamespace(username=None, password=None)
    assert apply_mqtt_credentials(_FakeClient(), holder.username or "", holder.password or "") is False
