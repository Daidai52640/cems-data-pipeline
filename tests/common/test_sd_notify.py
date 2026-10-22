# -*- coding: utf-8 -*-
"""sd_notify 的最小实现：无 NOTIFY_SOCKET 时必须是 no-op，绝不抛异常。

⚠️ 本目录禁止 import 网络客户端库（见 test_offline_guard.py 的静态检查），
   所以这里用 ``types.SimpleNamespace`` 顶替 socket 模块，不 import socket。
"""

from __future__ import annotations

import types

from src.common import sd_notify


def test_without_notify_socket_is_noop(monkeypatch):
    monkeypatch.delenv(sd_notify.ENV_NOTIFY_SOCKET, raising=False)
    assert sd_notify.notify_socket_path() is None
    assert sd_notify.notify("READY=1") is False
    assert sd_notify.notify_ready() is False
    assert sd_notify.notify_watchdog() is False


def test_abstract_socket_name_is_prefixed(monkeypatch):
    monkeypatch.setenv(sd_notify.ENV_NOTIFY_SOCKET, "@/org/freedesktop/systemd1/notify")
    assert sd_notify.notify_socket_path() == "\0/org/freedesktop/systemd1/notify"


def test_relative_path_is_rejected(monkeypatch):
    monkeypatch.setenv(sd_notify.ENV_NOTIFY_SOCKET, "notify.sock")
    assert sd_notify.notify_socket_path() is None


class _FakeSock:
    """记录 connect/sendall 的假 socket（上下文管理器协议与真 socket 一致）。"""

    instances: list["_FakeSock"] = []

    def __init__(self, family, kind):
        self.family = family
        self.kind = kind
        self.connected: list[str] = []
        self.sent: list[bytes] = []
        _FakeSock.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def connect(self, path):
        self.connected.append(path)

    def sendall(self, data: bytes):
        self.sent.append(data)


def _fake_socket_module() -> types.SimpleNamespace:
    return types.SimpleNamespace(AF_UNIX=1, SOCK_DGRAM=2, socket=_FakeSock)


def test_messages_are_sent_to_systemd(monkeypatch):
    _FakeSock.instances = []
    monkeypatch.setenv(sd_notify.ENV_NOTIFY_SOCKET, "/run/systemd/notify")
    monkeypatch.setattr(sd_notify, "socket", _fake_socket_module())

    assert sd_notify.notify_ready() is True
    assert sd_notify.notify_watchdog() is True
    assert sd_notify.notify_stopping() is True

    # 每次 notify() 都新建一个 socket（与真实现一致），所以跨实例汇总
    connected = [path for sock in _FakeSock.instances for path in sock.connected]
    sent = [data for sock in _FakeSock.instances for data in sock.sent]
    assert connected == ["/run/systemd/notify"] * 3
    assert sent == [b"READY=1", b"WATCHDOG=1", b"STOPPING=1"]


def test_send_failure_is_swallowed(monkeypatch):
    class _Broken(_FakeSock):
        def connect(self, path):
            raise OSError("no such socket")

    monkeypatch.setenv(sd_notify.ENV_NOTIFY_SOCKET, "/run/systemd/notify")
    fake = _fake_socket_module()
    fake.socket = _Broken
    monkeypatch.setattr(sd_notify, "socket", fake)
    assert sd_notify.notify_watchdog() is False
