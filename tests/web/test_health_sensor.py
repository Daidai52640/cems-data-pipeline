# -*- coding: utf-8 -*-
"""真机有效性判据（/api/health 的 sensor_ok）的测试。

守的纪律：**healthy 表示"链路在流"，sensor_ok 表示"真机读数有效"**。
真机失效时链路照常流（device 写越界哨兵 → 接入层拒收 → 库里只少两列），
所以必须有一条独立判据把它叫出来；但这条判据不能把整体 health 判成 503，
否则 nginx 的 depends_on=web healthy 会因为一个传感器起不来。

导入 web_dashboard 会尝试连 Redis（连不上自动降级，约 0.6 秒），与
test_web_contract_consts.py 的做法一致。
"""
from __future__ import annotations

from datetime import datetime, timedelta

from src.web import web_dashboard as dash


class _FakeCursor:
    def __init__(self, value):
        self._value = value
        self.sql = ""

    def execute(self, sql):
        self.sql = sql

    def fetchone(self):
        return (self._value,)


class _FakeConn:
    def __init__(self, value):
        self._cursor = _FakeCursor(value)

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def test_count_invalid_points_reads_count(monkeypatch):
    conn = _FakeConn(7)
    monkeypatch.setattr(dash.taosrest, "connect", lambda **kwargs: conn)
    assert dash.count_invalid_points(300) == 7
    assert "cems_point_invalid" in conn._cursor.sql
    assert "now - 300s" in conn._cursor.sql


def test_count_invalid_points_returns_none_on_error(monkeypatch):
    def boom(**kwargs):
        raise RuntimeError("td down")

    monkeypatch.setattr(dash.taosrest, "connect", boom)
    assert dash.count_invalid_points() is None


def test_sensor_verdict_below_threshold_is_valid(monkeypatch):
    monkeypatch.setattr(dash, "_sensor_invalid_since", None)
    monkeypatch.setattr(dash, "_sensor_critical_at", None)
    out = dash.sensor_invalid_verdict(dash.INVALID_POINTS_THRESHOLD - 1)
    assert out["sensor_ok"] is True
    assert out["sensor_reason"] == "valid"


def test_sensor_verdict_at_threshold_is_invalid_and_resets(monkeypatch):
    monkeypatch.setattr(dash, "_sensor_invalid_since", None)
    monkeypatch.setattr(dash, "_sensor_critical_at", None)
    out = dash.sensor_invalid_verdict(dash.INVALID_POINTS_THRESHOLD)
    assert out["sensor_ok"] is False
    assert out["sensor_reason"] == "invalid_points"
    assert dash._sensor_invalid_since is not None

    # 恢复：清空计时，回到 valid
    out2 = dash.sensor_invalid_verdict(0)
    assert out2["sensor_ok"] is True
    assert dash._sensor_invalid_since is None
    assert dash._sensor_critical_at is None


def test_sensor_verdict_unknown_count_is_not_a_failure():
    assert dash.sensor_invalid_verdict(None) == {
        "sensor_ok": None,
        "sensor_reason": "invalid_count_unavailable",
    }


def _stub_health(monkeypatch, invalid_count):
    now_text = (datetime.now() - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    row = (now_text,) + tuple(0.0 for _ in dash.POINTS)
    monkeypatch.setattr(dash, "query_recent", lambda minutes=1: [row])
    monkeypatch.setattr(dash, "count_invalid_points", lambda seconds=300: invalid_count)
    monkeypatch.setattr(dash, "_sensor_invalid_since", None)
    monkeypatch.setattr(dash, "_sensor_critical_at", None)
    return dash.app.test_client()


def test_health_ok_when_sensor_valid(monkeypatch):
    client = _stub_health(monkeypatch, 0)
    response = client.get("/api/health")
    assert response.status_code == 200
    body = response.get_json()
    assert body["healthy"] is True
    assert body["sensor_ok"] is True
    assert body["invalid_points_threshold"] == dash.INVALID_POINTS_THRESHOLD
    assert body["invalid_points_window_seconds"] == dash.INVALID_WINDOW_SECONDS


def test_health_flags_sensor_invalid_but_keeps_link_healthy(monkeypatch):
    client = _stub_health(monkeypatch, 99)
    response = client.get("/api/health")
    # 关键：HTTP 200 + healthy=True（链路在流），sensor_ok=False（真机读数无效）
    assert response.status_code == 200
    body = response.get_json()
    assert body["healthy"] is True
    assert body["sensor_ok"] is False
    assert body["sensor_reason"] == "invalid_points"
    assert body["invalid_points_last_window"] == 99
