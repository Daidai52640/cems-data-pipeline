# -*- coding: utf-8 -*-
"""在订阅端容器内验证 webhook 投递（用真实模块 + 真实网络，不碰数据库）。"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, "/app")

from src.common.alarm_judge import AlarmEvent            # noqa: E402
from src.platform import subscriber_to_td as sub         # noqa: E402


def event() -> AlarmEvent:
    return AlarmEvent(
        ts="2026-10-02 19:10:00", event_id="nox|instant|e2e", phase="start",
        point="nox", judge_type="instant", converted=55.13, raw_value=48.0, o2=6.1,
        limit_value=50.0, o2_reference=6.0, over_samples=3, window_seconds=15.0,
        judge_version="v1.0.0", trigger_ts="2026-10-02 19:09:00", reason="e2e",
    )


class W:
    def __init__(self) -> None:
        self.sqls: list[str] = []

    def write(self, sqls: list[str]) -> int:
        self.sqls.extend(sqls)
        return len(sqls)


class T:
    def event_insert_sql(self, e: AlarmEvent) -> str:
        return "INSERT_EVENT"

    def push_insert_sql(self, e: AlarmEvent, channel: str, status: str = "recorded") -> str:
        return f"INSERT_PUSH|{channel}|{status}"


url = os.environ.get("ALARM_PUSH_URL", "")
channel = os.environ.get("ALARM_PUSH_CHANNEL", "webhook").strip() or "webhook"
print(f"  ALARM_PUSH_URL     = {url!r}")
print(f"  ALARM_PUSH_CHANNEL = {channel!r}")
w = W()
sub.publish_alarm_events(w, T(), [event()], channel)
st = next((s.split("|")[2] for s in w.sqls if s.startswith("INSERT_PUSH|")), "?")
print(f"  推送状态 = {st}")
print(f"  事件行仍写入 = {'INSERT_EVENT' in w.sqls}")
sys.exit(0 if st == "sent" else 1)
