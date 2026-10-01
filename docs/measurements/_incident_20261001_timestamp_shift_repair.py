"""修复演练造成的错位数据：把误写到 10:38 的 11 行搬回 18:38，并逐项核对。

背景（诚实记录）：
scripts/measure_cache_invalidation.py 的 read_rows() 直接截断 REST 返回的
ISO8601 **UTC** 串（'2026-10-01T10:38:02.000Z'），没有转成本地时区，
于是 DELETE 之后又把 11 行写回了 UTC 时刻。本脚本把它纠正过来。
"""
from __future__ import annotations

import base64
import json
import subprocess
import urllib.request
from datetime import datetime, timedelta, timezone

TD = "http://127.0.0.1:6041/rest/sql"
AUTH = "Basic cm9vdDp0YW9zZGF0YQ=="
LOCAL_TZ = timezone(timedelta(hours=8))      # 容器时区 Asia/Shanghai
TABLE = "cems.plant1_device1"
COLS = ("so2", "nox", "dust", "o2", "humidity", "flow", "temp", "pressure", "velocity")

import sys
sys.path.insert(0, r"F:\Project1\cems-data-pipeline")


def sql(text: str) -> dict:
    req = urllib.request.Request(TD, data=text.encode("utf-8"),
                                 headers={"Authorization": AUTH, "Content-Type": "text/plain"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def taos(statement: str) -> str:
    out = subprocess.run(["docker", "exec", "tdengine", "taos", "-s", statement],
                         capture_output=True, text=True, encoding="utf-8", errors="replace")
    text = (out.stdout or "") + (out.stderr or "")
    if out.returncode != 0 or "DB error" in text:
        raise RuntimeError(f"taos 失败: {statement}\n{text}")
    return text


def parse_utc(value: str) -> datetime:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1]
    text = text.replace("T", " ").split(".")[0]
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def read_rows(start: str, end: str) -> list[dict]:
    payload = sql(
        f"SELECT ts, {', '.join(COLS)} FROM {TABLE} "
        f"WHERE ts >= '{start}' AND ts < '{end}' ORDER BY ts ASC"
    )
    rows = []
    for raw in payload["data"]:
        row = {"ts": parse_utc(raw[0]).astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")}
        for index, name in enumerate(COLS, start=1):
            row[name] = float(raw[index])
        rows.append(row)
    return rows


WRONG_START, WRONG_END = "2026-10-01 10:38:00", "2026-10-01 10:39:00"
RIGHT_START, RIGHT_END = "2026-10-01 18:38:00", "2026-10-01 18:39:00"

wrong = read_rows(WRONG_START, WRONG_END)
print(f"误写的行数 = {len(wrong)}")
for row in wrong:
    print("  ", row["ts"], row["so2"])
assert len(wrong) == 11, f"预期 11 行，实际 {len(wrong)}"

right_before = read_rows(RIGHT_START, RIGHT_END)
print(f"目标分钟现有行数 = {len(right_before)}（应为 0）")

# 1) 删掉误写的行
taos(f"DELETE FROM {TABLE} WHERE ts >= '{WRONG_START}' AND ts < '{WRONG_END}';")
# 2) 按本地时间写回
for row in wrong:
    local_ts = row["ts"]                     # 10:38:xx UTC -> 18:38:xx 本地
    hour, rest = local_ts[11:13], local_ts[13:]
    local_ts = f"{local_ts[:11]}{int(hour) + 8:02d}{rest}"
    values = ", ".join(str(row[name]) for name in COLS)
    taos(f"INSERT INTO {TABLE} (ts, {', '.join(COLS)}) VALUES ('{local_ts}', {values});")
print("已写回 18:38 分钟")

right_after = read_rows(RIGHT_START, RIGHT_END)
wrong_after = read_rows(WRONG_START, WRONG_END)
print(f"核对：18:38 行数 = {len(right_after)}，10:38 行数 = {len(wrong_after)}")
same_ts = [r["ts"] for r in right_after] == [r["ts"] for r in wrong]
same_vals = all(
    all(abs(a[name] - b[name]) < 1e-6 for name in COLS)
    for a, b in zip(right_after, wrong)
)
print(f"核对：时间戳序列一致 = {same_ts}，逐值一致 = {same_vals}")
print(json.dumps(right_after, ensure_ascii=False, indent=1))
