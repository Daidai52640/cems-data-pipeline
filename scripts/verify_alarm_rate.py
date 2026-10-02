# -*- coding: utf-8 -*-
r"""排放超标告警 · 事件率验收（ADR-0002 §5 的两条量化判据，离线、不碰库）。

判据：
  A) SIM_SPIKE_ENABLE=0 跑 1 小时 → 事件表新增 start 行数 = 0
  B) 默认参数（SIM_SPIKE_PROB=0.004）跑 1 小时 → 出现 start/end 成对记录，
     且**小时事件数 <= 12 个/测点**（不出现告警风暴）

做法：用 src/device/simulator.py 的**真**仿真信号（同种子、同一小时、5 秒一条 = 720 条），
     喂给 AlarmJudge（默认判据参数），统计每个测点的 START 数。
     ⚠️ SPIKE_ENABLED 是模块导入期读的环境变量，所以两种模式必须分**两个进程**跑：
        python verify_alarm_rate.py --mode off
        python verify_alarm_rate.py --mode default
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = r"F:\Project1\cems-data-pipeline"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.common.alarm_judge import (   # noqa: E402
    COLUMN_TO_NAME,
    PHASE_END,
    PHASE_START,
    AlarmConfig,
    AlarmJudge,
)
from src.common.points import LIMITS, ZS_TARGETS, to_reference_o2   # noqa: E402
from src.device.simulator import CemsSimulator                      # noqa: E402

# ⚠️ 必须用**带时区**的时刻：设备层传的就是 datetime.now(timezone.utc)（simulator.SIM_EPOCH 也是 UTC）。
#    仿真时间轴上的一小时，与设备层同一时基。
HOUR_START = datetime(2026, 10, 2, 12, 0, 0, tzinfo=timezone.utc)
STEP_SECONDS = 5.0                                 # 网关轮询周期
SAMPLES = int(3600 / STEP_SECONDS)                 # 720 条
MAX_EVENTS_PER_POINT = 12                          # ADR §5 的事件率上限


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("off", "default"), required=True)
    args = parser.parse_args()

    simulator = CemsSimulator(seed=20261001)
    judge = AlarmJudge(AlarmConfig())
    starts = {column: 0 for column in ZS_TARGETS}
    ends = {column: 0 for column in ZS_TARGETS}
    raw_over = {column: 0 for column in ZS_TARGETS}
    conv_over = {column: 0 for column in ZS_TARGETS}
    peak = {column: 0.0 for column in ZS_TARGETS}

    for index in range(SAMPLES):
        moment = HOUR_START + timedelta(seconds=STEP_SECONDS * index)
        ts = moment.strftime("%Y-%m-%d %H:%M:%S")
        measured = simulator.sample_all(moment)
        o2 = measured["O2"]
        references = {
            column: to_reference_o2(measured[COLUMN_TO_NAME[column]], o2)
            for column in ZS_TARGETS
        }
        for column in ZS_TARGETS:
            raw = measured[COLUMN_TO_NAME[column]]
            limit = LIMITS[COLUMN_TO_NAME[column]]
            peak[column] = max(peak[column], references[column])
            raw_over[column] += 1 if raw > limit else 0
            conv_over[column] += 1 if references[column] > limit else 0
        for event in judge.on_sample(ts, measured, references):
            if event.phase == PHASE_START:
                starts[event.point] += 1
            elif event.phase == PHASE_END:
                ends[event.point] += 1

    print(f"模式 = {args.mode}（SIM_SPIKE_ENABLE={'0' if args.mode == 'off' else '1'}, "
          f"SIM_SPIKE_PROB={os.getenv('SIM_SPIKE_PROB', '0.004')}）")
    print(f"回放 {HOUR_START:%Y-%m-%d %H:%M} 起 1 小时、{SAMPLES} 条 5 秒样本（种子 20261001）")
    print(f"{'测点':<7}{'折算峰值':>10}{'标干越限条数':>14}{'折算越限条数':>14}"
          f"{'START':>8}{'END':>6}")
    for column in ZS_TARGETS:
        print(f"{column:<7}{peak[column]:>10.3f}{raw_over[column]:>14}{conv_over[column]:>14}"
              f"{starts[column]:>8}{ends[column]:>6}")
    print(f"判据统计: {judge.stats}")

    if args.mode == "off":
        total = sum(starts.values())
        print(f"\n[A 判据] SIM_SPIKE_ENABLE=0 → 事件数 = {total}")
        print("  PASS" if total == 0 else "  FAIL", "期望 0 条 START")
        return 0 if total == 0 else 1

    worst = max(starts.values())
    paired = all(starts[column] > 0 and ends[column] > 0 for column in ZS_TARGETS)
    print(f"\n[B 判据] 默认参数 → 单测点最多 {worst} 个事件（上限 {MAX_EVENTS_PER_POINT}）；"
          f"start/end 都出现 = {paired}")
    ok = worst <= MAX_EVENTS_PER_POINT and paired
    print("  PASS" if ok else "  FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
