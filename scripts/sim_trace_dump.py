# -*- coding: utf-8 -*-
"""子进程：按固定假时钟输出一整段仿真读数（JSON），供可复现性比对脚本调用。

单独成文件而不是 `python -c`：
  1. 系统随机化哈希种子（PYTHONHASHSEED）在独立进程里默认是随机的，
     用它跑两次能真正验证"种子派生不依赖进程内随机盐"
  2. 子进程只输出数据，比对逻辑留在父进程，职责清楚

用法（由 scripts/verify_sim_reproducible.py 调用）：
    python scripts/sim_trace_dump.py [种子] [起始时刻] [点数]
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS                                 # noqa: E402
from src.device.modbus_server import encode                          # noqa: E402
from src.device.simulator import CemsSimulator                       # noqa: E402
from src.device import simulator as SIM                              # noqa: E402

DEFAULT_SEED: int = 20261001
DEFAULT_START: str = "2026-03-01T00:00:00+00:00"
DEFAULT_CYCLES: int = 200
STEP_SECONDS: float = 2.0      # 与设备层刷新周期一致


def main(argv: list[str]) -> int:
    """把 (种子, 假时钟) 下的读数逐点打印成 JSON 行。"""
    seed = int(argv[1]) if len(argv) > 1 else DEFAULT_SEED
    start = datetime.fromisoformat(argv[2]) if len(argv) > 2 else datetime.fromisoformat(
        DEFAULT_START
    )
    cycles = int(argv[3]) if len(argv) > 3 else DEFAULT_CYCLES

    simulator = CemsSimulator(seed=seed)
    payload = {
        "seed": seed,
        "epoch": SIM.SIM_EPOCH.isoformat(),
        "start": start.astimezone(timezone.utc).isoformat(),
        "step_seconds": STEP_SECONDS,
        "cycles": cycles,
        "readings": {
            point.name: [
                simulator.sample(point.name, start + timedelta(seconds=STEP_SECONDS * i))
                for i in range(cycles)
            ]
            for point in POINTS
        },
        "registers": [
            [
                encode(simulator.sample(point.name, start + timedelta(seconds=STEP_SECONDS * i)),
                       point.scale)
                for point in POINTS
            ]
            for i in range(cycles)
        ],
    }
    # 用 json.dumps 里最紧凑的写法 + 固定键序，方便父进程直接逐字节比对
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
