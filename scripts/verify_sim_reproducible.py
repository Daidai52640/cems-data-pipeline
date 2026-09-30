# -*- coding: utf-8 -*-
"""验收自检 3/3：可复现性——同种子同时间参数必须逐值一致。

用法：
    python scripts/verify_sim_reproducible.py

为什么不是"同进程跑两遍"：
    同进程里比对只能证明函数是纯的，证明不了"跨进程/跨机器可复现"。
    真实场景（故障演练重放）是**换个进程**甚至换台机器跑，
    所以这里一律起独立子进程，并故意让两个子进程拿到不同的 PYTHONHASHSEED，
    以验证种子派生没有依赖进程内的随机盐。

输出四段证据：
    A. 同一进程内重复采样同种子的快照指纹
    B. 两个独立子进程（不同 PYTHONHASHSEED）逐值比对
    C. 换种子必须产生不同序列（否则"种子生效"无从谈起）
    D. 不设 SIM_SEED 时回落到确定性默认种子
    E. 结束码：全部通过 0，任一不通过 1
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS                                 # noqa: E402
from src.device.simulator import CemsSimulator                       # noqa: E402

TRACE_SCRIPT: Path = PROJECT_ROOT / "scripts" / "sim_trace_dump.py"
SEED: int = 20261001
OTHER_SEED: int = 20261002
START: str = "2026-03-01T00:00:00+00:00"
CYCLES: int = 200
STEP_SECONDS: float = 2.0

FAILURES: list[str] = []


def _check(condition: bool, description: str) -> None:
    """记录一条验收判定；失败时收进 FAILURES，最后统一汇总。"""
    print(f"  [{'PASS' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def fingerprint(values: list[float]) -> str:
    """给一串读数算个短指纹，方便人眼比对。"""
    text = ",".join(repr(value) for value in values)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def run_child(seed: int, hash_seed: str, clear_seed_env: bool = False) -> dict[str, object]:
    """起一个独立子进程产出读数，返回解析后的 JSON。

    PYTHONHASHSEED 故意每次不同：如果实现里用了 hash() 派生种子，
    这两个进程就会给出不同结果（这正是要防的坑）。
    clear_seed_env=True 时额外清掉 SIM_SEED，用来验证"不设 SIM_SEED 也有确定性默认值"。
    """
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = hash_seed
    env["PYTHONIOENCODING"] = "utf-8"
    if clear_seed_env:
        env.pop("SIM_SEED", None)
    completed = subprocess.run(
        [sys.executable, str(TRACE_SCRIPT), str(seed), START, str(CYCLES)],
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return json.loads(completed.stdout)


def section_a_in_process() -> None:
    """A. 同一进程内：同种子重复算，指纹必须一致。"""
    print("\n=== A. 同进程内重复采样（种子=%d，%d 个周期 × 每 %.0fs）===" % (SEED, CYCLES, STEP_SECONDS))
    simulator = CemsSimulator(seed=SEED)
    moments = [datetime.fromisoformat(START) + timedelta(seconds=STEP_SECONDS * i)
               for i in range(CYCLES)]
    first = {
        point.name: [simulator.sample(point.name, moment) for moment in moments]
        for point in POINTS
    }
    second = {
        point.name: [simulator.sample(point.name, moment) for moment in moments]
        for point in POINTS
    }
    print(f"{'测点':<9}{'指纹':>18}{'一致':>8}")
    all_same = True
    for point in POINTS:
        same = first[point.name] == second[point.name]
        all_same = all_same and same
        print(f"{point.name:<9}{fingerprint(first[point.name]):>18}{'是' if same else '否':>8}")
    _check(all_same, "同一进程内重复采样，全部测点逐值一致")


def section_b_across_processes() -> dict[str, object]:
    """B. 两个独立子进程（不同 PYTHONHASHSEED）逐值比对。"""
    print("\n=== B. 两个独立进程逐值比对（同一 SIM_SEED，不同 PYTHONHASHSEED）===")
    left = run_child(SEED, "0")
    right = run_child(SEED, "12345")
    print(f"  进程1: PYTHONHASHSEED=0     读数指纹={fingerprint_flat(left)}")
    print(f"  进程2: PYTHONHASHSEED=12345 读数指纹={fingerprint_flat(right)}")

    left_readings = left["readings"]
    right_readings = right["readings"]
    assert isinstance(left_readings, dict) and isinstance(right_readings, dict)
    mismatched: list[str] = []
    for point in POINTS:
        if left_readings[point.name] != right_readings[point.name]:
            mismatched.append(point.name)
    _check(not mismatched, f"两个独立进程的 {len(POINTS)} 个测点读数逐值一致")

    register_same = left["registers"] == right["registers"]
    _check(register_same, "两个独立进程的寄存器编码结果逐值一致")

    # 逐点打印前 5 个值，让"逐值一致"这件事在输出里看得见
    print(f"  {'测点':<9}{'前 5 个物理量（进程1）':<46}{'与进程2一致':>10}")
    for point in POINTS:
        head = [f"{value:.4f}" for value in left_readings[point.name][:5]]
        same = left_readings[point.name] == right_readings[point.name]
        print(f"  {point.name:<9}{' '.join(head):<46}{'是' if same else '否':>10}")
    return left


def fingerprint_flat(payload: dict[str, object]) -> str:
    """对整个 payload 的读数部分算指纹（跨进程比对用）。"""
    readings = payload["readings"]
    return hashlib.sha256(
        json.dumps(readings, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]


def section_c_seed_sensitivity() -> None:
    """C. 换种子必须换序列，否则"种子可控"是假的。"""
    print("\n=== C. 换种子必须产生不同序列（种子=%d vs %d）===" % (SEED, OTHER_SEED))
    left = run_child(SEED, "0")
    right = run_child(OTHER_SEED, "0")
    left_readings = left["readings"]
    right_readings = right["readings"]
    assert isinstance(left_readings, dict) and isinstance(right_readings, dict)
    identical: list[str] = [
        point.name
        for point in POINTS
        if left_readings[point.name] == right_readings[point.name]
    ]
    print(f"  两个种子下序列完全相同的测点: {identical if identical else '无（都不同）'}")
    _check(not identical, "换种子后每个测点的序列都变了")


def section_d_default_seed() -> None:
    """D. 不设 SIM_SEED 时的默认种子也必须是确定性的（不是随机默认值）。"""
    print("\n=== D. 不设 SIM_SEED 的默认行为 ===")
    explicit = run_child(SEED, "0")
    defaulted = run_child(SEED, "999", clear_seed_env=True)
    print(f"  显式种子 {SEED} 的读数指纹 = {fingerprint_flat(explicit)}")
    print(f"  不设 SIM_SEED 的读数指纹   = {fingerprint_flat(defaulted)}")
    _check(
        explicit["readings"] == defaulted["readings"],
        "不设 SIM_SEED 时回落到固定默认种子，读数与显式指定默认种子一致",
    )


def main() -> int:
    """依次跑完四段自检，返回进程退出码。"""
    print("CEMS 仿真信号可复现性自检（跨进程比对，不需要 Docker）")
    print(f"子进程脚本: {TRACE_SCRIPT.relative_to(PROJECT_ROOT)}")
    section_a_in_process()
    section_b_across_processes()
    section_c_seed_sensitivity()
    section_d_default_seed()

    print("\n================ 结论 ================")
    if FAILURES:
        print(f"不通过 {len(FAILURES)} 项:")
        for item in FAILURES:
            print(f"  - {item}")
        return 1
    print("全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
