# -*- coding: utf-8 -*-
"""验收自检：无法折算时的**传输哨兵值**（HJ 212-2025 §8.1.1 d)，不需要 Docker）。

用法：
    python scripts/verify_reference_sentinel.py

输出四段证据：
    A. 契约层：O2 >= 21% 时 to_reference_o2() 仍返回 nan（数学语义）
       → to_transmit_value(nan) == 9999.99（协议语义）
    B. 回归：O2 < 21% 的折算值与**改动前的算式逐位一致**（float.hex() 比对）
    C. 接入层端到端（离线，不连 MQTT / TDengine）：
       reference_values() 仍是 nan，而 transmit_reference_values() /
       record_reference() / latest_reference_values() 三个出站出口都是哨兵值
    D. 结束码：全部通过 0，任一不通过 1（可直接进 CI）

⚠️ 口径纪律（本脚本严格照做）：
    - 现行折算值一律调 points.to_reference_o2()，本脚本不重写公式；
      B 段的"改动前算式"只用于证明逐位一致，不是第二份实现
    - 哨兵值是**协议编码值**，不参与判定：9999.99 大于任何限值，
      本脚本同时反证"拿它判超标会误报"，把这条纪律钉死
    - 不新建 TDengine 列、不写库：折算值是导出量（见 points.py 顶部说明）
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import (   # noqa: E402
    LIMITS,
    O2_REFERENCE,
    ZS_TARGETS,
    ZS_UNAVAILABLE_SENTINEL,
    over_limit,
    to_reference_o2,
    to_transmit_value,
)
from src.platform import subscriber_to_td as SUB   # noqa: E402

# ---- 检查参数 ----
UNAVAILABLE_O2: tuple[float, ...] = (21.0, 21.5, 22.0, 25.0)   # 分母 <= 0："算不出来"
AVAILABLE_O2: tuple[float, ...] = (20.9, 9.0, 6.0, 3.0)        # 分母 > 0：必须逐位不变
SAMPLE: dict[str, float] = {"Dust": 4.0, "SO2": 30.0, "NOx": 40.0}
POLLUTANTS: tuple[str, ...] = ("Dust", "SO2", "NOx")

FAILURES: list[str] = []


def _check(condition: bool, description: str) -> None:
    """记录一条验收判定；失败时收进 FAILURES，最后统一汇总。"""
    print(f"  [{'PASS' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def _legacy_reference(value: float, o2: float, o2_ref: float = O2_REFERENCE) -> float:
    """**改动前**的算式原样复刻（仅 B 段逐位对照用，不是第二份实现）。"""
    denominator = 21.0 - o2
    if denominator <= 0.0:
        return float("nan")
    return value * (21.0 - o2_ref) / denominator


def _payload(o2: float) -> dict[str, float]:
    """构造一条只含折算相关测点的报文（键 = MQTT 字段名）。"""
    values = dict(SAMPLE)
    values["O2"] = o2
    return values


def section_a_contract() -> None:
    """A. 契约层：数学语义返回 nan，协议编码给出哨兵值。"""
    print("\nA. 契约层：O2 >= 21% 数学上仍是 nan，传输编码成哨兵值")
    print(f"     {'O2(%)':>6}  {'折算值(数学语义)':>16}  {'传输值(协议语义)':>16}")
    for o2 in UNAVAILABLE_O2:
        raw = to_reference_o2(SAMPLE["SO2"], o2)
        sent = to_transmit_value(raw)
        print(f"     {o2:>6.1f}  {raw!r:>16}  {sent:>16.2f}")
        _check(math.isnan(raw), f"O2={o2}% 折算值本身仍是 nan（数学语义未被改动）")
        _check(
            sent == ZS_UNAVAILABLE_SENTINEL,
            f"O2={o2}% 传输值 == {ZS_UNAVAILABLE_SENTINEL}（不是 nan、不是 0、不是实测值）",
        )

    _check(
        ZS_UNAVAILABLE_SENTINEL == 9999.99,
        "哨兵值常量 = +9999.99（HJ 212-2025 §8.1.1 d) 的缺省数据类型最大值）",
    )
    _check(
        to_transmit_value(float("inf")) == ZS_UNAVAILABLE_SENTINEL
        and to_transmit_value(float("-inf")) == ZS_UNAVAILABLE_SENTINEL,
        "±inf 同属'无法计算'，一并编码成哨兵值",
    )


def section_b_regression() -> None:
    """B. 回归：O2 < 21% 的折算值与改动前逐位一致。"""
    print("\nB. 回归：O2 < 21% 的折算值与改动前**逐位一致**（float.hex 比对）")
    header = f"     {'测点':<5} {'O2(%)':>6} {'标干值':>8} {'改动前算式':>24} {'现行实现':>24}  一致"
    print(header)
    for o2 in AVAILABLE_O2:
        for name in POLLUTANTS:
            before = _legacy_reference(SAMPLE[name], o2)
            now = to_reference_o2(SAMPLE[name], o2)
            bitwise_same = before == now and before.hex() == now.hex()
            print(
                f"     {name:<5} {o2:>6.1f} {SAMPLE[name]:>8.1f} {before:>24.10f} "
                f"{now:>24.10f}  {'是' if bitwise_same else '否'}"
            )
            _check(bitwise_same, f"{name} @ O2={o2}% 折算值逐位一致（{now.hex()}）")
            _check(
                to_transmit_value(now) == now,
                f"{name} @ O2={o2}% 有限折算值经传输编码后原样不变",
            )


def section_c_access_layer() -> None:
    """C. 接入层端到端（离线）：出站出口是哨兵值，数学层仍是 nan。"""
    print("\nC. 接入层端到端（离线，不连 MQTT / TDengine）")

    # C1 算不出来的一条报文：两层必须各守各的语义
    values_bad = _payload(22.0)
    math_layer = SUB.reference_values(values_bad)
    tx_layer = SUB.transmit_reference_values(values_bad)
    print(f"     O2=22.0%  reference_values()          -> "
          + " ".join(f"{column}={math_layer[column]!r}" for column in ZS_TARGETS))
    print(f"     O2=22.0%  transmit_reference_values() -> "
          + " ".join(f"{column}={tx_layer[column]:.2f}" for column in ZS_TARGETS))
    for column in ZS_TARGETS:
        _check(
            math.isnan(math_layer[column]),
            f"reference_values()[{column}] 仍是 nan（数学层没有被协议编码污染）",
        )
        _check(
            tx_layer[column] == ZS_UNAVAILABLE_SENTINEL,
            f"transmit_reference_values()[{column}] = 哨兵值（出站值不是 nan）",
        )

    # C2 快照与日志读的是同一份出站值
    snapshot = SUB.record_reference("2026-10-01 00:00:00", values_bad)
    latest = SUB.latest_reference_values()
    _check(
        all(snapshot[column] == ZS_UNAVAILABLE_SENTINEL for column in ZS_TARGETS),
        "record_reference() 返回哨兵值（抽样日志打的就是它）",
    )
    _check(
        all(latest[column] == ZS_UNAVAILABLE_SENTINEL for column in ZS_TARGETS),
        "latest_reference_values() 快照也是哨兵值",
    )
    _check(
        SUB.latest_reference_timestamp() == "2026-10-01 00:00:00",
        "快照时间戳同步更新",
    )

    # C3 算得出来的一条报文不受影响（手算对照，O2=9% → 放大 15/12 = 1.25 倍）
    values_ok = _payload(9.0)
    tx_ok = SUB.transmit_reference_values(values_ok)
    print(f"     O2=9.0%   transmit_reference_values() -> "
          + " ".join(f"{column}={tx_ok[column]:.4f}" for column in ZS_TARGETS))
    for column in ZS_TARGETS:
        name = SUB.COLUMN_TO_NAME[column]
        manual = SAMPLE[name] * (21.0 - O2_REFERENCE) / (21.0 - 9.0)
        _check(
            tx_ok[column] == manual,
            f"{column} @ O2=9% 传输值 = 手算 {manual:.4f}（有限值未被编码改变）",
        )
    _check(tx_ok["so2"] == 37.5, "对照：SO2 标干 30 @ O2=9% → 30 × 15/12 = 37.5")

    # C4 口径纪律反证：哨兵值一旦混进判定必然误报
    _check(
        all(ZS_UNAVAILABLE_SENTINEL > LIMITS[name] for name in POLLUTANTS),
        "哨兵值大于全部限值 → 混进判定必然误报，故判定只认 reference_values() 的 nan",
    )
    _check(
        over_limit("SO2", ZS_UNAVAILABLE_SENTINEL),
        "反证：把哨兵值交给 over_limit() 会判成超标（这就是不许混层的证据）",
    )


def main() -> int:
    """依次跑完三段自检，返回进程退出码。"""
    print("=" * 78)
    print("验收自检：无法计算折算浓度时的传输哨兵值 —— HJ 212-2025 §8.1.1 d)")
    print("=" * 78)
    section_a_contract()
    section_b_regression()
    section_c_access_layer()

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
