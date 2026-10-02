# -*- coding: utf-8 -*-
"""设备维度 scope 的**唯一真源**：给测量/分析脚本提供「按哪台设备统计」的参数与 SQL 谓词。

为什么需要这个模块
-----------------------------------------------------------------------------
`cems_data` 是**多设备共用的超级表**，TAG 是 `(plant, device)`。多设备上线后，
同一张表里同时存在 device1 / device2（压测时还会有 `plant=loadtest` 的标签）
的多条曲线。测量脚本如果裸读 `SELECT ... FROM cems.cems_data`：

- `measure_completeness` 的「实际条数」= 各设备条数之和 → 完整率被撑到 **>100%**
  （实测同一 5 分钟窗口：混读 290 行，device1 单独 59 行）；
- `measure_latency` 的 `ORDER BY ts DESC LIMIT n` 取回的是**所有设备混排**的最近 n 行，
  「库内可查」样本里混进别的设备，逐条延迟被算错；
- `measure_resend_success` 的对账分母（该补条数）与 `COUNT(*)` 分母都会把
  另一台设备的行算进来，补传成功率不再可分设备；
- `analyze_gateway_resend_lag` 的「走补传路径占比」分母同样被放大；
- `verify_report_coverage` 的前置核查 SQL 会把两台设备的行一起聚合，
  断档分钟会被另一台设备的数据填上，核查结论反了。

所以**默认单设备（device1）时必须与改造前逐位一致**，多设备时用 `--device` 分开统计。
本模块只做两件事：读 scope、生成 SQL 谓词；不改任何测量口径。

用法
-----------------------------------------------------------------------------
    from _device_scope import TD_DEVICE, add_device_argument, device_predicate, resolve_device

    parser.add_argument(...)
    add_device_argument(parser)                      # 与 verify_pipeline_live.py 同款 --device
    scope = resolve_device(args.device)              # 空值回落到 env TD_DEVICE（默认 device1）
    sql = (f"SELECT COUNT(*) FROM cems.cems_data WHERE ts >= ... "
           f"AND {device_predicate(scope)}")

import 方式：脚本都在 `scripts/` 下、以「脚本所在目录」在 `sys.path[0]` 运行，
所以直接 `from _device_scope import ...` 即可（与既有的 `scripts/_td_ops.py` 同一约定）。
"""

from __future__ import annotations

import argparse
import os
from typing import Final, Optional

#: 设备 TAG 的默认值：与接入层（`src/platform/subscriber_to_td.py` 的 `TD_DEVICE`）
#: 以及展示层（`src/web/cache.py` 的 `DEVICE_SCOPE`）保持一致 —— 单设备部署下就是 device1。
TD_DEVICE: Final[str] = os.getenv("TD_DEVICE", "device1")


def resolve_device(device: Optional[str] = None) -> str:
    """把 `--device` / 调用方传入的 scope 归一成一个非空设备名。

    空值（None / 空串 / 纯空白）回落到模块级 `TD_DEVICE`（= env `TD_DEVICE` 或 device1），
    保证「不传 --device」时与改造前的单设备行为完全一致（零回归）。
    """
    text = (device or "").strip()
    return text if text else TD_DEVICE


def add_device_argument(
    parser: argparse.ArgumentParser,
    help_text: str = "",
) -> None:
    """给解析器加统一的 `--device`（默认取 env `TD_DEVICE`，即 device1）。

    默认值直接写 `os.getenv("TD_DEVICE", "device1")` 而不是模块常量，
    这样 `--help` 里显示的默认值与实际生效值一致，也便于用环境变量整体切换。
    """
    parser.add_argument(
        "--device",
        default=os.getenv("TD_DEVICE", "device1"),
        help=help_text
        or (
            "按设备 TAG 过滤（默认 device1，与接入层一致）。"
            "多设备部署时**必须**指定，否则两台设备的行会被混在一起统计"
        ),
    )


def device_predicate(device: Optional[str] = None) -> str:
    """返回**裸条件** `device = 'device1'`，由调用方决定怎么拼进 WHERE。

    返回裸条件（不带前导 `AND` / `WHERE`）是有意的：各脚本的 WHERE 子句形态不同
    （有的只有设备、有的还有 `ts > ? AND ts <= ?`），带前导 `AND` 会逼出
    `WHERE 1 = 1 AND ...` 这种占位写法，反而掩盖真实条件。

    设备名来自命令行或环境变量，不是外部不可信输入；这里仍做一次白名单校验
    （只允许字母/数字/下划线/中划线），避免把引号拼进 SQL 造成注入
    （见 AGENTS.md §5「SQL 注入」）。
    """
    scope = resolve_device(device)
    if not all(char.isalnum() or char in "_-" for char in scope):
        raise SystemExit(f"--device 取值非法（只允许字母/数字/下划线/中划线）: {scope!r}")
    return f"device = '{scope}'"


def device_clause(device: Optional[str] = None) -> str:
    """返回完整的 `WHERE` 子句：只有设备维度时用它，省得调用方写 `WHERE {pred}`。"""
    return f"WHERE {device_predicate(device)}"
