# -*- coding: utf-8 -*-
"""`scripts/serial_bridge.py` 的单元测试 —— **全项目唯一写入真机值的那段代码**。

============================ 为什么单独给它补测试 ============================
独立评审（2026-10-11）指出：桥**逻辑不轻**（轮询线程 + 锁 + 失败计数 + 陈旧三态 + 导入期自检），
却是**全项目质量最低、保护最少**的一段 —— 无测试、不入镜像、不受版本守卫、写死 `COM3`。
而它是**链路上的强制依赖**：它错了，真机数据就错了，而且**下游看不见**。

⭐ 好在它的关键函数**都是纯的**（不需要 COM 口、不需要 broker），可以离线测：

| 被测 | 为什么它值得测 |
|---|---|
| `_split_words` | 把寄存器值还原成字节 —— **int16 有符号解析的第一环**，错了温度就变成 6543.9 |
| `_check_stale_sentinels` | 判据是"越界值必须**同时**超出两套契约的量程" —— 有人放宽量程它就失效 |
| `Bridge.snapshot` | ⭐ **陈旧三态**（新鲜 / 陈旧 / 从未读到）—— 它决定下游能不能误用旧值 |

⚠️ **不测**：真实串口 I/O、Modbus TCP 服务端、`poll_forever`（需要硬件或 socket），
这些留给 `真机接入.md` 的现场实验承证。
"""

from __future__ import annotations

import pathlib
import sys

import pytest

# scripts/ 不在包路径里（扁平约定，见 scripts/README.md），显式挂上去
_SCRIPTS = pathlib.Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import serial_bridge as sb  # noqa: E402


# ---------------------------------------------------------------- 字节还原

def test_split_words_is_big_endian() -> None:
    """⭐ 手册原文"16bits 数据高字节在前" ⇒ 高字节先出。

    实测样本：`02 31` → 561 → 56.1 %RH；`01 11` → 273 → 27.3 degC。
    """
    assert sb._split_words(0x0231) == bytes([0x02, 0x31])
    assert sb._split_words(0x0111) == bytes([0x01, 0x11])


def test_split_words_preserves_int16_negative_twos_complement() -> None:
    """⭐ 负温走的是**补码位型**：`FF 9F` → -97 → -9.7 degC（不是 6543.9）。

    这一条是"温度是 int16"这个契约字段的**下游落点** —— 桥只负责原样搬运。
    """
    assert sb._split_words(0xFF9F) == bytes([0xFF, 0x9F])
    assert int.from_bytes(sb._split_words(0xFF9F), "big", signed=True) == -97


# ---------------------------------------------------------------- 越界哨兵

def test_stale_sentinels_are_out_of_range_for_both_contracts() -> None:
    """⭐ 陈旧时写的越界值，必须**同时**超出 CEMS 与变送器两套契约的量程。

    只超出一套是不够的：路径 B（`POINT_SET=transmitter`）只读 2 个寄存器、
    **看不到桥的状态位**，它唯一能察觉"值无效"的途径就是**值本身越界**。
    """
    from src.common import points_transmitter
    from src.common.points import POINTS as CEMS_POINTS

    cases = (
        ("Humidity", sb.STALE_HUMIDITY_RAW, "AirHumidity"),
        ("Temp", sb.STALE_TEMPERATURE_RAW, "AirTemp"),
    )
    for cems_name, raw, tx_name in cases:
        value = raw / sb.SCALE
        cems = next(p for p in CEMS_POINTS if p.name == cems_name)
        tx = next(p for p in points_transmitter.POINTS if p.name == tx_name)
        assert not (cems.low <= value <= cems.high), f"{cems_name} 没超出 CEMS 量程"
        assert not (tx.low <= value <= tx.high), f"{cems_name} 没超出变送器量程"


def test_zero_would_be_in_range_hence_is_never_used_as_sentinel() -> None:
    """⭐⭐ **为什么哨兵不能用 0** —— 真机教我们的坑。

    实测该变送器读不存在的寄存器会**沉默返回全 0**；而 `0` 落在两套契约的量程内，
    会被当成真实读数一路入库。所以哨兵必须是**越界**值。
    """
    from src.common.points import POINTS as CEMS_POINTS

    cems = next(p for p in CEMS_POINTS if p.name == "Temp")
    assert cems.low <= 0.0 <= cems.high, "前提变了：0 现在不在量程内，本测试的论证需重写"
    assert sb.STALE_TEMPERATURE_RAW / sb.SCALE > cems.high


def test_import_time_self_check_rejects_a_too_small_sentinel() -> None:
    """⭐ 自检真的会拦：把哨兵调到量程内 ⇒ 必须抛 ValueError。

    防的是"有人把 Temp 量程放宽到 310，而哨兵还是 301.0"——
    那时"无效标记"会**悄悄变成合法读数**，且极难发现。
    """
    original = sb.STALE_TEMPERATURE_RAW
    try:
        sb.STALE_TEMPERATURE_RAW = 26 * sb.SCALE   # 26 degC 落在 CEMS [0,300] 内
        with pytest.raises(ValueError, match="越界哨兵失效"):
            sb._check_stale_sentinels()
    finally:
        sb.STALE_TEMPERATURE_RAW = original


# ---------------------------------------------------------------- 陈旧三态

def _bridge(**kwargs: int) -> sb.Bridge:
    b = sb.Bridge(port="COM3", baud=4800, interval=1.0)
    for k, v in kwargs.items():
        setattr(b, k, v)
    return b


def test_snapshot_fresh_passes_the_real_values_through() -> None:
    b = _bridge(_last_humidity=648, _last_temperature=251, _have_value=True, _failures=0)
    snap = b.snapshot()
    assert snap[:2] == [648, 251]
    assert snap[2] == sb.STATUS_FRESH


def test_snapshot_stale_returns_sentinels_not_the_last_good_value() -> None:
    """⭐⭐ **本桥最关键的一条行为**：陈旧时不返回旧值，改返回越界值。

    只把"陈旧"写进状态位是不够的 —— 那只是**把洞留给下一个消费者**。
    把"无效"做进**值本身**，任何读它的人都无法误用。
    """
    b = _bridge(_last_humidity=648, _last_temperature=251, _have_value=True, _failures=1)
    snap = b.snapshot()
    assert snap[0] == sb.STALE_HUMIDITY_RAW, "陈旧时仍在返回旧湿度值 —— 静默假数据"
    assert snap[1] == sb.STALE_TEMPERATURE_RAW, "陈旧时仍在返回旧温度值 —— 静默假数据"
    assert snap[2] == sb.STATUS_STALE


def test_snapshot_before_first_successful_read_is_stale() -> None:
    """从未读到过 ⇒ 同样是**陈旧**（不能因为"还没有旧值"就当作新鲜）。"""
    b = _bridge(_have_value=False, _failures=0)
    snap = b.snapshot()
    assert snap[0] == sb.STALE_HUMIDITY_RAW
    assert snap[1] == sb.STALE_TEMPERATURE_RAW
    assert snap[2] == sb.STATUS_STALE


def test_snapshot_reports_cumulative_failures_capped() -> None:
    """累计失败数只增不减（清零点在 `_last_*`，不在计数）—— 它是"出过几次事"的历史。"""
    b = _bridge(_have_value=True, _failures=1, _total_failures=0x1_0000)
    assert b.snapshot()[3] == 0xFFFF, "计数未按 uint16 上限截断"
