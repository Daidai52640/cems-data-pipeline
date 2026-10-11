# -*- coding: utf-8 -*-
"""RS485（Modbus RTU）→ Modbus TCP 桥 —— 模拟现场那台"串口服务器"。

============================ 为什么需要它 ============================
项目 1 的**网关只讲 Modbus TCP**（`src/gateway/gateway.py` 用 `ModbusTcpClient`），
而真机是一台挂在 **COM 口**上的 RS485 设备。**Docker 容器访问不到 Windows 的 COM 口**，
所以真机不能直接被容器读。

真实 CEMS/数采现场的做法就是**串口服务器**：把 RS485 转成 Modbus TCP，
网关按 TCP 读。本脚本在 **Windows 宿主**上扮演这个角色：

    [温湿度变送器] --RS485/Modbus RTU--> COM3
                                            |
                                    serial_bridge.py（本脚本，跑在宿主）
                                            |
                                    Modbus TCP  :5021
                                            |
                              [项目 1 的网关容器]（代码零改动）

⭐ 这样**不需要给网关加 RTU 支持** —— 现场本来也是这么接的。
   而"我亲手接过 485 线、跑过 RTU 帧"这件事，见
   `docs/runbooks/真机接入.md` 的 S2/S4（有原始字节）。

============================ 暴露的寄存器 ============================
从站地址 **1**，保持寄存器（功能码 03）：

  | 地址     | 内容           | 类型     | 标度 | 说明 |
  |----------|----------------|----------|------|------|
  | `0x0000` | 湿度           | uint16   | ÷10  | %RH |
  | `0x0001` | 温度           | **int16**| ÷10  | degC（补码） |
  | `0x0002` | ⭐ **桥状态**  | uint16   | —    | `0`=新鲜，`1`=陈旧（读失败） |
  | `0x0003` | 连续读失败次数 | uint16   | —    | 每次成功清零 |

⭐⭐ **为什么要有 0x0002 / 0x0003 这两个寄存器**：
实测发现这台变送器**读错地址时返回全 0 而不报异常**（见 `docs/runbooks/真机接入.md` S4 发现 1）。
"沉默就返回 0"是**静默的假数据**。本桥**故意不重复这个错误**：
读失败时**保留上一次的好值**，同时把 `0x0002` 置 `1` —— 让下游**能看见"这份数是陈旧的"**，
而不是把 0 当成真实湿度。

============================ 用法（在 Windows 宿主上跑）============================
    python scripts/serial_bridge.py --port COM3

    # 换个 TCP 端口 / 改轮询周期
    python scripts/serial_bridge.py --port COM3 --tcp-port 5021 --interval 1.0

⚠️ 本脚本**不能放进容器**：容器拿不到宿主的 COM 口。
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from typing import Final, Optional

# 让 `from src.common.points import ...` 能工作（脚本以自身所在目录为 sys.path[0]）
PROJECT_ROOT: Final = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pymodbus.datastore import (  # noqa: E402
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSlaveContext,
)
from pymodbus.server import StartTcpServer  # noqa: E402

from src.common.points import decode_register  # noqa: E402

LOGGER: Final[logging.Logger] = logging.getLogger("scripts.serial_bridge")

# ---- 真机参数（来自手册，已实测核对；详见 docs/runbooks/真机接入.md）----
SLAVE_ID: Final[int] = 0x01
REG_HUMIDITY: Final[int] = 0x0000
REG_TEMPERATURE: Final[int] = 0x0001
REG_COUNT: Final[int] = 2

#: 手册明写"16bits 数据高字节在前" → 标准 Modbus 字节序
ENDIAN: Final[str] = "AB"
DTYPE_HUMIDITY: Final[str] = "uint16"
DTYPE_TEMPERATURE: Final[str] = "int16"  # ⭐ 温度是**有符号**的：-40~125 degC

#: 两个测点各自的寄存器换算系数（值 = 原始整数 / SCALE）
SCALE: Final[int] = 10

# ---- 桥自己对外暴露的寄存器布局 ----
BRIDGE_REG_HUMIDITY: Final[int] = 0
BRIDGE_REG_TEMPERATURE: Final[int] = 1
BRIDGE_REG_STATUS: Final[int] = 2
BRIDGE_REG_FAILURES: Final[int] = 3
BRIDGE_REG_TOTAL: Final[int] = 4

STATUS_FRESH: Final[int] = 0
STATUS_STALE: Final[int] = 1


def _split_words(raw: int) -> bytes:
    """把 pymodbus 给出的 16 位寄存器值还原成两个字节（高字节在前），供契约函数解码。"""
    return bytes([(raw >> 8) & 0xFF, raw & 0xFF])


class Bridge:
    """轮询 COM 口，把读数写进对外的 Modbus TCP 数据块；读失败则保留旧值并置状态位。"""

    def __init__(self, port: str, baud: int, interval: float) -> None:
        self.port = port
        self.baud = baud
        self.interval = interval
        self._lock = threading.Lock()
        self._last_humidity = 0
        self._last_temperature = 0
        self._failures = 0
        self._total_failures = 0
        self._have_value = False

    # -------------------------------------------------- 串口侧
    def _make_client(self):
        from pymodbus.client import ModbusSerialClient

        return ModbusSerialClient(
            port=self.port,
            baudrate=self.baud,
            bytesize=8,
            parity="N",
            stopbits=1,
            timeout=1.0,
        )

    def poll_forever(self) -> None:
        """轮询循环。串口断了就重连，**绝不因为读失败而让桥退出**。"""
        while True:
            client = self._make_client()
            try:
                if not client.connect():
                    LOGGER.error("串口打开失败: %s（%.1fs 后重试）", self.port, self.interval)
                    with self._lock:
                        self._failures += 1
                        self._total_failures += 1
                    time.sleep(self.interval)
                    continue
                LOGGER.info("串口已连接: %s @ %d 8N1, 从站 %d", self.port, self.baud, SLAVE_ID)
                while True:
                    self._poll_once(client)
                    time.sleep(self.interval)
            except Exception as exc:  # noqa: BLE001 - 桥必须活着，任何异常都只记不抛
                LOGGER.error("轮询异常（将重连）: %s", exc)
                with self._lock:
                    self._failures += 1
                    self._total_failures += 1
                time.sleep(self.interval)
            finally:
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass

    def _poll_once(self, client) -> None:
        try:
            response = client.read_holding_registers(
                address=REG_HUMIDITY, count=REG_COUNT, slave=SLAVE_ID
            )
            if response.isError():
                raise IOError(f"Modbus 异常响应: {response}")
            humidity_raw = int(
                decode_register(_split_words(response.registers[0]), DTYPE_HUMIDITY, ENDIAN)
            )
            temperature_raw = int(
                decode_register(_split_words(response.registers[1]), DTYPE_TEMPERATURE, ENDIAN)
            )
        except Exception as exc:  # noqa: BLE001 - 读失败是常态（拔线/干扰），不能崩
            with self._lock:
                self._failures += 1
                self._total_failures += 1
            LOGGER.warning("读失败（第 %d 次连续失败，保留上次好值）: %s", self._failures, exc)
            return

        with self._lock:
            self._last_humidity = humidity_raw
            self._last_temperature = temperature_raw & 0xFFFF  # int16 的补码位型
            self._failures = 0
            self._have_value = True

        LOGGER.info(
            "湿度 %.1f %%RH ｜ 温度 %.1f degC",
            humidity_raw / SCALE,
            temperature_raw / SCALE,
        )

    # -------------------------------------------------- TCP 侧
    def snapshot(self) -> list[int]:
        with self._lock:
            stale = STATUS_STALE if (self._failures > 0 or not self._have_value) else STATUS_FRESH
            return [
                self._last_humidity,
                self._last_temperature,
                stale,
                min(self._total_failures, 0xFFFF),
            ]


def build_context() -> ModbusServerContext:
    """建服务端上下文。

    ⚠️ **必须 `single=False`**（照 `src/device/modbus_server.py` 的既有结论）：
    pymodbus 在 `single=True` 下会把**任何**请求的 unit id 改写成内部地址 0 ——
    于是"请求一个不存在的从站"会**成功返回**这份唯一数据，**既不报错也无异常码**。
    那种静默错在本项目已经踩过一次，这里不再踩。
    """
    block = ModbusSequentialDataBlock(0, [0] * BRIDGE_REG_TOTAL)
    context = ModbusSlaveContext(di=block, co=block, hr=block, ir=block)
    return ModbusServerContext(slaves={SLAVE_ID: context}, single=False)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="RS485(Modbus RTU) → Modbus TCP 桥：把 COM 口上的变送器暴露成 TCP 从站"
    )
    parser.add_argument("--port", default="COM3", help="串口名，如 COM3（默认 COM3）")
    parser.add_argument("--baud", type=int, default=4800, help="波特率（本机默认 4800）")
    parser.add_argument("--tcp-port", type=int, default=5021, help="对外 TCP 端口（默认 5021，避开设备层 5020）")
    parser.add_argument("--tcp-host", default="127.0.0.1", help="对外监听地址（默认仅本机）")
    parser.add_argument("--interval", type=float, default=1.0, help="串口轮询周期秒（默认 1.0）")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )

    bridge = Bridge(port=args.port, baud=args.baud, interval=args.interval)
    context = build_context()

    threading.Thread(target=bridge.poll_forever, name="serial-poller", daemon=True).start()

    # 定时把桥的快照刷进数据块
    def refresh_loop() -> None:
        while True:
            try:
                context[SLAVE_ID].setValues(3, 0, bridge.snapshot())
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("刷新数据块失败: %s", exc)
            time.sleep(0.5)

    threading.Thread(target=refresh_loop, name="datastore-refresh", daemon=True).start()

    LOGGER.info(
        "桥已启动：%s @ %d  →  Modbus TCP %s:%d（从站 %d，寄存器 %d 个）",
        args.port, args.baud, args.tcp_host, args.tcp_port, SLAVE_ID, BRIDGE_REG_TOTAL,
    )
    LOGGER.info("对外寄存器: 0=湿度x10  1=温度x10(int16)  2=状态(0新鲜/1陈旧)  3=累计读失败次数")
    StartTcpServer(context=context, address=(args.tcp_host, args.tcp_port))
    return 0


if __name__ == "__main__":
    sys.exit(main())
