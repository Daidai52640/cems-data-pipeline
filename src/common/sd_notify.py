"""systemd 的 sd_notify 最小实现（READY / WATCHDOG / STOPPING）。

============================ 为什么需要它 ============================
`Restart=always` 只能覆盖"进程退出"这一类故障。工业现场更常见的是**进程活着但不再出数**：

- 串口假死（设备仍在，读请求永远超时，但进程没崩）；
- broker 假连（TCP 还在、PUBACK 永远不来）；
- 线程死锁 / 采集循环被卡住。

这几种情况下 `systemctl status` 一直是 `active (running)`，看门狗**看不出问题**。

`WatchdogSec=` 的语义正好补这一类：服务必须在超时前主动发 `WATCHDOG=1`，
发不出来就视为"活着但没心跳"，systemd 杀掉并重启它。**心跳由"采集循环每一圈"发出**，
所以它衡量的是"数据还在流动"，而不是"进程还在"。

============================ 设计约束 ============================
- ⚠️ **不 import 任何第三方库**：纯 stdlib；容器里没有 systemd 时也照常工作。
- ⚠️ **没有 `NOTIFY_SOCKET` 时是 no-op（返回 False）**：普通 `python -m ...` 运行、
  单元测试、容器里跑都不受影响，不需要任何开关。
- 发送失败（socket 不存在、权限不足）只返回 False，**绝不抛异常** —— 一个可选的
  可观测性通道不该把采集主链路拖死。
- 抽象命名空间（`@/org/...`）按 systemd 约定转成 `\\0` 开头的地址。

============================ 用法 ============================
    from src.common.sd_notify import notify_ready, notify_watchdog

    notify_ready()          # 启动完成、开始对外服务
    ...循环...
    notify_watchdog()       # 每圈一次（间隔必须小于 unit 里的 WatchdogSec）
"""

from __future__ import annotations

import os
import socket
from typing import Final, Optional

#: systemd 约定的环境变量名（不在 systemd 下运行时为空）
ENV_NOTIFY_SOCKET: Final[str] = "NOTIFY_SOCKET"

READY: Final[str] = "READY=1"
WATCHDOG: Final[str] = "WATCHDOG=1"
STOPPING: Final[str] = "STOPPING=1"


def notify_socket_path() -> str | None:
    """返回可用的 notify socket 地址；未在 systemd 下运行时返回 None。"""
    raw = os.environ.get(ENV_NOTIFY_SOCKET, "").strip()
    if not raw:
        return None
    # systemd 的抽象命名空间写法：@/org/freedesktop/systemd1/notify -> \0...
    if raw.startswith("@"):
        return "\0" + raw[1:]
    if raw.startswith("/"):
        return raw
    return None


def notify(message: str) -> bool:
    """把一条 sd_notify 消息发给 systemd；成功返回 True，任何失败返回 False。"""
    path = notify_socket_path()
    if path is None:
        return False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(path)
            sock.sendall(message.encode("utf-8"))
        return True
    except OSError:
        # socket 不存在 / 权限不足 / 平台不支持 AF_UNIX：都按"没有心跳通道"处理
        return False
    except Exception:  # noqa: BLE001 - 可观测性通道绝不向上抛
        return False


def notify_ready() -> bool:
    """告诉 systemd：启动完成（`Type=notify` 必须发，否则 unit 超时失败）。"""
    return notify(READY)


def notify_watchdog() -> bool:
    """报告一次心跳（`WatchdogSec=` 生效时，超时未发会被 systemd 重启）。"""
    return notify(WATCHDOG)


def notify_stopping() -> bool:
    """告诉 systemd：正在优雅退出（`Restart=always` 下可帮助识别主动停止）。"""
    return notify(STOPPING)
