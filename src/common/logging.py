# -*- coding: utf-8 -*-
"""控制台日志初始化的**唯一真源**（网关/设备/接入/展示四个进程共用）。

============================ 为什么需要这个模块 ============================
`setup_logging()` 原来在 4 个入口文件里各抄了一份**逐字节相同**的实现：

    src/gateway/gateway.py            src/device/modbus_server.py
    src/platform/subscriber_to_td.py  src/web/web_dashboard.py

连 `LOG_LEVEL` / `LOG_FORMAT` / `LOG_DATEFMT` 三个常量也各抄了 4 遍
（4 份取值完全相同，见下）。重复的代价不是"多打几行字"，而是**改一处漏三处**：
比如以后要加日志文件、要把格式改成带进程名、要按环境变量调级别，就必须记得
在 4 个文件里同步改；漏掉的那个进程会安静地用它自己的老格式，日志拼在一起
才对不齐 —— 而且没有任何测试会失败。

所以这里只做一件事：把"级别 + 格式 + 时间格式"这三个决策集中到一处，
4 个入口统一 `from src.common.logging import setup_logging`。

⚠️ **日志来源仍然靠各个文件自己的 `LOGGER` 区分**，不在本模块统一：
`logging.getLogger("gateway")` / `"device.modbus_server"` /
`"platform.subscriber_to_td"` / `"web.dashboard"` 这 4 个名字保持不变，
所以 `%(name)s` 打出来的内容与重构前**逐字节一致**。
============================================================================
"""

from __future__ import annotations

import logging
from typing import Final

#: 统一日志级别。默认 INFO，与重构前 4 个文件的默认值一致。
#: ⚠️ 这里**故意不读环境变量**：本次重构只做去重，不改默认值（保持 INFO）。
LOG_LEVEL: Final[int] = logging.INFO

#: 统一日志格式。`%(name)s` 是区分 4 个进程来源的关键，别删。
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

#: 统一时间格式，与 `%(asctime)s` 配套。
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"


def setup_logging() -> None:
    """初始化日志：控制台输出，级别由 LOG_LEVEL 统一控制。

    ⚠️ `force=True` 与重构前一致：重复调用会**清掉已有 handler 重新配置**，
    这样"入口先 config 一次、模块 import 时又 config 一次"不会打出双份日志。

    目前只挂控制台（`basicConfig` 默认的 StreamHandler）；文件 handler 是以后的事。
    """
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        force=True,
    )
