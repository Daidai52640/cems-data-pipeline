# -*- coding: utf-8 -*-
"""控制台日志初始化（网关/设备/接入/展示四个进程共用）。

setup_logging() 原来在 4 个入口文件里各抄了一份逐字节相同的实现，三个常量也各抄 4 遍。
重复的代价是改一处漏三处：以后加日志文件、改格式、调级别，漏掉的那个进程会安静地用老格式，
日志拼在一起才对不齐。这里把"级别+格式+时间格式"三个决策集中到一处。

日志来源仍靠各文件自己的 LOGGER 区分（getLogger("gateway"/"device.modbus_server"/...），
本模块不统一，%(name)s 打出来的内容与重构前逐字节一致。
"""

from __future__ import annotations

import logging
from typing import Final

#: 统一日志级别。默认 INFO，与重构前 4 个文件的默认值一致。
#: 故意不读环境变量：本次重构只做去重，不改默认值。
LOG_LEVEL: Final[int] = logging.INFO

#: 统一日志格式。%(name)s 是区分 4 个进程来源的关键，别删。
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

#: 统一时间格式，与 %(asctime)s 配套。
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"


def setup_logging() -> None:
    """初始化日志：控制台输出，级别由 LOG_LEVEL 统一控制。

    force=True：重复调用会清掉已有 handler 重新配置，避免"入口 config 一次、模块 import 时
    又 config 一次"打出双份日志。目前只挂控制台，文件 handler 是以后的事。
    """
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        force=True,
    )
