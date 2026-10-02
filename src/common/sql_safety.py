# -*- coding: utf-8 -*-
"""会被拼进 SQL 的**标识符 / tag 值**白名单（全仓库唯一真源）。

============================ 为什么需要这个模块 ============================
库名、超级表名、厂区/设备 tag 值都是**直接字符串拼接**进 SQL 的（TDengine REST 接口
不接受绑定参数，见 `src/platform/subscriber_to_td.py` 的 DDL/DML 拼装）。拼进去的值
只有两类可用字符，所以用白名单而不是转义：

    ^[A-Za-z_][A-Za-z0-9_]{0,62}$

这条规则最早只落在平台接入层（原 `subscriber_to_td.SQL_NAME_RE`，配合 `validate_config()`
在启动阶段拒绝非法配置）。展示层的读路径同样把 `TD_PLANT` / `TD_DEVICE` 拼进 tag 过滤
（`src/web/report.py`、`src/web/web_dashboard.py`），但当时以"设备维度只进缓存键哈希、
不进 SQL"为由跳过了校验 —— 这个前提后来不成立：读路径确实拿它做了 tag 过滤。

后果有两条，第二条比注入更常见：
  1. `TD_DEVICE="device1' OR '1'='1"` ⇒ `WHERE ... AND device = 'device1' OR '1'='1'`，
     过滤条件被 `OR` 短路，注入成立；
  2. `TD_DEVICE="Device1"`（大小写不符）⇒ SQL 合法但**命中 0 行**：报表全空、接口仍
     200、容器仍 healthy，**没有任何报错**。这类"静默全绿"才是现场最容易踩到的。

所以本模块只做一件事：把那条正则做成**唯一真源**，平台层与展示层共用；
非法值一律**拒绝启动**（`SystemExit`），不允许降级成 WARNING —— 静默降级正是上面第 2 条的来源。

⚠️ 本模块**只依赖标准库**（`re`）。这是有意的：展示层要 import 它，而平台层模块
（`subscriber_to_td.py`）会 import `paho.mqtt` 等重依赖，把常量放在那边等于把
MQTT 客户端拖进 Web 容器。
============================================================================
"""

from __future__ import annotations

import re
from typing import Final

#: 合法值：字母/下划线开头，之后字母、数字、下划线，总长 1~63。
#: 长度 63 与 TDengine 标识符上限量级一致（表名 = `{plant}_{device}`，两侧都受此限）。
SQL_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

#: 规则文案：错误信息里复用，保证两边的提示逐字一致
SQL_NAME_RULE: Final[str] = "只允许字母、数字、下划线，且以字母或下划线开头（1~63 字符）"


def is_sql_name(value: object) -> bool:
    """值是否满足白名单（非字符串一律 False，不做隐式 str() 转换）。"""
    return isinstance(value, str) and SQL_NAME_RE.match(value) is not None


def check_sql_name(value: object, *, what: str, env_var: str = "") -> str:
    """校验并返回 `value`；不合法时抛 `SystemExit`（拒绝启动，不是 WARNING）。

    参数：
        what    —— 人话描述这个值是什么（进错误信息，例如 "展示层 tag device"）
        env_var —— 该值来自哪个环境变量（进错误信息，便于现场直接改配置）

    ⚠️ 为什么是 `SystemExit` 而不是 `ValueError`：这两层的调用点都在**模块导入期**
    （配置常量求值 / 启动自检），进程还没开始服务。抛 `ValueError` 会被上层的
    `try/except` 顺手吞掉、降级成一条日志；容器照起、报表全空、health 照绿。
    与平台层 `subscriber_to_td.main()` 的处理一致：宁可不启动，也不要带病运行。
    """
    if is_sql_name(value):
        return value  # type: ignore[return-value]  # is_sql_name 已保证是 str
    source = f"（环境变量 {env_var}）" if env_var else ""
    raise SystemExit(
        f"{what}{source} 不满足 SQL 标识符/tag 白名单：{value!r}；{SQL_NAME_RULE}。"
        "该值会被拼进 SQL 的 tag 过滤条件，非法值会导致注入或静默 0 行，拒绝启动。"
    )
