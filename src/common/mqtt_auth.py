"""MQTT 凭据的统一下发口（可选）。

============================ 为什么单独一个模块 ============================
broker 一旦对局域网开放（边缘节点要发布数据），匿名可写就是最大的暴露面：
同网段任何主机都能伪造监测数据。EMQX 打开认证以后，网关与接入层必须带上凭据。

两条纪律：

1. ⚠️ **默认空 = 行为不变**：没配 `MQTT_USERNAME` 时完全不碰 client，
   与改造前逐字节一致（本机演示、单元测试、老部署都不受影响）。
2. ⚠️ **不在本模块读环境变量**：由调用方把值传进来，便于单测，也避免"导入即读配置"
   那种在测试里必须先设环境变量的写法。

用法：

    from src.common.mqtt_auth import apply_mqtt_credentials
    apply_mqtt_credentials(client, MQTT_USERNAME, MQTT_PASSWORD)
"""

from __future__ import annotations

from typing import Any


def apply_mqtt_credentials(client: Any, username: str, password: str) -> bool:
    """给 paho client 设置用户名口令；username 为空时不做任何改动。

    返回 True 表示设置了凭据。`password` 可以为空串（有些 broker 只校验用户名）。
    """
    name = (username or "").strip()
    if not name:
        return False
    client.username_pw_set(name, password if password != "" else None)
    return True
