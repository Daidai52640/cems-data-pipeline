"""补传前的「订阅端就绪」判据（2026-10-25）。

============================ 这个模块补的是什么 ============================
broker 重启后有一个窗口：网关已经连上、开始补传缓存，但订阅端**还没重新订阅生效**。
此时 broker 会把网关发来的报文当成「当前无订阅」直接丢掉，却照常回 PUBACK。
于是就出现最坏的一种失真：**网关显示「broker 已确认」、缓存已删，下游一条没收到**，
库里留下永久缺口（实测复现见 docs/evidence 与 _worklog/2026-10-25.md）。

修复思路：订阅端连上并订阅成功后**主动发心跳**；网关在补传/直发之前要求
「最近一次心跳足够新，并且发生在本轮重连之后」。等不到就继续把数据放本地缓存，
最多等 `SUBSCRIBER_READY_WAIT_MAX` 秒后降级放行（打 WARNING），
避免订阅端真挂掉时数据永远出不去。

为什么不用 retained 消息做「就绪标记」：retained 存在 broker 里，broker 重启后
（数据卷丢失时）它可能和会话一起没了，也可能残留一条重启前的旧标记，
让网关误以为订阅端已就绪 —— 反而把这道门变成假的。用「周期心跳 + 时间窗」更稳。

本模块只放**纯函数**，不读环境变量、不碰 paho client，方便单测逐条盯住边界。
"""

from __future__ import annotations


def default_ready_topic(data_topic: str) -> str:
    """从数据主题推导「订阅端就绪」主题：`cems/plant1/data` → `cems/status/subscriber/plant1`。

    主题段不足（如自定义得很短）时退回 plant1 的默认值，保证多实例部署时
    每个厂区各有各的就绪通道，不会互相冒充。
    """
    parts = [p for p in (data_topic or "").split("/") if p]
    if len(parts) >= 3:
        return f"{parts[0]}/status/subscriber/{parts[1]}"
    return "cems/status/subscriber/plant1"


def ready_verdict(
    *,
    required: bool,
    last_beat: float | None,
    after_connect: float | None,
    now: float,
    max_age: float,
    wait_started: float | None,
    wait_max: float,
) -> tuple[bool, str]:
    """判断现在能不能往下游发数据。

    参数都是时间戳（monotonic 秒）：
      last_beat      —— 最近一次收到订阅端心跳的时刻，None = 从没收到过
      after_connect  —— 本轮 MQTT 连接建立的时刻，None = 还没连过
      wait_started   —— 网关第一次判定「未就绪」的时刻，None = 还没开始等

    返回 (True, "ok") / (True, "degraded-wait-timeout") / (False, 原因)。
    只有 "ok" 与 "not-required" 是正常放行；degraded 表示等超时了、带告警放行。
    """
    if not required:
        return True, "not-required"
    if last_beat is None:
        reason = "no-beat"
    elif after_connect is not None and last_beat < after_connect:
        # 心跳是上一轮连接留下的：broker 重启 / 网关重连之后它还没发过新的心跳
        reason = "stale-before-reconnect"
    elif now - last_beat > max_age:
        reason = "beat-stale"
    else:
        return True, "ok"

    if wait_started is not None and now - wait_started >= wait_max:
        return True, "degraded-wait-timeout"
    return False, reason
