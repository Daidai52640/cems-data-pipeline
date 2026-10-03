# -*- coding: utf-8 -*-
"""数采网关：轮询 Modbus 设备读数并加时间戳经 MQTT 上送，断网时写本地缓存、恢复后自动补传。"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Final, Optional

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusTcpClient

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import POINTS, REG_BASE, REG_COUNT   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================
# 所有连接参数都支持用环境变量覆盖，默认值与"本机直接运行"完全一致；
# 容器化部署时由 docker-compose.yml 注入服务名（如 MODBUS_HOST=device）。

# ---- Modbus 设备（对应 modbus_server.py）----
MODBUS_HOST: Final[str] = os.getenv("MODBUS_HOST", "localhost")
MODBUS_PORT: Final[int] = int(os.getenv("MODBUS_PORT", "5020"))
MODBUS_UNIT: Final[int] = int(os.getenv("MODBUS_UNIT", "1"))   # 从站地址

# ---- 寄存器地图 ----
# 测点定义（字段名/地址）、寄存器数量与换算系数统一来自 src/common/points.py

# ---- 报文时间戳格式 ----
TS_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"

# ---- MQTT（对应 EMQX）----
MQTT_HOST: Final[str] = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT: Final[int] = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC: Final[str] = os.getenv("MQTT_TOPIC", "cems/plant1/data")
MQTT_QOS: Final[int] = int(os.getenv("MQTT_QOS", "1"))
MQTT_KEEPALIVE: Final[int] = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_CLIENT_ID: Final[str] = os.getenv("MQTT_CLIENT_ID", "cems-gateway-plant1")

# ---- 采集节奏与重连 ----
POLL_INTERVAL: Final[float] = float(os.getenv("POLL_INTERVAL", "5.0"))                  # 轮询周期（秒）
MODBUS_RETRY_INTERVAL: Final[float] = float(os.getenv("MODBUS_RETRY_INTERVAL", "5.0"))  # 断线重连间隔（秒）

# ---- 断网缓存：单写者所有权模型（相对项目根目录推导，换机器不用改）----
# cache.jsonl          只由采集主循环追加写：新采集到的数据
# inflight-<seq>.jsonl 只由补传线程持有：正在补传的在途批次（段名唯一，见下）
# 两个写者各写各的文件，靠"原子改名"交接，不存在整文件回写抹掉对方数据的情况。
# 旧版在途文件叫 cache.jsonl.sending（固定名）；上一轮遗留的 .sending 仍会被读进来补发
# （见 _read_segment_lines），保证版本升级不会把盘上已缓存的数据漏掉。
#
# ★ 缓存目录必须**每实例一份**（多设备部署的关键约束）：
#   网关上所有缓存/续传状态都落在 DATA_DIR 里的固定文件名上（cache.jsonl、
#   inflight-<seq>.jsonl、.gateway_alive）。两个网关实例若共用同一个目录，
#   后果不是"慢一点"，而是**两个实例互相把对方的数据发出去**：
#     - 段号由目录内文件推导（_next_segment_path），A 实例会接着 B 实例的段号写；
#     - 补传是"整目录接手"（_take_over_pending 扫 glob），A 会把 B 缓存的报文
#       发到 A 自己的主题上 → 数据张冠李戴，且两边都以为发成功了；
#     - `.gateway_alive` 心跳互相覆盖，healthcheck 判断的不再是本实例的采集循环。
#   所以第二台设备必须给独立的 GATEWAY_DATA_DIR（compose 里挂在各自的卷上）。
GATEWAY_DATA_DIR_ENV: Final[str] = os.getenv("GATEWAY_DATA_DIR", "").strip()


def resolve_data_dir(raw: str) -> Path:
    """把 GATEWAY_DATA_DIR 解析成绝对路径；空值回落历史默认值 PROJECT_ROOT/data。

    - 未设置 → `PROJECT_ROOT/data`，与改造前**逐字节一致**（单设备现状不变）
    - 绝对路径 → 原样使用（容器里的推荐写法，如 `/app/data-plant2`）
    - 相对路径 → 相对**项目根**解析（不是 CWD）：容器里 PROJECT_ROOT 就是 /app，
      所以 `data/plant2` 与 `/app/data/plant2` 指向同一处，本机运行与容器运行
      不会因为启动目录不同而分叉。
    """
    if not raw:
        return PROJECT_ROOT / "data"
    candidate = Path(raw).expanduser()
    return candidate if candidate.is_absolute() else (PROJECT_ROOT / candidate)


DATA_DIR: Final[Path] = resolve_data_dir(GATEWAY_DATA_DIR_ENV)
CACHE_FILE: Final[Path] = DATA_DIR / "cache.jsonl"
SENDING_FILE: Final[Path] = DATA_DIR / "cache.jsonl.sending"   # 旧版固定名（只读兼容，不再写入）
HEARTBEAT_FILE: Final[Path] = DATA_DIR / ".gateway_alive"   # 供容器 healthcheck 判断循环是否还在转
CACHE_LOCK: Final[threading.Lock] = threading.Lock()   # 保护两个缓存文件的换手动作

# ---- 在途批次的段名（★ 唯一，只增不减；永不被第二次改名覆盖）----
# 旧写法用固定名 cache.jsonl.sending，每次取批都要"覆盖"上一轮残留：
# 覆盖完成 → 合并批写回之间，残留只在内存里，此时被硬杀就永久丢那批数据（见 _take_over_pending）。
# 改成段名之后没有任何覆盖动作：spool 改名成一个新的段，旧段原封不动留在盘上、按 seq 升序优先发送。
# ⚠️ 崩溃恢复不需要额外状态文件：段列表与下一个段号都由文件名推导。
SEGMENT_PREFIX: Final[str] = "inflight-"
SEGMENT_SUFFIX: Final[str] = ".jsonl"
SEGMENT_GLOB: Final[str] = f"{SEGMENT_PREFIX}*{SEGMENT_SUFFIX}"
#: 本批落在哪些段（`_take_over_pending` 维护）
#:  - `[0]` = 本批自己的段（滚动回写/收尾只动它）
#:  - 其余 = 被合并进来的更老残留段（数据已并入 `[0]` 后即删除，避免下轮重复补发）
BATCH_SEGMENTS: list[Path] = []

# 缓存容量上限：长期断网时文件会一直涨，写满磁盘后每条数据都会静默丢。
# 超过上限就按"丢最旧、保最新"裁剪，并升 CRITICAL —— 宁可丢最早的，也别把盘写满。
CACHE_MAX_BYTES: Final[int] = int(os.getenv("CACHE_MAX_BYTES", str(64 * 1024 * 1024)))
CACHE_TRIM_RATIO: Final[float] = float(os.getenv("CACHE_TRIM_RATIO", "0.5"))  # 裁剪后保留的比例

# ---- 发送确认与补传节奏 ----
# 注意：paho 的 publish() 返回 rc=0 只代表"消息进了本机发送队列"，
# 不代表 broker 收到。QoS=1 必须等 PUBACK，所以这里统一用 wait_for_publish 确认。
# 单条等 PUBACK 的秒数 / 补传在途窗口条数 / 补传重试间隔（秒）
PUBLISH_ACK_TIMEOUT: Final[float] = float(os.getenv("PUBLISH_ACK_TIMEOUT", "5.0"))
RESEND_WINDOW: Final[int] = int(os.getenv("RESEND_WINDOW", "100"))
RESEND_RETRY_INTERVAL: Final[float] = float(os.getenv("RESEND_RETRY_INTERVAL", "30.0"))

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("gateway")


# ==================== 2. 日志 ====================

def setup_logging() -> None:
    """初始化日志：控制台输出，级别由 LOG_LEVEL 统一控制。"""
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        force=True,
    )


# ==================== 3. 设备读取 ====================

def _read_holding_registers(client: ModbusTcpClient, address: int, count: int) -> Any:
    """读保持寄存器；兼容 pymodbus 新老版本（老版用 slave=，新版改名 device_id=）。"""
    try:
        return client.read_holding_registers(address=address, count=count, slave=MODBUS_UNIT)
    except TypeError:
        return client.read_holding_registers(address=address, count=count, device_id=MODBUS_UNIT)


def read_device(client: ModbusTcpClient) -> Optional[dict[str, float]]:
    """读设备全部测点寄存器并换算成真实值，返回 {测点名: 数值}；失败返回 None（不抛异常）。"""
    try:
        response = _read_holding_registers(client, REG_BASE, REG_COUNT)
    except Exception as exc:
        LOGGER.error("读 Modbus 设备异常: %s", exc)
        return None

    if response is None or response.isError():
        LOGGER.error("读 Modbus 设备失败（设备无响应或返回异常码）: %s", response)
        return None

    try:
        # 返回的寄存器块从 REG_BASE 开始，所以下标要减去起始地址
        return {
            point.name: response.registers[point.address - REG_BASE] / point.scale
            for point in POINTS
        }
    except (IndexError, TypeError) as exc:
        LOGGER.error("Modbus 返回数据不完整: %s", exc)
        return None


def close_modbus(client: Optional[ModbusTcpClient]) -> None:
    """安全关闭 Modbus 连接，忽略关闭过程中的异常。"""
    if client is None:
        return
    try:
        client.close()
    except Exception as exc:
        LOGGER.debug("关闭 Modbus 连接时出错（忽略）: %s", exc)


def connect_modbus() -> Optional[ModbusTcpClient]:
    """连接 Modbus 设备；失败返回 None，由主循环稍后重试。"""
    client = ModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    try:
        if client.connect():
            LOGGER.info("Modbus 已连接: %s:%d 从站%d", MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT)
            return client
        LOGGER.error(
            "连不上 Modbus 设备 %s:%d（确认 modbus_server.py 在跑），%.0f 秒后重试",
            MODBUS_HOST, MODBUS_PORT, MODBUS_RETRY_INTERVAL,
        )
    except Exception as exc:
        LOGGER.error("Modbus 连接异常: %s", exc)
    close_modbus(client)
    return None


def verify_unit_readable(client: ModbusTcpClient) -> bool:
    """启动期预检：本实例配置的从站**真的能被读到**吗？读不到就拒绝启动。

    ★ 为什么需要这道预检（2026-10-03 加）：
      设备层是"一个 TCP 服务端、多个从站"（`SLAVE_IDS=1,2`），而每个网关实例按
      `MODBUS_UNIT` 各读一个从站。**两个开关是分开的**：`SLAVE_IDS` 在使用方 `.env`，
      `--profile plant2` 在命令行。只开后者时，设备层只建了从站 1，而 plant2 网关
      去读从站 2 —— 此时**一条数据也读不到**（修复前更糟：会静默拿到从站 1 的数据，
      于是两台曲线逐值相等、容器全 healthy、无任何报错）。

      "连得上 TCP" ≠ "读得到本从站的数据"：`client.connect()` 只证明端口通。
      所以这里必须**真读一次寄存器**，把"配置错"变成**启动即失败**（容器红），
      而不是"容器健康但永远不出数、只能去日志里挖"。

    返回 True 表示预检通过；False 表示本从站读不到（调用方据此拒绝启动）。
    """
    reason = ""
    try:
        response = _read_holding_registers(client, REG_BASE, REG_COUNT)
        if response is None or response.isError():
            reason = f"无响应/返回异常码（{response}）"
    except Exception as exc:                       # noqa: BLE001 —— 预检要兜住一切读失败
        # 实测（pymodbus 3.6.9）：读未登记的从站会在这里以 "Unable to decode request"
        # 之类的解码异常冒出来（服务端对未登记从站不作答），所以两个分支必须给同一套提示
        reason = f"读取抛异常（{type(exc).__name__}: {exc}）"

    if reason:
        LOGGER.critical(
            "启动预检失败: 读 Modbus 从站%d %s。"
            "⚠️ 最可能的原因：本实例的 MODBUS_UNIT=%d 不在设备层的 SLAVE_IDS 里 —— "
            "设备层只对**已登记**的从站提供数据。多设备是**两个开关**："
            "① 使用方 .env 里 SLAVE_IDS 要包含 %d（如 SLAVE_IDS=1,2）并重建 device 容器；"
            "② 起第二套实例要带 --profile plant2。只开②不开①就会命中这条。"
            "（本实例拒绝启动，而不是空跑或拿别的从站的数据。设备层是 %s:%d）",
            MODBUS_UNIT, reason, MODBUS_UNIT, MODBUS_UNIT, MODBUS_HOST, MODBUS_PORT,
        )
        return False
    LOGGER.info("启动预检通过: 从站%d 可读（%d 个寄存器）", MODBUS_UNIT, REG_COUNT)
    return True


# ==================== 4. 断网续传（★ 核心逻辑，勿改动） ====================

def _read_lines(path: Path) -> list[str]:
    """读文件的非空行；文件不存在返回空列表，读失败记 error 后返回空列表。"""
    try:
        if not path.exists():
            return []
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        LOGGER.error("[缓存] 读取 %s 失败: %s", path.name, exc)
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


def _write_lines(path: Path, lines: list[str]) -> bool:
    """整文件写入：先写 .tmp 再原子替换，避免写到一半被别人读到半个文件。"""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
        os.replace(tmp, path)
        return True
    except OSError as exc:
        LOGGER.error("[缓存] 写入 %s 失败: %s", path.name, exc)
        return False


def save_to_cache(payload: str, reason: str = "离线") -> bool:
    """把一条数据追加到 cache.jsonl 队尾（唯一写者是采集主循环）。

    写后 flush + fsync：这样进程被 kill / 机器断电时，已写的数据才真的在盘上，
    否则可能停在 OS page cache 里，重启后尾部数据凭空消失。
    """
    try:
        with CACHE_LOCK:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            with CACHE_FILE.open("a", encoding="utf-8") as fp:
                fp.write(payload + "\n")
                fp.flush()
                os.fsync(fp.fileno())
            _trim_cache_if_oversize()
        LOGGER.info("[缓存] %s，数据已入本地队列: %s", reason, payload)
        return True
    except OSError as exc:
        # 磁盘满 / 无写权限：这条数据丢了，而且后续每条都会丢，必须显眼
        LOGGER.critical("[缓存] 写入本地失败，本条数据丢失: %s | %s", exc, payload)
        return False


def _trim_cache_if_oversize() -> None:
    """缓存超过容量上限时，按"丢最旧、保最新"裁剪（调用方需已持有 CACHE_LOCK）。"""
    try:
        size = CACHE_FILE.stat().st_size
    except OSError as exc:
        LOGGER.error("[缓存] 读取缓存大小失败: %s", exc)
        return
    if size <= CACHE_MAX_BYTES:
        return

    lines = _read_lines(CACHE_FILE)
    keep = max(1, int(len(lines) * CACHE_TRIM_RATIO))
    dropped = len(lines) - keep
    LOGGER.critical(
        "[缓存] 已积压 %.1f MB 超过上限 %.1f MB，丢弃最旧的 %d 条（保留最新 %d 条）—— "
        "请尽快恢复与 broker 的连接",
        size / 1024 / 1024, CACHE_MAX_BYTES / 1024 / 1024, dropped, keep,
    )
    _write_lines(CACHE_FILE, lines[dropped:])


def check_data_dir_writable() -> bool:
    """启动时确认缓存目录可写；不可写要立刻喊出来，而不是等第一条数据丢的时候才发现。"""
    probe = DATA_DIR / ".write_probe"
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError as exc:
        LOGGER.critical("[缓存] 数据目录不可写，断网缓存会全部失败: %s (%s)", DATA_DIR, exc)
        return False


def touch_heartbeat() -> None:
    """刷新存活心跳：采集主循环每转一圈写一次，容器 healthcheck 靠它判断循环有没有卡死。

    比"端口在不在听"更有意义 —— 网关没有监听端口，进程活着但循环卡死时
    只有心跳文件能反映出来。写失败不致命（比如磁盘满），降级成 debug。
    """
    try:
        HEARTBEAT_FILE.write_text(str(time.time()), encoding="utf-8")
    except OSError as exc:
        LOGGER.debug("写心跳文件失败（忽略）: %s", exc)


def _publish_async(client: mqtt.Client, payload: str, tag: str) -> Optional[mqtt.MQTTMessageInfo]:
    """把消息交给 paho 发送队列并返回消息句柄；入队失败返回 None。

    rc == MQTT_ERR_SUCCESS 只能说明"进了本机队列"，必须再等 PUBACK 才算送达。
    """
    try:
        info = client.publish(MQTT_TOPIC, payload, qos=MQTT_QOS)
    except Exception as exc:      # paho 在边界上可能抛多种异常，不能让它掀翻采集循环
        LOGGER.error("[%s] 发布异常: %s | %s", tag, exc, payload)
        return None
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        LOGGER.error("[%s] 入队失败 rc=%s | %s", tag, info.rc, payload)
        return None
    return info


def _wait_published(info: mqtt.MQTTMessageInfo, payload: str, tag: str) -> bool:
    """等 broker 的 PUBACK；超时或消息从未发出都算失败，由调用方落盘重试。

    ★ paho 的 wait_for_publish(timeout) 超时是**静默返回**（只有 rc 有问题才抛异常），
    所以"没抛异常"绝不等于送达，必须再用 is_published() 复核，
    否则超时的消息会被当成已送达而不再落盘，进程一退就丢。
    """
    try:
        info.wait_for_publish(timeout=PUBLISH_ACK_TIMEOUT)
    except (ValueError, RuntimeError) as exc:
        LOGGER.warning(
            "[%s] %.0f 秒内未收到 PUBACK（%s），本条转入缓存: %s",
            tag, PUBLISH_ACK_TIMEOUT, exc, payload,
        )
        return False
    if not info.is_published():
        LOGGER.warning(
            "[%s] 等待 %.0f 秒仍未收到 PUBACK，本条转入缓存: %s",
            tag, PUBLISH_ACK_TIMEOUT, payload,
        )
        return False
    LOGGER.debug("[%s] 已确认送达 %s", tag, payload)
    return True


def publish(client: mqtt.Client, payload: str, tag: str) -> bool:
    """发布一条并等 PUBACK；只有 broker 确认收到才返回 True。"""
    info = _publish_async(client, payload, tag)
    return info is not None and _wait_published(info, payload, tag)


def _segment_seq(path: Path) -> int:
    """段文件名里的序号；名字不合规范返回 -1（新段号从 max+1 起，不受歪名字影响）。"""
    name = path.name
    if not (name.startswith(SEGMENT_PREFIX) and name.endswith(SEGMENT_SUFFIX)):
        return -1
    digits = name[len(SEGMENT_PREFIX):-len(SEGMENT_SUFFIX)]
    return int(digits) if digits.isdigit() else -1


def _in_flight_segments() -> list[Path]:
    """全部在途段，按 seq 升序（= 时间序，最老的先发）。目录不存在时返回空列表。"""
    if not DATA_DIR.is_dir():
        return []
    return sorted(DATA_DIR.glob(SEGMENT_GLOB), key=_segment_seq)


def _next_segment_path() -> Path:
    """下一个段名 = 现有最大 seq + 1（不读任何状态文件，纯由文件名推导）。"""
    seqs = [_segment_seq(path) for path in _in_flight_segments()]
    return DATA_DIR / f"{SEGMENT_PREFIX}{max(seqs, default=0) + 1:06d}{SEGMENT_SUFFIX}"


def _read_segment_lines(path: Optional[Path]) -> list[str]:
    """读一个段的行；路径为 None 或文件不存在时返回空列表。

    ⚠️ 必须用 `_read_lines` 的"逐行 strip"口径读——它顺带兼容旧版 `cache.jsonl.sending`
    里 EOF 行缺 `\\n` 的写法（`splitlines` 不要求末行有换行符）。
    """
    return _read_lines(path) if path is not None else []


def _collect_leftover_segments() -> list[Path]:
    """收集"上一轮遗留、需要并入本批"的段（旧版固定名 `.sending` 也在内）。"""
    segments: list[Path] = []
    if SENDING_FILE.exists():
        segments.append(SENDING_FILE)
    segments.extend(_in_flight_segments())
    return [path for path in segments if _read_lines(path)]


def _leftover_lines_in(segments: Sequence[Path]) -> list[str]:
    """按给定顺序读残留段的内容（旧数据在前 = 发送顺序）。"""
    merged: list[str] = []
    for segment in segments:
        merged.extend(_read_segment_lines(segment))
    return merged


def _drop_segments(segments: Sequence[Path]) -> list[str]:
    """数据已并入本批段之后，删掉这些残留段（避免下一轮重复补发）。返回失败的名字。

    ⚠️ 调用时机只有一个：数据**已经**落到本批段之后。删早了会丢，删晚了只是重复发。
    """
    failed: list[str] = []
    for segment in segments:
        try:
            segment.unlink(missing_ok=True)
        except OSError as exc:
            LOGGER.error("[补传] 删除残留段 %s 失败: %s", segment.name, exc)
            failed.append(segment.name)
    return failed


def _take_over_pending() -> list[str]:
    """接管待补传数据：把 cache.jsonl 原子改名成唯一段名，返回按时序的批次。

    改名之后，主循环新采的数据继续追加到全新的 cache.jsonl，补传线程只动段文件，
    两个写者各写各的文件 —— 这样才不会出现"补传结束整文件回写、把期间新追加的数据抹掉"。
    上次补传中断残留的段排在本批队首，保证旧数据永远先于新数据重发。

    ★ 段名唯一（`inflight-<seq>.jsonl`，seq 只增不减）：**没有任何一次改名会覆盖已有文件**。
    旧写法把 cache.jsonl 改名成固定的 .sending，会**覆盖**上一轮残留的 .sending；而合并批
    要等 `_write_lines` 才落盘，于是在"覆盖完成 → 合并批写完"之间，上一轮的残留只存在于内存里，
    此时进程被硬杀（SIGKILL / 断电 / 容器 kill -9）那批数据就永久消失（实测 2 条全丢）。
    段方案从结构上消除了这个窗口：改名只新建文件，旧段原封不动留在盘上。
    """
    global BATCH_SEGMENTS
    with CACHE_LOCK:
        leftovers = _collect_leftover_segments()
        if not CACHE_FILE.exists() and not leftovers:
            BATCH_SEGMENTS = []
            return []

        if CACHE_FILE.exists():
            segment = _next_segment_path()
            try:
                os.replace(CACHE_FILE, segment)   # ★ 原子交接（目标名唯一，不覆盖任何已有文件）
            except OSError as exc:
                LOGGER.error("[补传] 接管缓存文件失败: %s", exc)
                return []
            current = _read_lines(segment)
            if leftovers:
                # 把残留段并进本批段，**写成功之后**才删残留：任何一步被杀都不会丢数据
                #（被杀在写之前 → 残留段还在；被杀在删之前 → 最坏下一轮重复发一次）。
                # ⚠️ 写失败时绝不能删残留段——那才是把数据唯一副本抹掉。
                merged = _leftover_lines_in(leftovers) + current
                if _write_lines(segment, merged):
                    _drop_segments(leftovers)
                else:
                    LOGGER.error(
                        "[补传] 合并残留段到 %s 失败，保留残留段（本轮照发，不删任何副本）",
                        segment.name,
                    )
            BATCH_SEGMENTS = [segment]
        else:
            # 没有新数据可接管：直接接着补残留段（它们就是本批）
            BATCH_SEGMENTS = list(leftovers)

        batch = _leftover_lines_in(BATCH_SEGMENTS)
        return batch


def _finish_resend(remaining: list[str], confirmed: int) -> None:
    """补传收尾：未确认的行放回 cache.jsonl 队首，写成功后才删本批的段文件。

    ★ 回写失败时绝不能删段文件 —— 它是这批数据当下唯一的副本，
    删掉就等于整批永久丢失。失败就原样留着，等下次补传接着处理。
    """
    global BATCH_SEGMENTS
    with CACHE_LOCK:
        # 始终重写 cache.jsonl：remaining 为空时就是清空成空文件。
        # 保持这个文件一直存在（哪怕是空的），避免"文件突然消失"让人以为数据丢了。
        if not _write_lines(CACHE_FILE, remaining + _read_lines(CACHE_FILE)):
            LOGGER.critical(
                "[补传] 回写 %s 失败，保留段文件（本批 %d 条未确认）等待下次重试: %s",
                CACHE_FILE.name, len(remaining),
                ", ".join(path.name for path in BATCH_SEGMENTS) or SENDING_FILE.name,
            )
            return
        segments = BATCH_SEGMENTS
        BATCH_SEGMENTS = []
        _drop_segments(segments)

    if confirmed:
        LOGGER.info("[补传完成] broker 已确认 %d 条", confirmed)
    if remaining:
        LOGGER.warning("[补传未完成] %d 条未确认，已放回缓存队首等待重试", len(remaining))


def has_backlog() -> bool:
    """缓存里是否还有待补传的数据（空文件和残留的空段都不算）。

    ⚠️ 必须把**在途段**（`inflight-<seq>.jsonl`）也算进来，否则会出现"数据滞留但永不补传"：
    硬杀发生在取批过程中时，leftover 只落在段文件里；此时 `cache.jsonl` 可能为空，
    若这里只看 `cache.jsonl`/旧 `.sending`，重启后就不会触发补传，**残留段永久滞留 = 真丢数据**。
    修复前 `.sending` 是固定名，这个检查顺带覆盖了它；改成唯一段名后必须显式覆盖。
    """
    try:
        if CACHE_FILE.exists() and CACHE_FILE.stat().st_size > 0:
            return True
        # 旧版固定名 + 新版在途段；`_collect_leftover_segments` 已按"文件非空"过滤
        return bool(_collect_leftover_segments())
    except OSError as exc:
        LOGGER.error("[缓存] 检查积压失败: %s", exc)
        return False


def resend_cache(client: mqtt.Client) -> int:
    """按时间顺序补传缓存；每条等 PUBACK，只有确认送达的才从缓存移除。

    必须跑在独立线程里：paho 的 wait_for_publish() 要等网络线程处理 PUBACK，
    在 on_connect 回调（本身就在网络线程里）中调用会自等死锁。
    """
    batch = _take_over_pending()
    if not batch:
        return 0

    LOGGER.info("[补传] 开始补传 %d 条，在途窗口 %d 条", len(batch), RESEND_WINDOW)
    remaining: list[str] = []
    confirmed = 0

    for offset in range(0, len(batch), RESEND_WINDOW):
        chunk = batch[offset:offset + RESEND_WINDOW]
        infos = [(line, _publish_async(client, line, "补传")) for line in chunk]
        for line, info in infos:
            if info is not None and _wait_published(info, line, "补传"):
                confirmed += 1
            else:
                remaining.append(line)
        # 滚动压缩：把"未确认 + 还没发"写回**本批自己的段**（`BATCH_SEGMENTS[0]`）。
        # 任一时刻被杀进程，最多只影响正在飞的那一窗，其余数据都还在盘上。
        _write_lines(BATCH_SEGMENTS[0], remaining + batch[offset + len(chunk):])
        LOGGER.debug(
            "[补传] 进度 %d/%d，未确认 %d 条",
            min(offset + RESEND_WINDOW, len(batch)), len(batch), len(remaining),
        )

    _finish_resend(remaining, confirmed)
    return confirmed


class ResendWorker:
    """补传线程管理：同一时刻只跑一个补传，且绝不占用 MQTT 回调线程。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stopped = threading.Event()
        self._last_trigger = 0.0

    @property
    def running(self) -> bool:
        """当前是否有补传任务在跑。"""
        return self._thread is not None and self._thread.is_alive()

    def trigger(self, client: mqtt.Client) -> bool:
        """立刻触发一次补传；已停止或正在补传时忽略（不阻塞调用方）。"""
        if self._stopped.is_set() or self.running:
            return False
        self._last_trigger = time.monotonic()
        self._thread = threading.Thread(
            target=self._run, args=(client,), name="resend-cache", daemon=True,
        )
        self._thread.start()
        return True

    def trigger_if_due(self, client: mqtt.Client, interval: float) -> bool:
        """积压重试：距上次触发超过 interval 才再补一次，避免忙等。"""
        if time.monotonic() - self._last_trigger < interval:
            return False
        return self.trigger(client)

    def _run(self, client: mqtt.Client) -> None:
        """线程入口：拿不到锁说明已有补传在跑，直接退出。"""
        if not self._lock.acquire(blocking=False):
            return
        try:
            resend_cache(client)
        except Exception:      # 线程边界：任何异常都不能让补传线程静默死掉
            LOGGER.exception("[补传] 补传线程异常退出")
        finally:
            self._lock.release()

    def stop(self, timeout: float = 15.0) -> None:
        """退出前调用：等补传把手里的数据写回盘上，避免收尾丢数据。"""
        self._stopped.set()
        thread = self._thread
        if thread is None or not thread.is_alive():
            return
        thread.join(timeout=timeout)
        if thread.is_alive():
            LOGGER.warning(
                "[补传] 线程未在 %.0f 秒内收尾，未确认的数据保留在段文件 %s 里",
                timeout, SEGMENT_GLOB,
            )


RESEND_WORKER: Final[ResendWorker] = ResendWorker()


# ==================== 5. MQTT 回调 ====================

def on_connect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    """连接/重连成功 → 触发补传（补传跑在独立线程，不阻塞网络回调线程）。"""
    if reason_code == 0:
        LOGGER.info("MQTT 已连接: %s:%d", MQTT_HOST, MQTT_PORT)
        RESEND_WORKER.trigger(client)   # ★ 重连成功 → 补传缓存
    else:
        LOGGER.error("MQTT 连接失败 reason_code=%s，数据将写入本地缓存", reason_code)


def on_disconnect(
    client: mqtt.Client,
    userdata: Any,
    flags: Any,
    reason_code: Any,
    properties: Any,
) -> None:
    """连接断开 → 提示进入断网缓存模式（后台 loop 会自动重连）。"""
    if reason_code != 0:
        LOGGER.error("MQTT 连接断开 reason_code=%s，后续数据写入本地缓存", reason_code)


# ==================== 6. 主流程 ====================

def build_payload(values: dict[str, float]) -> str:
    """把全部测点值组装成上送报文：时间戳 + 各测点 K=V + 数据标记。

    格式: "2026-09-10 12:00:00 SO2=35.2 NOx=18.5 ... Pressure=101.3 Flag=N"
    """
    body = " ".join(f"{point.name}={values[point.name]}" for point in POINTS)
    return f"{time.strftime(TS_FORMAT)} {body} Flag=N"


def main() -> None:
    """网关主流程：连 MQTT → 连设备 → 循环"读→换算→加时间戳→（直发/缓存）"。"""
    setup_logging()
    check_data_dir_writable()     # 断网缓存目录不可写要立刻喊出来，别等丢数据才发现
    LOGGER.info(
        "网关启动: 设备 %s:%d 从站%d → MQTT %s:%d 主题 %s (QoS=%d) client_id=%s",
        MODBUS_HOST, MODBUS_PORT, MODBUS_UNIT, MQTT_HOST, MQTT_PORT, MQTT_TOPIC, MQTT_QOS,
        MQTT_CLIENT_ID,
    )
    # 多实例部署时这一行是排查"缓存串味"的第一现场：它必须每实例一份、互不相同
    LOGGER.info(
        "断网续传已启用，缓存目录: %s（GATEWAY_DATA_DIR=%s）",
        DATA_DIR, GATEWAY_DATA_DIR_ENV or "未设置，用默认 PROJECT_ROOT/data",
    )

    # ---- 连 MQTT：connect_async + loop_start，broker 暂时不可用也会后台自动重连 ----
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    try:
        mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
        mqtt_client.loop_start()
    except Exception as exc:
        LOGGER.error("MQTT 初始化失败: %s，数据将先写本地缓存", exc)

    # ---- 连 Modbus：连不上不退出进程，进主循环后持续重试 ----
    modbus_client = connect_modbus()

    # ---- 启动预检：本实例的从站必须真的读得到，否则拒绝启动 ----
    # ⚠️ 与"连不上设备"的处置**故意不同**：连不上（设备还没起）是暂时状态，值得重试；
    #    而"连上了但读不到本从站"是**配置错**（MODBUS_UNIT 不在设备层 SLAVE_IDS 里），
    #    重试一万次也不会好，只会让容器一直 healthy 却永远不出数（只能去日志里挖）。
    if modbus_client is not None and not verify_unit_readable(modbus_client):
        close_modbus(modbus_client)
        raise SystemExit(1)

    count = 0
    try:
        while True:
            touch_heartbeat()      # 每圈刷一次：healthcheck 靠它判断循环还活着

            # ★ 本迭代"触发补传之前"的在途状态快照 —— 这是本轮该不该入队的唯一依据。
            # 必须在这里取：下面的 trigger_if_due 只是启动一个补传线程，它不该反过来
            # 把本轮数据挤进队列。否则会形成闭环：触发 → 本轮入队 → 队列非空 →
            # 下一个节流周期又是"触发的同时入队" → 队列永远有一行、每 6 条就有 1 条
            # 走"缓存 30 秒再补传"的慢路径（成因见 docs/adr/0003-补传入队判定修正.md）。
            resend_in_flight = RESEND_WORKER.running

            # 补传重试放在循环最开头：它只跟 broker 有关，不该被设备侧故障挡住。
            # 放在循环末尾时，下面两处 continue（设备没连上 / 读失败）会把它整段跳过，
            # 结果设备一坏、积压就再也补不出去，要等到下一次重连事件才动。
            if mqtt_client.is_connected() and has_backlog():
                RESEND_WORKER.trigger_if_due(mqtt_client, RESEND_RETRY_INTERVAL)

            if modbus_client is None:
                time.sleep(MODBUS_RETRY_INTERVAL)
                modbus_client = connect_modbus()
                continue

            data = read_device(modbus_client)
            if data is None:
                # 读失败：重建连接后重试，不退出进程
                LOGGER.error("本轮采集失败，重建 Modbus 连接")
                close_modbus(modbus_client)
                modbus_client = None
                time.sleep(MODBUS_RETRY_INTERVAL)
                continue

            count += 1
            payload = build_payload(data)


            if mqtt_client.is_connected() and not resend_in_flight:
                # ★ 在线且本轮开始前没有在途补传批次：直接发；没收到 PUBACK 就落盘，绝不静默丢
                if not publish(mqtt_client, payload, tag=f"第{count}条 在线直发"):
                    save_to_cache(payload, reason="未收到 PUBACK")
            elif not mqtt_client.is_connected():
                # ★ 断网：写缓存，数据不丢
                save_to_cache(payload, reason="断网")
            else:
                # 本轮开始前已有在途补传批次（那批数据比本轮旧）：新数据先排队。
                # ⚠️ 顺序语义是"**近似时序**"，不是严格全局 FIFO：只有"本轮开始前就在途的
                #    批次"会让本轮样本排队；被 RESEND_RETRY_INTERVAL 节流挡在队列里、
                #    尚未取批的旧行，以及本轮自己刚触发的批次，都不会让本轮排队。
                #    （改动前同样不满足全局 FIFO：实测 58 条逆序全部是补传行。）
                save_to_cache(payload, reason="补传中")

            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        LOGGER.info("网关已停止（Ctrl+C）")
    except Exception:
        LOGGER.exception("网关异常退出")
    finally:
        RESEND_WORKER.stop()      # 先让补传把未确认的数据落回盘，再断开
        close_modbus(modbus_client)
        try:
            mqtt_client.disconnect()
        except Exception as exc:
            LOGGER.debug("断开 MQTT 时出错（忽略）: %s", exc)
        mqtt_client.loop_stop()


if __name__ == "__main__":
    main()
