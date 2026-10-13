# -*- coding: utf-8 -*-
"""展示层查询结果缓存（Redis）——只缓存"已闭合时间窗"的 TDengine 查询结果。

正确性边界（本模块存在的唯一理由）：

1. 只缓存已闭合、且已确认落库完毕的时间窗。进缓存须同时满足：
   查询上界 <= 库内最新数据时间戳 - 安全边界（60s）；窗口内无新写入。
   不满足就直连 TDengine。当前秒/当前分钟的数据永不进缓存。
   用"数据时间戳"而不是墙钟，是因为容器虚拟时钟相对宿主机有 100ms 漂移，
   而测点时间戳直接来自库内行，同一时钟源无跨时钟误差。

2. 安全边界默认 60s：网关补传慢路径实测滞后 P50 31s / max 32.1s，
   边界必须严格大于补传滞后，否则迟到的补传行会落进"已闭合"窗口。

3. 读侧再自检：命中时若发现缓存条目窗口上界 >= 水位，判脏并回源。
   专门兜住"水位丢失/被清空后缓存键仍存活"。

4. 缓存 report.query_aggregate / report.query_raw 的聚合结果行。
   原始点曲线（query_raw 可能覆盖当前秒）不进缓存。

5. 已知残留风险：超过 60s 才补到的行（EMQX mqueue 20000 条约 27h、网关缓存 64MB）
   会撞穿边界，表现为"窗口已缓存但后来又来了一条老数据"。最坏情况是该窗口数据在 TTL
   （300s）内偏旧，不是永久错。报表是分钟/小时级均值，窗口内少 1 条对均值
   影响远小于 ROUND_DIGITS=2 的分辨力；告警判据是连续窗口越限，不依赖单点。

6. 缓存键带设备维度（TD_PLANT / TD_DEVICE）：同一时间窗对不同设备是不同数据，
   少了这一维第二台设备会命中第一台的条目。设备维度同时进缓存键哈希和 SQL tag
   过滤，取值按 tag 字符集白名单校验（sql_safety.py），非法值导入期拒绝启动。

Redis 不可用时整体退化为空操作：所有查询直连 TDengine，结果与加缓存前一致。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import zlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Final, Optional

# 让 src/common 能被导入：三种启动方式都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# tag 值白名单（与平台接入层共用；只依赖标准库）
from src.common.sql_safety import SQL_NAME_RULE, check_sql_name, is_sql_name   # noqa: E402

# ==================== 1. 配置区 ====================

REDIS_HOST: Final[str] = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT: Final[int] = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: Final[int] = int(os.getenv("REDIS_DB", "0"))
# 连接/读写超时取小值：缓存是加速手段，Redis 抖动不该拖慢对外响应
REDIS_TIMEOUT_SECONDS: Final[float] = float(os.getenv("REDIS_TIMEOUT_SECONDS", "0.20"))

# 改缓存值编码格式时必须 +1，否则会读到旧格式
CACHE_VERSION: Final[str] = "v1"
KEY_PREFIX: Final[str] = f"cems:{CACHE_VERSION}:q:"
STATS_PREFIX: Final[str] = f"cems:{CACHE_VERSION}:cnt:"
KEY_DATA_TS: Final[str] = f"cems:{CACHE_VERSION}:meta:data_ts"
KEY_MINMAX: Final[str] = f"cems:{CACHE_VERSION}:meta:minmax"
KEY_MINMAX_TS: Final[str] = f"cems:{CACHE_VERSION}:meta:minmax_ts"
KEY_EVENTS: Final[str] = f"cems:{CACHE_VERSION}:meta:events"
KEY_LAST_QUERY: Final[str] = f"cems:{CACHE_VERSION}:meta:last_query"

# 分钟最大值表裁剪：只保留最近 N 个分钟。
# 这个值决定"哪些窗口能被缓存"，须 >= 被缓存窗口的最长跨度。
# 默认 3000 分钟 ≈ 50h，覆盖日报表(24h)+最近一小时分钟报表+跨天自由报表。
# 内存量级：3000 条 hash 字段 ≈ 每字段 ~40 B，合计约 120 KB，可忽略。
MINMAX_KEEP: Final[int] = int(os.getenv("CACHE_MINMAX_KEEP", "3000"))

# 总开关：0/false 时整层缓存停用。唯一真源放这里，report.py 通过 cache.CACHE_FLAG 读它。
CACHE_FLAG: Final[bool] = os.getenv("CACHE_ENABLED", "1").strip().lower() not in (
    "0", "false", "no", "off", "",
)
# 查询上界必须比"水位"早这么多秒才允许进缓存
MARGIN_SECONDS: Final[int] = int(os.getenv("CACHE_MARGIN_SECONDS", "60"))
# 缓存条目 TTL：兜底失效（水位丢失/外部改库/超长滞后补传）
TTL_SECONDS: Final[int] = int(os.getenv("CACHE_TTL_SECONDS", "300"))
# 结果集大于这个字节数就不进缓存
MAX_VALUE_BYTES: Final[int] = int(os.getenv("CACHE_MAX_VALUE_BYTES", "1048576"))
STATS_TTL_SECONDS: Final[int] = int(os.getenv("CACHE_STATS_TTL_SECONDS", "86400"))
# 水位键 TTL：水位丢失只会让缓存暂时停用，7 天足够长
WATERMARK_TTL_SECONDS: Final[int] = int(os.getenv("CACHE_WATERMARK_TTL_SECONDS", "604800"))

COUNTERS: Final[tuple[str, ...]] = (
    "hit", "miss", "stale", "dirty", "unverifiable", "query", "bypass_future",
)

# ---- 日志 ----
# 级别/格式/时间格式见 src/common/logging.py，本模块不复制那三个常量。
LOGGER: Final[logging.Logger] = logging.getLogger("web.cache")

# ---- 设备维度（数据归属）----
# 缓存键必须带"这条查询结果属于哪台设备"。取值沿用平台接入层同名变量。
TD_PLANT: Final[str] = os.getenv("TD_PLANT", "plant1").strip()
TD_DEVICE: Final[str] = os.getenv("TD_DEVICE", "device1").strip()


def _device_scope(plant: str, device: str) -> str:
    """(plant, device) -> 缓存命名空间里的设备维度串；空值回落默认值并告警。"""
    if not plant:
        LOGGER.warning("TD_PLANT 为空，设备维度回落默认值 plant1（请检查环境变量）")
        plant = "plant1"
    if not device:
        LOGGER.warning("TD_DEVICE 为空，设备维度回落默认值 device1（请检查环境变量）")
        device = "device1"
    return f"{plant}/{device}"


DEVICE_SCOPE: Final[str] = _device_scope(TD_PLANT, TD_DEVICE)
#: DEVICE_SCOPE 的拆解形式。未校验的原始拆解，仅供诊断；拼 SQL 一律用 device_tag_parts()。
DEVICE_SCOPE_PARTS: Final[tuple[str, str]] = tuple(DEVICE_SCOPE.split("/", 1))  # type: ignore[assignment]


def device_tag_parts() -> tuple[str, str]:
    """返回已通过 tag 白名单校验的 (plant, device)，供拼 SQL tag 过滤的读路径使用。

    三个读路径都把它拼成 AND plant='...' AND device='...'。值来自环境变量，
    不校验就有两条真实后果：SQL 注入，或大小写不符导致命中 0 行（静默全绿）。
    每次调用都重新校验，让"校验"与"拼进 SQL"在同一处。
    """
    plant, device = DEVICE_SCOPE_PARTS
    check_sql_name(plant, what="展示层查询 tag plant", env_var="TD_PLANT")
    check_sql_name(device, what="展示层查询 tag device", env_var="TD_DEVICE")
    return plant, device


# 导入期自检：非法 tag 值拒绝启动。单设备默认值合法，正常部署零行为变化。
device_tag_parts()


def _watermark_suffix(device: str) -> str:
    """水位键的设备后缀：默认设备不加后缀（键名与改造前逐字节相同），别的设备加短哈希。"""
    if device == DEVICE_SCOPE:
        return ""
    return ":" + hashlib.sha1(device.encode("utf-8")).hexdigest()[:12]


def watermark_keys(device: str = DEVICE_SCOPE) -> tuple[str, str, str]:
    """返回 (data_ts 键, 分钟最大值键, 分钟清单键)——三者必须属于同一台设备。

    不要绕开这个函数直接拼 KEY_*：漏加设备维度就会回到"设备 A 的数据把设备 B
    还没写完的窗口判成已闭合"那个失真。
    """
    suffix = _watermark_suffix(device)
    return KEY_DATA_TS + suffix, KEY_MINMAX + suffix, KEY_MINMAX_TS + suffix


def scope_split(scope: str) -> tuple[str, str]:
    """把 scope 串（plant/device）拆成 tag 两段，带白名单校验。

    scope 为空 ⇒ 回落到本实例配置的设备。
    非法值抛 ValueError（不是 SystemExit：脏请求参数不该把 worker 打死），由路由翻成 400。
    """
    if not scope:
        return device_tag_parts()
    plant, _, device = scope.partition("/")
    if not is_sql_name(plant) or not is_sql_name(device):
        raise ValueError(f"设备维度 {scope!r} 不合法：{SQL_NAME_RULE}")
    return plant, device


def scope_from_request(plant: Optional[str], device: Optional[str]) -> str:
    """请求参数 (plant, device) -> scope 串；缺省/只给一半都按本实例配置补齐。

    前端只需传 device 也能工作。非法值抛 ValueError。
    """
    want_plant = (plant or "").strip()
    want_device = (device or "").strip()
    if not want_plant and not want_device:
        return DEVICE_SCOPE
    for name, value in (("plant", want_plant), ("device", want_device)):
        if value and not is_sql_name(value):
            raise ValueError(f"{name} 参数不合法：{value!r}；{SQL_NAME_RULE}")
    return f"{want_plant or TD_PLANT}/{want_device or TD_DEVICE}"


# ==================== 2. 客户端（懒连接 + 全链路降级） ====================

class CacheClient:
    """Redis 薄封装：连接失败一律退化为"无缓存"，不把异常抛给查询路径。"""

    def __init__(self) -> None:
        self.enabled: bool = False
        self.last_error: str = ""
        self._client: Any = None
        self._client_module: Any = None

    # ---- 连接管理 ----

    def _connect(self) -> Any:
        """尝试（重新）建立连接；失败返回 None 并记录原因，不抛异常。"""
        if self._client_module is None:
            try:
                import redis as redis_module          # 延迟导入：没装依赖时也能起服务
            except Exception as exc:                  # pragma: no cover - 依赖缺失分支
                self.last_error = f"未安装 redis 依赖: {exc}"
                return None
            self._client_module = redis_module
        try:
            client = self._client_module.Redis(
                host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                socket_connect_timeout=REDIS_TIMEOUT_SECONDS,
                socket_timeout=REDIS_TIMEOUT_SECONDS,
                decode_responses=True,
                client_name="cems-web-cache",
            )
            client.ping()
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            return None
        self.last_error = ""
        LOGGER.info("Redis 缓存已连接: %s:%d/%d", REDIS_HOST, REDIS_PORT, REDIS_DB)
        return client

    def init(self) -> None:
        """启动时显式初始化一次，并跑一遍命令自检。

        连不上只是打日志，不影响 Web 服务启动；命令自检失败也要告警——
        那意味着"缓存看着开着、其实一次都不会命中"。
        """
        self._client = self._connect()
        self.enabled = self._client is not None
        if not self.enabled:
            LOGGER.warning("Redis 不可用，缓存已停用（查询直连 TDengine）: %s", self.last_error)
            return
        failures = self.preflight(f"{KEY_PREFIX}selftest")
        if failures:
            LOGGER.warning(
                "Redis 命令自检未全部通过，缓存可能静默失效（请按下面清单核对 redis-py 版本）: %s",
                failures,
            )
        else:
            LOGGER.info("Redis 命令自检通过（set/get/incr/hset/hgetall/scan/delete + 分钟最大值读写）")

    def _handle_failure(self, exc: Exception) -> None:
        """一次操作失败即认为连接已坏：置为不可用，下一次调用会自动重连。"""
        self.last_error = f"{type(exc).__name__}: {exc}"
        self._client = None
        self.enabled = False
        LOGGER.warning("Redis 操作失败，缓存临时停用（下次请求自动重连）: %s", self.last_error)

    def _cmd(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """执行一条 Redis 命令；未连接时先重连一次。成功返回结果，失败返回 None。"""
        if not self.enabled:
            return None
        client = self._client
        if client is None:
            client = self._connect()
            if client is None:
                return None
            self._client = client
        try:
            return getattr(client, name)(*args, **kwargs)
        except Exception as exc:
            self._handle_failure(exc)
            return None

    # ---- 基本读写 ----

    def get_str(self, key: str) -> Optional[str]:
        value = self._cmd("get", key)
        return value if isinstance(value, str) else None

    def set_str(self, key: str, value: str, ttl: int) -> bool:
        return self._cmd("set", key, value, ex=ttl) is not None

    def incr(self, key: str, ttl: int = STATS_TTL_SECONDS) -> None:
        if self._cmd("incr", key) == 1:          # 刚创建时补一次 TTL
            self._cmd("expire", key, ttl)

    def purge_queries(self) -> int:
        """删除全部查询缓存条目（返回删除条数）。水位键保留。"""
        deleted = 0
        cursor = 0
        while True:
            reply = self._cmd("scan", cursor=cursor, match=f"{KEY_PREFIX}*", count=500)
            if not reply:
                break
            cursor, keys = int(reply[0]), list(reply[1])
            if keys:
                removed = self._cmd("delete", *keys)
                deleted += int(removed or 0)
            if cursor == 0:
                break
        return deleted

    def note_event(self, reason: str) -> None:
        """记录一次失效事件（原因 + 计数器），供文档/演练取证。"""
        self.incr(f"{KEY_EVENTS}:{reason}")

    def preflight(self, scratch: str) -> list[str]:
        """启动自检：把本模块用到的 Redis 命令各跑一遍，返回失败清单。

        容错会把"命令用错"也伪装成"Redis 挂了"，后果隐蔽：接口正常、数据新鲜、
        只是缓存从未命中。自检把这类错误在启动时就暴露成 WARN。
        这里不走 _cmd：探针失败不该触发 _handle_failure 删掉连接。

        探针集合里的每条命令都必须与生产代码逐参数同形：只探"命令存在"是没用的 ——
        本机踩到的正是"命令存在、封装返回静默空"，以及"命令本身可用、但参数形状在
        某个 redis-py 版本上不被接受"。
        """
        client = self._client
        if client is None:
            return ["未连接"]
        def probe_minute_max() -> Any:
            """与 note_minute_max / observed_window_max 同形：写两个分钟再读回来。"""
            client.delete(f"{scratch}:minmax")
            client.hset(f"{scratch}:minmax", mapping={
                "2026-01-01 00:00:00": "2026-01-01 00:00:00",
                "2026-01-01 00:01:00": "2026-01-01 00:01:05",
            })
            got = client.hgetall(f"{scratch}:minmax")
            if got.get("2026-01-01 00:01:00") != "2026-01-01 00:01:05":
                raise AssertionError(f"分钟最大值读回 {got!r}，与写入不一致")
            if client.hlen(f"{scratch}:minmax") != 2:
                raise AssertionError("分钟最大值条目数不是 2")
            client.delete(f"{scratch}:minmax")
            return got

        probes: list[tuple[str, Callable[[], Any]]] = [
            ("ping", lambda: client.ping()),
            ("set/get", lambda: (client.set(scratch, "1", ex=30), client.get(scratch))[1]),
            ("incr", lambda: client.incr(f"{scratch}:n")),
            ("hset/hgetall/hlen/hdel", probe_minute_max),
            ("scan", lambda: client.scan(cursor=0, match=f"{scratch}*", count=10)),
            ("delete", lambda: client.delete(scratch, f"{scratch}:n", f"{scratch}:z")),
        ]
        failures: list[str] = []
        for name, probe in probes:
            try:
                probe()
            except Exception as exc:
                failures.append(f"{name}: {type(exc).__name__}: {exc}")
        return failures

    # ---- 类型化读取 ----

    def get_int(self, key: str) -> Optional[int]:
        raw = self.get_str(key)
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    def get_float(self, key: str) -> Optional[float]:
        raw = self.get_str(key)
        if raw is None:
            return None
        try:
            return float(raw)
        except ValueError:
            return None


CLIENT: Final[CacheClient] = CacheClient()


# ==================== 3. 时间戳解析与编码 ====================
# 统一按"去掉时区标记的 19 位本地时间串"处理：
# 与 report.format_ts() 输出口径一致；零填充意味着字典序 == 时间序。

TS_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"


def ts_text(value: Any) -> str:
    """任意 TDengine 时间戳值 -> 'YYYY-MM-DD HH:MM:SS'（19 字符）。"""
    if isinstance(value, datetime):
        return value.strftime(TS_FORMAT)
    return str(value)[:19].replace("T", " ")


def parse_ts_text(text: str) -> Optional[datetime]:
    """'YYYY-MM-DD HH:MM:SS' -> naive datetime；解析不了返回 None（当没有水位处理）。"""
    for fmt in (TS_FORMAT, "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    return None


def build_key(
    kind: str,
    unit: str,
    start_text: str,
    end_text: str,
    device: str = DEVICE_SCOPE,
) -> str:
    """查询缓存键：设备维度 + kind/unit/起止时间 全部进键。

    起止时间用已对齐到窗口边界的值，同一分钟内重复请求键稳定，跨分钟自然换键。
    设备维度放哈希材料最前面（数据归属维度），少了它第二台设备会命中第一台的条目。
    """
    raw = f"{device}|{kind}|{unit}|{start_text}|{end_text}"
    return KEY_PREFIX + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def _source_key(cache_key: str) -> str:
    """缓存键 -> 可读的来源描述（诊断用）。"""
    return cache_key[len(KEY_PREFIX):] if cache_key.startswith(KEY_PREFIX) else cache_key


# ==================== 4. 窗口闭合判据 ====================

def _closure(end: datetime, device: str = DEVICE_SCOPE) -> tuple[bool, str, str]:
    """判断查询上界是否已"闭合"。返回 (是否闭合, 判据, 水位文本)。

    判据用"最新数据时间戳"而不是容器墙钟：时间戳直接来自库内行，与产生它的
    时钟同源，无跨时钟误差。
    """
    data_ts_key, _minmax_key, _minmax_ts_key = watermark_keys(device)
    data_text = CLIENT.get_str(data_ts_key)
    if not data_text:
        return False, "无 data_ts 水位（还没有观测到库内数据）", ""
    data_ts = parse_ts_text(data_text)
    if data_ts is None:
        return False, f"data_ts 水位无法解析: {data_text!r}", data_text
    if end > data_ts - timedelta(seconds=MARGIN_SECONDS):
        return False, "窗口未闭合（上界未早于 data_ts-边界）", data_text
    return True, "已闭合", data_text


def _ready(where: str) -> bool:
    """缓存是否真的可用（开关开着 且 Redis 连得上）；不可用时打一次日志。"""
    if not CACHE_FLAG:
        return False
    if CLIENT.enabled:
        return True
    LOGGER.info("[%s] 缓存不可用，直连 TDengine: %s", where, CLIENT.last_error)
    return False


# ==================== 5. 编码/解码 ====================

def encode_rows(rows: list[tuple[Any, ...]], observed: dict[str, Any]) -> Optional[str]:
    """结果行 + 当时观测到的窗口指纹 -> JSON 字符串（zlib 压缩后 base64）。

    压缩：分钟报表 60 行 × 11 列约 12KB，压缩后约 1.5KB。解压失败当脏数据处理。
    observed 必须和结果行一起存：它是"这条缓存用哪一版数据算出来的"的证据。
    """
    import base64
    payload = {
        "rows": [[_encode_cell(cell) for cell in row] for row in rows],
        "observed_state": observed,
    }
    raw = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    if len(raw) > MAX_VALUE_BYTES:
        return None
    return base64.b64encode(zlib.compress(raw, 6)).decode("ascii")


def decode_rows(blob: str) -> Optional[tuple[list[tuple[Any, ...]], Any]]:
    """JSON 字符串 -> (结果行, 当时的观测状态)；任何异常都返回 None（调用方回源）。"""
    import base64
    try:
        payload = json.loads(zlib.decompress(base64.b64decode(blob)).decode("utf-8"))
    except Exception as exc:
        LOGGER.warning("缓存值解码失败，按未命中处理: %s", exc)
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
        return None
    return [tuple(row) for row in payload["rows"]], payload.get("observed_state")


def _encode_cell(cell: Any) -> Any:
    if isinstance(cell, datetime):
        return cell.strftime(TS_FORMAT)
    if isinstance(cell, (str, int, float, bool)) or cell is None:
        return cell
    return str(cell)


# ==================== 6. 水位更新（由无缓存的查询顺带完成） ====================
# 两个水位都由 /api/data 顺带喂：它按定义不缓存、每次都真读库，
# 看到的行就是"库里此刻真实有什么"的一手证据。
#   data_ts —— 最新数据时间戳：窗口闭合判据
#   minmax  —— 分钟 -> 该分钟观测到的最大时间戳：写后失效判据
#
# 为什么是"分钟最大值"这个形状（三个失败版本换来的）：
#   1) 用最新数据时间戳当失效水位：失效永不触发（它永远比历史窗口新）。
#   2) 用可缓存区间的最大时间戳：哨兵行的时间戳比它旧，永远不触发。
#   3) 用逐行时间戳集合：能触发但误杀——/api/data 每次都重新看到最近 10 分钟全部行，
#      把它们全登记成新写入，最近 10 分钟窗口每次都判脏，缓存命中率掉到 0。
#   最终：按分钟归档，只记该分钟最晚那条。只有该分钟真的多了更晚的数据才判脏。
#
# 水位按设备分开：2026-10-03 起 web 可切设备（V3 双设备），全局一份水位的前提消失。
# 不改的话：设备 A 的新数据会把设备 B 还没写完的窗口判成已闭合，B 的缓存返回偏旧数据。
# 默认设备键名不加后缀，单设备部署键与行为逐字节不变。
KEY_MINMAX_TTL_SECONDS: Final[int] = int(os.getenv("CACHE_MINMAX_TTL_SECONDS", "86400"))


def note_data_ts(latest: str, device: str = DEVICE_SCOPE) -> None:
    """记录"观测到的最新数据时间戳"。只前进不后退。按设备各记一份。"""
    latest = (latest or "")[:19]
    if len(latest) < 19 or not _ready("note_data_ts"):
        return
    data_ts_key, _minmax_key, _minmax_ts_key = watermark_keys(device)
    current = CLIENT.get_str(data_ts_key) or ""
    if latest > current:
        CLIENT.set_str(data_ts_key, latest, WATERMARK_TTL_SECONDS)


def note_minute_max(observed: list[str], device: str = DEVICE_SCOPE) -> int:
    """登记"每个分钟里观测到的最大时间戳"。返回被更新的分钟数。

    按设备各记一份：两台设备共用一份分钟最大值会搅乱彼此的窗口指纹。
    """
    unique = sorted({text[:19] for text in observed if text and len(text) >= 19})
    if not unique or not _ready("note_minute_max"):
        return 0
    _data_ts_key, minmax_key, minmax_ts_key = watermark_keys(device)
    current = CLIENT._cmd("hgetall", minmax_key) or {}
    updates: dict[str, str] = {}
    for text in unique:
        minute = text[:16] + ":00"
        best = max([text] + ([current[minute]] if minute in current else []))
        if current.get(minute) != best:
            updates[minute] = best
    if not updates:
        return 0
    CLIENT._cmd("hset", minmax_key, mapping=updates)
    CLIENT._cmd("hset", minmax_ts_key, mapping={minute: minute for minute in updates})
    CLIENT._cmd("expire", minmax_key, MINMAX_KEEP * 900)
    CLIENT._cmd("expire", minmax_ts_key, MINMAX_KEEP * 900)
    _trim_minute_max(device)
    return len(updates)


def _trim_minute_max(device: str = DEVICE_SCOPE) -> None:
    """只保留最近 MINMAX_KEEP 个分钟（按分钟文本排序，等价于按时间排序）。"""
    _data_ts_key, minmax_key, minmax_ts_key = watermark_keys(device)
    count = CLIENT._cmd("hlen", minmax_key)
    if count is None or int(count) <= MINMAX_KEEP:
        return
    minutes = CLIENT._cmd("hkeys", minmax_key) or []
    for minute in sorted(minutes)[: int(count) - MINMAX_KEEP]:
        CLIENT._cmd("hdel", minmax_key, minute)
        CLIENT._cmd("hdel", minmax_ts_key, minute)


def observed_window_max(
    range_start_key: str, range_end_key: str, device: str = DEVICE_SCOPE,
) -> Optional[str]:
    """窗口 [range_start, range_end) 内"观测到的最大数据时间戳"（诊断用）。"""
    detail = observed_window_detail(range_start_key, range_end_key, device)
    if not detail:
        return None
    return max(detail.values())


def observed_window_detail(
    range_start_key: str, range_end_key: str, device: str = DEVICE_SCOPE,
) -> dict[str, str]:
    """窗口内逐分钟观测到的最大时间戳：{分钟 -> 该分钟最大 ts}。

    失效判据必须用逐分钟明细而不是整个窗口的最大值：一个 1 小时窗口 60 个分钟，
    若新数据落在较早的分钟里而较晚分钟本来就有更大值，"窗口最大值"根本不变，判据失灵。
    改成逐分钟明细比对后，任意一个分钟变动都能被发现。
    """
    _data_ts_key, minmax_key, _minmax_ts_key = watermark_keys(device)
    all_max = CLIENT._cmd("hgetall", minmax_key)
    if not all_max:
        return {}
    return {
        minute: value for minute, value in all_max.items()
        if range_start_key <= minute < range_end_key
    }


def signature(
    range_start_key: str, range_end_key: str, device: str = DEVICE_SCOPE,
) -> tuple[str, int]:
    """窗口的"内容指纹"：窗口内被观测到的那些分钟的 (分钟, 最大值) 序列。

    返回 (指纹, 观测到的分钟数)。未观测到的分钟不参与——它们"确定没数据"。
    观测表只覆盖最近 50h，对一小时前的窗口只有最近 10 分钟被观测。
    一开始想把"未观测"也逐个分钟哈希进指纹，后果是：30 天曲线窗口有 43200 个分钟，
    每次请求都要循环 43200 次 → 实测 p50 从 16ms 涨到 63ms（比不加缓存还慢）。
    只哈希观测到的分钟，代价与"窗口内被观测的分钟数"成正比，判据仍成立：
    迟到写入把未观测分钟变成已观测 → 失效；暴露更大值 → 失效；纯稳态 → 命中。
    """
    detail = observed_window_detail(range_start_key, range_end_key, device)
    digest = hashlib.sha1()
    for minute in sorted(detail):
        digest.update(minute.encode("utf-8"))
        digest.update(b"=")
        digest.update(str(detail[minute]).encode("utf-8"))
        digest.update(b"|")
    return digest.hexdigest()[:16], len(detail)


def dirty_count(device: str = DEVICE_SCOPE) -> int:
    _data_ts_key, minmax_key, _minmax_ts_key = watermark_keys(device)
    value = CLIENT._cmd("hlen", minmax_key)
    return int(value or 0)


# ==================== 7. 缓存查询主流程 ====================

def prepare(
    kind: str,
    unit: str,
    start: datetime,
    end: datetime,
    device: str = DEVICE_SCOPE,
) -> dict[str, Any]:
    """为一个查询准备缓存上下文（设备维度 + 缓存键 + 是否允许缓存）。

    在这里单独算出 range_start_key / range_end_key（19 字符串），
    后面存值和查值都用同一对键，避免口径漂移。
    device 随 context 全程带着走，已烘焙进 key。
    """
    start_text = start.strftime(TS_FORMAT)
    end_text = end.strftime(TS_FORMAT)
    return {
        "kind": kind,
        "unit": unit,
        "device": device,
        "key": build_key(kind, unit, start_text, end_text, device),
        "range_start_key": start_text,
        "range_end_key": end_text,
    }


def query_cached(
    context: dict[str, Any],
    where: str,
    fetch: Callable[[], list[tuple[Any, ...]]],
) -> list[tuple[Any, ...]]:
    """带缓存的取数：命中返回缓存行，未命中/不可用则调用 fetch() 并写缓存。

    命中判定三道（任何一道不过就回源）：
      ① 写后失效：库里本窗口观测到的最大值和缓存存的那一版不一致 → 内容变过；
      ② 读侧自检：条目的窗口上界已追平/越过 data_ts 水位 → 条目不可信；
      ③ 未闭合：窗口本身还没写完（含当前秒）→ 根本不缓存。

    设备维度在两处：context["key"]（条目命名空间）与 context["device"]（水位命名空间，
    见 watermark_keys）。所以这三道判定判的都是"这台设备"的这个窗口，既不会命中到
    别的设备的条目，也不会被别的设备的数据误判成"已闭合"。
    """
    if not _ready(where):
        return fetch()

    key = context["key"]
    device = context.get("device") or DEVICE_SCOPE
    blob = CLIENT.get_str(key)
    if blob is not None:
        decoded = decode_rows(blob)
        current_sig, observed = signature(
            context["range_start_key"], context["range_end_key"], device)
        data_ts_key, _minmax_key, _minmax_ts_key = watermark_keys(device)
        data_text = CLIENT.get_str(data_ts_key) or ""
        if decoded is None:
            CLIENT._cmd("delete", key)                      # 值坏了：清掉，走回源
        else:
            rows, stored = decoded
            stored_sig = stored.get("sig") if isinstance(stored, dict) else None
            if stored_sig is None:
                # 缓存值里没有新鲜度标记——无法验证，一律当未命中
                CLIENT.incr(f"{STATS_PREFIX}stale")
                CLIENT.note_event("stale_unverifiable_entry")
                CLIENT._cmd("delete", key)
                LOGGER.info("[%s] 缓存条目缺少新鲜度标记，作废并回源", where)
            elif stored_sig != current_sig:
                # ① 窗口内容变过
                CLIENT.incr(f"{STATS_PREFIX}dirty")
                CLIENT.note_event("dirty_window_changed")
                CLIENT._cmd("delete", key)
                LOGGER.info("[%s] 窗口 %s ~ %s 内容已变（指纹 %s -> %s），缓存作废并回源",
                            where, context["range_start_key"], context["range_end_key"],
                            stored_sig, current_sig)
            elif data_text and context["range_end_key"] >= data_text:
                # ② 水位已经进到这个窗口里
                CLIENT.incr(f"{STATS_PREFIX}stale")
                CLIENT.note_event("stale_end_ge_watermark")
                CLIENT._cmd("delete", key)
                LOGGER.info("[%s] 缓存条目已过期（窗口上界 %s >= 水位 %s），回源",
                            where, context["range_end_key"], data_text)
            else:
                CLIENT.incr(f"{STATS_PREFIX}hit")
                return rows

    closed, reason, data_text = _closure(
        parse_ts_text(context["range_end_key"]) or datetime.min, device)
    if not closed:
        # 未闭合：直连，不读写缓存（不缓存当前秒数据的落地点）
        CLIENT.incr(f"{STATS_PREFIX}bypass_future")
        CLIENT.incr(f"{STATS_PREFIX}query")
        LOGGER.debug("[%s] 不进缓存（%s），直连 TDengine", where, reason)
        return fetch()

    CLIENT.incr(f"{STATS_PREFIX}miss")
    CLIENT.incr(f"{STATS_PREFIX}query")
    rows = fetch()
    # 存值前记下这一版结果对应哪一份观测指纹——命中判定 ① 要用它
    current_sig, observed = signature(
        context["range_start_key"], context["range_end_key"], device)
    encoded = encode_rows(rows, {"sig": current_sig, "observed_minutes": observed})
    if encoded is None:
        LOGGER.info("[%s] 结果集超过 %d 字节，跳过缓存", where, MAX_VALUE_BYTES)
        return rows
    CLIENT.set_str(key, encoded, TTL_SECONDS)
    # 诊断串带上设备维度：多设备部署时能看出最近缓存的是哪台设备的哪个窗口
    CLIENT.set_str(
        KEY_LAST_QUERY,
        f"{where}|{context['device']}|{context['range_start_key']}|{context['range_end_key']}",
        86400,
    )
    return rows


# ==================== 8. 统计与运维接口 ====================

def stats() -> dict[str, Any]:
    """缓存统计快照（供 /api/cache/stats 与测量脚本取证）。

    统计不按设备拆：各 web 实例共用同一个 Redis DB，命中/未命中是共享缓存的事实。
    响应里用 device_scope 说明这些计数覆盖哪台设备。
    """
    counters = {name: (CLIENT.get_int(f"{STATS_PREFIX}{name}") or 0) for name in COUNTERS}
    hit, miss, stale = counters["hit"], counters["miss"], counters["stale"]
    lookups = hit + miss + stale
    keys = 0
    if CLIENT.enabled:
        cursor = 0
        while True:
            reply = CLIENT._cmd("scan", cursor=cursor, match=f"{KEY_PREFIX}*", count=500)
            if not reply:
                break
            cursor = int(reply[0])
            keys += len(reply[1])
            if cursor == 0:
                break
    return {
        # enabled = 运营开关 + 连接状态的合成结果；两个分项一并给出便于排障
        "enabled": CACHE_FLAG and CLIENT.enabled,
        "cache_flag": CACHE_FLAG,
        "redis_connected": CLIENT.enabled,
        "last_error": CLIENT.last_error,
        "redis": f"{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
        "device_scope": DEVICE_SCOPE,
        "config": {
            "margin_seconds": MARGIN_SECONDS,
            "ttl_seconds": TTL_SECONDS,
            "max_value_bytes": MAX_VALUE_BYTES,
            "version": CACHE_VERSION,
            "device_scope": DEVICE_SCOPE,
        },
        "counters": counters,
        "lookups": lookups,
        "hit_rate": (hit / lookups) if lookups else None,
        # 命中率分母只算"允许进缓存的查询"（bypass_future 不计入）
        "hit_rate_note": "hit/(hit+miss+stale)；bypass_future（窗口未闭合、按设计直连）不计入分母",
        "cache_keys": keys,
        "minute_max_entries": dirty_count(),
        "watermarks": {
            # 水位已按设备隔离，这里报本实例配置的那台设备的水位
            "device_scope": DEVICE_SCOPE,
            "data_ts": CLIENT.get_str(watermark_keys(DEVICE_SCOPE)[0]),
            "last_query": CLIENT.get_str(KEY_LAST_QUERY),
        },
        "events": {
            reason: (CLIENT.get_int(f"{KEY_EVENTS}:{reason}") or 0)
            for reason in (
                "stale_end_ge_watermark",
                "stale_unverifiable_entry",
                "dirty_window_changed",
            )
        },
    }


def clear() -> dict[str, Any]:
    """手动清空全部查询缓存（演练与排障用）。返回删除条数。

    清理不按设备拆：语义是把缓存层清干净，缓存可随时重建。
    只清一半会留下别的设备的条目，制造"清了但还在"的假象。
    """
    deleted = CLIENT.purge_queries()
    return {"deleted": deleted, "enabled": CLIENT.enabled}


def source_of(cache_key: str) -> str:
    """缓存键的可读来源（外部诊断用）。"""
    return _source_key(cache_key)


# 设备维度打一条日志：多设备部署下"这台 web 给哪台设备做缓存"必须一眼可见。
# 它必须与平台接入层的 TD_PLANT/TD_DEVICE 一致：读路径已经按这两个值过滤
# （report.py / web_dashboard.py 用 device_tag_parts() 拼 tag 条件），
# 所以这里写错就不只是"缓存命名空间对不上"，而是读的是别人的数据。
# 本行与 CLIENT.init() 都在导入时执行，那时日志系统可能还没配置，
# 所以 web_dashboard.main() 在 setup_logging() 之后会再打一条同样的。
LOGGER.info("查询缓存设备维度: %s（缓存键按设备隔离，键格式见 build_key）", DEVICE_SCOPE)
CLIENT.init()
