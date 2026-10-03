# -*- coding: utf-8 -*-
"""展示层查询结果缓存（Redis）——只缓存"已闭合时间窗"的 TDengine 查询结果。

================================ 正确性边界 ================================
这是本模块存在的唯一理由：**缓存命中时返回的是旧数据，必须证明它不会让报表/告警判错。**

1. **只缓存已经闭合、且已经确认落库完毕的时间窗。**
   一条查询要进缓存必须同时满足三个条件（见 `_closure()`）：
     a. 查询上界 <= (最近一次观测到的**库内数据时间戳** − 安全边界)；
     b. 查询上界 <= (最近一次观测到的**库内数据写入时间戳** − 安全边界)；
     c. 窗口内没有发生过新的写入。
   不满足就**直连 TDengine**，不进缓存、也不读缓存。**当前秒/当前分钟的数据永远不进缓存。**

   a 用"数据时间戳"而不是"墙钟"是有意的：容器虚拟时钟相对宿主机有 100 ms 级漂移
   （见 `docs/reference/容器时钟漂移.md`），拿墙钟判"窗口关没关"会算错；而 12 个测点的
   时间戳直接来自库内行，是**同一个时钟源、无跨时钟误差**的判据。

2. **安全边界（默认 60 s）的取值依据**：网关补传慢路径实测滞后 P50 31 s / max 32.1 s
   （见 `docs/reference/性能与可靠性指标.md` §4.2、`docs/adr/0003-补传入队判定修正.md`）。
   边界必须严格大于补传滞后，否则一条迟到的补传行会落进"已闭合"的窗口里。
   实测最大值 32.1 s → 取 60 s，余量 1.9 倍。

3. **读侧再自检一次（`stale` 计数）**：命中时若发现"缓存条目的窗口上界 >= 水位"，
   判为脏并回源。这条与 TTL 无关，专门兜住"水位丢失/被清空后缓存键仍存活"的漏洞。

4. **什么会被缓存**：`report.query_aggregate` / `report.query_raw` 的结果行。
   即分钟/日报表、自由报表、Excel 导出、自由区间曲线（聚合粒度）共用同一个出口。
   **原始点曲线（`query_raw`）不进缓存** —— 它可能覆盖"当前秒"，属当前数据。

5. **已知残留风险（不掩盖）**：
   - 超过 60 s 才补到的行（EMQX mqueue 20000 条约 27 h、网关缓存 64 MB）会撞穿安全边界，
     表现为"窗口已缓存但后来又来了一条老数据"。此时按 a/b 两个条件都判为"已闭合"，
     缓存不会自动失效，要等 TTL 到期。这是**有界的**：最坏情况是该窗口的数据在
     `CACHE_TTL_SECONDS`（默认 300 s）内偏旧，不是永久错。
   - 未覆盖"外部直接改库"（例如人工 UPDATE 一条历史行）：a/b 都不会动，要靠 TTL。

   对报表/告警的影响面：报表是分钟/小时级均值，窗口内少 1 条（1/12 到 1/60 的权重）
   对均值的影响远小于 ROUND_DIGITS=2 的分辨力；告警判据是连续窗口越限
   （见 `docs/adr/0002-告警判据选型.md`），不依赖单点。因此上述残留风险不会把"达标"判成"超标"。

6. **缓存键带设备维度**（`TD_PLANT` / `TD_DEVICE`，见配置区与 `build_key`）：
   同一个时间窗对不同设备是**不同的数据**，少了这一维，第二台设备查同一时间窗就会命中
   第一台写下的条目、把别人的数据当成自己的返回。所以设备维度是键的一部分。

   ★ **键格式因此变了**：改造前写入的条目（哈希材料里没有设备维度）在新代码下**永远不会再被命中**，
     它们不需要任何人清理，会按 `TTL_SECONDS`（默认 300 s）自行过期。
     这里**不做双写、也不做旧键兼容读取** —— 双写会让同一份数据同时落在两个键下，
     而"一个窗口只有一条缓存、命中判定（写后失效）比对的就是那一条"是这套判据能成立的前提
     （见 `query_cached` 的三道命中判定）。为一次可接受的缓存失效引入双写，是本末倒置。

   ⚠️ 设备维度**同时**进缓存键哈希与 SQL 的 tag 过滤（读路径见 §6 末尾列出的三处），
      所以取值必须按 tag 字符集做白名单校验：规则与平台接入层共用
      `src/common/sql_safety.py` 的 `SQL_NAME_RE`，非法值在**导入期**直接拒绝启动
      （`device_tag_parts()`，见 §1 设备维度配置区）。
      过程记录：早先"只进哈希、不进 SQL"的前提已经不成立 —— 读路径确实拿它做了过滤，
      而当时没有校验，`TD_DEVICE="Device1"` 这种大小写不符会让三个读路径全部命中 0 行、
      接口仍 200、容器仍 healthy，没有任何报错（静默全绿）。
============================================================================

Redis 不可用时本模块**整体退化为空操作**：所有查询直连 TDengine，结果与加缓存前逐字节一致。
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

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# tag 值白名单（唯一真源，与平台接入层共用；只依赖标准库，不拖 paho 之类的重依赖进 Web）
from src.common.sql_safety import check_sql_name   # noqa: E402

# ==================== 1. 配置区 ====================

REDIS_HOST: Final[str] = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT: Final[int] = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: Final[int] = int(os.getenv("REDIS_DB", "0"))
# 连接/读写超时都取小值：缓存是**加速手段**，任何一次 Redis 抖动都不该拖慢对外响应
REDIS_TIMEOUT_SECONDS: Final[float] = float(os.getenv("REDIS_TIMEOUT_SECONDS", "0.20"))

# 缓存版本号：改缓存值编码格式时必须 +1，否则会读到旧格式的值
CACHE_VERSION: Final[str] = "v1"
# 键前缀；`*` 便于 redis-cli 查看与 /api/cache/clear 批量删除
KEY_PREFIX: Final[str] = f"cems:{CACHE_VERSION}:q:"
# 计数键（命中/未命中/脏命中/回源次数）
STATS_PREFIX: Final[str] = f"cems:{CACHE_VERSION}:cnt:"
# 水位键
KEY_DATA_TS: Final[str] = f"cems:{CACHE_VERSION}:meta:data_ts"       # 最新数据时间戳（窗口闭合判据）
KEY_MINMAX: Final[str] = f"cems:{CACHE_VERSION}:meta:minmax"         # 分钟 -> 该分钟观测到的最大时间戳
KEY_MINMAX_TS: Final[str] = f"cems:{CACHE_VERSION}:meta:minmax_ts"   # 分钟 -> 该分钟的时间戳（用于排序/裁剪）
KEY_EVENTS: Final[str] = f"cems:{CACHE_VERSION}:meta:events"         # 失效原因记账
KEY_LAST_QUERY: Final[str] = f"cems:{CACHE_VERSION}:meta:last_query"  # 最近一次查询的窗口（诊断用）

# 分钟最大值表的裁剪：只保留最近 N 个分钟。
#
# ★ 这个值直接决定"哪些窗口能被缓存"，取值要按**被缓存窗口的最长跨度**来定：
#   失效判据要求"窗口内每一个分钟都在观测表里"，所以观测表覆盖时长必须 >= 窗口跨度。
#   默认 3000 分钟 ≈ 50 h：覆盖"日报表（24 h）+ 最近一小时分钟报表 + 跨天自由报表"。
#   比它更早的窗口（例如 30 天曲线、月报表）观察不到 → 按设计**不写缓存**、直连查库，
#   不会返回无法验证的旧数据（见 query_cached 里的 unverifiable 分支）。
#   内存量级：3000 条 hash 字段 ≈ 每字段 ~40 B，合计约 120 KB，可忽略。
MINMAX_KEEP: Final[int] = int(os.getenv("CACHE_MINMAX_KEEP", "3000"))

# 总开关：0/false 时**整层缓存停用**（查询全部直连 TDengine，行为与加缓存前一致）。
# 唯一真源放在这里：report.py 通过 cache.CACHE_FLAG 读它，避免两处各解析一遍环境变量、
# 出现"统计说开着、查询路径却关着"这种自相矛盾的状态。
CACHE_FLAG: Final[bool] = os.getenv("CACHE_ENABLED", "1").strip().lower() not in (
    "0", "false", "no", "off", "",
)
# 安全边界：查询上界必须比"水位"早这么多秒才允许进缓存
MARGIN_SECONDS: Final[int] = int(os.getenv("CACHE_MARGIN_SECONDS", "60"))
# 缓存条目 TTL：兜底失效（水位丢失/外部改库/超长滞后补传）
TTL_SECONDS: Final[int] = int(os.getenv("CACHE_TTL_SECONDS", "300"))
# 结果集大于这个字节数就不进缓存（避免一条大查询把 Redis 撑满）
MAX_VALUE_BYTES: Final[int] = int(os.getenv("CACHE_MAX_VALUE_BYTES", "1048576"))
# 计数键 TTL：够长即可，只是为了让统计不至于永久累积
STATS_TTL_SECONDS: Final[int] = int(os.getenv("CACHE_STATS_TTL_SECONDS", "86400"))
# 水位键 TTL：水位丢失只会让缓存暂时停用（不会返回旧数据），7 天足够长
WATERMARK_TTL_SECONDS: Final[int] = int(os.getenv("CACHE_WATERMARK_TTL_SECONDS", "604800"))

COUNTERS: Final[tuple[str, ...]] = (
    "hit", "miss", "stale", "dirty", "unverifiable", "query", "bypass_future",
)

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("web.cache")

# ---- 设备维度（数据归属）----
# 缓存键必须带"这条查询结果属于哪台设备"：同一个时间窗对不同设备是不同的数据。
# 取值沿用平台接入层已经落地的同名变量与默认值（src/platform/subscriber_to_td.py:79-80），
# 单设备部署下就是 plant1 / device1，与改造前逐字节一致（键内容变了，但服务的行为口径没变）。
#
# ★ 唯一真源在这里：report.py 通过 `cache.DEVICE_SCOPE` 引用，不再各自解析一遍环境变量 ——
#   两处各解析一次，迟早会出现"查询按 A 设备、缓存键按 B 设备"这种自相矛盾的状态
#   （与 CACHE_FLAG 同一个理由）。
TD_PLANT: Final[str] = os.getenv("TD_PLANT", "plant1").strip()
TD_DEVICE: Final[str] = os.getenv("TD_DEVICE", "device1").strip()


def _device_scope(plant: str, device: str) -> str:
    """(plant, device) -> 缓存命名空间里的设备维度串；空值回落默认值并告警。

    回落而不是"用空串"：`TD_PLANT` 为空会得到 `/device1` 这种看起来像路径、实际少了一半
    信息的串，两个都不设时更是所有设备挤进同一个命名空间 —— 那正是本模块要修掉的隐患。
    这种情况必须能从启动日志里看出来（级别取 WARNING：这是配置回落（有默认值兜底），不是错误，但必须能从启动日志看出来）。
    """
    if not plant:
        LOGGER.warning("TD_PLANT 为空，设备维度回落默认值 plant1（请检查环境变量）")
        plant = "plant1"
    if not device:
        LOGGER.warning("TD_DEVICE 为空，设备维度回落默认值 device1（请检查环境变量）")
        device = "device1"
    return f"{plant}/{device}"


DEVICE_SCOPE: Final[str] = _device_scope(TD_PLANT, TD_DEVICE)
#: `DEVICE_SCOPE` 的拆解形式 `(plant, device)`。
#: ⚠️ 这是**未校验的原始拆解**，仅供诊断/展示用；要拼进 SQL 的调用方一律用
#: `device_tag_parts()`（唯一带白名单校验的出口，见下）。
DEVICE_SCOPE_PARTS: Final[tuple[str, str]] = tuple(DEVICE_SCOPE.split("/", 1))  # type: ignore[assignment]


def device_tag_parts() -> tuple[str, str]:
    """返回**已通过 tag 白名单校验**的 `(plant, device)`，供拼 SQL tag 过滤的读路径使用。

    ★ 为什么三个读路径统一走这个函数而不是直接读 `DEVICE_SCOPE_PARTS`：
      `report.query_aggregate` / `report.query_raw` / `web_dashboard.query_recent`
      都把它拼成 `AND plant = '{plant}' AND device = '{device}'`。这两个值来自环境变量，
      不校验就有两条真实后果（都不是理论风险）：
        · `device1' OR '1'='1` ⇒ 条件被 OR 短路，注入成立；
        · `Device1`（大小写不符）⇒ SQL 合法但命中 0 行：11 条路由全空报表、
          接口 200、容器 healthy，**一条报错都没有**（现场更难查的就是这条）。
      校验规则与平台接入层同源（`src/common/sql_safety.py`），非法值 `SystemExit`。

    ⚠️ 本函数在**每次调用时**重新校验（正则可忽略不计的代价），这样"校验"与
      "拼进 SQL"在代码上是同一处，而不是靠"别处已经校验过了"的约定维持。
      实践中非法值在下面那次导入期调用就已经拦下，请求路径走不到这里。
    """
    plant, device = DEVICE_SCOPE_PARTS
    check_sql_name(plant, what="展示层查询 tag plant", env_var="TD_PLANT")
    check_sql_name(device, what="展示层查询 tag device", env_var="TD_DEVICE")
    return plant, device


#: 导入期自检：非法 tag 值**拒绝启动**（`SystemExit`），与平台接入层同一口径。
#: 单设备默认值 plant1 / device1 合法，所以正常部署零行为变化。
device_tag_parts()


# ==================== 2. 客户端（懒连接 + 全链路降级） ====================

class CacheClient:
    """Redis 薄封装：连接失败一律退化为"无缓存"，绝不把异常抛给查询路径。

    `self._client is None` 且 `self.enabled` 为真 => 正在重试连接；
    `self.enabled` 为假 => 连不上或显式关闭，所有方法都是空操作。
    """

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

        连不上只是打日志，不影响 Web 服务启动；命令自检失败也要显式告警 ——
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
        """启动自检：把本模块真正用到的 Redis 命令各跑一遍，返回失败清单。

        为什么必须有这一步（不是洁癖，是踩出来的）：
        `_cmd` 的容错是"任何异常 => 认为 Redis 不可用 => 整层降级为直连查库"。
        这个容错对"Redis 真的挂了"是对的，但它会把**代码/C 端 API 用错**
        （例如给 `zrangebyscore` 传了当前 redis-py 不认的 `desc=`）
        也一并伪装成"Redis 挂了"。后果特别隐蔽：接口一切正常、数据永远新鲜、
        只是缓存从未命中，看起来像"缓存效果不明显"而不是"缓存是坏的"。
        自检把这类错误在启动那一刻就暴露成日志里的一条 WARN。

        ⚠️ 这里**不走 `_cmd`**：自检就是要看"命令本身能不能跑通"，
        不能因为一条探针失败就触发 _handle_failure 把连接删掉（那会让后面全部探针假失败）。
        """
        client = self._client
        if client is None:
            return ["未连接"]
        # 探针集合里的每条命令都必须与生产代码**逐参数同形**：
        # 只探"命令存在"是没用的 —— 本机踩到的正是"命令存在、封装返回静默空"、
        # 以及"命令本身可用、但参数形状在某个 redis-py 版本上不被接受"。
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

    # ---- 类型化读取（类型不对一律当"没有"，不留半截状态） ----

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
#   - 与 report.format_ts() 的输出口径一致（str(value)[:19]）
#   - 零填充意味着**字典序 == 时间序**，可以直接用字符串比较判先后

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
    """查询缓存键：**设备维度** + kind/unit/起止时间 全部进键。

    ⚠️ 起止时间用的是**已对齐到窗口边界**的值（调用方在 report 里先对齐再进这里），
    所以同一分钟内重复请求的键是稳定的，而跨过分钟边界就会自然换键 —— 不需要另做失效。

    ★ 设备维度（`device`，形如 `plant1/device1`）放在哈希材料**最前面**：它是**数据归属**维度，
      kind/unit/时间都是"对这台设备的哪一段"的进一步筛选。少了它，第二台设备查同一时间窗
      就会算出同一个键、命中第一台的条目，把别人的数据当成自己的返回（模块头第 6 条）。

    ⚠️ `device` 的默认值是本实例配置的设备（`DEVICE_SCOPE`），单设备部署不必显式传；
       但**调用点应该显式传**（report.py 就是这么写的），这样"这个键属于哪台设备"在调用处可见，
       而不用回头查环境变量。
    """
    raw = f"{device}|{kind}|{unit}|{start_text}|{end_text}"
    return KEY_PREFIX + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def _source_key(cache_key: str) -> str:
    """缓存键 -> 可读的来源描述（诊断用，写入 KEY_LAST_QUERY）。"""
    return cache_key[len(KEY_PREFIX):] if cache_key.startswith(KEY_PREFIX) else cache_key


# ==================== 4. 窗口闭合判据 ====================

def _closure(end: datetime) -> tuple[bool, str, str]:
    """判断查询上界是否已"闭合"。返回 (是否闭合, 判据, 水位文本)。

    判据用的是"最新**数据时间戳**"（而不是容器墙钟）：
    时间戳直接来自库内行，与产生它的时钟同源，天然没有跨时钟误差。
    """
    data_text = CLIENT.get_str(KEY_DATA_TS)
    if not data_text:
        return False, "无 data_ts 水位（还没有观测到库内数据）", ""
    data_ts = parse_ts_text(data_text)
    if data_ts is None:
        return False, f"data_ts 水位无法解析: {data_text!r}", data_text
    if end > data_ts - timedelta(seconds=MARGIN_SECONDS):
        return False, "窗口未闭合（上界未早于 data_ts-边界）", data_text
    return True, "已闭合", data_text


def _ready(where: str) -> bool:
    """缓存是否**真的**可用（开关开着 且 Redis 连得上）；不可用时打一次日志。"""
    if not CACHE_FLAG:
        return False
    if CLIENT.enabled:
        return True
    LOGGER.info("[%s] 缓存不可用，直连 TDengine: %s", where, CLIENT.last_error)
    return False


# ==================== 5. 编码/解码 ====================

def encode_rows(rows: list[tuple[Any, ...]], observed: dict[str, Any]) -> Optional[str]:
    """结果行 + 当时观测到的窗口指纹 -> JSON 字符串（zlib 压缩后 base64）。

    为什么压缩：分钟报表 1 小时 = 60 行 × 11 列，JSON 约 12 KB，
    压缩后约 1.5 KB，Redis 内存和网络往返都小一个量级。解压失败会当脏数据处理。

    `observed` 必须和结果行**一起存**：它是"这条缓存是用哪一版数据算出来的"的证据。
    只存结果不存它，就没有任何办法在后续请求里判断"窗口内容变过没有"。
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
# 两个水位都由 `/api/data` 顺带喂：它按定义不缓存、每次都真的读库，
# 所以它看到的行就是"库里此刻真实有什么"的一手证据，不需要额外查询。
#
#   data_ts —— **最新**数据时间戳：回答"一个历史窗口是不是已经写完了"（窗口闭合判据）
#   minmax  —— **分钟 -> 该分钟观测到的最大时间戳**：回答"这个窗口的内容有没有变过"（写后失效判据）
#
# ★ 为什么是"分钟最大值"这个形状（三个失败版本换来的，不要轻易改）：
#   1) 用"最新数据时间戳"当失效水位：失效永不触发 —— 它永远比历史窗口新。
#   2) 用"可缓存区间的最大时间戳"当失效水位：哨兵行的时间戳比它旧，永远不触发。
#   3) 用"逐行时间戳的集合"：能触发，但**误杀** —— /api/data 每次都会重新看到
#      最近 10 分钟的全部行，把它们全登记成"新写入"，于是最近 10 分钟的窗口
#      每次请求都判脏、缓存命中率掉到 0（实测 dirty 计数 = miss 计数）。
#   最终落地的形状是：按分钟归档，只记"该分钟里最晚的那条"。
#   判据变成"缓存这条窗口时看到的最大值" vs "库里现在的最大值"——
#   只有**该分钟真的多了一条更晚的数据**才判脏；重复看到同一批行不会误杀。
#
# ---- 水位为什么**不**按设备分开（判断，含证据与耦合条件）----
# ⚠️ 状态更新（2026-10-02）：下面第 1 条改造项（读路径按设备过滤）**已经落地**
#    （commit d9e599d；report.py 的聚合/原始点 SQL、web_dashboard.py:155-159 都用
#    `cache.device_tag_parts()` 拼 `AND plant = '...' AND device = '...'`，
#    该函数同时做 tag 白名单校验，见 §1）。
#    第 2、3 条（水位键按设备拆）**仍未做**，是已知的剩余项，不在本次改动范围内。
#    所以本节原来的第一条论据（"观测流里没有设备维"）已经不成立，别照着它继续论证。
#
# 当前结论：水位（KEY_DATA_TS / KEY_MINMAX / KEY_MINMAX_TS）仍是**全局一份**。
#
# 证据（读代码即可复现，不需要起 Redis / 不需要造第二台设备的数据）：
#   · 水位的**观测源**是 `/api/data`（web_dashboard.api_data → query_recent），它现在的 SQL 是
#     `FROM {TD_DB}.{TD_STABLE} WHERE ts >= now - Nm AND ts <= now AND plant = '...'
#      AND device = '...'`（web_dashboard.py:155-159），**已按设备过滤**；
#     report.py 的聚合 SQL 同样带这一组 tag 条件（report.py:257-261）。
#   · 也就是说，这条观测流里现在**有**"哪台设备"这一维，但存水位的 Redis 键没有：
#     每个 web 实例观测的是自己那台设备，却把结果写进同一份全局键。
#
# 失真什么时候会真的发生（前提条件，不是"现在就有"）：
#   若按设备起**多个 web 实例**并**共用一个 Redis DB**，共用一份全局 data_ts，
#   设备 A 的新数据会把设备 B 还没写完的窗口判成"已闭合"（`_closure` 的 a 条件被放松），
#   于是 B 的条目会返回偏旧的数据；而且这种偏旧**不会被写后失效兜住** —— B 的迟到行落在
#   一个"全局分钟最大值本来就没变"的分钟里（A 在同一分钟里有更晚的行），signature 不变、判不出脏。
#   这就是 observed_window_detail 那条注释里"窗口最大值不变 → 判据失灵"的同类漏洞，
#   只不过这次是被别的设备的数据填住的。
#   当前 docker-compose.yml 只有**一个** web 实例（web 的 TD_PLANT/TD_DEVICE 固定 plant1/device1），
#   观测流里只有那台设备的行，所以这条失真尚未激活；多 web 实例共用一个 Redis 时才会激活。
#
# 真要多实例前的改法（一次改完，别只改一半）：
#   1) ✅ 读路径加设备过滤（report.py 的聚合 SQL、web_dashboard.query_recent，按 tag 过滤）—— 已做；
#   2) ⏳ KEY_DATA_TS / KEY_MINMAX / KEY_MINMAX_TS 三个键加设备维度；
#   3) ⏳ note_data_ts / note_minute_max / _closure / observed_window_detail / signature
#      都带上设备参数（水位的语义变成"这台设备的库内数据的最新时间戳"）。
KEY_MINMAX_TTL_SECONDS: Final[int] = int(os.getenv("CACHE_MINMAX_TTL_SECONDS", "86400"))


def note_data_ts(latest: str) -> None:
    """记录"观测到的最新数据时间戳"。只前进不后退。"""
    latest = (latest or "")[:19]
    if len(latest) < 19 or not _ready("note_data_ts"):
        return
    current = CLIENT.get_str(KEY_DATA_TS) or ""
    if latest > current:
        CLIENT.set_str(KEY_DATA_TS, latest, WATERMARK_TTL_SECONDS)


def note_minute_max(observed: list[str]) -> int:
    """登记"每个分钟里观测到的最大时间戳"（HGETALL 一次读回，写入用 HSET 批量）。

    返回被更新的分钟数（值确实变大才算更新）。
    """
    unique = sorted({text[:19] for text in observed if text and len(text) >= 19})
    if not unique or not _ready("note_minute_max"):
        return 0
    current = CLIENT._cmd("hgetall", KEY_MINMAX) or {}
    updates: dict[str, str] = {}
    for text in unique:
        minute = text[:16] + ":00"
        best = max([text] + ([current[minute]] if minute in current else []))
        if current.get(minute) != best:
            updates[minute] = best
    if not updates:
        return 0
    CLIENT._cmd("hset", KEY_MINMAX, mapping=updates)
    # 记录分钟本身（用于按时间裁剪）与 TTL
    CLIENT._cmd("hset", KEY_MINMAX_TS, mapping={minute: minute for minute in updates})
    CLIENT._cmd("expire", KEY_MINMAX, MINMAX_KEEP * 900)
    CLIENT._cmd("expire", KEY_MINMAX_TS, MINMAX_KEEP * 900)
    _trim_minute_max()
    return len(updates)


def _trim_minute_max() -> None:
    """只保留最近 MINMAX_KEEP 个分钟（按分钟文本排序，等价于按时间排序）。"""
    count = CLIENT._cmd("hlen", KEY_MINMAX)
    if count is None or int(count) <= MINMAX_KEEP:
        return
    minutes = CLIENT._cmd("hkeys", KEY_MINMAX) or []
    for minute in sorted(minutes)[: int(count) - MINMAX_KEEP]:
        CLIENT._cmd("hdel", KEY_MINMAX, minute)
        CLIENT._cmd("hdel", KEY_MINMAX_TS, minute)


def observed_window_max(range_start_key: str, range_end_key: str) -> Optional[str]:
    """窗口 [range_start, range_end) 内"观测到的最大数据时间戳"（诊断用）。"""
    detail = observed_window_detail(range_start_key, range_end_key)
    if not detail:
        return None
    return max(detail.values())


def observed_window_detail(range_start_key: str, range_end_key: str) -> dict[str, str]:
    """窗口内**逐分钟**观测到的最大时间戳：{分钟 -> 该分钟最大 ts}。

    ⚠️⚠️ 为什么失效判据必须用"逐分钟明细"而不是"整个窗口的最大值"（踩过）：
    一个 1 小时窗口里有 60 个分钟。若某次更新落在**较早**的分钟里，
    而较晚的分钟本来就有更大的最大值，那么"窗口最大值"根本不变 ——
    判据失灵，缓存永不失效。
    实测：窗口 [18:36, 18:41)，18:40 分钟的 max 是 18:40:55；
    往 18:38 分钟插一条 18:38:56，窗口最大值仍是 18:40:55 → 判据完全没反应。
    改成"逐分钟明细整体比对"后，任意一个分钟变动都能被发现。
    """
    all_max = CLIENT._cmd("hgetall", KEY_MINMAX)
    if not all_max:
        return {}
    return {
        minute: value for minute, value in all_max.items()
        if range_start_key <= minute < range_end_key
    }


def signature(range_start_key: str, range_end_key: str) -> tuple[str, int]:
    """窗口的"内容指纹"：窗口内**被观测到的那些分钟**的 (分钟, 最大值) 序列。

    返回 (指纹, 观测到的分钟数)。未观测到的分钟不参与 —— 它们"确定没数据"。

    ★ 为什么"未观测"可以当成确定状态（第五个版本，也是最终版）：
      观测表只覆盖最近 MINMAX_KEEP 分钟（默认 3000 min ≈ 50 h），
      而 /api/data 只喂最近 10 分钟。所以对一小时前的窗口，观测集合里
      **只有它最近 10 分钟那一小段**，其余分钟根本没记录。
      一开始我想把"未观测"也逐个分钟哈希进指纹，后果是：
      30 天曲线窗口有 43200 个分钟，每次请求都要循环 43200 次 → 实测 p50
      从 16 ms 涨到 63 ms（比不加缓存还慢）。
      改成只哈希观测到的分钟，代价与"窗口内被观测的分钟数"成正比（约 10 条），
      判据仍然成立：
        - 迟到写入把原本未观测的分钟变成已观测 → 观测集合变化 → 失效（要抓的）；
        - 也把已观测分钟的更大值暴露出来 → 取值变化 → 失效；
        - 纯稳态 → 集合与取值都不变 → 命中。
      残留风险与 TTL 兜底见模块头部"已知残留风险"。
    """
    detail = observed_window_detail(range_start_key, range_end_key)
    digest = hashlib.sha1()
    for minute in sorted(detail):
        digest.update(minute.encode("utf-8"))
        digest.update(b"=")
        digest.update(str(detail[minute]).encode("utf-8"))
        digest.update(b"|")
    return digest.hexdigest()[:16], len(detail)


def dirty_count() -> int:
    value = CLIENT._cmd("hlen", KEY_MINMAX)
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

    在这里单独算出 `range_start_key` / `range_end_key`（19 字符串），
    后面存值和查值都用同一对键，避免"存的时候用 datetime、比的时候用字符串"这种口径漂移。

    ★ `device` 随 `context` 全程带着走（`_execute_cached` → `query_cached` 都不必再解析环境变量），
      它已经"烘焙"进 `key`：命中和写入都只可能发生在同一台设备的命名空间里。
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

    `fetch` 只在需要回源时才会被调用，所以"未命中"的代价 = 原来的查询代价。

    命中判定有三道（任何一道不过就回源，绝不返回可疑数据）：
      ① 写后失效：库里"本窗口观测到的最大时间戳"和缓存条目存的那一版不一致 → 内容变过；
      ② 读侧自检：条目的窗口上界已经追平/越过 data_ts 水位 → 水位已进窗口，条目不可信；
      ③ 未闭合：窗口本身还没写完（含当前秒）→ 根本不缓存。

    ★ 设备维度已经在 `context["key"]` 里（由 `prepare` 烘焙，见模块头第 6 条）：
      读写都只发生在**这一台设备**的命名空间内，所以这三道判定判的都是"这台设备的这个窗口"，
      不存在"命中到另一台设备的条目"这条路径。水位仍是全局一份（理由见 §6）。
    """
    if not _ready(where):
        return fetch()

    key = context["key"]
    blob = CLIENT.get_str(key)
    if blob is not None:
        decoded = decode_rows(blob)
        current_sig, observed = signature(context["range_start_key"], context["range_end_key"])
        data_text = CLIENT.get_str(KEY_DATA_TS) or ""
        if decoded is None:
            CLIENT._cmd("delete", key)                      # 值坏了：清掉，走回源
        else:
            rows, stored = decoded
            stored_sig = stored.get("sig") if isinstance(stored, dict) else None
            if stored_sig is None:
                # 缓存值里没有"这一版对应哪一份观测指纹" —— 无法验证新鲜度，一律当未命中。
                CLIENT.incr(f"{STATS_PREFIX}stale")
                CLIENT.note_event("stale_unverifiable_entry")
                CLIENT._cmd("delete", key)
                LOGGER.info("[%s] 缓存条目缺少新鲜度标记，作废并回源", where)
            elif stored_sig != current_sig:
                # ① 窗口内容变过：某个分钟的最大值变了，或原本未观测的分钟出现了数据
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

    closed, reason, data_text = _closure(parse_ts_text(context["range_end_key"]) or datetime.min)
    if not closed:
        # 未闭合：直连，不读写缓存（这就是"不缓存当前秒数据"的落地点）
        CLIENT.incr(f"{STATS_PREFIX}bypass_future")
        CLIENT.incr(f"{STATS_PREFIX}query")
        LOGGER.debug("[%s] 不进缓存（%s），直连 TDengine", where, reason)
        return fetch()

    CLIENT.incr(f"{STATS_PREFIX}miss")
    CLIENT.incr(f"{STATS_PREFIX}query")
    rows = fetch()
    # 存值前记下"这一版结果对应哪一份观测指纹"——命中判定 ① 要用它
    current_sig, observed = signature(context["range_start_key"], context["range_end_key"])
    encoded = encode_rows(rows, {"sig": current_sig, "observed_minutes": observed})
    if encoded is None:
        LOGGER.info("[%s] 结果集超过 %d 字节，跳过缓存", where, MAX_VALUE_BYTES)
        return rows
    CLIENT.set_str(key, encoded, TTL_SECONDS)
    # 诊断串里带上设备维度：多设备部署时"最近一次被缓存的是哪台设备的哪个窗口"要能直接读出来
    CLIENT.set_str(
        KEY_LAST_QUERY,
        f"{where}|{context['device']}|{context['range_start_key']}|{context['range_end_key']}",
        86400,
    )
    return rows


# ==================== 8. 统计与运维接口 ====================

def stats() -> dict[str, Any]:
    """缓存统计快照（供 /api/cache/stats 与测量脚本取证）。

    ★ 统计**不按设备拆**（判断，理由三条）：
      1) 这些计数是"缓存层整体好不好用"的度量：多设备部署下各 web 实例本来就在**同一个 Redis DB**
         里读写，命中/未命中是这份共享缓存的事实，按设备拆开后单看任何一台都读不出全局健康度；
      2) 计数键带设备维度还有个前提做不到：Redis 侧的计数没有"谁写的"信息，拆键只是把口径改成
         "本实例"，读到的人却容易以为那是全局 —— 反而更容易误判；
      3) 但读数的人必须知道这份全局视图覆盖了哪些设备，所以这里显式下发 `device_scope`
         （本实例的设备维度）。多实例部署时各实例的 `device_scope` 不同、`counters` 是同一份。
      真要按设备归因，改法是给计数键也加设备维度（`...:cnt:{scope}:hit`），代价是丢掉全局视图、
      并要同步改测量脚本（scripts/measure_cache_invalidation.py）的口径 —— 当前不做。
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
        # enabled  = 运营开关 + 连接状态的**合成结果**（测量脚本认这一个字段）
        # 两个分项也一并给出：0 开关 / 连不上 Redis 是两种完全不同的排障路径
        "enabled": CACHE_FLAG and CLIENT.enabled,
        "cache_flag": CACHE_FLAG,
        "redis_connected": CLIENT.enabled,
        "last_error": CLIENT.last_error,
        "redis": f"{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
        # 本实例的**设备维度**：上面这些计数/键数/水位是全局视图，这一项说明它们里包含谁
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
        # 命中率分母口径：只算"允许进缓存的查询"（bypass_future 不计入），
        # 计入会把"当前窗口本来就不该缓存"的请求算成未命中，人为压低命中率。
        "hit_rate_note": "hit/(hit+miss+stale)；bypass_future（窗口未闭合、按设计直连）不计入分母",
        "cache_keys": keys,
        "minute_max_entries": dirty_count(),
        "watermarks": {
            "data_ts": CLIENT.get_str(KEY_DATA_TS),
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

    ★ 清理**不按设备拆**（判断）：这个接口的语义是"把缓存层清干净"，缓存本身是易失的、
      随时可以重建，所以"清空"没有任何正确性代价。按设备只清自己那一份反而有两个坏处：
      ① 留下别的设备的条目，制造"清了但好像还在"的假象；
      ② 演练/对照测量时要的正是"一个空的缓存层"，只清一半会让前后对照失真。
      按前缀删除天然只覆盖当前版本命名空间（`cems:{CACHE_VERSION}:q:`），不会碰到水位与计数。
    """
    deleted = CLIENT.purge_queries()
    return {"deleted": deleted, "enabled": CLIENT.enabled}


def source_of(cache_key: str) -> str:
    """缓存键的可读来源（外部诊断用）。"""
    return _source_key(cache_key)


# 设备维度在这里打一条日志：多设备部署下"这台 web 给哪台设备做缓存"必须一眼可见。
# ⚠️ 它必须与平台接入层的 TD_PLANT/TD_DEVICE 一致：读路径**已经**按这两个值过滤
#    （report.py / web_dashboard.py 用 device_tag_parts() 拼 tag 条件，见 §6），
#    所以这里写错就不只是"缓存命名空间对不上"，而是**读的是别人的数据**。
# ⚠️ 本行与下面的 CLIENT.init() 都在**导入时**执行，那时日志系统可能还没配置
#    （web_dashboard 是"先 import、后 setup_logging"），这类 INFO 记录会被丢弃 ——
#    所以 web_dashboard.main() 在 setup_logging() 之后会再打一条同样的（保证 docker logs 里看得到）。
LOGGER.info("查询缓存设备维度: %s（缓存键按设备隔离，键格式见 build_key）", DEVICE_SCOPE)
CLIENT.init()
