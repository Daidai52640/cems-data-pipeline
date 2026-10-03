# -*- coding: utf-8 -*-
"""Web 展示大屏：Flask 提供 TDengine 近段数据接口，前端用 ECharts 画各烟气测点的实时曲线。

⚠️ 测点数**不写死**：页面标题与曲线都用 `points.py` 的契约现算（历史上标题曾长期写着
"8 测点"而契约早已是 9，改契约时没人会记得回来改这个数）。
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Final, Optional
from urllib.parse import quote

import taosrest
from flask import Flask, Response, jsonify, request
from werkzeug.exceptions import HTTPException

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import COLUMNS   # noqa: E402
from src.common.points import POINTS as CONTRACT_POINTS   # noqa: E402
from src.web import report   # noqa: E402
from src.web import report_export   # noqa: E402
from src.web import cache   # noqa: E402
from src.web import curve   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================
# 连接参数支持环境变量覆盖，默认值与本机直接运行一致；
# 容器化部署时由 docker-compose.yml 注入服务名（如 TD_URL=http://tdengine:6041）。

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = os.getenv("TD_URL", "http://localhost:6041")
TD_USER: Final[str] = os.getenv("TD_USER", "root")
TD_PASS: Final[str] = os.getenv("TD_PASS", "taosdata")
TD_DB: Final[str] = os.getenv("TD_DB", "cems")
TD_STABLE: Final[str] = os.getenv("TD_STABLE", "cems_data")

# ---- 测点列 ----
# 列名与顺序统一来自 src/common/points.py，这里不再抄一份。
POINTS: Final[tuple[str, ...]] = COLUMNS

# ---- 图表面板（small multiples）：一个量纲一根轴，绝不共用刻度 ----
# ★ 为什么不是一个图里挂 4 根 Y 轴（改造前的做法，实测"乱"）：
#   9 个测点的量程差 3 个数量级（流量 0~65000、压力 85~105），
#   把量程差得远的曲线塞进同一根轴，小量程的那条会被压成一条直线（看不出任何变化），
#   左右挂 4 根轴又会引出"哪条线读哪根轴"的误读 —— 这是数据可视化里公认的坑
#   （双重轴图的批评见 Datawrapper《Dual-axis charts》：easily misread；ECharts 手册也只
#    建议左右各一根轴）。所以改成**按量纲拆成多个上下排列的面板**，每格一根自己的 Y 轴，
#   横轴共用并联动 —— 形状可比、数值可读，且不存在"读错轴"。
#
# 分组依据是**单位 + 量程数量级**（列名/单位来自测点契约，这里只声明怎么分组与配色）：
CHART_PANELS: Final[tuple[tuple[str, str, tuple[str, ...]], ...]] = (
    ("污染物浓度", "mg/m3", ("dust", "so2", "nox")),
    ("氧含量 / 湿度", "%", ("o2", "humidity")),
    ("烟气流量", "m3/h", ("flow",)),
    ("烟气流速", "m/s", ("velocity",)),
    ("烟气温度", "degC", ("temp",)),
    ("烟气压力", "kPa", ("pressure",)),
)

# 测点在图表上的呈现方式：中文名 / 所属面板下标 / 配色
POINT_PRESENTATION: Final[dict[str, tuple[str, int, str]]] = {
    "so2": ("SO2", 0, "#ef4444"),
    "nox": ("NOx", 0, "#3b82f6"),
    "dust": ("颗粒物", 0, "#a855f7"),
    "o2": ("O2", 1, "#22c55e"),
    "humidity": ("湿度", 1, "#06b6d4"),
    "flow": ("流量", 2, "#84cc16"),
    "velocity": ("流速", 3, "#14b8a6"),
    "temp": ("温度", 4, "#f59e0b"),
    "pressure": ("压力", 5, "#ec4899"),
}


def build_point_views_json() -> str:
    """生成前端用的测点呈现配置（JSON），两个页面共用；缺配置直接报错，别带着半个图上线。"""
    views: list[dict[str, Any]] = []
    for point in CONTRACT_POINTS:
        if point.column not in POINT_PRESENTATION:
            raise RuntimeError(f"测点 {point.column} 缺少图表呈现配置（POINT_PRESENTATION）")
        label, axis, color = POINT_PRESENTATION[point.column]
        views.append({
            "key": point.column, "label": label, "unit": point.unit,
            "axis": axis, "color": color,
        })
    return json.dumps(views, ensure_ascii=False)


POINT_VIEWS_JSON: Final[str] = build_point_views_json()


def build_panels_json() -> str:
    """生成前端用的面板定义（JSON）。契约里的测点必须**恰好**落在某一个面板里，否则报错。"""
    placed = [key for _title, _unit, keys in CHART_PANELS for key in keys]
    missing = [point.column for point in CONTRACT_POINTS if point.column not in placed]
    if missing:
        raise RuntimeError(f"测点 {missing} 没有被分到任何图表面板（CHART_PANELS）")
    if len(placed) != len(set(placed)):
        raise RuntimeError("CHART_PANELS 里有测点被分到了两个面板")
    # 每个面板的轴名用契约里的单位，不在这里另写一份单位表
    units = {point.column: point.unit for point in CONTRACT_POINTS}
    panels = [
        {"title": title, "unit": unit, "points": list(keys),
         "unitsMatch": all(units[key] == unit for key in keys)}
        for title, unit, keys in CHART_PANELS
    ]
    return json.dumps(panels, ensure_ascii=False)


PANELS_JSON: Final[str] = build_panels_json()

# ---- 查询与刷新 ----
QUERY_MINUTES: Final[int] = int(os.getenv("QUERY_MINUTES", "10"))        # 查询最近 N 分钟数据
REFRESH_SECONDS: Final[int] = int(os.getenv("REFRESH_SECONDS", "5"))     # 前端自动刷新间隔（秒）
QUERY_LIMIT: Final[int] = int(os.getenv("QUERY_LIMIT", "5000"))          # 单次查询最多返回多少点

# ---- Web 服务 ----
WEB_HOST: Final[str] = os.getenv("WEB_HOST", "0.0.0.0")   # 监听所有网卡，局域网可访问
WEB_PORT: Final[int] = int(os.getenv("WEB_PORT", "5000"))
WEB_THREADS: Final[int] = int(os.getenv("WEB_THREADS", "8"))   # waitress 工作线程数

# ---- 健康判据（GET /api/health，见 §4）----
# 判据是**数据新鲜度**，不是"库连得上"。原来只看"能不能连库"，
# 于是"网关 MQTT 断 / 网关只写缓存补不出去 / 订阅端连着但不入库 / 设备刷新线程死 /
# tag 拼错导致查询恒 0 行"这些故障全都 200 + ok，容器 healthy、无告警。
#
# 阈值 = 轮询周期 × 因子（默认 5.0 s × 3 = 15 s）：
#   · 周期真源是网关的 POLL_INTERVAL（单设备/双设备实测都在 11~12 条/分钟 ≈ 5 s 一条），
#     web 侧的覆盖率口径（src/web/report.py）已经在用同一个变量，这里沿用同一个。
#   · 因子取 3：允许连续丢 2 条样本而不误报（本机实测间隔均值 5.15 s，15 s 有 2.9 倍余量），
#     同时远小于"人工发现不了"的量级 —— 一个整分钟没有任何新数据必然判不健康。
POLL_INTERVAL_SECONDS: Final[float] = float(os.getenv("POLL_INTERVAL", "5.0"))
HEALTH_STALE_POLL_FACTOR: Final[int] = int(os.getenv("HEALTH_STALE_POLL_FACTOR", "3"))
HEALTH_STALE_AFTER_SECONDS: Final[float] = POLL_INTERVAL_SECONDS * HEALTH_STALE_POLL_FACTOR

# ---- 报表导出 ----
XLSX_MIME: Final[str] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
# 给老浏览器用的 ASCII 兜底名；中文名走 Content-Disposition 的 filename*
XLSX_ASCII_NAME: Final[str] = "CEMS_report.xlsx"

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("web.dashboard")


# ==================== 2. 日志与异常 ====================

def setup_logging() -> None:
    """初始化日志：控制台输出，级别由 LOG_LEVEL 统一控制。"""
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        force=True,
    )


class TdQueryError(RuntimeError):
    """TDengine 查询失败（用于把底层异常统一成接口层可识别的错误）。"""


def new_trace_id() -> str:
    """生成一个短追踪 ID：对外只给这个，细节留在服务端日志里查。"""
    return uuid.uuid4().hex[:12]


def safe_error(exc: Exception, where: str) -> tuple[dict[str, Any], int]:
    """把内部异常收敛成对外可返回的内容：通用文案 + 追踪 ID，细节只进日志。"""
    trace_id = new_trace_id()
    LOGGER.exception("[%s] 处理失败 trace_id=%s", where, trace_id)
    return {"error": "服务内部错误，请稍后重试", "trace_id": trace_id}, 500


# ==================== 3. 数据查询 ====================

def query_recent(minutes: int = QUERY_MINUTES, scope: str = "") -> list[tuple[Any, ...]]:
    """查 TDengine 最近 N 分钟数据，按时间升序返回 [(ts, 各测点值...), ...]。

    查询失败抛 TdQueryError（由接口层兜住，不影响 Web 进程存活）。

    两个约束是必须的：
      ts <= now —— 库里一旦有时间戳在"未来"的脏数据（时钟跳变等），
                   只写 ts >= now - N m 会把它们全捞回来，"最近 10 分钟"直接失真
      LIMIT     —— 查询结果不能无上限膨胀，否则响应体会随着积压越滚越大
    取数用"倒序 + LIMIT"再翻转，保证截断时留下的是**最新**的 N 个点。

    ★ **按 plant/device tag 过滤**：`cems_data` 是多设备共用的超级表（TAG = plant/device），
    不过滤就会把两台设备的点按 ts 混排成一条曲线。取值同源于 `cache.DEVICE_SCOPE_PARTS`
    （与缓存键同源，避免"查询按 A 设备、键按 B 设备"）。
    ⚠️ 这条读路径同时是缓存**水位**的观测源（下面 note_data_ts / note_minute_max）——
      加了设备过滤后，水位观测到的才是"本设备"的写入进度。
    """
    conn: Any = None
    try:
        conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
        cur = conn.cursor()
        columns = ", ".join(POINTS)
        # 请求级设备维度（空 = 本实例配置的那台）；非法值抛 ValueError，由路由翻成 400
        plant, device = cache.scope_split(scope)
        cur.execute(
            f"SELECT ts, {columns} FROM {TD_DB}.{TD_STABLE} "
            f"WHERE ts >= now - {minutes}m AND ts <= now "
            f"AND plant = '{plant}' AND device = '{device}' "
            f"ORDER BY ts DESC LIMIT {QUERY_LIMIT}"
        )
        rows = list(cur.fetchall())[::-1]      # 翻转成时间升序，画曲线从左到右
    except Exception as exc:
        LOGGER.error("查询 TDengine 失败: %s", exc)
        raise TdQueryError(str(exc)) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                LOGGER.debug("关闭 TDengine 连接时出错（忽略）: %s", exc)
    return rows


# ==================== 4. 接口 ====================

app = Flask(__name__)


def request_scope() -> str:
    """从查询参数取设备维度（`?plant=&device=`），返回 `plant/device` 串。

    - 都不传 ⇒ 本实例配置的那台设备（`TD_PLANT/TD_DEVICE`），单设备部署行为不变；
    - 只传 `device` ⇒ plant 用本实例配置值补齐（前端只需要传一台设备名）；
    - 非法值抛 `ValueError`，由路由翻成 400 —— **不静默回落**：
      静默回落会让"我想看 device2，页面却给了 device1 的数据"这种错看不出来。
    """
    return cache.scope_from_request(request.args.get("plant"), request.args.get("device"))


def query_devices() -> list[dict[str, str]]:
    """库里**有数据的**全部 (plant, device)，供页面上的设备选择器用。

    ★ 为什么用 `SELECT DISTINCT plant, device` 而不是 `SHOW TABLES`：
      子表会留下空壳（C19 血缘演练的 `trace_run*` 表就是空表），
      按行 DISTINCT 只返回真正有数据的设备 —— 实测正是 plant1/device1 与 plant2/device2 两台。
    """
    conn: Any = None
    try:
        conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT plant, device FROM {TD_DB}.{TD_STABLE}")
        rows = list(cur.fetchall())
    except Exception as exc:
        LOGGER.error("查询设备列表失败: %s", exc)
        raise TdQueryError(str(exc)) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                LOGGER.debug("关闭 TDengine 连接时出错（忽略）: %s", exc)
    devices = [{"plant": str(row[0]), "device": str(row[1])} for row in rows]
    # 本实例配置的那台排在最前，其余按名称稳定排序 —— 选择器顺序别每次刷新都跳
    # ⚠️ 设备维度的唯一真源在 cache（`TD_PLANT/TD_DEVICE` 是那边的配置项），这里不要另读环境变量
    default_plant, default_device = cache.DEVICE_SCOPE_PARTS
    devices.sort(key=lambda item: (item["plant"] != default_plant or item["device"] != default_device,
                                   item["plant"], item["device"]))
    return devices


@app.route("/api/devices")
def api_devices() -> tuple[Response, int] | Response:
    """接口0：库里有数据的设备清单 + 本实例默认设备（页面选择器用，直连 TDengine 不缓存）。"""
    try:
        devices = query_devices()
    except TdQueryError as exc:
        body, _status = safe_error(exc, "GET /api/devices")
        return jsonify({**body, "devices": [], "default_scope": cache.DEVICE_SCOPE}), 503
    return jsonify({"devices": devices, "default_scope": cache.DEVICE_SCOPE})


@app.route("/api/data")
def api_data() -> tuple[Response, int] | Response:
    """接口1：返回最近数据（JSON），前端每 REFRESH_SECONDS 秒调用一次。

    这个接口**没有缓存**（它按定义包含"当前秒"）。它对缓存层的唯一作用是
    **顺带喂两个水位**（见 src/web/cache.py §6）：
      - data_ts：最新数据时间戳 → 判断"一个历史窗口是不是已经写完了"
      - 分钟最大值：每个分钟里观测到的最晚一条 → 判断"哪个窗口的内容变过"，
        变了就把对应缓存条目作废（这就是"数据更新后缓存多久刷新"的机制）
    两者都来自刚读出来的真实行，不额外查库、不依赖容器墙钟。
    """
    try:
        scope = request_scope()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        rows = query_recent(QUERY_MINUTES, scope)
    except TdQueryError as exc:
        # 查库失败不崩服务：返回空数据 + 通用提示，细节只在服务端日志里
        body, _ = safe_error(exc, "GET /api/data")
        empty: dict[str, Any] = {"ts": [], "scope": scope, **body}
        empty.update({name: [] for name in POINTS})
        return jsonify(empty), 503

    if rows:
        timestamps = [cache.ts_text(row[0]) for row in rows]
        cache.note_data_ts(max(timestamps), scope)
        # ⚠️ 这里**不能**先按"是否比边界旧"过滤：分钟最大值必须覆盖**每一个**观测到的分钟。
        # 先前加了这道过滤，后果是"窗口里新来的那条恰好比边界新"就被丢掉，
        # 该分钟的最大值不变 → 缓存不失效（实测 200 s 内测不到刷新）。
        # 不做过滤也不会误杀：比边界新的分钟本来就不满足"窗口已闭合"，
        # 压根不会进缓存，它的最大值怎么变都无所谓（见 _closure）。
        cache.note_minute_max(timestamps, scope)

    payload: dict[str, Any] = {"ts": [str(row[0]) for row in rows],   # 时间轴
                              "scope": scope}                      # 这两个数来自哪台设备
    for index, name in enumerate(POINTS, start=1):                    # 各测点序列
        payload[name] = [row[index] for row in rows]
    return jsonify(payload)


def latest_sample_age_seconds(rows: list[tuple[Any, ...]]) -> Optional[float]:
    """最近一条样本的时间戳距离"现在"多少秒（行里没有可解析时间戳时返回 None）。

    ⚠️ 时间戳统一走 `cache.ts_text()` + `cache.parse_ts_text()`（去掉时区标记的 19 位本地串），
    与缓存水位的口径**完全同源**：容器时区与 TDengine 一致（TZ=Asia/Shanghai），
    容器虚拟时钟相对宿主有 100 ms 级漂移（见缓存模块头部），对 15 s 量级的判据无影响。
    直接拿 datetime 相减会在"带时区的 aware datetime"上抛 TypeError，所以先归一到本地串。
    """
    if not rows:
        return None
    moment = cache.parse_ts_text(cache.ts_text(rows[-1][0]))
    if moment is None:
        return None
    return (datetime.now() - moment).total_seconds()


@app.route("/api/health")
def api_health() -> tuple[Response, int] | Response:
    """接口2：健康检查 —— 判据是**数据新鲜度**，不是"库连得上"。

    ★ 为什么原来的判据不够（这次修的就是它）：
      原实现只探"能不能连上库并查到最近 1 分钟的行"，且 `rows_last_1min == 0` 也返回 200。
      于是下面这些故障**全部**表现为 200 + 容器 healthy + 无告警：
        网关 MQTT 断 / 网关只写本地缓存补不出去 / 订阅端连着但不入库 /
        设备刷新线程死 / tag 拼错导致查询恒 0 行（静默全绿）。
      现在两条判据任一不满足即 503，并在 `reason` 里给出原因：
        ① `rows_last_1min == 0`          → reason=no_rows_last_1min
        ② 最近一条样本距今 > 阈值         → reason=stale_data（阈值 = POLL_INTERVAL × 因子，默认 15 s）
      查库失败仍是 503（reason=td_unreachable）。

    ★ 字段兼容：`td` / `rows_last_1min` 语义未变（分别是"库探针"与"最近 1 分钟条数"）；
      `ok` 的语义**升级为"真健康"**，与 HTTP 状态严格一致（200 ⇔ ok=true），
      并新增同义字段 `healthy`、`reason`、`data_age_seconds`、`stale_after_seconds`。
      需要"库是否连得上"这个子判据的调用方读 `td` 字段。
      （仓库内唯一的读者是 start.ps1：它只匹配 `"ok": false` 打一条提示，不会因为多一个
        原因而失效；scripts/measure_nginx_redis.py 只量接口耗时，不解析字段。）
    """
    threshold = HEALTH_STALE_AFTER_SECONDS
    try:
        rows = query_recent(1)
    except TdQueryError as exc:
        body, _ = safe_error(exc, "GET /api/health")
        return jsonify({
            "ok": False, "healthy": False, "td": "down", "rows_last_1min": 0,
            "reason": "td_unreachable", "data_age_seconds": None,
            "stale_after_seconds": threshold, **body,
        }), 503

    age = latest_sample_age_seconds(rows)
    payload: dict[str, Any] = {
        "td": "up",
        "rows_last_1min": len(rows),
        "data_age_seconds": None if age is None else round(age, 3),
        "stale_after_seconds": threshold,
    }
    if not rows:
        reason = "no_rows_last_1min"
    elif age is None:
        reason = "unparsable_ts"
    elif age > threshold:
        reason = "stale_data"
    else:
        payload.update({"ok": True, "healthy": True, "reason": "fresh"})
        return jsonify(payload)

    payload.update({"ok": False, "healthy": False, "reason": reason})
    LOGGER.warning(
        "健康检查不通过：reason=%s rows_last_1min=%d data_age_seconds=%s 阈值=%ss",
        reason, payload["rows_last_1min"], payload["data_age_seconds"], threshold,
    )
    return jsonify(payload), 503


@app.route("/api/cache/stats")
def api_cache_stats() -> Response:
    """接口2b：缓存统计（命中率 / 查库次数 / 水位）—— 仅供测量与排障。

    ⚠️ 默认 nginx 配置把 `/api/cache/` 整个路径 return 403，不对公网暴露内部计数。
    测量脚本直连 web 容器端口取数。

    ★ 统计**不按设备拆**：计数是"这个 Redis 命名空间里缓存层整体好不好用"的度量，多设备部署下
      各实例共用同一个 Redis DB，拆开反而读不出全局健康度。响应里用 `device_scope` 说明这些计数
      覆盖的是哪台设备（理由与取舍见 `cache.stats()` 的 docstring）。
    """
    return jsonify(cache.stats())


@app.route("/api/cache/clear", methods=["POST"])
def api_cache_clear() -> Response:
    """接口2c：清空全部查询缓存（演练/排障用）。返回删除条数。

    ★ 清理**不按设备拆**：语义是"把缓存层清干净"，缓存可随时重建，所以全清没有代价；
      只清本设备那一份会留下别的设备的条目，制造"清了但还在"的假象，也让演练的空白基线失真
      （理由见 `cache.clear()` 的 docstring）。
    """
    return jsonify(cache.clear())


@app.route("/")
def index() -> Response:
    """接口3：返回大屏页面（ECharts 画图）。"""
    html = (
        HTML_PAGE
        .replace("__REFRESH_MS__", str(REFRESH_SECONDS * 1000))
        .replace("__REFRESH_SEC__", str(REFRESH_SECONDS))
        .replace("__POINTS_JSON__", POINT_VIEWS_JSON)
            .replace("__PANELS_JSON__", PANELS_JSON)
            # ⚠️ 测点数从契约现算，不写死：标题曾长期写着"8 测点"而契约早已是 9
            #    （改契约时没人会记得改标题 —— 让它跟着 points.py 走就不会再错）
            .replace("__POINT_COUNT__", str(len(POINTS)))
    )
    return Response(html, mimetype="text/html")


# ==================== 5. 报表接口（分钟/日/月/自由） ====================
# 聚合全部在 TDengine 侧用 INTERVAL + AVG 完成，见 src/web/report.py

def report_response(
    where: str,
    builder: Callable[[], dict[str, Any]],
) -> tuple[Response, int] | Response:
    """报表接口统一出口：参数错 400（文案直接可用），查库错走 safe_error 转 503。"""
    try:
        return jsonify(builder())
    except ValueError as exc:
        # 设备维度不合法（`request_scope`）与时间参数不合法同一层级：都是 400
        LOGGER.warning("[%s] 参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except report.ReportParamError as exc:
        LOGGER.warning("[%s] 参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except report.ReportQueryError as exc:
        # 查库失败统一 503（和 /api/data 一致）；safe_error 只负责脱敏与记日志
        body, _status = safe_error(exc, where)
        return jsonify(body), 503


@app.route("/api/report/minute")
def api_report_minute() -> tuple[Response, int] | Response:
    """分钟报表：默认最近 1 小时，按分钟聚合均值。"""
    return report_response(
        "GET /api/report/minute",
        lambda: report.minute_report(request.args.get("start"), request.args.get("end"), request_scope()),
    )


@app.route("/api/report/day")
def api_report_day() -> tuple[Response, int] | Response:
    """日报表：默认今天，按小时聚合，固定 24 个整点。"""
    return report_response(
        "GET /api/report/day",
        lambda: report.day_report(request.args.get("date"), request_scope()),
    )


@app.route("/api/report/month")
def api_report_month() -> tuple[Response, int] | Response:
    """月报表：默认当月，按天聚合，按当月实际天数补齐。"""
    return report_response(
        "GET /api/report/month",
        lambda: report.month_report(request.args.get("year"), request.args.get("month"), request_scope()),
    )


@app.route("/api/report/custom")
def api_report_custom() -> tuple[Response, int] | Response:
    """自由报表：默认最近 24 小时，按小时聚合，可跨天/跨月。"""
    return report_response(
        "GET /api/report/custom",
        lambda: report.custom_report(request.args.get("start"), request.args.get("end"), request_scope()),
    )


@app.route("/report")
def report_page() -> Response:
    """报表页面：4 类时间维度聚合，数据表格 + 覆盖率面板。

    ⚠️ 本页**只有表格，没有图表**（ECharts 只挂在 `/` 实时大屏上）；
    注释历史上写的是"折线图 + 数据表格"，与实现不符，已改回事实。
    """
    return Response(REPORT_PAGE.replace("__POINTS_JSON__", POINT_VIEWS_JSON)
            # ⚠️ 测点数从契约现算，不写死：标题曾长期写着"8 测点"而契约早已是 9
            #    （改契约时没人会记得改标题 —— 让它跟着 points.py 走就不会再错）
            .replace("__POINT_COUNT__", str(len(POINTS))), mimetype="text/html")


@app.route("/api/curve")
def api_curve() -> tuple[Response, int] | Response:
    """自由区间曲线：按跨度自动分级降采样，返回结构与 /api/data 一致（列式）+ 粒度元信息。

    实时模式仍然用 /api/data（契约不变）；长区间必须走这里，
    否则原始点查询会被 LIMIT 静默截断，看起来像"前面那段没数据"。
    """
    where = "GET /api/curve"
    try:
        return jsonify(curve.build_curve(
            request.args.get("start"), request.args.get("end"), request_scope()))
    except ValueError as exc:
        LOGGER.warning("[%s] 设备参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except (curve.CurveParamError, report.ReportParamError) as exc:
        LOGGER.warning("[%s] 参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except report.ReportQueryError as exc:
        body, _status = safe_error(exc, where)
        return jsonify(body), 503


@app.route("/api/report/export")
def api_report_export() -> tuple[Response, int] | Response:
    """报表导出（Excel）：一个接口覆盖四类报表，type 决定用哪套口径。

    聚合与参数校验全部复用 report.py，导出路径不重写 SQL，保证与页面数据一致。
    """
    where = "GET /api/report/export"
    try:
        content, filename = report_export.export_workbook(request.args, request_scope())
    except ValueError as exc:
        LOGGER.warning("[%s] 设备参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except (report_export.ExportParamError, report.ReportParamError) as exc:
        # 参数问题返回 400 + JSON，绝不能把错误信息塞进 xlsx 里让用户下载
        LOGGER.warning("[%s] 参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except report.ReportQueryError as exc:
        body, _status = safe_error(exc, where)
        return jsonify(body), 503

    # 中文文件名必须放在 filename*（RFC 5987）里，只给 filename 的话浏览器会乱码
    disposition = (
        f'attachment; filename="{XLSX_ASCII_NAME}"; '
        f"filename*=UTF-8''{quote(filename)}"
    )
    return Response(
        content,
        mimetype=XLSX_MIME,
        headers={"Content-Disposition": disposition},
    )


@app.errorhandler(Exception)
def handle_unexpected_error(exc: Exception) -> tuple[Response, int] | HTTPException:
    """兜底错误处理：未预期异常记日志并返回 JSON，HTTP 异常（404 等）按原状态码返回。"""
    if isinstance(exc, HTTPException):
        # 404/405 这类是正常的 HTTP 语义，不是服务故障，不该记 error 也不该变成 500
        return exc
    body, status = safe_error(exc, "未预期的接口异常")
    return jsonify(body), status


@app.route("/favicon.ico")
def favicon() -> tuple[str, int]:
    """浏览器会自动请求图标，直接返回 204，避免刷出一堆无意义的 404 日志。"""
    return "", 204


# ==================== 5. 前端页面（HTML + ECharts） ====================

HTML_PAGE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CEMS 实时监测大屏</title>
<!-- 本地加载 ECharts（文件随镜像一起发布，离线/内网环境不会白屏） -->
<script src="/static/echarts.min.js"></script>
<style>
  /* hidden 属性必须真的隐藏：.controls 的 display:flex 会盖过浏览器默认的 [hidden]{display:none}，
     不写这条的话，实时模式下"起/止/查询"也会一直显示，用户会误以为已经切到自由区间 */
  [hidden] { display:none !important; }
  body { margin:0; padding:20px; background:#0f172a; font-family: sans-serif; }
  h1 { color:#e2e8f0; font-size:20px; margin:0 0 16px; }
  #chart { width:100%; height:70vh; background:#0f172a; }
  #status { color:#94a3b8; font-size:13px; margin-top:10px; }
  #status.err { color:#f87171; }
  .modes { display:flex; gap:8px; margin-bottom:10px; }
  .mode { padding:5px 14px; border:1px solid #334155; border-radius:8px; cursor:pointer;
          background:#1e293b; color:#cbd5e1; font-size:13px; user-select:none; }
  .mode.active { background:#0ea5e9; border-color:#0ea5e9; color:#0f172a; font-weight:600; }
  .controls { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-bottom:10px; }
  .controls label { font-size:13px; color:#94a3b8; }
  .controls input { background:#1e293b; color:#e2e8f0; border:1px solid #334155;
                    border-radius:6px; padding:5px 8px; font-size:13px; }
  .controls button { background:#0ea5e9; color:#0f172a; border:0; border-radius:6px;
                     padding:6px 16px; font-size:13px; font-weight:600; cursor:pointer; }
  .controls button:disabled { background:#475569; color:#94a3b8; cursor:not-allowed; }
  .devices { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin:0 0 10px; }
  .devices .cap { font-size:13px; color:#94a3b8; }
  .devices .dev { padding:4px 12px; border:1px solid #334155; border-radius:999px; cursor:pointer;
                  background:#1e293b; color:#cbd5e1; font-size:13px; user-select:none; }
  .devices .dev.active { background:#22c55e; border-color:#22c55e; color:#0f172a; font-weight:600; }
  .devices .dev.muted { opacity:.45; cursor:default; }
  .panelnote { font-size:12px; color:#64748b; margin:-4px 0 8px; }
</style>
</head>
<body>
  <h1>CEMS 烟气在线监测 · 曲线（__POINT_COUNT__ 测点）</h1>
  <div style="font-size:13px;margin-bottom:10px;">
    <a href="/report" style="color:#38bdf8;text-decoration:none;">报表 →</a>
  </div>
  <div class="devices" id="devices"><span class="cap">设备：</span></div>
  <div class="modes" id="modes">
    <span class="mode" data-mode="realtime">实时</span>
    <span class="mode" data-mode="free">自由区间</span>
  </div>
  <div class="controls" id="free-controls" hidden>
    <label>起</label> <input type="datetime-local" id="free-start">
    <label>止</label> <input type="datetime-local" id="free-end">
    <button id="free-query">查询</button>
  </div>
  <div class="panelnote">以下每个面板是**各自独立的 Y 轴刻度**（按量纲分组，避免把量程相差几个数量级的曲线画在同一根轴上）；横轴共用，鼠标悬停会联动。</div>
  <div id="chart"></div>
  <div id="status">加载中...</div>

<script>
(function() {
  var chartEl = document.getElementById('chart');
  var status = document.getElementById('status');
  var REFRESH_MS = __REFRESH_MS__;
  var REFRESH_SEC = __REFRESH_SEC__;

  // 测点表与面板表由后端注入（真源是 src/common/points.py + CHART_PANELS / POINT_PRESENTATION）
  // p.axis 现在是**面板下标**：每个面板一格、一根自己的 Y 轴（见 CHART_PANELS 的注释）
  var POINTS = __POINTS_JSON__;
  var PANELS = __PANELS_JSON__;
  // 画布高度按面板数现算（每格一根自己的 Y 轴），页面纵向滚动
  var PANEL_TOP = 46, PANEL_H = 92, PANEL_GAP = 26, PANEL_BOTTOM = 62;
  var ALL_AXES = PANELS.map(function(panel, index) { return index; });
  chartEl.style.height = (PANEL_TOP + PANELS.length * (PANEL_H + PANEL_GAP)
    - PANEL_GAP + PANEL_BOTTOM) + 'px';
  var AXIS_STYLE = { color: '#94a3b8' };
  // 单次请求超时：fetch 默认不会超时，请求卡住时 finally 不执行、轮询会悄悄停掉
  var FETCH_TIMEOUT_MS = 15000;

  // ---- 折算值 / 超标判据（/api/data 与 /api/curve 都不含限值，按固定口径在前端现算）----
  // 只对三个浓度测点折算，口径同 src/common/points.py（基准氧 6%，21−6=15）：
  //   折算值 = 实测值 × 15 / (21 − O2)
  // O2 > 19% 时分母过小、折算结果没意义 → 不折算（显示 “—”）；
  // 其他测点（O2、湿度、流量、温度、压力、流速）不折算。
  var ZS_MAP = { dust: true, so2: true, nox: true };
  var ZS_LIMITS = { dust: 5, so2: 35, nox: 50 };   // mg/m3：颗粒物 / SO2 / NOx
  var O2_FOR_ZS_MAX = 19;
  var COLOR_OVER = '#f87171';

  function isFiniteNum(v) {
    return typeof v === 'number' && isFinite(v);
  }

  // 算折算值；不该折算 / 缺实测或缺 O2 / O2>19% / 分母≤0 时统一返回 null（界面显示 “—”）
  function toConverted(key, measured, o2) {
    if (!ZS_MAP[key] || !isFiniteNum(measured) || !isFiniteNum(o2)) { return null; }
    if (o2 > O2_FOR_ZS_MAX) { return null; }
    var denominator = 21 - o2;
    if (denominator <= 0) { return null; }
    return measured * 15 / denominator;
  }

  // 悬停里的数字统一保留最多 2 位小数；null / 非有限数显示 “—”
  function fmtNum(v) {
    if (!isFiniteNum(v)) { return '—'; }
    return String(Math.round(v * 100) / 100);
  }

  if (typeof echarts === 'undefined') {
    status.className = 'err';
    status.textContent = '图表库加载失败（网络问题），请检查网络后刷新';
    return;
  }

  var myChart = echarts.init(chartEl);
  var mode = 'realtime';      // realtime | free
  var timer = null;           // 只记实时模式的下一轮定时器，切模式时要能取消
  var busy = false;
  // ---- 图例交互状态（用户在图例里关掉某条曲线，不能被 5 秒后的新数据重置） ----
  // ⚠️ 这两份状态是"用户意图"的唯一真源：每次 setOption 都把 legend.selected 显式带回去，
  //    否则一旦发生整帧重建（notMerge），ECharts 会退回"全部选中"的默认值 ——
  //    表现就是"只想看两条曲线，5 秒后又被重置成九条"。
  var hiddenPoints = {};      // 测点 label -> true：用户在图例里关掉了它
  var overMarks = {};         // 测点 key -> 该测点的超标散点数据（最近一帧），供图例切换时同步显隐
  // 轴的种类：category（实时，固定窗口）| time（自由区间，可能跨天）。它决定本帧能否走 merge：
  // merge 模式下上一种轴的 xAxis.data 会残留到另一种轴上，所以轴类型一变必须整帧重建。
  var lastAxisMode = null;
  // 视图代次：切模式就 +1。在途请求回来时若代次对不上，说明它属于上一个模式，
  // 必须整帧丢弃 —— 否则上一轮实时请求回来会把自由区间的画面和状态栏盖回去（表现为"闪一下"）
  var viewToken = 0;

  // 图例状态的唯一真源是**图表自己的** legend.selected：用户点图例会改它，
  // 任何 dispatchAction 也会改它。只靠事件回调记账会漏（legendSelect /
  // legendUnSelect 触发的是 legendselected / legendunselected 两个**不同**事件），
  // 所以这里一律先读回图表状态，再决定要不要把成对的散点一起收起。
  function readLegendSelected(ev) {
    if (ev && ev.selected) { return ev.selected; }
    try {
      var o = myChart.getOption();
      return (o && o.legend && o.legend[0] && o.legend[0].selected) || null;
    } catch (e) { return null; }
  }

  function applyLegendSelected(sel) {
    if (!sel) { return; }
    POINTS.forEach(function(p) {
      if (Object.prototype.hasOwnProperty.call(sel, p.label)) {
        hiddenPoints[p.label] = (sel[p.label] === false);
      }
    });
  }

  // 超标散点不是图例条目（不占图例行），要手工跟着主线一起显隐，
  // 否则会出现"曲线被关掉了、红点还孤零零飘着"。
  function syncOverMarks() {
    POINTS.forEach(function(p) {
      if (!ZS_MAP[p.key]) { return; }
      myChart.setOption({
        series: [{
          id: 'over-' + p.key,
          data: hiddenPoints[p.label] ? [] : (overMarks[p.key] || [])
        }]
      });
    });
  }

  ['legendselectchanged', 'legendselected', 'legendunselected'].forEach(function(name) {
    myChart.on(name, function(ev) {
      applyLegendSelected(readLegendSelected(ev));
      syncOverMarks();
    });
  });


  // ---- 设备维度（V3 双设备）：入口就是这一排按钮，不再"只在库里、页面上看不见" ----
  var deviceBox = document.getElementById('devices');
  var scope = '';                      // '' = 用后端配置的默认设备
  var defaultScope = '';
  function scopeLabel() {
    if (!scope) { return defaultScope || '(默认)'; }
    return scope;
  }
  function scopeQuery() {
    if (!scope) { return ''; }
    var parts = scope.split('/');       // 'plant/device'
    return 'plant=' + encodeURIComponent(parts[0]) + '&device=' + encodeURIComponent(parts[1]);
  }
  function withScope(url) {
    var q = scopeQuery();
    if (!q) { return url; }
    return url + (url.indexOf('?') >= 0 ? '&' : '?') + q;
  }
  function paintDevices() {
    Array.prototype.forEach.call(deviceBox.querySelectorAll('.dev'), function(el) {
      var on = (el.getAttribute('data-scope') === scope);
      el.className = 'dev' + (on ? ' active' : '');
    });
  }
  function loadDevices() {
    fetch('/api/devices')
      .then(function(res) { return res.json(); })
      .then(function(body) {
        defaultScope = body.default_scope || '';
        var list = body.devices || [];
        if (!list.length) { deviceBox.textContent = '设备：库里还没有数据'; return; }
        list.forEach(function(item) {
          var span = document.createElement('span');
          span.className = 'dev';
          span.setAttribute('data-scope', item.plant + '/' + item.device);
          span.title = 'plant=' + item.plant + ' device=' + item.device;
          span.textContent = item.device + '（' + item.plant + '）';
          span.addEventListener('click', function() {
            var next = item.plant + '/' + item.device;
            if (next === defaultScope) { next = ''; }   // 默认设备用不带参数的 URL
            if (next === scope) { return; }
            scope = next;
            paintDevices();
            if (mode === 'realtime') { loadRealtime(); } else { loadFree(); }
          });
          deviceBox.appendChild(span);
        });
        paintDevices();
      })
      .catch(function() { deviceBox.textContent = '设备：取设备列表失败（接口 /api/devices）'; });
  }

  function showError(msg) {
    status.className = 'err';
    status.textContent = msg;
  }

  function showInfo(msg) {
    status.className = '';
    status.textContent = msg;
  }

  function val(id) {
    return document.getElementById(id).value;
  }

  function fetchWithTimeout(url) {
    var controller = new AbortController();
    var timeoutId = setTimeout(function() { controller.abort(); }, FETCH_TIMEOUT_MS);
    return fetch(url, { signal: controller.signal })
      .finally(function() { clearTimeout(timeoutId); });
  }

  function describeFetchError(err) {
    return (err && err.name === 'AbortError')
      ? '请求超时（超过 ' + (FETCH_TIMEOUT_MS / 1000) + ' 秒没有响应），稍后会自动重试'
      : '后端连不上（确认 web_dashboard.py 在跑）';
  }

  function traceSuffix(body) {
    return (body && body.trace_id) ? '（追踪号 ' + body.trace_id + '）' : '';
  }

  function toLocalInput(date) {
    function pad2(n) { return (n < 10 ? '0' : '') + n; }
    return date.getFullYear() + '-' + pad2(date.getMonth() + 1) + '-' + pad2(date.getDate())
      + 'T' + pad2(date.getHours()) + ':' + pad2(date.getMinutes());
  }

  function initFreeInputs() {
    var now = new Date();
    var hourMs = 3600 * 1000;
    if (!val('free-end')) {
      document.getElementById('free-end').value = toLocalInput(now);
    }
    if (!val('free-start')) {
      document.getElementById('free-start').value =
        toLocalInput(new Date(now.getTime() - 6 * hourMs));
    }
  }

  // 两个接口返回的都是列式结构，共用这一个渲染函数
  // useTimeAxis：自由区间的点可能跨天跨月，category 轴会把标签挤成一团，改用 time 轴
  function render(d, useTimeAxis) {
    var axisMode = useTimeAxis ? 'time' : 'category';
    // 先把图表里当前的图例选中态读回来 —— 它是用户意图，绝不能被这一帧的新数据覆盖
    applyLegendSelected(readLegendSelected(null));
    // ★ 只有"轴类型变了"才整帧重建；同类型的两帧之间走 merge。
    //   notMerge=true 会把用户的图例选中态、图例滚动位置、tooltip 当前指向一起丢掉，
    //   并且每帧都重放一次入场动画 —— 这正是"实时刷新把交互重置掉"的根因。
    var mustReset = (axisMode !== lastAxisMode);
    lastAxisMode = axisMode;

    // 每个面板一个 grid + 一组 x/y 轴，横轴联动（axisPointer.link），
    // 于是"每个量纲自己的刻度"与"同一条时间线"两件事同时成立
    var xAxis = PANELS.map(function(panel, index) {
      var last = (index === PANELS.length - 1);
      return {
        gridIndex: index,
        type: useTimeAxis ? 'time' : 'category',
        data: useTimeAxis ? undefined : d.ts,
        axisLabel: last ? AXIS_STYLE : { show: false },
        axisTick: { show: last },
        axisLine: { lineStyle: { color: '#334155' } }
      };
    });
    var yAxis = PANELS.map(function(panel, index) {
      return {
        gridIndex: index,
        type: 'value',
        name: panel.title + ' (' + panel.unit + ')',
        nameTextStyle: AXIS_STYLE,
        axisLabel: AXIS_STYLE,
        // ⚠️ scale:true = 不强制包含 0。压力这种 85~105 的量程如果从 0 起，曲线会被压平
        //    （这正是改造前把流量/温度/压力塞进同一根轴的病根）。
        scale: true,
        splitNumber: 3,
        splitLine: { lineStyle: { color: 'rgba(51,65,85,0.5)' } }
      };
    });
    var o2Column = d.o2 || [];
    var series = POINTS.map(function(p) {
      var values = d[p.key] || [];
      var data = useTimeAxis
        ? d.ts.map(function(ts, index) { return [ts.replace(' ', 'T'), values[index]]; })
        : values;
      return {
        // 稳定 id：merge 时按 id 认人，保证"这条线还是这条线"，不会张冠李戴
        id: 'pt-' + p.key,
        name: p.label,
        type: 'line',
        xAxisIndex: p.axis,
        yAxisIndex: p.axis,
        data: data,
        smooth: true,
        showSymbol: false,
        connectNulls: false,     // 该窗口没数据就断开，不要拿相邻点连过去
        itemStyle: { color: p.color }
      };
    });

    // 超标点：折算值 > 限值（严格大于，**等于限值算达标**）才在曲线上标红；
    // O2>19% 算不出折算值的点不判、不标。红点画在实测值位置（即曲线本身的那个点上）。
    POINTS.forEach(function(p) {
      if (!ZS_MAP[p.key]) { return; }
      var values = d[p.key] || [];
      var marks = [];
      for (var i = 0; i < values.length; i++) {
        var zs = toConverted(p.key, values[i], o2Column[i]);
        if (zs !== null && zs > ZS_LIMITS[p.key]) {
          var x = useTimeAxis ? d.ts[i].replace(' ', 'T') : d.ts[i];
          marks.push([x, values[i]]);
        }
      }
      overMarks[p.key] = marks;          // 记住最近一帧，图例切换时不用重算
      series.push({
        id: 'over-' + p.key,
        name: p.label + '·超标点',
        type: 'scatter',
        xAxisIndex: p.axis,
        yAxisIndex: p.axis,
        // 对应的曲线被图例关掉时，它的超标点一起收起（否则只剩红点飘在空图上）
        data: hiddenPoints[p.label] ? [] : marks,
        symbolSize: 9,
        z: 10,
        silent: true,
        tooltip: { show: false },    // tooltip 统一走下面的三行格式，散点不单独占行
        itemStyle: { color: COLOR_OVER, borderColor: '#fecaca', borderWidth: 1 }
      });
    });

    var legendSelected = {};
    POINTS.forEach(function(p) { legendSelected[p.label] = !hiddenPoints[p.label]; });

    myChart.setOption({
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#1e293b',
        borderColor: '#334155',
        textStyle: { color: '#e2e8f0' },
        // 三个浓度测点各显示 实测 / 折算 / 限值 三行；折算不可用（O2>19% 等）显示 “—”；
        // 其余测点保持单行。折算值 > 限值时在折算行后用红字注 “（超标）”。
        formatter: function(params) {
          if (!params || !params.length) { return ''; }
          var dataIndex = params[0].dataIndex;
          var html = '<div style="font-weight:600;margin-bottom:4px;">' + d.ts[dataIndex] + '</div>';
          POINTS.forEach(function(p) {
            var dot = '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
              + 'background:' + p.color + ';margin-right:6px;"></span>';
            var indent = '<span style="display:inline-block;width:10px;margin-right:6px;"></span>';
            var measured = (d[p.key] || [])[dataIndex];
            if (ZS_MAP[p.key]) {
              var zs = toConverted(p.key, measured, o2Column[dataIndex]);
              var over = zs !== null && zs > ZS_LIMITS[p.key];
              html += dot + p.label + '　实测：' + fmtNum(measured)
                + (p.unit ? ' ' + p.unit : '') + '<br/>'
                + indent + '折算：' + fmtNum(zs)
                + (over ? '<span style="color:' + COLOR_OVER + ';font-weight:600;">（超标）</span>' : '')
                + '<br/>'
                + indent + '限值：' + ZS_LIMITS[p.key]
                + (p.unit ? ' ' + p.unit : '') + '<br/>';
            } else {
              html += dot + p.label + '：' + fmtNum(measured)
                + (p.unit ? ' ' + p.unit : '') + '<br/>';
            }
          });
          return html;
        }
      },
      legend: {
        data: POINTS.map(function(p) { return p.label; }),
        // 显式把用户的选择带回来：本帧是 merge 时它本来就还在，
        // 本帧是整帧重建（轴类型切换）时靠它把选择恢复回去。
        selected: legendSelected,
        textStyle: { color: '#cbd5e1' },
        type: 'scroll'
      },
      // 面板纵向排布：顶部留给共享图例，底部留给时间轴 + 缩放条
      grid: PANELS.map(function(panel, index) {
        return {
          left: 74, right: 24,
          top: PANEL_TOP + index * (PANEL_H + PANEL_GAP),
          height: PANEL_H, containLabel: false
        };
      }),
      axisPointer: {
        link: [{ xAxisIndex: 'all' }],       // 悬停联动：一条竖线贯穿所有面板
        label: { backgroundColor: '#334155' }
      },
      // 横轴缩放：实时模式下也能拖选一段时间窗，且刷新不会把它重置
      //（不写 start/end —— merge 时才留得住用户的缩放；整帧重建时自然回到全量）
      dataZoom: [
        { type: 'inside', xAxisIndex: ALL_AXES, filterMode: 'none' },
        { type: 'slider', xAxisIndex: ALL_AXES, filterMode: 'none',
          bottom: 8, height: 18, borderColor: '#334155',
          textStyle: { color: '#94a3b8' }, fillerColor: 'rgba(14,165,233,0.18)',
          handleStyle: { color: '#0ea5e9' } }
      ],
      animationDurationUpdate: 300,
      xAxis: xAxis,
      yAxis: yAxis,
      series: series
    }, mustReset);
  }

  function latestText(d) {
    return POINTS.map(function(p) {
      var arr = d[p.key] || [];
      return p.label + '=' + arr[arr.length - 1];
    }).join('  ');
  }

  // 补齐网格后 ts 不为空、但全是 null —— 这种情况也要说"该区间无数据"
  function hasAnyValue(d) {
    return POINTS.some(function(p) {
      return (d[p.key] || []).some(function(v) { return v !== null && v !== undefined; });
    });
  }

  // ---- 实时模式：仍然用 /api/data，按 REFRESH_MS 定时刷新 ----
  function loadRealtime() {
    var token = viewToken;
    fetchWithTimeout(withScope('/api/data'))
      .then(function(res) { return res.json(); })
      .then(function(d) {
        if (token !== viewToken) { return; }      // 已经切走了，这帧作废
        if (d.error) {
          showError('查库失败：' + d.error + (d.trace_id ? '（追踪号 ' + d.trace_id + '）' : ''));
          return;
        }
        if (!d.ts.length) {
          showInfo('暂无数据（确认 gateway 和 subscriber_to_td 在跑）');
          return;
        }
        render(d, false);
        showInfo('设备 ' + scopeLabel() + ' | 实时模式 | 最近 ' + d.ts[d.ts.length - 1] + ' | '
          + latestText(d) + ' | 每' + REFRESH_SEC + '秒自动刷新');
      })
      .catch(function(err) {
        if (token === viewToken) {
          showError(describeFetchError(err));
        }
      })
      .finally(function() {
        // 上一轮结束后才排下一轮（后端变慢不会堆请求）；切到自由区间后不再排，
        // 否则固定窗口的自动刷新会把用户选的区间覆盖掉
        if (token === viewToken && mode === 'realtime') {
          timer = setTimeout(loadRealtime, REFRESH_MS);
        }
      });
  }

  // ---- 自由区间模式：走 /api/curve，一次一查，不自动刷新 ----
  function loadFree() {
    if (busy) { return; }
    // 点「查询」就代表要看自由区间：万一控件被误显在实时模式下，这里也先把轮询停掉，
    // 否则 5 秒后一轮自动刷新会把刚查到的曲线盖回去
    if (mode !== 'free') { applyMode('free'); }
    var start = val('free-start');
    var end = val('free-end');
    if (!start || !end) {
      showError('请先选择起止时间');
      return;
    }
    busy = true;
    document.getElementById('free-query').disabled = true;
    showInfo('查询中…');
    var token = viewToken;
    var url = withScope('/api/curve?start=' + encodeURIComponent(start) + '&end=' + encodeURIComponent(end));
    fetchWithTimeout(url)
      .then(function(res) {
        return res.json().then(function(body) { return { ok: res.ok, body: body }; });
      })
      .then(function(r) {
        if (token !== viewToken) { return; }      // 已经切走了，别用旧结果盖新画面
        if (!r.ok) {
          showError('查询失败：' + (r.body.error || '未知错误') + traceSuffix(r.body));
          return;
        }
        var d = r.body;
        render(d, true);
        if (!d.ts.length || !hasAnyValue(d)) {
          showInfo('该区间无数据（' + d.start + ' ~ ' + d.end + '）');
          return;
        }
        // 必须显式告诉用户画的是原始点还是均值，别让人把均值曲线当原始曲线看
        showInfo(d.start + ' ~ ' + d.end + ' | 粒度：' + d.unit_label + ' | ' + d.points + ' 点'
          + (d.downsampled ? '（已降采样）' : '（原始点，未降采样）'));
      })
      .catch(function(err) {
        if (token === viewToken) {
          showError(describeFetchError(err));
        }
      })
      .finally(function() {
        // busy 要无条件放开：否则被切模式作废的那次请求会把按钮永久锁住
        busy = false;
        document.getElementById('free-query').disabled = false;
      });
  }

  // 只切模式的状态与 UI（不触发查询），供切换按钮和"点查询时兜底"共用
  function applyMode(next) {
    mode = next;
    viewToken += 1;              // 让上一个模式在途的请求作废，避免它回来时闪一下
    if (timer) {                 // 切模式先取消已排的定时器，避免两种模式互相覆盖
      clearTimeout(timer);
      timer = null;
    }
    Array.prototype.forEach.call(document.querySelectorAll('.mode'), function(el) {
      var on = el.getAttribute('data-mode') === next;
      el.className = 'mode' + (on ? ' active' : '');
    });
    document.getElementById('free-controls').hidden = (next !== 'free');
  }

  function setMode(next) {
    applyMode(next);
    if (next === 'realtime') {
      loadRealtime();
    } else {
      initFreeInputs();
      showInfo('自由区间模式：选好起止时间后点「查询」（该模式不自动刷新）');
    }
  }

  document.getElementById('modes').addEventListener('click', function(ev) {
    var target = ev.target.getAttribute && ev.target.getAttribute('data-mode');
    if (target && target !== mode) { setMode(target); }
  });
  document.getElementById('free-query').addEventListener('click', loadFree);
  window.addEventListener('resize', function() { myChart.resize(); });

  loadDevices();
  setMode('realtime');
})();
</script>
</body>
</html>
"""


# ==================== 6. 报表页面（4 类时间维度聚合 · 表格 + 覆盖率面板） ====================

REPORT_PAGE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CEMS 数据报表</title>
<style>
  [hidden] { display:none !important; }   /* 同上：别让 display:flex 盖过 hidden */
  body { margin:0; padding:20px; background:#0f172a; color:#e2e8f0; font-family:sans-serif; }
  h1 { font-size:20px; margin:0 0 4px; }
  .nav { font-size:13px; margin-bottom:14px; }
  .nav a { color:#38bdf8; text-decoration:none; }
  .tabs { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px; }
  .tab { padding:6px 16px; border:1px solid #334155; border-radius:8px; cursor:pointer;
         background:#1e293b; color:#cbd5e1; font-size:13px; user-select:none; }
  .tab.active { background:#0ea5e9; border-color:#0ea5e9; color:#0f172a; font-weight:600; }
  .controls { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom:12px; }
  .controls label { font-size:13px; color:#94a3b8; }
  input, select { background:#1e293b; color:#e2e8f0; border:1px solid #334155;
                  border-radius:6px; padding:5px 8px; font-size:13px; }
  button { background:#0ea5e9; color:#0f172a; border:0; border-radius:6px; padding:6px 16px;
           font-size:13px; font-weight:600; cursor:pointer; }
  button:disabled { background:#475569; color:#94a3b8; cursor:not-allowed; }
  #status { color:#94a3b8; font-size:13px; margin:8px 0; }
  #status.err { color:#f87171; }
  .scroll { max-height:60vh; overflow:auto; border:1px solid #1e293b; border-radius:8px; }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th, td { border-bottom:1px solid #1e293b; padding:4px 8px; text-align:right; white-space:nowrap; }
  th:first-child, td:first-child { text-align:left; position:sticky; left:0; background:#0f172a; }
  th { color:#94a3b8; font-weight:600; position:sticky; top:0; background:#0f172a; }
  /* 覆盖率卡片 */
  .cards { display:flex; gap:10px; flex-wrap:wrap; margin:10px 0 12px; }
  .card { background:#1e293b; border:1px solid #334155; border-radius:8px;
          padding:10px 16px; min-width:120px; }
  .card-label { font-size:12px; color:#94a3b8; margin-bottom:4px; }
  .card-value { font-size:20px; font-weight:600; color:#e2e8f0; }
  .card-value.gray { color:#94a3b8; }
  .card-value.red { color:#f87171; }
  /* 窗口状态三态 + 进行中：达标绿 / 超标红 / 数据不足灰（缺数据绝不画成绿）/ 进行中描边 */
  .badge { display:inline-block; padding:2px 10px; border-radius:999px;
           font-size:12px; font-weight:600; white-space:nowrap; }
  .badge-ok { background:rgba(34,197,94,.15); color:#22c55e; }
  .badge-over { background:rgba(248,113,113,.15); color:#f87171; }
  .badge-ins { background:rgba(148,163,184,.18); color:#94a3b8; }
  .badge-pending { background:transparent; border:1px solid #475569; color:#94a3b8; }
</style>
</head>
<body>
  <h1>CEMS 数据报表 · 时间维度聚合均值</h1>
  <div class="nav"><a href="/">← 返回实时大屏</a></div>

  <div class="tabs" id="tabs">
    <div class="tab" data-tab="minute">分钟</div>
    <div class="tab" data-tab="day">日</div>
    <div class="tab" data-tab="month">月</div>
    <div class="tab" data-tab="custom">自由</div>
  </div>

  <div class="controls">
    <span data-ctl="minute">
      <label>起</label> <input type="datetime-local" id="minute-start">
      <label>止</label> <input type="datetime-local" id="minute-end">
    </span>
    <span data-ctl="day" hidden>
      <label>日期</label> <input type="date" id="day-date">
    </span>
    <span data-ctl="month" hidden>
      <label>年</label> <input type="number" id="month-year" style="width:90px">
      <label>月</label> <select id="month-month"></select>
    </span>
    <span data-ctl="custom" hidden>
      <label>起</label> <input type="datetime-local" id="custom-start">
      <label>止</label> <input type="datetime-local" id="custom-end">
    </span>
    <button id="query">查询</button>
    <button id="refresh">刷新</button>
    <button id="export">导出 Excel</button>
  </div>

  <div id="status">选择时间范围后点「查询」</div>
  <div class="cards" id="coverage" hidden>
    <div class="card"><div class="card-label">整体覆盖率</div>
      <div class="card-value" id="cov-overall">—</div></div>
    <div class="card"><div class="card-label">覆盖率门限</div>
      <div class="card-value" id="cov-threshold">—</div></div>
    <div class="card"><div class="card-label">数据不足窗口数</div>
      <div class="card-value gray" id="cov-insufficient">—</div></div>
    <div class="card"><div class="card-label">断档数</div>
      <div class="card-value red" id="cov-gap">—</div></div>
  </div>
  <div class="scroll">
    <table id="table"><thead></thead><tbody></tbody></table>
  </div>

<script>
(function() {
  // 测点表由后端注入（真源 src/common/points.py），本页不再自己抄一份
  var POINTS = __POINTS_JSON__;
  var statusEl = document.getElementById('status');
  var queryBtn = document.getElementById('query');
  var refreshBtn = document.getElementById('refresh');
  var exportBtn = document.getElementById('export');
  var activeTab = 'minute';
  var busy = false;

  // ---- 折算值 / 超标判据（口径与实时大屏一致：折算 = 实测 × 15 / (21 − O2)，基准氧 6%）----
  var ZS_KEYS = ['dust', 'so2', 'nox'];
  var ZS_LABELS = { dust: '颗粒物', so2: 'SO2', nox: 'NOx' };
  var ZS_LIMITS = { dust: 5, so2: 35, nox: 50 };   // mg/m3
  var O2_FOR_ZS_MAX = 19;                          // O2 > 19% 不折算

  function isFiniteNum(v) {
    return typeof v === 'number' && isFinite(v);
  }

  function toConverted(key, measured, o2) {
    if (!isFiniteNum(measured) || !isFiniteNum(o2)) { return null; }
    if (o2 > O2_FOR_ZS_MAX) { return null; }
    var denominator = 21 - o2;
    if (denominator <= 0) { return null; }
    return measured * 15 / denominator;
  }

  function fmtNum(v) {
    if (!isFiniteNum(v)) { return '—'; }
    return String(Math.round(v * 100) / 100);
  }

  // 窗口状态（三态 + 进行中）。顺序不能乱：
  //   pending（窗口未结束）→ 进行中；
  //   insufficient（覆盖率 < 门限）→ 数据不足（灰），**既不判达标也不判超标**；
  //   任一浓度折算值 > 限值（严格大于，等于算达标）→ 超标（红）；
  //   浓度值全部算不出折算 → 同样按数据不足（灰），没有浓度依据不能给绿；
  //   其余 → 达标（绿）。
  function windowStatus(pt) {
    if (pt.pending) { return { cls: 'badge-pending', text: '进行中' }; }
    if (pt.insufficient) { return { cls: 'badge-ins', text: '数据不足' }; }
    var over = false;
    var judged = 0;
    ZS_KEYS.forEach(function(k) {
      var zs = toConverted(k, pt[k], pt.o2);
      if (zs !== null) {
        judged += 1;
        if (zs > ZS_LIMITS[k]) { over = true; }
      }
    });
    if (over) { return { cls: 'badge-over', text: '超标' }; }
    if (judged === 0) { return { cls: 'badge-ins', text: '数据不足' }; }
    return { cls: 'badge-ok', text: '达标' };
  }

  // 状态徽标悬停提示：逐污染物给出 折算 / 限值，方便核对
  function statusTitle(pt) {
    return ZS_KEYS.map(function(k) {
      return ZS_LABELS[k] + ' 折算 ' + fmtNum(toConverted(k, pt[k], pt.o2))
        + ' / 限值 ' + ZS_LIMITS[k];
    }).join('\\n');
  }

  function pad2(n) { return (n < 10 ? '0' : '') + n; }
  function toLocalInput(d) {
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate())
      + 'T' + pad2(d.getHours()) + ':' + pad2(d.getMinutes());
  }
  function val(id) { return document.getElementById(id).value; }

  function initControls() {
    var now = new Date();
    var HOUR_MS = 3600 * 1000;
    document.getElementById('minute-end').value = toLocalInput(now);
    document.getElementById('minute-start').value = toLocalInput(new Date(now.getTime() - HOUR_MS));
    document.getElementById('custom-end').value = toLocalInput(now);
    document.getElementById('custom-start').value =
      toLocalInput(new Date(now.getTime() - 24 * HOUR_MS));
    document.getElementById('day-date').value =
      now.getFullYear() + '-' + pad2(now.getMonth() + 1) + '-' + pad2(now.getDate());
    document.getElementById('month-year').value = now.getFullYear();
    var sel = document.getElementById('month-month');
    for (var m = 1; m <= 12; m++) {
      var opt = document.createElement('option');
      opt.value = String(m);
      opt.textContent = m + ' 月';
      if (m === now.getMonth() + 1) { opt.selected = true; }
      sel.appendChild(opt);
    }
  }

  // 当前 Tab 对应的查询参数；查询和导出共用，保证"屏幕上看到的 = 导出文件里的"
  function currentParams() {
    var enc = encodeURIComponent;
    if (activeTab === 'minute') {
      return 'start=' + enc(val('minute-start')) + '&end=' + enc(val('minute-end'));
    }
    if (activeTab === 'day') {
      return 'date=' + enc(val('day-date'));
    }
    if (activeTab === 'month') {
      return 'year=' + enc(val('month-year')) + '&month=' + enc(val('month-month'));
    }
    return 'start=' + enc(val('custom-start')) + '&end=' + enc(val('custom-end'));
  }

  function buildUrl() {
    return '/api/report/' + activeTab + '?' + currentParams();
  }

  function buildExportUrl() {
    return '/api/report/export?type=' + activeTab + '&' + currentParams();
  }

  function showStatus(msg, isErr) {
    statusEl.textContent = msg;
    statusEl.className = isErr ? 'err' : '';
  }

  // 查询/导出期间禁用全部按钮，并按动作显示"查询中…"/"导出中…"
  function setBusy(on, action) {
    busy = on;
    queryBtn.disabled = on;
    refreshBtn.disabled = on;
    exportBtn.disabled = on;
    queryBtn.textContent = (on && action === 'query') ? '查询中…' : '查询';
    exportBtn.textContent = (on && action === 'export') ? '导出中…' : '导出 Excel';
  }

  // 表格是唯一主视图：窗口内没数据的测点显示 —（不能显示 0，否则和"值就是 0"混淆）
  function renderTable(points) {
    var head = '<tr><th>时间</th>';
    POINTS.forEach(function(p) {
      head += '<th>' + p.label + (p.unit ? ' (' + p.unit + ')' : '') + '</th>';
    });
    head += '<th>采样条数</th><th>状态</th></tr>';
    document.querySelector('#table thead').innerHTML = head;

    var rows = '';
    points.forEach(function(pt) {
      rows += '<tr><td>' + pt.ts + '</td>';
      POINTS.forEach(function(p) {
        var v = pt[p.key];
        rows += '<td>' + (v === null || v === undefined ? '—' : v) + '</td>';
      });
      rows += '<td>' + (pt.n === null || pt.n === undefined ? '—' : pt.n) + '</td>';
      var st = windowStatus(pt);
      rows += '<td style="text-align:center;"><span class="badge ' + st.cls
        + '" title="' + statusTitle(pt) + '">' + st.text + '</span></td></tr>';
    });
    document.querySelector('#table tbody').innerHTML = rows;
  }

  // 覆盖率卡片：overall/threshold 按百分比显示（overall 为 null 时显示 —），
  // insufficient_windows / gap_count 直接显示接口给的计数，不重算。
  function pctText(v) {
    return isFiniteNum(v) ? (v * 100).toFixed(1) + '%' : '—';
  }
  function countText(v) {
    return (v === null || v === undefined) ? '—' : String(v);
  }
  function renderCoverage(cov) {
    var el = document.getElementById('coverage');
    if (!cov) { el.hidden = true; return; }
    el.hidden = false;
    document.getElementById('cov-overall').textContent = pctText(cov.overall);
    document.getElementById('cov-threshold').textContent = pctText(cov.threshold);
    document.getElementById('cov-insufficient').textContent =
      countText(cov.insufficient_windows);
    document.getElementById('cov-gap').textContent = countText(cov.gap_count);
  }

  function query() {
    if (busy) { return; }
    setBusy(true, 'query');
    showStatus('查询中…', false);
    fetch(buildUrl())
      .then(function(res) {
        return res.json().then(function(body) { return { ok: res.ok, body: body }; });
      })
      .then(function(r) {
        if (!r.ok) {
          var extra = r.body.trace_id ? '（追踪号 ' + r.body.trace_id + '）' : '';
          showStatus('查询失败：' + (r.body.error || '未知错误') + extra, true);
          return;
        }
        var points = r.body.points || [];
        renderTable(points);
        renderCoverage(r.body.coverage);
        if (points.length) {
          showStatus('共 ' + points.length + ' 个窗口（' + r.body.unit + '）　'
            + r.body.start + ' ~ ' + r.body.end, false);
        } else {
          showStatus('该时间范围内没有数据（' + r.body.start + ' ~ ' + r.body.end + '）', false);
        }
      })
      .catch(function() {
        showStatus('后端连不上（确认 web_dashboard.py 在跑）', true);
      })
      .finally(function() { setBusy(false); });
  }

  // 导出复用当前参数：屏幕上看到的窗口范围，就是导出文件里的范围
  function exportExcel() {
    if (busy) { return; }
    setBusy(true, 'export');
    showStatus('导出中…', false);
    fetch(buildExportUrl())
      .then(function(res) {
        if (!res.ok) {
          // 参数非法/查库失败都是 JSON（不是 xlsx），按错误展示，绝不当文件下载
          return res.json()
            .then(function(body) {
              var extra = body.trace_id ? '（追踪号 ' + body.trace_id + '）' : '';
              showStatus('导出失败：' + (body.error || '未知错误') + extra, true);
            })
            .catch(function() { showStatus('导出失败：HTTP ' + res.status, true); });
        }
        var disposition = res.headers.get('Content-Disposition') || '';
        return res.blob().then(function(blob) { saveBlob(blob, filenameFrom(disposition)); });
      })
      .catch(function() {
        showStatus('后端连不上（确认 web_dashboard.py 在跑）', true);
      })
      .finally(function() { setBusy(false); });
  }

  // 中文文件名在 Content-Disposition 的 filename* 里（RFC 5987），要解码再用
  function filenameFrom(disposition) {
    var matched = /filename\\*=UTF-8''([^;]+)/i.exec(disposition);
    if (matched) { return decodeURIComponent(matched[1]); }
    var plain = /filename="([^"]+)"/i.exec(disposition);
    return plain ? plain[1] : 'CEMS_report.xlsx';
  }

  function saveBlob(blob, filename) {
    var url = URL.createObjectURL(blob);
    var link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    showStatus('已导出：' + filename, false);
  }

  function switchTab(name) {
    activeTab = name;
    Array.prototype.forEach.call(document.querySelectorAll('.tab'), function(el) {
      var on = el.getAttribute('data-tab') === name;
      el.className = 'tab' + (on ? ' active' : '');
    });
    Array.prototype.forEach.call(document.querySelectorAll('[data-ctl]'), function(el) {
      el.hidden = el.getAttribute('data-ctl') !== name;
    });
    query();
  }

  document.getElementById('tabs').addEventListener('click', function(ev) {
    var tab = ev.target.getAttribute && ev.target.getAttribute('data-tab');
    if (tab) { switchTab(tab); }
  });
  queryBtn.addEventListener('click', query);
  refreshBtn.addEventListener('click', query);   // 手动刷新：不自动轮询
  exportBtn.addEventListener('click', exportExcel);

  initControls();
  switchTab('minute');
})();
</script>
</body>
</html>
"""


# ==================== 6. 主流程 ====================

def main() -> None:
    """启动 Web 服务（阻塞运行）。"""
    setup_logging()
    LOGGER.info("Web 大屏启动: http://localhost:%d （局域网: http://<本机IP>:%d）", WEB_PORT, WEB_PORT)
    LOGGER.info("数据源: %s 库=%s 超级表=%s 最近 %d 分钟", TD_URL, TD_DB, TD_STABLE, QUERY_MINUTES)
    # 缓存层的设备维度：cache.py 在**导入时**已经算好，但那会儿日志系统还没配置（setup_logging 在
    # 这个函数里才跑），导入时的 INFO 记录会被丢弃 —— 所以在这里补一次，保证 `docker logs` 里能看见
    # "这台 web 给哪台设备做缓存"（多设备部署排查时第一个要确认的就是它）。
    LOGGER.info("查询缓存设备维度: %s（缓存键按设备隔离，见 src/web/cache.py）", cache.DEVICE_SCOPE)
    try:
        serve_forever()
    except OSError as exc:
        # 端口被占/无权限属于启动失败：必须以非 0 退出码结束，
        # 否则编排层（compose / 脚本）看到的是"正常退出"，不会告警也不会重启
        LOGGER.critical("Web 服务启动失败（端口 %d 可能被占用）: %s", WEB_PORT, exc)
        raise SystemExit(1) from exc
    except Exception:
        LOGGER.exception("Web 服务异常退出")
        raise SystemExit(1)
    finally:
        LOGGER.info("Web 大屏已停止")


def serve_forever() -> None:
    """用 waitress（生产级 WSGI 服务器）起服务；没装时回落到 Flask 自带服务器。

    Flask 自带的 werkzeug 开发服务器会在日志里警告"不要用于生产"，
    而且没有正经的并发处理；因此它只用于本机开发自测，
    对外提供服务（含容器编排部署）时必须走 waitress。
    """
    try:
        from waitress import serve as waitress_serve
    except ImportError:
        LOGGER.warning("未安装 waitress，回落到 Flask 开发服务器（仅供本机开发自测，勿用于对外服务）")
        app.run(host=WEB_HOST, port=WEB_PORT, debug=False, threaded=True)
        return
    LOGGER.info("使用 waitress 启动（生产级 WSGI，线程数 %d）", WEB_THREADS)
    waitress_serve(app, host=WEB_HOST, port=WEB_PORT, threads=WEB_THREADS, ident="cems-web")


if __name__ == "__main__":
    main()
