# -*- coding: utf-8 -*-
"""Web 展示大屏：Flask 提供 TDengine 近段数据接口，前端用 ECharts 画各烟气测点的实时曲线。

测点数不写死：页面标题与曲线都用 points.py 的契约现算。
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Final, Optional
from urllib.parse import quote

import taosrest
from flask import Flask, Response, jsonify, redirect, request
from werkzeug.exceptions import HTTPException

# 让 src/common 能被导入：三种启动方式都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.alarm_judge import AlarmConfig   # noqa: E402
from src.common.logging import setup_logging   # noqa: E402
from src.common.points import COLUMNS   # noqa: E402
from src.common.points import O2_DRY_BASIS, O2_REFERENCE, ZS_TARGETS   # noqa: E402
from src.common.points import POINTS as CONTRACT_POINTS   # noqa: E402
from src.web import report   # noqa: E402
from src.web import report_export   # noqa: E402
from src.web import cache   # noqa: E402
from src.web import curve   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================
# 连接参数支持环境变量覆盖；容器化部署时由 docker-compose.yml 注入服务名。

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = os.getenv("TD_URL", "http://localhost:6041")
TD_USER: Final[str] = os.getenv("TD_USER", "root")
TD_PASS: Final[str] = os.getenv("TD_PASS", "taosdata")
TD_DB: Final[str] = os.getenv("TD_DB", "cems")
TD_STABLE: Final[str] = os.getenv("TD_STABLE", "cems_data")

# ---- 测点列 ----
# 列名与顺序统一来自 src/common/points.py，这里不再抄一份。
POINTS: Final[tuple[str, ...]] = COLUMNS

# ---- 图表面板（small multiples）：一个量纲一根轴 ----
# 9 个测量程差 3 个数量级（流量 0~65000、压力 85~105），塞同一根轴小量程的会被压成直线，
# 双重 Y 轴又容易误读。所以按量纲拆成多个上下排列的面板，每格一根自己的 Y 轴。
# 分组依据是单位 + 量程数量级。
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
    """生成前端用的测点呈现配置（JSON），两个页面共用；缺配置直接报错。"""
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


def build_contract_consts_json() -> str:
    """生成前端要用的契约常量（限值 / 基准氧 / 折算分母下限），避免前端手抄。

    为什么必须注入而不是让前端写死：这些数字抄一份就不会跟着契约变。
    改基准氧（不同炉型不同）或改分母下限时，后端改了、前端还留着老数字，
    结果就是"后端判超标、前端显示达标"—— 和"折算值落库会漂"是同一类失效。

      限值      ← src/common/points.py 的 Point.limit（只取 ZS_TARGETS 三个折算对象）
      基准氧/干基氧 ← 同上的 O2_REFERENCE / O2_DRY_BASIS
      分母下限  ← alarm_judge 的判定门限，与 (O2_DRY_BASIS - o2) < 它 用同一个值
    """
    return json.dumps({
        "zsLimits": {p.column: p.limit for p in CONTRACT_POINTS if p.column in ZS_TARGETS},
        "zsKeys": list(ZS_TARGETS),
        "o2DryBasis": O2_DRY_BASIS,
        "o2Reference": O2_REFERENCE,
        "o2DenomMin": AlarmConfig.from_env().o2_denominator_min,
    }, ensure_ascii=False)


CONTRACT_CONSTS_JSON: Final[str] = build_contract_consts_json()


def build_panels_json() -> str:
    """生成前端用的面板定义（JSON）。契约里的测点必须恰好落在某一个面板里，否则报错。"""
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

# ---- 健康判据（GET /api/health）----
# 判据是数据新鲜度，不是"库连得上"。原来只看连接，于是网关断/补传堵/不入库/
# 线程死/tag拼错恒0行 这些故障全 200+ok、容器 healthy、无告警。
# 阈值 = 轮询周期 × 因子（默认 5.0s × 3 = 15s）：允许连续丢 2 条不误报。
# 周期真源是网关的 POLL_INTERVAL（单设备/双设备实测都在 11~12 条/分钟 ≈ 5s 一条），
# web 侧的覆盖率口径（src/web/report.py）已经在用同一个变量，这里沿用同一个。
# 因子取 3 的依据：本机实测间隔均值 5.15s，15s 有 2.9 倍余量（见
# docs/reference/性能与可靠性指标.md §4.2）；同时远小于"人工发现不了"的量级。
POLL_INTERVAL_SECONDS: Final[float] = float(os.getenv("POLL_INTERVAL", "5.0"))
HEALTH_STALE_POLL_FACTOR: Final[int] = int(os.getenv("HEALTH_STALE_POLL_FACTOR", "3"))
HEALTH_STALE_AFTER_SECONDS: Final[float] = POLL_INTERVAL_SECONDS * HEALTH_STALE_POLL_FACTOR

# ---- 数据新鲜度指示（页面上"最后一条数据：N 秒前"）----
# 这个阈值和 HEALTH_STALE_AFTER_SECONDS 刻意不同：
#   /api/health 是编排层判据，超了要让容器 unhealthy → 必须敏感（15s）；
#   页面指示是给人看的，真机偶发补传滞后（最大 32.1s）就闪红会让人被训练成"红色=正常抖动"。
#   所以展示层阈值更宽，默认 30s。
FRESHNESS_WARN_SECONDS: Final[float] = float(os.getenv("FRESHNESS_WARN_SECONDS", "30"))

# ---- "连续 N 分钟无入库"的主动告警（2026-10-11 新增）----
# 与 FRESHNESS_WARN_SECONDS 的分工（别合并）：
#   FRESHNESS_WARN_SECONDS（30s）= 展示阈值——大屏胶囊变红，给人看；
#   STALE_ALERT_SECONDS（300s）= 告警阈值——连续停流超它才升 CRITICAL 日志。
# 有了红色胶囊只是"看得见"；"全绿零数据"最危险的是没人盯大屏。
# 这个计时器把"看得见"变成"叫得出来"：停流超阈值往 CRITICAL 打一行，谁收日志都能被叫醒。
# 状态是进程内的（web 单实例够用）；容器重启清零——重启本身就是新观测起点。
STALE_ALERT_SECONDS: Final[float] = float(os.getenv("STALE_ALERT_SECONDS", "300"))

#: 本轮"连续不健康"的起始时刻（健康一次就清空）。
_stale_since: Optional[datetime] = None

# ---- 真机有效性判据（2026-10-22）----
# 原来的 /api/health 只判"数据还在流"。真机失效时链路**照常流**：
#   device 读到的是越界哨兵（101/301）→ 网关照常 publish（read_device 不是 None）
#   → 接入层拒收那两个测点（库里只是少两列）→ 停流判据完全看不出来。
# 实测：485 断开 20 分钟，NRestarts=0、/api/health 全绿、小时结论 ok，只有日志和
# cems_point_invalid 表留痕。这条判据把"留痕"变成"接口与日志都能看见"。
#
# 口径：查 cems_point_invalid 最近 INVALID_WINDOW_SECONDS 秒的行数。
# 正常时该表 0 行；两个测点每 5 s 各记一条，失效时约 24 条/分钟。
# 阈值 12 ≈ 失效 30 秒。查不到表/库不可用时返回 None（不把 health 判死）。
#
# ⚠️ **不改变 healthy 的语义**：healthy 仍表示"链路在流"，sensor_ok 表示"真机读数有效"。
#    两者分开是有意的 —— nginx 的 depends_on 是 web healthy，真机坏了不该让展示层起不来。
INVALID_WINDOW_SECONDS: Final[int] = int(os.getenv("INVALID_WINDOW_SECONDS", "300"))
INVALID_POINTS_THRESHOLD: Final[int] = int(os.getenv("INVALID_POINTS_THRESHOLD", "12"))
INVALID_SENSOR_ALERT_SECONDS: Final[float] = float(os.getenv("INVALID_SENSOR_ALERT_SECONDS", "300"))
#: 无效测点表名（接入层写入的那张证据表；env 可覆盖，与 POINT_INVALID_STABLE 同名）
POINT_INVALID_STABLE: Final[str] = os.getenv("POINT_INVALID_STABLE", "cems_point_invalid")

#: 真机第一次被判"无效"的时刻（恢复即清空）
_sensor_invalid_since: Optional[datetime] = None
#: 上一次打 CRITICAL 的时刻（限流：持续无效时每 300 s 提醒一次，不刷屏）
_sensor_critical_at: Optional[datetime] = None

# ---- 报表导出 ----
XLSX_MIME: Final[str] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
# 给老浏览器用的 ASCII 兜底名；中文名走 Content-Disposition 的 filename*
XLSX_ASCII_NAME: Final[str] = "CEMS_report.xlsx"

# ---- 日志 ----
# 级别/格式见 src/common/logging.py；这里只留本进程的 LOGGER。
LOGGER: Final[logging.Logger] = logging.getLogger("web.dashboard")


# ==================== 2. 日志与异常 ====================
# setup_logging() 已提取到 src/common/logging.py，4 个入口共用同一份实现。


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

    两个约束：
      ts <= now —— 库里一旦有"未来"的脏数据（时钟跳变），只写 ts >= now-Nm 会全捞回来；
      LIMIT     —— 查询结果不能无上限膨胀。
    取数用"倒序 + LIMIT"再翻转，保证截断时留下的是最新 N 个点。

    按 plant/device tag 过滤：超级表多设备共用，不过滤会把两台设备的点混排成一条曲线。
    这条读路径同时是缓存水位的观测源。
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


def count_invalid_points(seconds: int = INVALID_WINDOW_SECONDS) -> Optional[int]:
    """查最近 N 秒的无效测点事件数；表不存在或库不可用时返回 None。

    这条查询**只服务健康判定**，失败不抛 TdQueryError：一张还不存在的证据表不该让
    /api/health 变成 503（那样会把"没配好"误报成"链路坏了"）。
    """
    conn: Any = None
    try:
        conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM {TD_DB}.{POINT_INVALID_STABLE} "
            f"WHERE ts >= now - {int(seconds)}s"
        )
        row = cur.fetchone()
    except Exception as exc:   # noqa: BLE001 - 见 docstring：宁可返回 None 也不判死 health
        LOGGER.warning("查询无效测点表失败（忽略，按 None 处理）: %s", exc)
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:   # noqa: BLE001
                LOGGER.debug("关闭 TDengine 连接时出错（忽略）: %s", exc)
    try:
        return int(row[0])
    except (TypeError, ValueError, IndexError):
        return None


def sensor_invalid_verdict(count: Optional[int]) -> dict[str, Any]:
    """把"最近 N 秒无效测点数"转成健康字段，并按持续时间升级日志。

    - count is None      → sensor_ok=None（无法判定，不当成故障）
    - count < 阈值       → sensor_ok=True，恢复时打一条 INFO 并清空计时
    - count >= 阈值      → sensor_ok=False，首次 WARNING；持续 ≥ INVALID_SENSOR_ALERT_SECONDS
                           之后每 300 s CRITICAL（限流，不刷屏）
    """
    global _sensor_invalid_since, _sensor_critical_at
    if count is None:
        return {"sensor_ok": None, "sensor_reason": "invalid_count_unavailable"}

    if count < INVALID_POINTS_THRESHOLD:
        if _sensor_invalid_since is not None:
            LOGGER.info(
                "真机读数恢复：最近 %d s 无效测点 %d 条（阈值 %d）",
                INVALID_WINDOW_SECONDS, count, INVALID_POINTS_THRESHOLD,
            )
        _sensor_invalid_since = None
        _sensor_critical_at = None
        return {"sensor_ok": True, "sensor_reason": "valid", "invalid_points_last_window": count}

    now = datetime.now()
    if _sensor_invalid_since is None:
        _sensor_invalid_since = now
        LOGGER.warning(
            "真机读数异常：最近 %d s 无效测点 %d 条（阈值 %d）—— 链路仍在流，但真机的"
            "Temp/Humidity 两个测点无效（设备层在写越界值、接入层已拒收）。"
            "排查顺序：① 485 的 USB/接线/供电 ② device 进程日志里的串口读失败 ③ 从站号与波特率",
            INVALID_WINDOW_SECONDS, count, INVALID_POINTS_THRESHOLD,
        )
    invalid_for = (now - _sensor_invalid_since).total_seconds()
    if invalid_for >= INVALID_SENSOR_ALERT_SECONDS and (
        _sensor_critical_at is None or (now - _sensor_critical_at).total_seconds() >= 300
    ):
        _sensor_critical_at = now
        LOGGER.critical(
            "⛔ 真机读数已连续 %.1f 分钟无效（最近 %d s 无效测点 %d 条）—— "
            "链路在流、库里只是少两列，但**真实物理量已经没了**。"
            "这不是抖动：请检查 485 接线/供电/串口占用。",
            invalid_for / 60.0, INVALID_WINDOW_SECONDS, count,
        )
    return {
        "sensor_ok": False,
        "sensor_reason": "invalid_points",
        "invalid_points_last_window": count,
        "sensor_invalid_for_seconds": round(invalid_for, 1),
        "sensor_alert_after_seconds": INVALID_SENSOR_ALERT_SECONDS,
    }


# ==================== 4. 接口 ====================

app = Flask(__name__)


def request_scope() -> str:
    """从查询参数取设备维度（?plant=&device=），返回 plant/device 串。

    - 都不传 ⇒ 本实例配置的那台设备；
    - 只传 device ⇒ plant 用本实例配置值补齐；
    - 非法值抛 ValueError，由路由翻成 400——不静默回落：
      静默回落会让"我想看 device2，页面却给了 device1 的数据"这种错看不出来。
    """
    return cache.scope_from_request(request.args.get("plant"), request.args.get("device"))


def device_label(plant: str, device: str) -> str:
    """页面上显示的设备名：仿真环境里 plant1/device1 -> 1号炉。

    只改显示，不动 tag：库里/接口里仍是 plant1/device1。tag 是数据归属，
    SQL 过滤、缓存键都按它走；页面上的名字是给人看的。
    规则：device 末尾数字 n -> {n}号炉；匹配不上就原样显示，不吞信息。
    """
    matched = re.search(r"(\d+)\s*$", device or "")
    if matched:
        return f"{int(matched.group(1))}号炉"
    return f"{plant}/{device}"


def query_devices() -> list[dict[str, str]]:
    """库里有数据的全部 (plant, device)，供页面设备选择器用。

    用 SELECT DISTINCT plant, device 而不是 SHOW TABLES：
    子表会留下空壳，按行 DISTINCT 只返回真正有数据的设备。
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
    devices = [
        {"plant": str(row[0]), "device": str(row[1]),
         "label": device_label(str(row[0]), str(row[1]))}
        for row in rows
    ]
    # 本实例配置的那台排在最前，其余按名称稳定排序——选择器顺序别每次刷新都跳
    # 设备维度的配置在 cache（TD_PLANT/TD_DEVICE），这里不要另读环境变量
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

    这个接口没有缓存（按定义包含当前秒）。它对缓存层的唯一作用是顺带喂两个水位：
      - data_ts：最新数据时间戳 → 判断历史窗口是否已写完
      - 分钟最大值：每个分钟观测到的最晚一条 → 判断哪个窗口内容变过，变了就作废缓存
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
        # 不能先按"是否比边界旧"过滤：分钟最大值必须覆盖每一个观测到的分钟。
        # 先前加了这道过滤，窗口里新来的那条恰好比边界新就被丢掉，该分钟最大值不变，
        # 缓存不失效。不做过滤也不会误杀：比边界新的分钟本来就不满足"窗口已闭合"。
        cache.note_minute_max(timestamps, scope)

    payload: dict[str, Any] = {"ts": [str(row[0]) for row in rows],   # 时间轴
                              "scope": scope}                      # 这两个数来自哪台设备
    for index, name in enumerate(POINTS, start=1):                    # 各测点序列
        payload[name] = [row[index] for row in rows]
    return jsonify(payload)


def latest_sample_age_seconds(rows: list[tuple[Any, ...]]) -> Optional[float]:
    """最近一条样本的时间戳距离"现在"多少秒（行里没有可解析时间戳时返回 None）。

    时间戳统一走 cache.ts_text() + cache.parse_ts_text()，与缓存水位口径同源。
    直接拿 datetime 相减会在 aware datetime 上抛 TypeError，所以先归一到本地串。
    """
    if not rows:
        return None
    moment = cache.parse_ts_text(cache.ts_text(rows[-1][0]))
    if moment is None:
        return None
    return (datetime.now() - moment).total_seconds()


@app.route("/api/health")
def api_health() -> tuple[Response, int] | Response:
    """接口2：健康检查——判据是数据新鲜度 + 真机有效性，不是"库连得上"。

    原来的判据只探"能不能连上库"，导致网关 MQTT 断/补传堵/不入库/线程死/tag拼错
    全部 200 + 容器 healthy + 无告警。现在两条判据任一不满足即 503：
      ① rows_last_1min == 0          → reason=no_rows_last_1min
      ② 最近一条样本距今 > 阈值       → reason=stale_data
    查库失败仍是 503（reason=td_unreachable）。

    ③ 真机失效（新增 2026-10-22）：最近 INVALID_WINDOW_SECONDS 秒的无效测点数
       ≥ INVALID_POINTS_THRESHOLD ⇒ sensor_ok=false（**healthy 仍为 true**：
       链路在流）。持续的无效会按 INVALID_SENSOR_ALERT_SECONDS 升级到 CRITICAL 日志。
    """
    global _stale_since   # "连续停流"计时（见 STALE_ALERT_SECONDS）
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
    # 真机有效性（区别于"链路在流"）：见文件上方 INVALID_* 与 sensor_invalid_verdict 的注释
    invalid_count = count_invalid_points()
    payload["invalid_points_window_seconds"] = INVALID_WINDOW_SECONDS
    payload["invalid_points_threshold"] = INVALID_POINTS_THRESHOLD
    if not rows:
        reason = "no_rows_last_1min"
    elif age is None:
        reason = "unparsable_ts"
    elif age > threshold:
        reason = "stale_data"
    else:
        # 恢复 ⇒ 清掉停流计时：下一次不健康从这一刻重新算
        _stale_since = None
        payload.update({"ok": True, "healthy": True, "reason": "fresh"})
        # healthy=链路在流；sensor_ok=真机读数有效（真机坏了不该让健康接口整体 503，
        # 否则 nginx 的 depends_on=web healthy 会因为一个传感器起不来）
        payload.update(sensor_invalid_verdict(invalid_count))
        return jsonify(payload)

    # ---- 不健康分支：维护"连续停流"计时，超阈值升到 CRITICAL ----
    now = datetime.now()
    if _stale_since is None:
        _stale_since = now
    stale_for = (now - _stale_since).total_seconds()
    payload.update({
        "ok": False,
        "healthy": False,
        "reason": reason,
        "stale_for_seconds": round(stale_for, 1),
        "stale_alert_after_seconds": STALE_ALERT_SECONDS,
    })
    if stale_for >= STALE_ALERT_SECONDS:
        # 主动告警：停流超阈值，按五层顺序排查
        LOGGER.critical(
            "⛔ 连续 %.1f 分钟无有效入库（告警阈值 %.0f s；reason=%s；最近 1 分钟 %d 条）"
            " —— 这不是抖动，是**停流**。按五层顺序排查："
            "① 设备层(device) ② 网关上/下行 ③ broker(emqx) ④ 接入层(subscriber) ⑤ 库(tdengine)",
            stale_for / 60.0, STALE_ALERT_SECONDS, reason, payload["rows_last_1min"],
        )
    else:
        LOGGER.warning(
            "健康检查不通过：reason=%s rows_last_1min=%d data_age_seconds=%s 阈值=%ss"
            "（已连续 %.0f s；到 %.0f s 仍未恢复就报 CRITICAL）",
            reason, payload["rows_last_1min"], payload["data_age_seconds"], threshold,
            stale_for, STALE_ALERT_SECONDS,
        )
    return jsonify(payload), 503


@app.route("/api/cache/stats")
def api_cache_stats() -> Response:
    """接口2b：缓存统计（命中率 / 查库次数 / 水位）—— 仅供测量与排障。

    nginx 把 /api/cache/ 整个路径 return 403，不对公网暴露内部计数。
    统计不按设备拆：各实例共用同一 Redis DB，拆开反而读不出全局健康度。
    """
    return jsonify(cache.stats())


@app.route("/api/cache/clear", methods=["POST"])
def api_cache_clear() -> Response:
    """接口2c：清空全部查询缓存（演练/排障用）。返回删除条数。

    清理不按设备拆：语义是把缓存层清干净，缓存可随时重建。
    只清一半会留下别的设备的条目，制造"清了但还在"的假象。
    """
    return jsonify(cache.clear())


def _render_page(template: str) -> Response:
    """页面模板统一注入：刷新周期 / 测点表 / 面板表 / 测点数 / 新鲜度阈值（都是契约现算）。"""
    html = (
        template
        .replace("__REFRESH_MS__", str(REFRESH_SECONDS * 1000))
        .replace("__REFRESH_SEC__", str(REFRESH_SECONDS))
        .replace("__POINTS_JSON__", POINT_VIEWS_JSON)
        .replace("__PANELS_JSON__", PANELS_JSON)
        .replace("__CONTRACT_CONSTS_JSON__", CONTRACT_CONSTS_JSON)
        .replace("__POINT_COUNT__", str(len(POINTS)))
        # 新鲜度阈值只注入一个数：判据实现留在服务端，前端只负责上色
        .replace("__FRESHNESS_WARN__", str(FRESHNESS_WARN_SECONDS))
    )
    return Response(html, mimetype="text/html")


@app.route("/")
def index() -> Response:
    """首页：监测数据表格（默认视图）。

    视图切换在左上角按钮（/chart）——表格是"按窗口看数"，曲线是"看形状"，
    两者共用同一套查询口径（/api/report/range）。
    """
    return _render_page(TABLE_PAGE)


@app.route("/chart")
def chart_page() -> Response:
    """曲线视图（ECharts 多面板小倍数图）。由首页左上角的按钮进入，可切回表格。"""
    return _render_page(HTML_PAGE)


# ==================== 5. 报表接口（分钟/日/月/自由） ====================
# 聚合全部在 TDengine 侧用 INTERVAL + AVG 完成，见 src/web/report.py

def report_response(
    where: str,
    builder: Callable[[], dict[str, Any]],
) -> tuple[Response, int] | Response:
    """报表接口统一出口：参数错 400，查库错走 safe_error 转 503。"""
    try:
        return jsonify(builder())
    except ValueError as exc:
        # 设备维度不合法与时间参数不合法同一层级：都是 400
        LOGGER.warning("[%s] 参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except report.ReportParamError as exc:
        LOGGER.warning("[%s] 参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except report.ReportQueryError as exc:
        # 查库失败统一 503；safe_error 只负责脱敏与记日志
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


@app.route("/api/report/range")
def api_report_range() -> tuple[Response, int] | Response:
    """区间报表（页面表格/曲线共用）：区间由 start/end 给，粒度由 unit=1m|1h|1d 给。"""
    return report_response(
        "GET /api/report/range",
        lambda: report.range_report(
            request.args.get("start"), request.args.get("end"),
            request.args.get("unit") or "1h", request_scope()),
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
    """老入口：报表页已改到首页（/），这里只做重定向，保证旧链接不断。"""
    return redirect("/")


@app.route("/api/curve")
def api_curve() -> tuple[Response, int] | Response:
    """自由区间曲线：按跨度自动分级降采样，返回结构与 /api/data 一致 + 粒度元信息。

    实时模式仍然用 /api/data；长区间必须走这里，
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
        _scope = request_scope()
        content, filename = report_export.export_workbook(
            request.args, _scope, device_label(*cache.scope_split(_scope)))
    except ValueError as exc:
        LOGGER.warning("[%s] 设备参数不合法: %s", where, exc)
        return jsonify({"error": str(exc)}), 400
    except (report_export.ExportParamError, report.ReportParamError) as exc:
        # 参数问题返回 400 + JSON，不能把错误信息塞进 xlsx 里让用户下载
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
        # 404/405 是正常 HTTP 语义，不是服务故障，不该记 error 也不该变成 500
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
  #chart { width:100%; height:64vh; background:#0f172a; }
  .legendbar { display:flex; align-items:center; flex-wrap:wrap; gap:14px; margin:6px 0 8px;
               font-size:13px; color:#cbd5e1; }
  .legendbar .lg { cursor:pointer; user-select:none; display:inline-flex; align-items:center;
                   gap:5px; }
  .legendbar .lg.off { color:#475569; }
  .legendbar .lg i { width:10px; height:10px; border-radius:50%; display:inline-block; }
  .legendbar .lg.off i { opacity:.35; }
  .legendbar .lgbtn { background:#1e293b; color:#cbd5e1; border:1px solid #334155;
                      border-radius:6px; padding:3px 10px; font-size:12px; cursor:pointer; }
  .legendbar #legend-items { display:inline-flex; flex-wrap:wrap; gap:14px; }
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
  /* 数据新鲜度指示：绿色=在流；超过 __FRESHNESS_WARN__ 秒变红加粗 —— 见 §5 的 loadFreshness */
  .fresh { display:inline-block; margin-left:12px; padding:4px 12px; border-radius:999px;
           font-size:13px; font-weight:600; border:1px solid transparent; vertical-align:middle; }
  .fresh.ok { background:#052e1a; color:#4ade80; border-color:#15803d; }
  .fresh.bad { background:#450a0a; color:#fca5a5; border-color:#b91c1c; font-weight:700; }
  .fresh.unknown { background:#1e293b; color:#94a3b8; border-color:#334155; }
</style>
</head>
<body>
  <h1>CEMS 烟气在线监测 · 曲线（__POINT_COUNT__ 测点）<span class="fresh unknown" id="fresh">数据新鲜度：加载中…</span></h1>
  <div style="font-size:13px;margin-bottom:10px;">
    <a href="/" style="color:#38bdf8;text-decoration:none;">☰ 返回表格视图</a>
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
  <div class="legendbar">
    <span id="legend-items"></span>
    <button class="lgbtn" id="lg-all">全选</button>
    <button class="lgbtn" id="lg-invert">反选</button>
  </div>
  <div id="chart"></div>
  <div id="status">加载中...</div>

<script>
(function() {
  var chartEl = document.getElementById('chart');
  var status = document.getElementById('status');
  var REFRESH_MS = __REFRESH_MS__;
  var REFRESH_SEC = __REFRESH_SEC__;
  // 单次请求超时：fetch 默认不会超时，请求卡住时 finally 不执行、轮询会悄悄停掉
  var FETCH_TIMEOUT_MS = 15000;
  var AXIS_STYLE = { color: '#94a3b8' };
  var COLOR_OVER = '#f87171';
  function isFiniteNum(v) {
    return typeof v === 'number' && isFinite(v);
  }
  function fmtNum(v) {
    if (v === null || v === undefined || !isFinite(v)) { return '—'; }
    return String(Math.round(v * 100) / 100);
  }

  // 测点表与面板表由后端注入（真源是 src/common/points.py + CHART_PANELS / POINT_PRESENTATION）
  // p.axis 现在是**面板下标**：每个面板一格、一根自己的 Y 轴（见 CHART_PANELS 的注释）
  var POINTS = __POINTS_JSON__;
  var PANELS = __PANELS_JSON__;
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
  var lastFrame = null;      // 最近一帧的数据，勾选变化时按同一份数据重画（不重新查库）
  // 视图代次：切模式就 +1。在途请求回来时若代次对不上，说明它属于上一个模式，
  // 必须整帧丢弃 —— 否则上一轮实时请求回来会把自由区间的画面和状态栏盖回去（表现为"闪一下"）
  var viewToken = 0;

  // ---- 顶部自绘图例：一行 9 个测点名（选中=本色，未选=灰）+ 全选 / 反选 ----
  // ---- 设备维度（双设备）：页面顶部那一排设备按钮 ----
  var deviceBox = document.getElementById('devices');
  var scope = '';                       // '' = 用后端配置的默认设备
  var defaultScope = '';
  var labelMap = {};                    // scope -> 显示名（1号炉 / 2号炉）
  function scopeLabel() {
    return labelMap[scope] || labelMap[defaultScope] || scope || defaultScope || '(默认)';
  }
  function scopeQuery() {
    if (!scope) { return ''; }
    var parts = scope.split('/');
    return 'plant=' + encodeURIComponent(parts[0]) + '&device=' + encodeURIComponent(parts[1]);
  }
  function withScope(url) {
    var q = scopeQuery();
    if (!q) { return url; }
    return url + (url.indexOf('?') >= 0 ? '&' : '?') + q;
  }
  function paintDevices() {
    Array.prototype.forEach.call(deviceBox.querySelectorAll('.dev'), function(el) {
      // ⚠️ `scope` 为空串表示"本实例的默认设备"，它也要高亮 —— 否则点回默认设备时
      //    两个按钮都不亮，看起来像"点了没反应 / 还停在另一台上"。
      var target = scope || defaultScope;
      var on = (el.getAttribute('data-scope') === target);
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
          span.textContent = item.label;
          labelMap[item.plant + '/' + item.device] = item.label;
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

  // 不用 ECharts 自带图例：它的反选/全选要额外派发 action，而且灰显样式改不动。
  // 勾选状态放 hiddenPoints，直接决定"这一帧画哪几条曲线"，轴也按可见条数切换。
  function paintLegend() {
    var box = document.getElementById('legend-items');
    if (!box) { return; }
    box.innerHTML = '';
    POINTS.forEach(function(p) {
      var span = document.createElement('span');
      span.className = 'lg' + (hiddenPoints[p.label] ? ' off' : '');
      span.innerHTML = '<i style="background:' + p.color + '"></i>' + p.label;
      span.addEventListener('click', function() {
        hiddenPoints[p.label] = !hiddenPoints[p.label];
        paintLegend();
        if (lastFrame) { render(lastFrame.d, lastFrame.useTimeAxis); }
      });
      box.appendChild(span);
    });
  }
  function setAllLegend(on) {
    POINTS.forEach(function(p) { hiddenPoints[p.label] = !on; });
    paintLegend();
    if (lastFrame) { render(lastFrame.d, lastFrame.useTimeAxis); }
  }
  function invertLegend() {
    POINTS.forEach(function(p) { hiddenPoints[p.label] = !hiddenPoints[p.label]; });
    paintLegend();
    if (lastFrame) { render(lastFrame.d, lastFrame.useTimeAxis); }
  }
  function showError(msg) {
    status.className = 'err';
    status.textContent = msg;
  }

  function showInfo(msg) {
    status.className = '';
    status.textContent = msg;
  }

  // ---- 数据新鲜度指示（"最后一条数据：N 秒前"） ----
  // ★ 取数走**已有的** /api/health：它内部用**已有的** query_recent() 查 TDengine 最新 ts，
  //   不新增容器、不新增依赖、不新造查询路径。本函数只负责把返回的两个数上色。
  // ⚠️ 阈值用页面注入的 FRESHNESS_WARN（默认 30 s），**不等于** /api/health 的
  //   stale_after_seconds（默认 15 s）—— 理由见 web_dashboard.py 配置区那一段注释。
  var FRESHNESS_WARN = __FRESHNESS_WARN__;
  var freshEl = document.getElementById('fresh');
  function setFreshness(text, cls) {
    if (!freshEl) { return; }
    freshEl.textContent = '数据新鲜度：' + text;
    freshEl.className = 'fresh ' + cls;
  }
  function loadFreshness() {
    fetch('/api/health')
      .then(function(res) { return res.json(); })
      .then(function(h) {
        var age = h.data_age_seconds;
        if (age === null || age === undefined) {
          setFreshness('库中无数据（' + (h.reason || '未知') + '）', 'bad');
          return;
        }
        var secs = Math.round(age * 10) / 10;
        if (age > FRESHNESS_WARN) {
          setFreshness('最后一条数据：' + secs + ' 秒前（已超过 ' + FRESHNESS_WARN + ' 秒，疑似停流）', 'bad');
        } else {
          setFreshness('最后一条数据：' + secs + ' 秒前', 'ok');
        }
      })
      .catch(function() { setFreshness('取新鲜度失败（/api/health 不可达）', 'unknown'); });
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
  // 单图 + 轴自适应（与平台一致）：
  //   可见 1 条 -> 线性轴，自动缩放到该序列真实范围（负数照常画）
  //   可见 >=2 条 -> 对数轴，下限固定 0.001，<=0 的值压到 0.001 画成一条底线
  // 因此"勾了几条"既决定画哪几条，也决定用哪种轴；任何一次勾选都要整帧重建。
  function render(d, useTimeAxis) {
    lastFrame = { d: d, useTimeAxis: useTimeAxis };
    var visible = POINTS.filter(function(p) { return !hiddenPoints[p.label]; });
    var axisKind = (visible.length >= 2) ? 'log' : 'lin';
    var key = (useTimeAxis ? 'time' : 'category') + ':' + axisKind + ':'
      + visible.map(function(p) { return p.key; }).join(',');
    var mustReset = (key !== lastAxisMode);
    lastAxisMode = key;

    var LOG_FLOOR = 0.001;
    var xAxis = useTimeAxis
      ? { type: 'time', axisLabel: Object.assign({}, AXIS_STYLE,
          { formatter: '{yyyy}/{MM}/{dd} {HH}' }) }
      : { type: 'category', data: d.ts, axisLabel: AXIS_STYLE };
    var yAxis = axisKind === 'log'
      ? { type: 'log', min: LOG_FLOOR, axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE,
          splitLine: { lineStyle: { color: 'rgba(51,65,85,0.5)' } } }
      : { type: 'value', scale: true, axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE,
          splitLine: { lineStyle: { color: 'rgba(51,65,85,0.5)' } } };

    var series = visible.map(function(p) {
      var values = d[p.key] || [];
      var data = useTimeAxis
        ? d.ts.map(function(ts, index) { return [ts.replace(' ', 'T'), values[index]]; })
        : values.slice();
      if (axisKind === 'log') {
        data = data.map(function(item) {
          if (item && item.length === 2) {
            var v = item[1];
            return [item[0], (v === null || v === undefined || v <= 0) ? LOG_FLOOR : v];
          }
          return (item === null || item === undefined || item <= 0) ? LOG_FLOOR : item;
        });
      }
      return {
        id: 'pt-' + p.key,
        name: p.label,
        type: 'line',
        data: data,
        smooth: true,
        showSymbol: false,
        connectNulls: false,
        itemStyle: { color: p.color }
      };
    });

    myChart.setOption({
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#1e293b',
        borderColor: '#334155',
        textStyle: { color: '#e2e8f0' },
        // 与平台一致：一行一个测点 "● 名称[单位]  数值"，只列当前可见的
        formatter: function(params) {
          if (!params || !params.length) { return ''; }
          var index = params[0].dataIndex;
          var html = '<div style="font-weight:600;margin-bottom:4px;">' + d.ts[index] + '</div>';
          visible.forEach(function(p) {
            var dot = '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
              + 'background:' + p.color + ';margin-right:6px;"></span>';
            html += dot + p.label + (p.unit ? '[' + p.unit + ']' : '') + '&emsp;&emsp;'
              + fmtNum((d[p.key] || [])[index]) + '<br/>';
          });
          return html;
        }
      },
      legend: { show: false },     // 图例由页面顶部那条自绘（可灰显 + 全选/反选）
      grid: { left: 96, right: 44, top: 26, bottom: 78 },
      axisPointer: { label: { backgroundColor: '#334155' } },
      dataZoom: [
        { type: 'inside', filterMode: 'none' },
        { type: 'slider', filterMode: 'none', bottom: 10, height: 18,
          borderColor: '#334155', textStyle: { color: '#94a3b8' },
          fillerColor: 'rgba(14,165,233,0.18)', handleStyle: { color: '#0ea5e9' } }
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
          loadFreshness();          // 与数据刷新同一节拍，不另开定时器
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

  paintLegend();
  document.getElementById('lg-all').addEventListener('click', function() { setAllLegend(true); });
  document.getElementById('lg-invert').addEventListener('click', invertLegend);
  loadDevices();
  setMode('realtime');
})();
</script>
</body>
</html>
"""


# ==================== 6. 报表页面（4 类时间维度聚合 · 表格 + 覆盖率面板） ====================





# ==================== 6. 主流程 ====================

TABLE_PAGE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>CEMS 监测数据（__POINT_COUNT__ 测点）</title>
<style>
  body { background:#0f172a; color:#e2e8f0; font-family:system-ui,-apple-system,"Segoe UI","Microsoft YaHei",sans-serif;
         margin:0; padding:14px 18px 26px; }
  h1 { font-size:19px; margin:0 0 12px; }
  .bar { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom:10px; }
  .bar.second { margin-bottom:12px; }
  .icon { padding:5px 11px; border:1px solid #334155; border-radius:6px; background:#1e293b; color:#cbd5e1;
          cursor:pointer; font-size:14px; text-decoration:none; }
  .icon:hover { border-color:#0ea5e9; color:#e2e8f0; }
  select, input[type=datetime-local] { background:#1e293b; color:#e2e8f0; border:1px solid #334155;
          border-radius:6px; padding:5px 8px; font-size:13px; }
  .tab { padding:5px 12px; border:1px solid #334155; border-radius:6px; background:#1e293b;
         color:#cbd5e1; font-size:13px; cursor:pointer; user-select:none; }
  .tab.active { background:#0ea5e9; border-color:#0ea5e9; color:#0f172a; font-weight:600; }
  .label { font-size:13px; color:#94a3b8; }
  .chk { font-size:13px; color:#cbd5e1; display:inline-flex; gap:5px; align-items:center; cursor:pointer; }
  button.primary { background:#0ea5e9; color:#0f172a; border:0; border-radius:6px; padding:6px 14px;
                   font-size:13px; font-weight:600; cursor:pointer; }
  button.primary:disabled { background:#475569; color:#94a3b8; cursor:not-allowed; }
  #tablewrap { overflow:auto; max-height:74vh; border:1px solid #334155; border-radius:8px; background:#0f172a; }
  table { border-collapse:separate; border-spacing:0; font-size:13px; white-space:nowrap; }
  th, td { border-right:1px solid #1e293b; border-bottom:1px solid #1e293b; padding:5px 10px; text-align:right; }
  th { background:#182234; color:#cbd5e1; font-weight:600; text-align:center; position:sticky; z-index:3; }
  thead tr:nth-child(1) th { top:0; }
  thead tr:nth-child(2) th { top:29px; }
  thead tr:nth-child(3) th { top:58px; }
  th.time, td.time { position:sticky; left:0; z-index:4; background:#182234; text-align:left; }
  td.time { background:#111c30; color:#e2e8f0; }
  tbody tr:nth-child(even) td { background:#131e31; }
  tbody tr:nth-child(even) td.time { background:#0f1929; }
  td.grp { background:#111a2b; }
  .eff { color:#4ade80; }
  .over { color:#f87171; font-weight:600; }
  .null { color:#64748b; }
  #status { font-size:13px; color:#94a3b8; margin-top:10px; }
  #status.err { color:#f87171; }
  /* 数据新鲜度指示：绿色=在流；超过 __FRESHNESS_WARN__ 秒变红加粗 —— 见 §5 的 loadFreshness */
  .fresh { display:inline-block; margin-left:12px; padding:4px 12px; border-radius:999px;
           font-size:13px; font-weight:600; border:1px solid transparent; vertical-align:middle; }
  .fresh.ok { background:#052e1a; color:#4ade80; border-color:#15803d; }
  .fresh.bad { background:#450a0a; color:#fca5a5; border-color:#b91c1c; font-weight:700; }
  .fresh.unknown { background:#1e293b; color:#94a3b8; border-color:#334155; }
</style>
</head>
<body>
  <h1>CEMS 监测数据 · __POINT_COUNT__ 测点<span class="fresh unknown" id="fresh">数据新鲜度：加载中…</span></h1>
  <div class="bar">
    <a class="icon" id="tochart" href="/chart" title="切换到曲线视图">☰</a>
    <select id="device"></select>
    <span class="tab active">常规监测因子</span>
    <span style="width:10px"></span>
    <span class="tab" data-unit="1d">日</span>
    <span class="tab" data-unit="1h">小时</span>
    <span class="tab" data-unit="1m">分钟</span>
    <input type="datetime-local" id="start">
    <span class="label">至</span>
    <input type="datetime-local" id="end">
    <button class="primary" id="query">🔍</button>
  </div>
  <div class="bar second">
    <span class="label">仅查看：</span>
    <label class="chk"><input type="checkbox" id="f-over"> 超标 (<span id="c-over">0</span>)</label>
    <label class="chk"><input type="checkbox" id="f-bad"> 异常 (<span id="c-bad">0</span>)</label>
    <span style="width:14px"></span>
    <label class="chk"><input type="checkbox" id="auto"> 开启自动刷新</label>
    <span style="width:14px"></span>
    <a class="icon" id="export" href="#" title="导出当前区间与粒度">⇤ 导出</a>
  </div>
  <div id="tablewrap"><table id="tbl"><thead id="thead"></thead><tbody id="tbody"></tbody></table></div>
  <div id="status">加载中...</div>

<script>
(function() {
  var REFRESH_SEC = __REFRESH_SEC__;
  var POINTS = __POINTS_JSON__;
  // 折算值有关的数字全部由后端注入，本页不手抄（真源 points.py + alarm_judge 的判定门限）
  var CC = __CONTRACT_CONSTS_JSON__;
  var ZS_LIMIT = CC.zsLimits;
  var GROUPS = [
    { title:'颗粒物',   unit:'mg/m3', key:'dust' },
    { title:'二氧化硫', unit:'mg/m3', key:'so2'  },
    { title:'氮氧化物', unit:'mg/m3', key:'nox'  }
  ];
  var PLAINS = ['o2', 'velocity', 'temp', 'humidity', 'pressure'];
  var unit = '1h';            // 1m | 1h | 1d
  var scope = '';             // plant/device，空 = 后端默认设备
  var defaultScope = '';
  var labelMap = {};          // scope -> 页面显示名（1号炉 / 2号炉）
  var rows = [];              // 最近一次查询回来的窗口
  var filtered = [];
  var timer = null;

  var el = function(id) { return document.getElementById(id); };
  var meta = {};
  POINTS.forEach(function(p) { meta[p.key] = p; });

  function fmt(v, digits) {
    if (v === null || v === undefined || (typeof v === 'number' && !isFinite(v))) { return null; }
    return String(Math.round(v * 1000) / 1000);
  }
  function toConverted(key, measured, o2) {
    if (measured === null || measured === undefined) { return null; }
    if (o2 === null || o2 === undefined) { return null; }
    // 与判定层同一条判据：(干基氧 - 实测氧) < 分母下限 ⇒ 放大倍数过大，折算不可信，不显示。
    // 不用 "o2 >= 某常数"：那个常数是手算出来的，改了 ALARM_O2_DENOM_MIN 就不会跟着变。
    if (CC.o2DryBasis - o2 < CC.o2DenomMin) { return null; }
    return measured * (CC.o2DryBasis - CC.o2Reference) / (CC.o2DryBasis - o2);
  }
  function pad2(n) { return (n < 10 ? '0' : '') + n; }
  function toLocalInput(d) {
    return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate())
      + 'T' + pad2(d.getHours()) + ':' + pad2(d.getMinutes());
  }
  function labelOf(ts) {
    // 与参考格式一致：分钟 'MM-DD HH:MM'、小时 'MM-DD HH~HH'、日 'MM-DD'
    var s = String(ts);
    if (unit === '1d') { return s.slice(5, 10); }
    if (unit === '1h') {
      var hh = parseInt(s.slice(11, 13), 10);
      if (hh === 0) { return s.slice(5, 10) + ' 00~01'; }
      return s.slice(5, 10) + ' ' + pad2(hh) + '~' + pad2(hh + 1);
    }
    return s.slice(5, 16);
  }
  function defaultsFor(u) {
    var now = new Date();
    var days = (u === '1d') ? 9 : 1;
    el('start').value = toLocalInput(new Date(now.getTime() - days * 24 * 3600 * 1000));
    el('end').value = toLocalInput(now);
  }
  function scopeQuery() {
    if (!scope) { return ''; }
    var parts = scope.split('/');
    return 'plant=' + encodeURIComponent(parts[0]) + '&device=' + encodeURIComponent(parts[1]);
  }
  function withScope(url) {
    var q = scopeQuery();
    if (!q) { return url; }
    return url + (url.indexOf('?') >= 0 ? '&' : '?') + q;
  }

  // ---- 表头：三行合并（污染物 = 颗粒物(单位) -> 浓度 -> 标干值/折算值；其余测点一列到底）----
  // ⚠️ 没有「设备标记」列：项目里不存在逐点有效性字段（接入层对整条报文收/拒，
  //    TDengine 每个测点只有一列数值），凭空造一列会让报表看起来有数据支撑而其实没有。
  function buildHead() {
    var r1 = '<tr><th class="time" rowspan="3">监测时间</th>';
    GROUPS.forEach(function(g) {
      r1 += '<th colspan="2">' + g.title + '(' + g.unit + ')</th>';
    });
    PLAINS.forEach(function(k) {
      r1 += '<th rowspan="3">' + meta[k].label + '(' + meta[k].unit + ')</th>';
    });
    var r2 = '<tr>';
    GROUPS.forEach(function() { r2 += '<th colspan="2">浓度</th>'; });
    var r3 = '<tr>';
    GROUPS.forEach(function() { r3 += '<th>标干值</th><th>折算值</th>'; });
    r1 += '</tr>'; r2 += '</tr>'; r3 += '</tr>';
    el('thead').innerHTML = r1 + r2 + r3;
  }
  function cell(v) {
    var s = fmt(v);
    return s === null ? '<span class="null">--</span>' : s;
  }

  function rowFlags(r) {
    var over = false, bad = false;
    var o2 = r.o2;
    GROUPS.forEach(function(g) {
      var measured = r[g.key];
      if (measured === null || measured === undefined) { bad = true; return; }
      var zs = toConverted(g.key, measured, o2);
      if (zs !== null && zs > ZS_LIMIT[g.key]) { over = true; }
    });
    PLAINS.forEach(function(k) { if (r[k] === null || r[k] === undefined) { bad = true; } });
    if (r.insufficient) { bad = true; }
    return { over: over, bad: bad };
  }

  function render() {
    var over = 0, bad = 0;
    rows.forEach(function(r) { var f = rowFlags(r); if (f.over) { over++; } if (f.bad) { bad++; } });
    el('c-over').textContent = over;
    el('c-bad').textContent = bad;
    var onlyOver = el('f-over').checked, onlyBad = el('f-bad').checked;
    filtered = rows.filter(function(r) {
      var f = rowFlags(r);
      if (onlyOver && !f.over) { return false; }
      if (onlyBad && !f.bad) { return false; }
      return true;
    });
    var html = '';
    filtered.forEach(function(r) {
      html += '<tr><td class="time">' + labelOf(r.ts) + '</td>';
      var o2 = r.o2;
      GROUPS.forEach(function(g) {
        var measured = r[g.key];
        var zs = toConverted(g.key, measured, o2);
        // 超标仍然用红字标出来（值本身带样式），不再单占一列"设备标记"
        var cls = (zs !== null && measured !== null && measured !== undefined
                   && zs > ZS_LIMIT[g.key]) ? ' class="over"' : '';
        html += '<td class="grp">' + cell(measured) + '</td><td class="grp"' + cls + '>'
              + cell(zs) + '</td>';
      });
      PLAINS.forEach(function(k) {
        var v = r[k];
        html += '<td>' + cell(v) + '</td>';
      });
      html += '</tr>';
    });
    el('tbody').innerHTML = html || '<tr><td colspan="13" style="text-align:center;color:#64748b">该区间没有数据</td></tr>';
  }

  function showStatus(msg, err) { el('status').className = err ? 'err' : ''; el('status').textContent = msg; }

  // ---- 数据新鲜度指示（"最后一条数据：N 秒前"） ----
  // ★ 与曲线页同一套：取数走**已有的** /api/health（内部用**已有的** query_recent 查库），
  //   不新增容器、不新增依赖、不新造查询路径。
  var FRESHNESS_WARN = __FRESHNESS_WARN__;
  function setFreshness(text, cls) {
    el('fresh').textContent = '数据新鲜度：' + text;
    el('fresh').className = 'fresh ' + cls;
  }
  function loadFreshness() {
    fetch('/api/health')
      .then(function(res) { return res.json(); })
      .then(function(h) {
        var age = h.data_age_seconds;
        if (age === null || age === undefined) {
          setFreshness('库中无数据（' + (h.reason || '未知') + '）', 'bad');
          return;
        }
        var secs = Math.round(age * 10) / 10;
        if (age > FRESHNESS_WARN) {
          setFreshness('最后一条数据：' + secs + ' 秒前（已超过 ' + FRESHNESS_WARN + ' 秒，疑似停流）', 'bad');
        } else {
          setFreshness('最后一条数据：' + secs + ' 秒前', 'ok');
        }
      })
      .catch(function() { setFreshness('取新鲜度失败（/api/health 不可达）', 'unknown'); });
  }

  function refresh() {
    var url = withScope('/api/report/range?unit=' + unit
      + '&start=' + encodeURIComponent(el('start').value)
      + '&end=' + encodeURIComponent(el('end').value));
    el('query').disabled = true;
    showStatus('查询中…', false);
    fetch(url).then(function(res) {
      return res.json().then(function(body) { return { ok: res.ok, body: body }; });
    }).then(function(r) {
      if (!r.ok) { showStatus('查询失败：' + (r.body.error || '未知错误'), true); return; }
      rows = r.body.points || [];
      render();
      var cov = r.body.coverage || {};
      var covText = (cov.overall === undefined || cov.overall === null) ? ''
        : '　覆盖率 ' + Math.round(cov.overall * 1000) / 10 + '%（门限 ' + Math.round((cov.threshold || 0) * 100) + '%，不足 ' + (cov.insufficient_windows || 0) + ' 个窗口）';
      showStatus(rows.length + ' 个窗口　' + r.body.start + ' ~ ' + r.body.end + covText
        + '　设备 ' + (labelMap[scope] || labelMap[defaultScope] || scope || defaultScope), false);
    }).catch(function() {
      showStatus('后端连不上（确认 web_dashboard.py 在跑）', true);
    }).finally(function() { el('query').disabled = false; });
  }

  function setUnit(next) {
    unit = next;
    Array.prototype.forEach.call(document.querySelectorAll('.tab[data-unit]'), function(t) {
      t.className = 'tab' + (t.getAttribute('data-unit') === next ? ' active' : '');
    });
    defaultsFor(next);
    refresh();
  }
  function syncExport() {
    el('export').href = withScope('/api/report/export?type=range&unit=' + unit
      + '&start=' + encodeURIComponent(el('start').value)
      + '&end=' + encodeURIComponent(el('end').value));
  }
  function autoTick() {
    loadFreshness();              // 与"自动刷新"同一节拍；未勾选时 initFreshness 的低频定时器兜底
    if (!el('auto').checked) { return; }
    refresh();
  }

  el('query').addEventListener('click', function() { refresh(); syncExport(); });
  document.querySelectorAll('.tab[data-unit]').forEach(function(t) {
    t.addEventListener('click', function() { setUnit(t.getAttribute('data-unit')); syncExport(); });
  });
  el('f-over').addEventListener('change', render);
  el('f-bad').addEventListener('change', render);
  el('auto').addEventListener('change', function() {
    if (timer) { clearInterval(timer); timer = null; }
    if (el('auto').checked) { timer = setInterval(autoTick, REFRESH_SEC * 1000); }
  });
  el('export').addEventListener('click', function() { syncExport(); });
  el('tochart').addEventListener('click', function(ev) {
    ev.preventDefault();
    var parts = scope.split('/');
    var q = 'unit=' + unit + '&start=' + encodeURIComponent(el('start').value)
      + '&end=' + encodeURIComponent(el('end').value);
    if (scope) { q += '&plant=' + encodeURIComponent(parts[0]) + '&device=' + encodeURIComponent(parts[1]); }
    window.location.href = '/chart?' + q;
  });

  fetch('/api/devices').then(function(res) { return res.json(); }).then(function(body) {
    defaultScope = body.default_scope || '';
    var list = body.devices || [];
    var sel = el('device');
    list.forEach(function(item) {
      var opt = document.createElement('option');
      opt.value = item.plant + '/' + item.device;
      opt.textContent = item.label;
      labelMap[opt.value] = item.label;
      if (opt.value === defaultScope || (list.length === 1 && !defaultScope)) { opt.selected = true; }
      sel.appendChild(opt);
    });
    scope = sel.value || '';
    sel.addEventListener('change', function() { scope = sel.value; refresh(); syncExport(); });
  }).catch(function() { showStatus('取设备列表失败（/api/devices）', true); });

  buildHead();
  defaultsFor(unit);
  setUnit(unit);
  syncExport();
  // ★ 新鲜度指示：与刷新周期同节拍固定轮询（**不**只在"自动刷新"勾选时才跑）——
  //   这一条是页面上的**告警面**，"没勾自动刷新"恰恰是最可能正在盯停流的时候。
  loadFreshness();
  setInterval(loadFreshness, REFRESH_SEC * 1000);
})();
</script>
</body>
</html>
"""


def main() -> None:
    """启动 Web 服务（阻塞运行）。"""
    setup_logging()
    LOGGER.info("Web 大屏启动: http://localhost:%d （局域网: http://<本机IP>:%d）", WEB_PORT, WEB_PORT)
    LOGGER.info("数据源: %s 库=%s 超级表=%s 最近 %d 分钟", TD_URL, TD_DB, TD_STABLE, QUERY_MINUTES)
    # cache.py 在导入时已算好设备维度，但那会儿日志系统还没配置（setup_logging 在这个函数里才跑），
    # 导入时的 INFO 会被丢弃——所以在这里补一次，保证 docker logs 里能看见
    LOGGER.info("查询缓存设备维度: %s（缓存键按设备隔离，见 src/web/cache.py）", cache.DEVICE_SCOPE)
    try:
        serve_forever()
    except OSError as exc:
        # 端口被占/无权限属于启动失败：必须以非 0 退出码结束，否则编排层看到"正常退出"不会告警
        LOGGER.critical("Web 服务启动失败（端口 %d 可能被占用）: %s", WEB_PORT, exc)
        raise SystemExit(1) from exc
    except Exception:
        LOGGER.exception("Web 服务异常退出")
        raise SystemExit(1)
    finally:
        LOGGER.info("Web 大屏已停止")


def serve_forever() -> None:
    """用 waitress（生产级 WSGI 服务器）起服务；没装时回落到 Flask 自带服务器。

    Flask 自带的 werkzeug 开发服务器会警告"不要用于生产"且没有正经并发处理，
    只用于本机开发自测。
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
