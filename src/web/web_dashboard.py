# -*- coding: utf-8 -*-
"""Web 展示大屏：Flask 提供 TDengine 近段数据接口，前端用 ECharts 画 8 个烟气测点的实时曲线。"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Final
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

# ---- 测点在图表上的呈现方式（中文名 / Y 轴分组 / 配色）----
# 列名和单位来自测点契约，这里只补"画在图上长什么样"。
# 实时大屏和报表页共用同一份，注入前端，避免页面上再各抄一份测点表。
# Y 轴分组：0=浓度(SO2/NOx/颗粒物) 1=O2/湿度 2=流量/温度/压力 3=流速
POINT_PRESENTATION: Final[dict[str, tuple[str, int, str]]] = {
    "so2": ("SO2", 0, "#ef4444"),
    "nox": ("NOx", 0, "#3b82f6"),
    "dust": ("颗粒物", 0, "#a855f7"),
    "o2": ("O2", 1, "#22c55e"),
    "humidity": ("湿度", 1, "#06b6d4"),
    "flow": ("流量", 2, "#84cc16"),
    "temp": ("温度", 2, "#f59e0b"),
    "pressure": ("压力", 2, "#ec4899"),
    "velocity": ("流速", 3, "#14b8a6"),
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

# ---- 查询与刷新 ----
QUERY_MINUTES: Final[int] = int(os.getenv("QUERY_MINUTES", "10"))        # 查询最近 N 分钟数据
REFRESH_SECONDS: Final[int] = int(os.getenv("REFRESH_SECONDS", "5"))     # 前端自动刷新间隔（秒）
QUERY_LIMIT: Final[int] = int(os.getenv("QUERY_LIMIT", "5000"))          # 单次查询最多返回多少点

# ---- Web 服务 ----
WEB_HOST: Final[str] = os.getenv("WEB_HOST", "0.0.0.0")   # 监听所有网卡，局域网可访问
WEB_PORT: Final[int] = int(os.getenv("WEB_PORT", "5000"))
WEB_THREADS: Final[int] = int(os.getenv("WEB_THREADS", "8"))   # waitress 工作线程数

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

def query_recent(minutes: int = QUERY_MINUTES) -> list[tuple[Any, ...]]:
    """查 TDengine 最近 N 分钟数据，按时间升序返回 [(ts, 各测点值...), ...]。

    查询失败抛 TdQueryError（由接口层兜住，不影响 Web 进程存活）。

    两个约束是必须的：
      ts <= now —— 库里一旦有时间戳在"未来"的脏数据（时钟跳变等），
                   只写 ts >= now - N m 会把它们全捞回来，"最近 10 分钟"直接失真
      LIMIT     —— 查询结果不能无上限膨胀，否则响应体会随着积压越滚越大
    取数用"倒序 + LIMIT"再翻转，保证截断时留下的是**最新**的 N 个点。
    """
    conn: Any = None
    try:
        conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
        cur = conn.cursor()
        columns = ", ".join(POINTS)
        cur.execute(
            f"SELECT ts, {columns} FROM {TD_DB}.{TD_STABLE} "
            f"WHERE ts >= now - {minutes}m AND ts <= now "
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
        rows = query_recent()
    except TdQueryError as exc:
        # 查库失败不崩服务：返回空数据 + 通用提示，细节只在服务端日志里
        body, _ = safe_error(exc, "GET /api/data")
        empty: dict[str, Any] = {"ts": [], **body}
        empty.update({name: [] for name in POINTS})
        return jsonify(empty), 503

    if rows:
        timestamps = [cache.ts_text(row[0]) for row in rows]
        cache.note_data_ts(max(timestamps))
        # ⚠️ 这里**不能**先按"是否比边界旧"过滤：分钟最大值必须覆盖**每一个**观测到的分钟。
        # 先前加了这道过滤，后果是"窗口里新来的那条恰好比边界新"就被丢掉，
        # 该分钟的最大值不变 → 缓存不失效（实测 200 s 内测不到刷新）。
        # 不做过滤也不会误杀：比边界新的分钟本来就不满足"窗口已闭合"，
        # 压根不会进缓存，它的最大值怎么变都无所谓（见 _closure）。
        cache.note_minute_max(timestamps)

    payload: dict[str, Any] = {"ts": [str(row[0]) for row in rows]}   # 时间轴
    for index, name in enumerate(POINTS, start=1):                    # 各测点序列
        payload[name] = [row[index] for row in rows]
    return jsonify(payload)


@app.route("/api/health")
def api_health() -> tuple[Response, int] | Response:
    """接口2：健康检查，确认 Web 与 TDengine 是否都通。"""
    try:
        rows = query_recent(1)
    except TdQueryError as exc:
        body, _ = safe_error(exc, "GET /api/health")
        return jsonify({"ok": False, "td": "down", **body}), 503
    return jsonify({"ok": True, "td": "up", "rows_last_1min": len(rows)})


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
        lambda: report.minute_report(request.args.get("start"), request.args.get("end")),
    )


@app.route("/api/report/day")
def api_report_day() -> tuple[Response, int] | Response:
    """日报表：默认今天，按小时聚合，固定 24 个整点。"""
    return report_response(
        "GET /api/report/day",
        lambda: report.day_report(request.args.get("date")),
    )


@app.route("/api/report/month")
def api_report_month() -> tuple[Response, int] | Response:
    """月报表：默认当月，按天聚合，按当月实际天数补齐。"""
    return report_response(
        "GET /api/report/month",
        lambda: report.month_report(request.args.get("year"), request.args.get("month")),
    )


@app.route("/api/report/custom")
def api_report_custom() -> tuple[Response, int] | Response:
    """自由报表：默认最近 24 小时，按小时聚合，可跨天/跨月。"""
    return report_response(
        "GET /api/report/custom",
        lambda: report.custom_report(request.args.get("start"), request.args.get("end")),
    )


@app.route("/report")
def report_page() -> Response:
    """报表页面：4 类时间维度聚合，折线图 + 数据表格。"""
    return Response(REPORT_PAGE.replace("__POINTS_JSON__", POINT_VIEWS_JSON), mimetype="text/html")


@app.route("/api/curve")
def api_curve() -> tuple[Response, int] | Response:
    """自由区间曲线：按跨度自动分级降采样，返回结构与 /api/data 一致（列式）+ 粒度元信息。

    实时模式仍然用 /api/data（契约不变）；长区间必须走这里，
    否则原始点查询会被 LIMIT 静默截断，看起来像"前面那段没数据"。
    """
    where = "GET /api/curve"
    try:
        return jsonify(curve.build_curve(request.args.get("start"), request.args.get("end")))
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
        content, filename = report_export.export_workbook(request.args)
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
</style>
</head>
<body>
  <h1>CEMS 烟气在线监测 · 曲线（8 测点）</h1>
  <div style="font-size:13px;margin-bottom:10px;">
    <a href="/report" style="color:#38bdf8;text-decoration:none;">报表 →</a>
  </div>
  <div class="modes" id="modes">
    <span class="mode" data-mode="realtime">实时</span>
    <span class="mode" data-mode="free">自由区间</span>
  </div>
  <div class="controls" id="free-controls" hidden>
    <label>起</label> <input type="datetime-local" id="free-start">
    <label>止</label> <input type="datetime-local" id="free-end">
    <button id="free-query">查询</button>
  </div>
  <div id="chart"></div>
  <div id="status">加载中...</div>

<script>
(function() {
  var chartEl = document.getElementById('chart');
  var status = document.getElementById('status');
  var REFRESH_MS = __REFRESH_MS__;
  var REFRESH_SEC = __REFRESH_SEC__;

  // 测点表由后端注入（真源是 src/common/points.py + POINT_PRESENTATION），页面不再自己抄一份。
  // axis: 0=左轴 浓度(mg/m3)；1=右轴1 O2/湿度(%)；2=右轴2 流量/温度/压力；3=右轴3 流速(m/s)
  var POINTS = __POINTS_JSON__;
  var AXIS_STYLE = { color: '#94a3b8' };
  // 单次请求超时：fetch 默认不会超时，请求卡住时 finally 不执行、轮询会悄悄停掉
  var FETCH_TIMEOUT_MS = 15000;

  if (typeof echarts === 'undefined') {
    status.className = 'err';
    status.textContent = '图表库加载失败（网络问题），请检查网络后刷新';
    return;
  }
  var myChart = echarts.init(chartEl);
  var mode = 'realtime';      // realtime | free
  var timer = null;           // 只记实时模式的下一轮定时器，切模式时要能取消
  var busy = false;
  // 视图代次：切模式就 +1。在途请求回来时若代次对不上，说明它属于上一个模式，
  // 必须整帧丢弃 —— 否则上一轮实时请求回来会把自由区间的画面和状态栏盖回去（表现为"闪一下"）
  var viewToken = 0;

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
    var xAxis = useTimeAxis
      ? { type: 'time', axisLabel: AXIS_STYLE }
      : { type: 'category', data: d.ts, axisLabel: AXIS_STYLE };
    var series = POINTS.map(function(p) {
      var values = d[p.key] || [];
      var data = useTimeAxis
        ? d.ts.map(function(ts, index) { return [ts.replace(' ', 'T'), values[index]]; })
        : values;
      return {
        name: p.label,
        type: 'line',
        yAxisIndex: p.axis,
        data: data,
        smooth: true,
        showSymbol: false,
        connectNulls: false,     // 该窗口没数据就断开，不要拿相邻点连过去
        itemStyle: { color: p.color }
      };
    });
    myChart.setOption({
      tooltip: { trigger: 'axis' },
      legend: {
        data: POINTS.map(function(p) { return p.label; }),
        textStyle: { color: '#cbd5e1' },
        type: 'scroll'
      },
      grid: { left: 60, right: 190, top: 60, bottom: 50 },
      animationDurationUpdate: 300,
      xAxis: xAxis,
      yAxis: [
        { type: 'value', name: '浓度 (mg/m3)', axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE },
        { type: 'value', name: 'O2/湿度 (%)', position: 'right',
          axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE },
        { type: 'value', name: '流量/温度/压力', position: 'right', offset: 60,
          axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE },
        { type: 'value', name: '流速 (m/s)', position: 'right', offset: 120,
          axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE }
      ],
      series: series
    }, true);
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
    fetchWithTimeout('/api/data')
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
        showInfo('实时模式 | 最近 ' + d.ts[d.ts.length - 1] + ' | ' + latestText(d)
          + ' | 每' + REFRESH_SEC + '秒自动刷新');
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
    var url = '/api/curve?start=' + encodeURIComponent(start) + '&end=' + encodeURIComponent(end);
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

  setMode('realtime');
})();
</script>
</body>
</html>
"""


# ==================== 6. 报表页面（4 类时间维度聚合 · 折线图 + 表格） ====================

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
    head += '<th>采样条数</th></tr>';
    document.querySelector('#table thead').innerHTML = head;

    var rows = '';
    points.forEach(function(pt) {
      rows += '<tr><td>' + pt.ts + '</td>';
      POINTS.forEach(function(p) {
        var v = pt[p.key];
        rows += '<td>' + (v === null || v === undefined ? '—' : v) + '</td>';
      });
      rows += '<td>' + (pt.n === null || pt.n === undefined ? '—' : pt.n) + '</td></tr>';
    });
    document.querySelector('#table tbody').innerHTML = rows;
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
