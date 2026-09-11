# -*- coding: utf-8 -*-
"""Web 展示大屏：Flask 提供 TDengine 近段数据接口，前端用 ECharts 画 8 个烟气测点的实时曲线。"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Callable, Final

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
# Y 轴分组：0=浓度(SO2/NOx/颗粒物) 1=O2/湿度 2=流量/温度/压力
POINT_PRESENTATION: Final[dict[str, tuple[str, int, str]]] = {
    "so2": ("SO2", 0, "#ef4444"),
    "nox": ("NOx", 0, "#3b82f6"),
    "dust": ("颗粒物", 0, "#a855f7"),
    "o2": ("O2", 1, "#22c55e"),
    "humidity": ("湿度", 1, "#06b6d4"),
    "flow": ("流量", 2, "#84cc16"),
    "temp": ("温度", 2, "#f59e0b"),
    "pressure": ("压力", 2, "#ec4899"),
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
    """接口1：返回最近数据（JSON），前端每 REFRESH_SECONDS 秒调用一次。"""
    try:
        rows = query_recent()
    except TdQueryError as exc:
        # 查库失败不崩服务：返回空数据 + 通用提示，细节只在服务端日志里
        body, _ = safe_error(exc, "GET /api/data")
        empty: dict[str, Any] = {"ts": [], **body}
        empty.update({name: [] for name in POINTS})
        return jsonify(empty), 503

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
  body { margin:0; padding:20px; background:#0f172a; font-family: sans-serif; }
  h1 { color:#e2e8f0; font-size:20px; margin:0 0 16px; }
  #chart { width:100%; height:70vh; background:#0f172a; }
  #status { color:#94a3b8; font-size:13px; margin-top:10px; }
  #status.err { color:#f87171; }
</style>
</head>
<body>
  <h1>CEMS 烟气在线监测 · 实时曲线（8 测点）</h1>
  <div style="font-size:13px;margin-bottom:12px;">
    <a href="/report" style="color:#38bdf8;text-decoration:none;">报表 →</a>
  </div>
  <div id="chart"></div>
  <div id="status">加载中...</div>

<script>
(function() {
  var chart = document.getElementById('chart');
  var status = document.getElementById('status');
  var REFRESH_MS = __REFRESH_MS__;
  var REFRESH_SEC = __REFRESH_SEC__;

  // 测点表由后端注入（真源是 src/common/points.py + POINT_PRESENTATION），页面不再自己抄一份。
  // axis: 0=左轴 浓度(mg/m3)；1=右轴1 O2/湿度(%)；2=右轴2 流量/温度/压力
  var POINTS = __POINTS_JSON__;
  var AXIS_STYLE = { color: '#94a3b8' };

  if (typeof echarts === 'undefined') {
    status.className = 'err';
    status.textContent = '图表库加载失败（网络问题），请检查网络后刷新';
    return;
  }
  var myChart = echarts.init(chart);

  function showError(msg) {
    status.className = 'err';
    status.textContent = msg;
  }

  function loadData() {
    fetch('/api/data')
      .then(function(res) { return res.json(); })
      .then(function(d) {
        if (d.error) {
          showError('查库失败：' + d.error + (d.trace_id ? '（追踪号 ' + d.trace_id + '）' : ''));
          return;
        }
        if (!d.ts.length) {
          status.className = '';
          status.textContent = '暂无数据（确认 gateway 和 subscriber_to_td 在跑）';
          return;
        }
        myChart.setOption({
          tooltip: { trigger: 'axis' },
          legend: {
            data: POINTS.map(function(p) { return p.label; }),
            textStyle: { color: '#cbd5e1' },
            type: 'scroll'
          },
          grid: { left: 60, right: 130, top: 60, bottom: 50 },
          animationDurationUpdate: 300,
          xAxis: { type: 'category', data: d.ts, axisLabel: AXIS_STYLE },
          yAxis: [
            { type: 'value', name: '浓度 (mg/m3)', axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE },
            { type: 'value', name: 'O2/湿度 (%)', position: 'right',
              axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE },
            { type: 'value', name: '流量/温度/压力', position: 'right', offset: 60,
              axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE }
          ],
          series: POINTS.map(function(p) {
            return {
              name: p.label,
              type: 'line',
              yAxisIndex: p.axis,
              data: d[p.key] || [],
              smooth: true,
              showSymbol: false,
              itemStyle: { color: p.color }
            };
          })
        });
        var latest = POINTS.map(function(p) {
          var arr = d[p.key] || [];
          return p.label + '=' + arr[arr.length - 1];
        }).join('  ');
        status.className = '';
        status.textContent = '最近更新: ' + d.ts[d.ts.length - 1] + ' | ' + latest
          + ' | 每' + REFRESH_SEC + '秒自动刷新';
      })
      .catch(function() {
        showError('后端连不上（确认 web_dashboard.py 在跑）');
      })
      .finally(function() {
        // 上一轮结束后才排下一轮：后端变慢时请求不会越堆越多
        setTimeout(loadData, REFRESH_MS);
      });
  }

  loadData();
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
<!-- 本地加载 ECharts（随镜像发布，离线/内网不白屏） -->
<script src="/static/echarts.min.js"></script>
<style>
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
  #chart { width:100%; height:48vh; }
  #status { color:#94a3b8; font-size:13px; margin:8px 0; }
  #status.err { color:#f87171; }
  .scroll { max-height:38vh; overflow:auto; border:1px solid #1e293b; border-radius:8px; }
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
  </div>

  <div id="status">选择时间范围后点「查询」</div>
  <div id="chart"></div>
  <div class="scroll">
    <table id="table"><thead></thead><tbody></tbody></table>
  </div>

<script>
(function() {
  // 测点表由后端注入（真源 src/common/points.py），本页不再自己抄一份
  var POINTS = __POINTS_JSON__;
  var AXIS_STYLE = { color: '#94a3b8' };
  var chartEl = document.getElementById('chart');
  var statusEl = document.getElementById('status');
  var queryBtn = document.getElementById('query');
  var refreshBtn = document.getElementById('refresh');
  var activeTab = 'minute';
  var busy = false;

  if (typeof echarts === 'undefined') {
    statusEl.className = 'err';
    statusEl.textContent = '图表库加载失败（确认 /static/echarts.min.js 存在）';
    return;
  }
  var chart = echarts.init(chartEl);

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

  function buildUrl() {
    if (activeTab === 'minute') {
      return '/api/report/minute?start=' + encodeURIComponent(val('minute-start'))
        + '&end=' + encodeURIComponent(val('minute-end'));
    }
    if (activeTab === 'day') {
      return '/api/report/day?date=' + encodeURIComponent(val('day-date'));
    }
    if (activeTab === 'month') {
      return '/api/report/month?year=' + encodeURIComponent(val('month-year'))
        + '&month=' + encodeURIComponent(val('month-month'));
    }
    return '/api/report/custom?start=' + encodeURIComponent(val('custom-start'))
      + '&end=' + encodeURIComponent(val('custom-end'));
  }

  function showStatus(msg, isErr) {
    statusEl.textContent = msg;
    statusEl.className = isErr ? 'err' : '';
  }

  function setBusy(on) {
    busy = on;
    queryBtn.disabled = on;
    refreshBtn.disabled = on;
    queryBtn.textContent = on ? '查询中…' : '查询';
  }

  // 表格与折线共用同一份 points：null 在表里显示 —，在图上断线
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

  function renderChart(points) {
    chart.setOption({
      tooltip: { trigger: 'axis' },
      legend: {
        data: POINTS.map(function(p) { return p.label; }),
        textStyle: { color: '#cbd5e1' },
        type: 'scroll'
      },
      grid: { left: 60, right: 130, top: 50, bottom: 50 },
      xAxis: {
        type: 'category',
        data: points.map(function(pt) { return pt.ts; }),
        axisLabel: AXIS_STYLE
      },
      yAxis: [
        { type: 'value', name: '浓度 (mg/m3)', axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE },
        { type: 'value', name: 'O2/湿度 (%)', position: 'right',
          axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE },
        { type: 'value', name: '流量/温度/压力', position: 'right', offset: 60,
          axisLabel: AXIS_STYLE, nameTextStyle: AXIS_STYLE }
      ],
      series: POINTS.map(function(p) {
        return {
          name: p.label,
          type: 'line',
          yAxisIndex: p.axis,
          data: points.map(function(pt) { return pt[p.key]; }),
          smooth: true,
          showSymbol: false,
          connectNulls: false,     // 该窗口没数据就断开，不要拿相邻点连过去
          itemStyle: { color: p.color }
        };
      })
    }, true);
  }

  function query() {
    if (busy) { return; }
    setBusy(true);
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
        renderChart(points);
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
  window.addEventListener('resize', function() { chart.resize(); });

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
    而且没有正经的并发处理；本机演示无所谓，对外提供服务时应当用 waitress。
    """
    try:
        from waitress import serve as waitress_serve
    except ImportError:
        LOGGER.warning("未安装 waitress，回落到 Flask 开发服务器（仅够本机演示）")
        app.run(host=WEB_HOST, port=WEB_PORT, debug=False, threaded=True)
        return
    LOGGER.info("使用 waitress 启动（生产级 WSGI，线程数 %d）", WEB_THREADS)
    waitress_serve(app, host=WEB_HOST, port=WEB_PORT, threads=WEB_THREADS, ident="cems-web")


if __name__ == "__main__":
    main()
