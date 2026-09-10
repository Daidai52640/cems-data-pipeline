# -*- coding: utf-8 -*-
"""Web 展示大屏：Flask 提供 TDengine 近段数据接口，前端用 ECharts 画 8 个烟气测点的实时曲线。"""

from __future__ import annotations

import logging
import os
from typing import Any, Final

import taosrest
from flask import Flask, Response, jsonify
from werkzeug.exceptions import HTTPException

# ==================== 1. 配置区（要改参数只动这里） ====================
# 连接参数支持环境变量覆盖，默认值与本机直接运行一致；
# 容器化部署时由 docker-compose.yml 注入服务名（如 TD_URL=http://tdengine:6041）。

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = os.getenv("TD_URL", "http://localhost:6041")
TD_USER: Final[str] = os.getenv("TD_USER", "root")
TD_PASS: Final[str] = os.getenv("TD_PASS", "taosdata")
TD_DB: Final[str] = os.getenv("TD_DB", "cems")
TD_STABLE: Final[str] = os.getenv("TD_STABLE", "cems_data")

# ---- 测点列（与超级表列名一致，顺序即返回给前端的字段顺序）----
POINTS: Final[tuple[str, ...]] = (
    "so2",        # 二氧化硫 mg/m3
    "nox",        # 氮氧化物 mg/m3
    "flow",       # 烟气流量 m3/s
    "dust",       # 颗粒物 mg/m3
    "o2",         # 氧含量 %
    "temp",       # 烟气温度 ℃
    "humidity",   # 烟气湿度 %
    "pressure",   # 烟气压力 kPa
)

# ---- 查询与刷新 ----
QUERY_MINUTES: Final[int] = int(os.getenv("QUERY_MINUTES", "10"))        # 查询最近 N 分钟数据
REFRESH_SECONDS: Final[int] = int(os.getenv("REFRESH_SECONDS", "5"))     # 前端自动刷新间隔（秒）

# ---- Web 服务 ----
WEB_HOST: Final[str] = os.getenv("WEB_HOST", "0.0.0.0")   # 监听所有网卡，局域网可访问
WEB_PORT: Final[int] = int(os.getenv("WEB_PORT", "5000"))

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


# ==================== 3. 数据查询 ====================

def query_recent(minutes: int = QUERY_MINUTES) -> list[tuple[Any, Any, Any, Any]]:
    """查 TDengine 最近 N 分钟数据，按时间升序返回 [(ts, 各测点值...), ...]。

    查询失败抛 TdQueryError（由接口层兜住，不影响 Web 进程存活）。
    """
    conn: Any = None
    try:
        conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
        cur = conn.cursor()
        columns = ", ".join(POINTS)
        # 按时间升序取最近数据（画曲线要按时间从左到右）
        cur.execute(
            f"SELECT ts, {columns} FROM {TD_DB}.{TD_STABLE} "
            f"WHERE ts >= now - {minutes}m ORDER BY ts ASC"
        )
        rows = list(cur.fetchall())
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
        # 查库失败不崩服务：返回空数据 + 错误信息，前端会提示
        empty: dict[str, Any] = {"ts": [], "error": str(exc)}
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
        return jsonify({"ok": False, "td": "down", "error": str(exc)}), 503
    return jsonify({"ok": True, "td": "up", "rows_last_1min": len(rows)})


@app.route("/")
def index() -> Response:
    """接口3：返回大屏页面（ECharts 画图）。"""
    html = (
        HTML_PAGE
        .replace("__REFRESH_MS__", str(REFRESH_SECONDS * 1000))
        .replace("__REFRESH_SEC__", str(REFRESH_SECONDS))
    )
    return Response(html, mimetype="text/html")


@app.errorhandler(Exception)
def handle_unexpected_error(exc: Exception) -> tuple[Response, int] | HTTPException:
    """兜底错误处理：未预期异常记日志并返回 JSON，HTTP 异常（404 等）按原状态码返回。"""
    if isinstance(exc, HTTPException):
        # 404/405 这类是正常的 HTTP 语义，不是服务故障，不该记 error 也不该变成 500
        return exc
    LOGGER.exception("接口处理异常: %s", exc)
    return jsonify({"error": str(exc)}), 500


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
<script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
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
  <div id="chart"></div>
  <div id="status">加载中...</div>

<script>
(function() {
  var chart = document.getElementById('chart');
  var status = document.getElementById('status');
  var REFRESH_MS = __REFRESH_MS__;
  var REFRESH_SEC = __REFRESH_SEC__;

  // 测点表：key 要与后端 /api/data 返回的字段一致
  // axis: 0=左轴 浓度(mg/m3)；1=右轴1 O2/湿度(%)；2=右轴2 流量/温度/压力
  var POINTS = [
    { key: 'so2',      label: 'SO2',    unit: 'mg/m3', axis: 0, color: '#ef4444' },
    { key: 'nox',      label: 'NOx',    unit: 'mg/m3', axis: 0, color: '#3b82f6' },
    { key: 'dust',     label: '颗粒物', unit: 'mg/m3', axis: 0, color: '#a855f7' },
    { key: 'o2',       label: 'O2',     unit: '%',     axis: 1, color: '#22c55e' },
    { key: 'humidity', label: '湿度',   unit: '%',     axis: 1, color: '#06b6d4' },
    { key: 'flow',     label: '流量',   unit: 'm3/s',  axis: 2, color: '#84cc16' },
    { key: 'temp',     label: '温度',   unit: '℃',     axis: 2, color: '#f59e0b' },
    { key: 'pressure', label: '压力',   unit: 'kPa',   axis: 2, color: '#ec4899' }
  ];
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
          showError('查库失败：' + d.error);
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
      });
  }

  loadData();
  setInterval(loadData, REFRESH_MS);   // 定时刷新
})();
</script>
</body>
</html>
"""


# ==================== 6. 主流程 ====================

def main() -> None:
    """启动 Flask 服务（阻塞运行）。"""
    setup_logging()
    LOGGER.info("Web 大屏启动: http://localhost:%d （局域网: http://<本机IP>:%d）", WEB_PORT, WEB_PORT)
    LOGGER.info("数据源: %s 库=%s 超级表=%s 最近 %d 分钟", TD_URL, TD_DB, TD_STABLE, QUERY_MINUTES)
    try:
        app.run(host=WEB_HOST, port=WEB_PORT, debug=False, threaded=True)
    except OSError as exc:
        LOGGER.error("Web 服务启动失败（端口 %d 可能被占用）: %s", WEB_PORT, exc)
    except Exception:
        LOGGER.exception("Web 服务异常退出")
    finally:
        LOGGER.info("Web 大屏已停止")


if __name__ == "__main__":
    main()
