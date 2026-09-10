# -*- coding: utf-8 -*-
"""Web 展示大屏：Flask 提供 TDengine 近段数据接口，前端用 ECharts 画 SO2/NOx/Flow 实时曲线。"""

from __future__ import annotations

import logging
from typing import Any, Final

import taosrest
from flask import Flask, Response, jsonify

# ==================== 1. 配置区（要改参数只动这里） ====================

# ---- TDengine（对应 taosAdapter 的 REST 接口）----
TD_URL: Final[str] = "http://localhost:6041"
TD_USER: Final[str] = "root"
TD_PASS: Final[str] = "taosdata"
TD_DB: Final[str] = "cems"
TD_STABLE: Final[str] = "cems_data"

# ---- 查询与刷新 ----
QUERY_MINUTES: Final[int] = 10         # 查询最近 N 分钟数据
REFRESH_SECONDS: Final[int] = 5        # 前端自动刷新间隔（秒）

# ---- Web 服务 ----
WEB_HOST: Final[str] = "0.0.0.0"       # 监听所有网卡，局域网可访问
WEB_PORT: Final[int] = 5000

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
    """查 TDengine 最近 N 分钟数据，按时间升序返回 [(ts, so2, nox, flow), ...]。

    查询失败抛 TdQueryError（由接口层兜住，不影响 Web 进程存活）。
    """
    conn: Any = None
    try:
        conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
        cur = conn.cursor()
        # 按时间升序取最近数据（画曲线要按时间从左到右）
        cur.execute(
            f"SELECT ts, so2, nox, flow FROM {TD_DB}.{TD_STABLE} "
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
        return (
            jsonify({"ts": [], "so2": [], "nox": [], "flow": [], "error": str(exc)}),
            503,
        )

    return jsonify({
        "ts": [str(row[0]) for row in rows],   # 时间轴
        "so2": [row[1] for row in rows],       # SO2 序列
        "nox": [row[2] for row in rows],       # NOx 序列
        "flow": [row[3] for row in rows],      # Flow 序列
    })


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
def handle_unexpected_error(exc: Exception) -> tuple[Response, int]:
    """兜底错误处理：任何未预期异常都记日志并返回 JSON，不让进程挂掉。"""
    LOGGER.exception("接口处理异常: %s", exc)
    return jsonify({"error": str(exc)}), 500


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
  <h1>CEMS 烟气在线监测 · 实时曲线</h1>
  <div id="chart"></div>
  <div id="status">加载中...</div>

<script>
(function() {
  var chart = document.getElementById('chart');
  var status = document.getElementById('status');
  var REFRESH_MS = __REFRESH_MS__;
  var REFRESH_SEC = __REFRESH_SEC__;

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
          legend: { data: ['SO2', 'NOx', 'Flow'], textStyle: { color: '#cbd5e1' } },
          grid: { left: 50, right: 50, top: 40, bottom: 40 },
          animationDurationUpdate: 300,
          xAxis: { type: 'category', data: d.ts, axisLabel: { color: '#94a3b8' } },
          yAxis: [
            { type: 'value', name: 'SO2/NOx (mg/m3)', axisLabel: { color: '#94a3b8' } },
            { type: 'value', name: 'Flow (m3/s)', axisLabel: { color: '#94a3b8' } }
          ],
          series: [
            { name: 'SO2', type: 'line', data: d.so2, smooth: true, showSymbol: false, itemStyle: { color: '#ef4444' } },
            { name: 'NOx', type: 'line', data: d.nox, smooth: true, showSymbol: false, itemStyle: { color: '#3b82f6' } },
            { name: 'Flow', type: 'line', yAxisIndex: 1, data: d.flow, smooth: true, showSymbol: false, itemStyle: { color: '#22c55e' } }
          ]
        });
        var latest = d.so2[d.so2.length - 1];
        status.className = '';
        status.textContent = '最近更新: ' + d.ts[d.ts.length - 1]
          + ' | 最新 SO2=' + latest + ' mg/m3 | 每' + REFRESH_SEC + '秒自动刷新';
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
