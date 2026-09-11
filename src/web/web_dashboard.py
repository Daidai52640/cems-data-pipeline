# -*- coding: utf-8 -*-
"""Web 展示大屏：Flask 提供 TDengine 近段数据接口，前端用 ECharts 画 8 个烟气测点的实时曲线。"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Final

import taosrest
from flask import Flask, Response, jsonify
from werkzeug.exceptions import HTTPException

# 让 src/common 能被导入：三种启动方式（python src/x.py、python -m src.x、任意 CWD）都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import COLUMNS   # noqa: E402

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
    )
    return Response(html, mimetype="text/html")


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
