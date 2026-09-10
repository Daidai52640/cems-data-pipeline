# -*- coding: utf-8 -*-
"""
Web 展示（Day 15）—— TDengine 数据实时曲线（项目1收尾）
功能：Flask 后端查 TDengine → 前端 ECharts 画 SO2/NOx/Flow 实时曲线
运行：python web_dashboard.py
      浏览器打开 http://localhost:5000 看大屏
      （需要 modbus_server.py + gateway.py + subscriber_to_td.py 在跑）
"""

import taosrest
from flask import Flask, jsonify

# ========== 配置 ==========
TD_URL = "http://localhost:6041"
TD_USER = "root"
TD_PASS = "taosdata"
REFRESH_MINUTES = 10   # 查询最近10分钟数据

app = Flask(__name__)


def query_recent(minutes=REFRESH_MINUTES):
    """查 TDengine 最近 N 分钟数据 → 返回 [(时间戳, so2, nox, flow), ...]"""
    conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
    cur = conn.cursor()
    # 按时间升序取最近数据（画曲线要按时间从左到右）
    cur.execute(
        f"SELECT ts, so2, nox, flow FROM cems.cems_data "
        f"WHERE ts >= now - {minutes}m ORDER BY ts ASC"
    )
    rows = cur.fetchall()
    conn.close()
    return rows


@app.route("/api/data")
def api_data():
    """接口1：返回最近数据（JSON），前端每5秒调用一次"""
    rows = query_recent()
    data = {
        "ts": [str(r[0]) for r in rows],   # 时间轴
        "so2": [r[1] for r in rows],       # SO2 序列
        "nox": [r[2] for r in rows],       # NOx 序列
        "flow": [r[3] for r in rows],      # Flow 序列
    }
    return jsonify(data)


@app.route("/")
def index():
    """接口2：返回网页页面（ECharts 画图）"""
    return HTML_PAGE


# ========== 前端页面（HTML + ECharts）==========
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
  if (typeof echarts === 'undefined') {
    status.textContent = '图表库加载失败（网络问题），请检查网络后刷新';
    return;
  }
  var myChart = echarts.init(chart);

  function loadData() {
    fetch('/api/data')
      .then(function(res) { return res.json(); })
      .then(function(d) {
        if (!d.ts.length) {
          status.textContent = '暂无数据（确认 gateway 和 subscriber_to_td 在跑）';
          return;
        }
        myChart.setOption({
          tooltip: { trigger: 'axis' },
          legend: { data: ['SO2', 'NOx', 'Flow'], textStyle: { color: '#cbd5e1' } },
          grid: { left: 50, right: 50, top: 40, bottom: 40 },
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
        status.textContent = '最近更新: ' + d.ts[d.ts.length - 1] + ' | 最新 SO2=' + latest + ' mg/m3 | 每5秒自动刷新';
      })
      .catch(function() { status.textContent = '后端连不上（确认 web_dashboard.py 在跑）'; });
  }

  loadData();
  setInterval(loadData, 5000);   // 每5秒刷新一次
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    print("打开浏览器访问: http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
