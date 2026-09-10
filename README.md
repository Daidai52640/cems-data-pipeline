# CEMS 工业数据采集演示项目

模拟环保 CEMS 烟气在线监测数据采集全链路：仿真设备 → 网关 → MQTT Broker → 平台接入 → 时序库 → Web 实时大屏。

## 技术栈

- 设备协议：Modbus TCP（功能码 03 读保持寄存器）
- 消息队列：MQTT 3.1.1 + EMQX（Docker 部署）
- 时序数据库：TDengine 3.x（Docker 部署，taospy 连接）
- Web 后端：Flask
- 前端图表：ECharts

## 架构分层（数据流向：上 → 下）

### 1. 设备层 `src/device/modbus_server.py`
- 功能：模拟 CEMS 分析仪，Modbus TCP 服务端
- 寄存器（均放大10倍存整数）：地址0=SO2、地址1=NOx、地址2=Flow、地址3=颗粒物Dust、地址4=O2、地址5=温度Temp、地址6=湿度Humidity、地址7=压力Pressure
- 监听端口：5020

### 2. 网关层 `src/gateway/gateway.py`
- 功能：轮询读设备寄存器 → 换算真实值（÷10）→ 加时间戳 → MQTT 发布
- 发布主题：`cems/plant1/data`
- QoS：1
- 断网续传：本地 JSONL 缓存（`data/cache.jsonl`），重连后自动补传
- 依赖：pymodbus、paho-mqtt

### 3. 平台接入层 `src/platform/subscriber_to_td.py`
- 功能：订阅 MQTT 主题 → 解析 8 个测点 → 写入 TDengine
- TDengine 超级表：`cems.cems_data`（ts, so2, nox, flow, dust, o2, temp, humidity, pressure）
- 老库自动升级：启动时 DESCRIBE 超级表，缺哪列用 ALTER STABLE 补哪列
- 标签：plant, device
- 依赖：paho-mqtt、taospy

### 4. 展示层 `src/web/web_dashboard.py`
- 功能：Flask 后端 + ECharts 前端实时曲线（8 测点 / 3 组 Y 轴）
- 接口：`GET /api/data` 返回最近 10 分钟数据（JSON）
- 健康检查：`GET /api/health` 返回 Web 与 TDengine 是否都通
- 刷新：前端每 5 秒自动拉取
- 监听：0.0.0.0:5000（局域网可访问）
- 依赖：flask、taospy

## 运行顺序

1. 启动容器：`docker start emqx`、`docker start tdengine`
2. `python src/device/modbus_server.py` — 仿真设备
3. `python src/gateway/gateway.py` — 网关
4. `python src/platform/subscriber_to_td.py` — 平台接入（入库）
5. `python src/web/web_dashboard.py` — Web 大屏
6. 浏览器访问 `http://localhost:5000`（局域网：`http://<电脑IP>:5000`）

## 辅助工具

- `src/web/query_demo.py` — 查询入库数据 + INTERVAL 时间聚合演示
- `src/platform/subscriber.py` — 旧版订阅端（仅打印，已废弃）
- `docs/legacy/` — MQTT 学习阶段产物（publisher.py 等，已废弃）
- `docs/` — 架构复习图（HTML）

## 环境依赖

见 `requirements.txt`。Python 3.12。
