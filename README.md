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
- 断网续传：本地 JSONL 缓存，重连后自动补传。缓存分两个文件、各有一个写者：
  - `data/cache.jsonl`——采集主循环追加写，放新数据
  - `data/cache.jsonl.sending`——补传线程持有，放在途批次
  两者用原子改名交接，所以补传期间新采的数据不会被覆盖；进程中途被杀也能接着补。
- 送达判定：QoS=1 必须等到 broker 的 **PUBACK** 才算送达（`publish()` 返回 `rc=0`
  只代表进了本机发送队列）。没等到 PUBACK 的数据一律转存缓存重试，不会静默丢弃。
- 补传节奏可用环境变量调：`PUBLISH_ACK_TIMEOUT`（单条确认超时）、
  `RESEND_WINDOW`（在途窗口条数）、`RESEND_RETRY_INTERVAL`（部分失败后的重试间隔）
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

## 方式一：Docker Compose 一键启动（推荐）

一条命令拉起 EMQX + TDengine + 四个服务，无需本地装 Python 环境：

```bash
docker compose up -d --build
```

第一次会构建镜像（装依赖），之后启动只要几秒。数据库初始化、建库建表、订阅、
采集全部自动完成，服务之间用健康检查排好启动顺序。

| 服务 | 容器名 | 地址 |
|---|---|---|
| MQTT Broker (EMQX) | emqx | `localhost:1883`，管理台 `localhost:18083` |
| 时序库 (TDengine) | tdengine | REST `localhost:6041` |
| 仿真设备 | cems-device | `localhost:5020` |
| 网关 | cems-gateway | — |
| 平台接入 | cems-subscriber | — |
| Web 大屏 | cems-web | `http://localhost:5000` |

常用命令：

```bash
docker compose ps                     # 看六个服务状态
docker compose logs -f gateway        # 跟踪某个服务日志
docker compose down                   # 停止（数据保留在具名卷里）
docker compose down -v                # 停止并连数据一起删
docker compose run --rm web python src/web/query_demo.py   # 在容器里跑查询演示
```

数据落地位置：TDengine 数据在具名卷 `cems-tdengine-data`，EMQX 在 `cems-emqx-data`，
网关断网缓存在宿主机的 `data/cache.jsonl`。

### 构建报 `auth.docker.io` 鉴权错误的处理

若 `docker compose up -d --build` 在最后一步报
`failed to fetch oauth token: ... auth.docker.io`，这是 Docker Desktop 开了
**containerd 镜像存储** + 该域名被 DNS 污染导致的（镜像其实已经构建出来了）。
两种解法：

```powershell
# 解法 A：改用传统构建器（推荐，一条命令）
$env:DOCKER_BUILDKIT=0; docker compose build; docker compose up -d
```

- 解法 B：Docker Desktop → Settings → General → 取消勾选
  "Use containerd for pulling and storing images"，重启 Docker Desktop 后重试。

## 方式二：本机直接运行（适合逐层调试）

先起依赖：`docker compose up -d emqx tdengine`，再开四个终端依次运行：

1. `python src/device/modbus_server.py` — 仿真设备
2. `python src/gateway/gateway.py` — 网关
3. `python src/platform/subscriber_to_td.py` — 平台接入（入库）
4. `python src/web/web_dashboard.py` — Web 大屏
5. 浏览器访问 `http://localhost:5000`（局域网：`http://<电脑IP>:5000`）

各服务的连接参数都在文件顶部配置区，且支持用环境变量覆盖
（如 `MQTT_HOST`、`TD_URL`），默认值就是本机 `localhost`，所以直接跑不用改代码。

## 辅助工具

- `src/web/query_demo.py` — 查询入库数据 + INTERVAL 时间聚合演示
- `src/platform/subscriber.py` — 旧版订阅端（仅打印，已废弃）
- `docs/legacy/` — MQTT 学习阶段产物（publisher.py 等，已废弃）
- `docs/` — 架构复习图（HTML）

## 环境依赖

见 `requirements.txt`。Python 3.12。

> 注意：`pymodbus` 已锁到 `<3.9`。3.9 起官方把 `ModbusSlaveContext` 改名、
> 把 `slave=` 参数改成 `device_id=`，设备层会直接 ImportError。
