# CEMS 工业数据采集演示项目

模拟环保 CEMS 烟气在线监测数据采集全链路：仿真设备 → 网关 → MQTT Broker → 平台接入 → 时序库 → Web 实时大屏。

![CEMS 数据采集链路架构](docs/diagrams/cems-pipeline-architecture.visual-check.2048x1320.dark.png)

## 技术栈

- 设备协议：Modbus TCP（功能码 03 读保持寄存器）
- 消息队列：MQTT 3.1.1 + EMQX（Docker 部署）
- 时序数据库：TDengine 3.x（Docker 部署，taospy 连接）
- Web 后端：Flask
- 前端图表：ECharts

## 架构分层（数据流向：上 → 下）

### 1. 设备层 `src/device/modbus_server.py`
- 功能：模拟 CEMS 分析仪，Modbus TCP 服务端
- 寄存器（**按测点各自的比例系数**放大存整数，系数见 `src/common/points.py` 的 `Point.scale`）：
  地址0=Flow、地址1=Dust、地址2=SO2、地址3=NOx、地址4=O2、地址5=Velocity、地址6=Temp、地址7=Humidity、地址8=Pressure
  ⚠️ 顺序即 HJ 212 上传值序；系数不是全局统一值（大数测点用更小的系数，避免 16 位寄存器溢出）
- 监听端口：5020
- **多从站（可同时模拟多台设备）**：一个 TCP 服务端可以同时提供多个 unit id（`SLAVE_IDS=1,2`），
  每个从站有**自己的寄存器数据块和自己的仿真器实例**（第 n 台的种子 = `SIM_SEED+(n-1)`，
  相位按 `SLAVE_PHASE_OFFSET` 错开）⇒ 两台设备的曲线肉眼可辨。
  这对应现场最常见的形态：一条链路（一个 IP:端口）后面挂多台仪表，数采侧按 unit id 分别读取，
  所以第二台设备**不需要第二个端口、第二个容器**。默认 `SLAVE_IDS=1`（单从站，行为与改造前一致）。

### 2. 网关层 `src/gateway/gateway.py`
- 功能：轮询读设备寄存器 → 按各测点系数换算真实值 → 加时间戳 → MQTT 发布
- 发布主题：`cems/plant1/data`
- QoS：1
- 断网续传：本地 JSONL 缓存，重连后自动补传。缓存分两种文件、各有一个写者：
  - `data/cache.jsonl`——采集主循环追加写，放新数据
  - `data/inflight-<seq>.jsonl`——补传线程持有，放在途批次（段名唯一，只增不减；
    旧版固定名 `cache.jsonl.sending` 只读兼容，不再写入）
  两者用原子改名交接，所以补传期间新采的数据不会被覆盖；进程中途被杀也能接着补。
  ⚠️ **保护范围只覆盖"已进入网关的数据"**；网关读不到设备（上游断）时源头无缓冲，那一段会真丢
  （实测与边界见 `docs/reference/` 与 `docs/runbooks/故障台账.md`）
- **缓存目录多实例必须分开**（`GATEWAY_DATA_DIR`）：缓存文件、在途段、心跳文件都是目录内的固定
  文件名，两个网关共用一个目录会互相接手对方的缓存并**发到自己的主题上**（数据张冠李戴）。
  不设该变量时用默认值 `PROJECT_ROOT/data`（单设备现状不变）；相对路径按项目根解析。
- 送达判定：QoS=1 必须等到 broker 的 **PUBACK** 才算送达（`publish()` 返回 `rc=0`
  只代表进了本机发送队列）。没等到 PUBACK 的数据一律转存缓存重试，不会静默丢弃。
- 补传节奏可用环境变量调：`PUBLISH_ACK_TIMEOUT`（单条确认超时）、
  `RESEND_WINDOW`（在途窗口条数）、`RESEND_RETRY_INTERVAL`（部分失败后的重试间隔）
- 断网缓存有容量上限（`CACHE_MAX_BYTES`，默认 64MB）：写满磁盘会让之后每条数据都丢，
  所以超限时按"丢最旧、保最新"裁剪并打 CRITICAL；写盘走 `flush + fsync`，
  进程被 kill 也不会丢尾部数据；启动时先校验缓存目录可写
- 依赖：pymodbus、paho-mqtt

### 3. 平台接入层 `src/platform/subscriber_to_td.py`
- 功能：订阅 MQTT 主题 → 解析 9 个测点 → 写入 TDengine
- 会话持久化：默认使用**非干净会话**（`MQTT_CLEAN_SESSION=0`）配合固定的 client_id。
  订阅端离线期间，broker 会为 `cems/plant1/data` 上 QoS≥1 的消息排队，重连后自动补投。
  若不持久化（干净会话），离线期间发布的报文会被 broker 直接丢弃，且发布端拿到的是 PUBACK，
  看日志一切正常 —— 属于静默丢数据。
  - broker 侧上限（EMQX 默认值）：会话保留 `session_expiry_interval=2h`、离线队列 `max_mqueue_len=1000` 条，
    超出即丢最旧的；需要更长的断线容忍时间要改 EMQX 配置
  - 把 `MQTT_CLEAN_SESSION` 置 1 可退回"离线即丢"，仅用于对比演示
- TDengine 超级表：`cems.cems_data`（ts, flow, dust, so2, nox, o2, velocity, temp, humidity, pressure + TAG plant/device）
- 老库自动升级：启动时 DESCRIBE 超级表，缺哪列用 ALTER STABLE 补哪列
- 子表命名：`{厂区}_{设备}`（如 `plant1_device1`）。TDengine 对**已存在**的子表会沿用
  第一次写入的 TAGS 且不报错，若拿厂区名当子表名，接入第二台设备时数据会被静默
  挂到第一台的标签下；启动时还会核对该子表已有标签是否与配置一致
  （子表名统一转小写后比对：TDengine 表名不区分大小写，不归一化就会查不到行、静默跳过检查）
- 启动自检：主题的厂区段（`cems/<厂区>/...`）与 `TD_PLANT` 不一致时给出 WARNING ——
  "订阅了 A 厂区的主题却写进 B 厂区的标签"是**静默错标**（入库成功、报表却挂错名），
  但主题命名允许自定义，所以只告警、不拒绝启动
- **多设备 = 每台设备一个接入实例**：本模块带状态的东西都是每设备一份（判据滑窗、折算值快照、
  持久会话 client_id），而"一个实例只写一张子表"正是多实例方案能保持单设备代码路径零改动的原因。
  取舍与边界见 `docs/adr/0008-多设备多实例路线.md`
- 数值校验：每个测点先过 `math.isfinite()`（挡 NaN/inf）再过量程白名单
  （`POINT_RANGES`，挡负数和超量程）。任一测点不合格就整条拒收 —— 因为 NaN/inf 会让
  TDengine 报 syntax error，整行连其余 8 个正常测点一起丢，不如提前拦下。
  拒收条数会累计在日志里（`报文已拒收（累计 N 条）`）
- 启动自检：库名/表名/标签做标识符白名单校验（这些值会拼进 SQL），不合法直接拒绝启动
- 标签：plant, device
- 依赖：paho-mqtt、taospy

### 4. 展示层 `src/web/web_dashboard.py`
- 功能：Flask 后端 + ECharts 前端实时曲线（9 测点 / 4 组 Y 轴）
- 接口：`GET /api/data` 返回最近 10 分钟数据（JSON）
- 健康检查：`GET /api/health` 返回 Web 与 TDengine 是否都通
- 刷新：前端每 5 秒自动拉取
- 监听：0.0.0.0:5000（局域网可访问）
- 依赖：flask、taospy

## 方式零：一键启动（双击即可，就绪后自动打开浏览器）

**双击 `一键启动.bat`** 就行。脚本会：构建并启动 8 个容器 → 轮询 `http://localhost:5000/api/health`
→ **确认 Web 真的能响应之后**才打开浏览器（不是盲等几秒就开）。

失败时不再假装成功：会打印占用端口的进程、`docker compose logs` 排查命令；
Docker Desktop 没启动时会自动尝试拉起并等引擎就绪。

| 命令 | 作用 |
|---|---|
| `一键启动.bat` | 默认 Docker 模式：启动全部服务 + 打开大屏 |
| `一键启动.bat -Mode Local` | 本机模式：EMQX/TDengine 走容器，4 个服务用 4 个 Python 窗口（逐层看日志） |
| `一键启动.bat -NoBrowser` | 只启动服务，不打开浏览器 |
| `一键启动.bat -NoBuild` | 跳过镜像构建（改过 `src/` 代码时不要加） |
| `一键启动.bat -Stop` | 停止全部容器（数据卷保留，数据不丢） |
| `.\start.ps1 -DryRun` | 只打印将要执行的动作，不实际启动（排查用） |

说明：`.bat` 只是外壳，真正逻辑在 `start.ps1`（同为项目文件，可直接传参运行）。

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

默认启动的就是上面这 8 个服务（含 nginx 与 redis）。**第二台设备**是另外两个容器
（`cems-gateway-plant2` / `cems-subscriber-plant2`），默认不启动，见 §多设备。

### 多设备（第二台设备）：已具备能力，配置即可

链路按 `(plant, device)` 建 TAG、按 `{plant}_{device}` 建子表，数据层本来就支持多台；
采集侧是"一个实例服务一台设备"，所以第二台设备 = **第二套实例**。开启需要两步
（两步都做才行：只起实例、设备层没有从站 2，第二个网关会读不到数据并在日志里报错）：

```ini
# .env：让设备层在同一个 5020 端口上同时提供从站 1 和 2
SLAVE_IDS=1,2
```

```bash
docker compose --profile plant2 up -d     # 追加启动第二套网关/接入实例
docker compose --profile plant2 ps        # 看两套实例
```

第二套的默认取值：从站 `2` → 主题 `cems/plant2/data` → 标签 `plant2/device2`
（子表 `plant2_device2`）→ 缓存目录 `./data/plant2`。要改厂区/设备名，改 `.env` 里的
`PLANT2` / `DEVICE2` / `MQTT_TOPIC2` / `GATEWAY2_CLIENT_ID` / `SUBSCRIBER2_CLIENT_ID`
（**注意主题的厂区段要与 `PLANT2` 一致**，不一致时接入层会在启动日志里给 WARNING）。

关闭第二套：

```bash
docker compose --profile plant2 stop gateway2 subscriber2
```

⚠️ 不带 `--profile` 的 `docker compose stop/down` **管不到**这两个容器（它们不在默认模型里）。

为什么是"每台设备一套实例"而不是"一个实例订阅通配主题、从主题解析设备号"、
为什么设备层用多从站而不是第二个端口，见 `docs/adr/0008-多设备多实例路线.md`。

### 端口暴露范围（默认只开本机）

所有端口默认只绑定 `127.0.0.1`，同网段的其它设备访问不到。原因很实际：
EMQX 的 1883 默认允许匿名发布（别人可以伪造数据）、18083 管理台默认 `admin/public`、
TDengine 6041 用的是演示口令 `root/taosdata`（可以读写删库）。绑到 `0.0.0.0`
等于把这三样一起交出去。

需要局域网看大屏时，在 `.env` 里改：

```ini
BIND_ADDR=0.0.0.0
```

**改完必须同时改掉 `TD_PASS` 和 EMQX 的默认口令**，否则就是上面那种情况。
`start.ps1` 在这种情况下会额外打一条告警。

常用命令：

```bash
docker compose ps                     # 看服务状态（默认 8 个）
docker compose logs -f gateway        # 跟踪某个服务日志
docker compose down                   # 停止（数据保留在具名卷里）
docker compose down -v                # 停止并连数据一起删
docker compose run --rm web python src/web/query_tool.py   # 在容器里核对入库情况
```

数据落地位置：TDengine 数据在具名卷 `cems-tdengine-data`，EMQX 在 `cems-emqx-data`，
网关断网缓存分别在宿主机的 `data/`（第一套）与 `data/plant2/`（第二套）。


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

本机模式下同样可以跑第二套实例（PowerShell 里设好环境变量再起一个终端即可）：

```powershell
# 终端 A：设备层同时提供从站 1、2
$env:SLAVE_IDS="1,2"; python src/device/modbus_server.py
# 终端 B：第二套网关（从站 2 → 主题 cems/plant2/data → 缓存目录 data/plant2）
$env:MODBUS_UNIT="2"; $env:MQTT_TOPIC="cems/plant2/data";
$env:MQTT_CLIENT_ID="cems-gateway-plant2"; $env:GATEWAY_DATA_DIR="data/plant2";
python src/gateway/gateway.py
# 终端 C：第二套接入（写 plant2_device2）
$env:MQTT_TOPIC="cems/plant2/data"; $env:MQTT_CLIENT_ID="cems-subscriber-plant2";
$env:TD_PLANT="plant2"; $env:TD_DEVICE="device2";
python src/platform/subscriber_to_td.py
```

## 辅助工具

- `src/web/query_tool.py` — 查询入库数据 + INTERVAL 时间聚合（运维排查用）
- `docs/` — 架构复习图（HTML）

## 环境依赖

见 `requirements.txt`。Python 3.12。

> 注意：`pymodbus` 已锁到 `<3.9`。3.9 起官方把 `ModbusSlaveContext` 改名、
> 把 `slave=` 参数改成 `device_id=`，设备层会直接 ImportError。
