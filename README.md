# CEMS 工业数据采集链路

一条端到端的工业烟气连续监测（CEMS）数据链路：**设备 → 网关采集（Modbus）→ MQTT → 接入层入库（TDengine）→ Web 实时大屏 / 报表**。
默认 **5 秒采集一次**；**9 个测点**；支持双设备接入与高并发吞吐验证。

> ⭐ **真机已接入**（2026-10-11）：一台 **RS485 温湿度变送器**（Modbus RTU，`4800/8/N/1`，只读）经
> **串口服务器模式**接入 —— 宿主脚本 `scripts/serial_bridge.py` 把 RTU 转成 Modbus TCP，
> **设备层把 2 个寄存器改由真机供给**，其余 7 个仍为仿真。
> ⭐ **网关 / 接入层 / 库表 / 大屏一行未改**，真机数据直接上了大屏（手捂传感器：湿度 60 % → 99 %）。
>
> **全过程与实测证据**（6 种异常实验、⭐ **两条与手册不符的发现**、掉线/自愈实测）见
> [`真机接入.md`](docs/runbooks/真机接入.md)；**实验基线**见
> [`真机接入-基线冻结v2.md`](docs/runbooks/真机接入-基线冻结v2.md)。
>
> **为什么串口侧留在宿主**：容器（WSL2）**拿不到 Windows COM 口** —— 实测 WSL2 里无 `/dev/ttyUSB*`、
> 容器里连 `/dev/ttyS*` 都没有，**且 WSL2 内核没有 ch341 模块**（所以光装 `usbipd` 也不够）。
> 这条路径恰好等价于现场那台**串口服务器**。
>
> **当前形态**：平台侧 **6 个容器**（emqx / tdengine / redis / subscriber / nginx / web）；`--profile sim` 再加设备层 + 网关（本机全仿真 = 8）；再叠加 `--profile plant2`（gateway2 / subscriber2 / web2）= **11**（双设备，含二号炉大屏）。边缘部署时南向两层在树莓派上，平台侧**不带** `sim`（见 `docs/runbooks/边缘节点下板.md`）。
>
> ⚠️ **语义边界（必须说清，否则会被误读）**：真机**只接 1 号炉**；`Temp`/`Humidity` 两列承载的是
> **环境**温湿度、**不是烟气参数**（本部署只作**数据链路演示**，不作环保口径依据）。
> **尚未做现场联调**（未与真实监测平台/环保平台对拍）。完整清单见文末「已知边界」。

> ✅ **最小验收 25 项全通过**（`python scripts\acceptance_min.py`，退出码 0）。**适用拓扑**：它要求仿真数据源 device 可达，故针对本机全仿真形态；要跑满 A1/A3 的双设备判据需 `--profile sim --profile plant2` 且 `.env` 设 `SLAVE_IDS=1,2`（11 个容器）。⚠️ 平台默认面只有 6 个容器、不含 device，**不能**跑这套验收；边缘部署（南向在树莓派 systemd）时 A7 的两项网关缓存判据会自动 SKIP。
> v4.0 首验于 2026-10-03；⭐ **2026-10-11 真机接入后重跑，仍 25/25**。
> ⚠️ 但它**不专门覆盖真机链路**（判据看的是链路整体健康，真机只占其中 2 个测点）。
> 验收单与边界声明见 [`ACCEPTANCE.md`](ACCEPTANCE.md)。

> **文档地图**：决策 → [`docs/README.md`](docs/README.md) 的 `adr/` 一节（**10 份**，
> 最新是 **0009 接入层逐点质量位** / **0010 数据合理性判据**）；
> 手册与台账 → 同上 `runbooks/` 一节；事实与实测 → `reference/` 一节。

---

## 架构分层

### 1. 设备层（Modbus TCP 仿真服务器）

- 纯 Python 实现的 Modbus TCP 从站，模拟一台 CEMS 监测设备，**9 个保持寄存器**对应 9 个测点：
  流量、颗粒物、SO2、NOx、O2、流速、温度、湿度、压力（定义见 `src/common/points.py`）。
- 寄存器值按正弦 + 日变化相位 + 小幅噪声生成，各测点有独立量程与编码（如 SO2 = `a21026`）。
- 支持多从站：`.env` 中 `SLAVE_IDS=1,2` 即同时仿真两台设备（unit id 严格分派，见下文「双设备 / 高并发」）。
- 端口 **5020**。
- ⭐ 真机接入有两种形态：容器部署用宿主上的 `scripts/serial_bridge.py`（RTU→Modbus TCP，`REAL_DEVICE_SOURCE=tcp`，**默认**）；
  设备进程与串口在同一台宿主时可**进程内直读**（`REAL_DEVICE_SOURCE=serial`），见 [`直读串口.md`](docs/runbooks/直读串口.md)。

### 2. 网关层（数据采集 + 断网缓存 + 补传）

- 每 5 秒通过 Modbus TCP 轮询 9 个寄存器，按 HJ 212 因子顺序组装成一条 JSON 样本。
- 通过 MQTT（QoS 1）发布到 EMQX；发布前在本地 `data/cache.jsonl` 留一份待确认记录，收到 broker 确认后删除。
- **断网时自动缓存**（容量上限 64MB，超出丢弃最旧数据），恢复后按 30 秒间隔自动补传，保证**网关→broker 这一段**的报文不丢。
- 双设备时每台设备一个独立网关实例（第二套为 profile `plant2`，缓存目录 `data/plant2/`）。

### 3. 接入层（MQTT 订阅 + 入库 TDengine）

- 订阅 MQTT 主题，将样本批量写入 TDengine 超级表 `cems.cems_data`（按设备名分子表，`KEEP 365d`）。
- **写库失败先落本地队列**：把原始报文追加到 `data/subscriber/spool.jsonl`（fsync 落盘），
  后台每 15 秒补传一轮，**入库成功才删**——修掉了旧「接入层无副本 · P1 未修」那个
  "回调返回→broker 删副本→写库失败即永久丢失"的洞（机制与崩溃安全见
  `src/platform/ingest_spool.py`）。仅当"落盘本身也失败（磁盘满/无权限）"或"积压超过
  容量上限（默认 256MB，丢最旧）"时才会丢，两者都打 CRITICAL。
- 告警判定（覆盖率门限 0.75、折算值严格大于限值判超标），事件落 `cems_alarm_event`、
  小时达标率落 `cems_hourly_verdict`、推送记录落 `cems_alarm_push`。
- 支持 MQTT Clean Session=0 + EMQX 持久会话，接入层重启期间消息由 broker 保留。
- 双设备时第二套接入实例独立消费、独立入库，互不影响。

### 4. 展示层（Flask + 监测数据表 / ECharts 曲线）

- ⭐ **版式对齐真实环保平台**（**不是自己设计的排版**；⚠️ **平台具体名称应单位要求不具名**，此处只描述可公开的版式特征）：
  表格页与导出的 Excel 都按**真实环保平台导出的报表版式**做 ——
  **标题行 + 信息行 + 三行合并表头**；污染物每组两列（**标干值 / 折算值**）；
  「监测时间」列**按粒度**写（分钟 / 小时 / 日三种写法一眼可辨，而不是统一日期格式，否则会丢掉"这个数覆盖哪一段"）；
  ⭐ **列名随粒度变**：小时表的同一个数按平台口径叫「**累计流量**」，其余粒度叫「流量」。
  连曲线页也是"**照平台**"改的（单图 + 轴自适应、图例带全选/反选）。
  > **证据**：`src/web/report_export.py`（4 处「平台口径」注释）、`src/web/report.py`、提交 `924546d`（三种报表统一为平台版式）、`127ad27`（曲线页照平台）、`90f8882`（表头改三行合并）。
  > ⚠️ **对齐的是"版式与口径"，不是"已对接环保平台"** —— 传输层（HJ 212 报文）**从未在真实对端验证**，见文末「已知边界」。
  > ⚠️ **本仓库不含任何来自真实平台的原始数据、报表文件或平台标识** —— 只保留"版式长什么样"这一层公开信息。
- **监测数据表**（`/`，进入即见）：两级表头的窗口表格（污染物含标干/折算/标记，其余测点含监测值/标记），可按设备、粒度（分钟/小时/日）、区间查看，支持超标/异常过滤、自动刷新、导出 Excel；`/chart` 是 ECharts 多面板曲线视图（每格一个量纲一根 Y 轴），左上角按钮在两视图间切换（曲线视图的 tooltip 对三个浓度测点
  （颗粒物、SO2、NOx）显示 **实测 / 折算 / 限值** 三行——
  折算值 = 实测值 × 15 / (21 − O2)；**O2 > 19% 时折算无意义，显示 "—"**。
- **超标标记**：以折算值与限值比较，**折算值 > 限值**的点在曲线上标红（等于限值算达标，不标）。
- **表格页**（`/`，旧 `/report` 会重定向到这里）：分钟/小时/日区间报表，覆盖率显示
  **整体覆盖率 overall、门限 threshold、数据不足窗口数 insufficient_windows、断档数 gap_count**；
  状态严格三态：**达标绿 / 超标红 / 数据不足灰**（缺数据绝不画成达标；窗口未结束另有"进行中"描边态）。
- Redis 查询缓存（TTL 300 秒），降低 TDengine 查询压力；`/api/cache/*` 经 nginx 对外 403。

---

## 快速开始

### 启动（Docker Compose）

本项目**统一用 Docker Compose 启动**（不再提供 `一键启动.bat` / `start.ps1` 这类一键启动脚本；Windows / Linux / macOS 命令一致）。

```bash
# ① 本机全仿真演示（设备层 + 网关也在这台机器上）：8 个服务
docker compose --profile sim up -d --build

# ② 平台侧（南向 device + 网关跑在边缘节点上，见 docs/runbooks/边缘节点下板.md）：6 个服务
#    ⚠️ 不要带 --profile sim：那会在这台机器上另起一个仿真从站 1，与真机数据混写同一张子表
docker compose up -d --build

# ③ 双设备全仿真（再加第二套网关 + 接入 + 二号炉大屏）：11 个容器
#    先在 .env 设 SLAVE_IDS=1,2；gateway2 依赖 sim 的 device，两个 profile 必须一起带
# docker compose --profile sim --profile plant2 up -d --build

# 查看运行状态
docker compose ps

# 跟踪网关日志（观察采集和补传）
docker compose logs -f gateway

# 停止并删除容器（数据保留在卷里）
docker compose down

# 停止并连数据一起删除
docker compose down -v
```

启动后访问 `http://localhost`（经 nginx 反代）。

### 换机器 / 断网部署（离线搬运）

四个基础设施镜像（EMQX / TDengine / Redis / nginx）在 compose 里按**具体版本 tag** 固定
（不用 `latest`；digest 台账见 `.env.example` 末节），应用镜像是本机构建产物。

**⚠️ 为什么是 tag 而不是 digest**：`docker save <镜像@sha256:...>` 导出的 tar 里
`RepoTags` 是 **null**（不带名字），`docker load` 回来之后 compose **无法解析该 digest 引用**，
会转去联网拉 —— 断网机器上就是硬失败。**导出必须按 tag。**

```powershell
# 源机：打包（应用镜像 + 四个基础镜像 → 一个 tar + sha256）
pwsh -NoProfile -File scripts\export_images.ps1

# 目标机（干净、无外网）：导入 → 起链路
pwsh -NoProfile -File scripts\import_images.ps1 -From <tar 所在目录>
docker compose --profile sim up -d --build   # 本机全仿真；基础层已在本地，--build 不需要联网
```

两个脚本都内置"按 digest 导出的包一律拒绝"的校验，并会核对包内镜像是否覆盖 compose 所需。
步骤与实测记录见 [`docs/runbooks/离线部署验证.md`](docs/runbooks/离线部署验证.md)。

### 访问入口

| 服务 | 地址 | 说明 |
|---|---|---|
| Nginx 统一入口 | http://localhost | 对外主入口，反代 Web 与 API |
| Web 直连后端 | http://localhost:5001 | 直连 web 容器（容器内端口 5000），对照 nginx 层差异 |
| 2 号炉大屏 | http://localhost:5002 | 双设备演示的第二套展示实例（compose 服务 `web2`，`--profile sim --profile plant2`） |
| EMQX Dashboard | http://localhost:18083 | MQTT 管理台。演示默认值是 `admin/public`；本部署**已改口令**（只存本机、不入库），且只绑回环 |
| TDengine | 6041（REST）/ 6030（原生） | 默认 root/taosdata |
| Modbus 设备 | 5020 | 仿真设备 Modbus TCP 端口 |

### 端口暴露 / 安全

默认所有端口只绑定 `127.0.0.1`（本机访问）。如需局域网/外网访问：

1. **只有 MQTT 需要单独对外**：设 `MQTT_BIND_ADDR=0.0.0.0`（边缘节点要从网外发布数据）。
   `BIND_ADDR` 仍保持默认，TDengine / 管理台 / nginx / web 不跟着暴露；
2. ⚠️ **开 MQTT 认证**（已实现）：EMQX 用 `password_based` + `built_in_database`，
   网关与接入层通过 `MQTT_USERNAME` / `MQTT_PASSWORD` 带凭据（见 `src/common/mqtt_auth.py`）。
   实测：匿名连接被拒（CONNACK 135），带凭据连接成功（CONNACK 0）；凭证只在
   `/etc/cems/edge.env`（600）与平台侧 `.env`（不入库），不进仓库；
3. **改掉 TDengine 口令**：在 `.env` 里设 `TD_PASS`（默认 `root/taosdata` 是演示值）；
4. ⚠️ **MQTT 目前是明文**：认证只解决"谁能连"，不解决"路上被看"。跨不可信网络必须再加 TLS
   （当前未做）；局域网内也不要长期开放 1883 到公网；
5. 不要把 5020（Modbus 设备）、6041（TDengine REST）暴露到公网；
6. 如需修改宿主机端口，设置 `NGINX_HOST_PORT`、`WEB_HOST_PORT` 等变量（见 `docker-compose.yml`）。

> **Docker Desktop 构建鉴权错误**：若 `docker compose up -d --build` 报
> `unauthorized: authentication required` / `not authorized`，是 Docker 客户端把本机构建请求
> 错误带上了 registry 凭证。处理：`docker logout`，或在 Docker Desktop 设置中登出 registry 账号后重试；
> 本项目镜像 `pull_policy: never`，构建不依赖任何 registry。
> （注意区分：这条只针对**构建**；四个基础设施镜像已按 digest 钉住，本来也不需要 registry 参与解析。）

---

## 双设备 / 高并发

双设备能力**已具备，配置即可**：开关一在 `.env` 设 `SLAVE_IDS=1,2`（设备层双从站），
开关二用 `docker compose --profile sim --profile plant2 up -d` 拉起第二套网关 + 接入实例 + 二号炉大屏（共 **11** 个容器：平台 6 + 仿真 device/gateway + gateway2/subscriber2/web2；`plant2` 依赖 `sim` 里的设备层，两个 profile 要一起带）；
两个开关缺一不可，且必须做"同一时刻逐值对比"验收。
高并发实测 N=50 下端到端 52.59 条/秒、missing=0；注意网关节拍硬上限约 1.05 条/秒/实例，扩容靠加实例。

- 启用步骤、日志证据、验收命令与回退：[`docs/runbooks/双设备启用与验证.md`](docs/runbooks/双设备启用与验证.md)
- 吞吐/延迟/资源数据与压测方法：[`docs/reference/并发与负载指标.md`](docs/reference/并发与负载指标.md)

---

## 能力清单

- **稳定采集**：5 秒周期，单设备 12 条/分钟、名义 720 条/小时；实测周期约 5.146 s。
- **断网缓存与补传**：64 MB 本地缓存；EMQX 断 181 秒演练，补传成功率 **100%（35/35）**，零丢失。
- **完整率**：名义完整率 97.22%（5 s 周期对 720 条/h），有效完整率 100%；断档判据 15 s。
- **直发延迟**：P50 0.58 s / P95 1.07 s（max 1.23 s）。
- **告警**：超阈值 / 恢复 / 小时达标率判定与落库，覆盖率门限 0.75，严格大于限值才判超标。
- **健康判据分两层**：`healthy` = 链路在流（数据新鲜度）；`sensor_ok` = 真机读数有效（最近 5 分钟无效测点数 < 阈值）。真机失效时 `healthy` 仍为真、`sensor_ok` 翻假并告警 —— 库里那两列是 NULL，不是 0、也不是仿真值。
- **折算与超标展示**：tooltip 实测/折算/限值三行、曲线超标红点、报表三态状态列（见「展示层」）。
- **查询缓存**：Redis 缓存 TTL 300 秒，命中率/水位可经 5001 端口 `/api/cache/stats` 查看。
- **备份与恢复**：备份到仓库外 `F:\cems-backup`（逻辑 + 物理），双设备 18 570 行备份 4.42 s；
  灾难恢复 RTO（另起实例）P50 14.788 s，自定目标 RTO ≤ 30 分钟。
- **高并发**：N=50 时 52.59 条/秒、missing=0、duplicate_ts=0（loadgen 直发 EMQX，绕过网关）。

---

## 文档索引

**验收（先看这个）**
- [ACCEPTANCE.md](ACCEPTANCE.md) —— 验收单：8 组需求逐条核对 + 明确未覆盖的边界 + 你自己复验的命令
- [最终验收报告](docs/runbooks/最终验收报告.md) —— v4.0 收官验收报告：方法、结果、本轮修掉的问题、诚实边界
- `python scripts\acceptance_min.py` —— 一条命令跑完 25 项判据（只读，退出码 0 = 全通过）

**运维操作（runbooks）**
- [运维手册](docs/runbooks/运维手册.md) —— 启停、健康检查、巡检、备份恢复入口、日志、常见问题、阈值速查
- [双设备启用与验证](docs/runbooks/双设备启用与验证.md) —— 两个开关、启用步骤、逐值对比验收、回退
- [容器重建与代码生效](docs/runbooks/容器重建与代码生效.md) —— 改代码后让容器真正生效的正确命令
- [故障台账](docs/runbooks/故障台账.md) —— 历次故障的现象、排查、根因、修复与数据影响
- [恢复演练与对账口径](docs/runbooks/恢复演练与对账口径.md) —— 备份/恢复步骤、RTO 台账、对账恒等式
- [全链路验收设计](docs/runbooks/全链路验收设计.md) —— 启动顺序、C1–C19 验收契约、冻结常量表
- [直读串口](docs/runbooks/直读串口.md) —— device 进程内直读 RS485：两种形态取舍、越界值语义、真机实测帧与端到端
- [nginx 对外入口](docs/runbooks/nginx-对外入口.md) —— Linux 宿主上把 80 端口跑起来：403 口径、`web` 占位转发、与 compose 形态的差异

**参考指标（reference）**
- [性能与可靠性指标](docs/reference/性能与可靠性指标.md) —— 延迟分位、完整率、补传成功率
- [并发与负载指标](docs/reference/并发与负载指标.md) —— 吞吐 vs N、网关硬上限、资源余量
- [HJ212 协议实现说明](docs/reference/hj212-协议实现说明.md)
- [HJ212 协议层独立验证记录](docs/reference/HJ212协议层独立验证记录.md)
- [容器时钟漂移](docs/reference/容器时钟漂移.md)

**设计决策（adr）**
- [0001 测点契约改版](docs/adr/0001-测点契约改版.md)
- [0002 告警判据选型](docs/adr/0002-告警判据选型.md)
- [0003 补传入队判定修正](docs/adr/0003-补传入队判定修正.md)
- [0004 引入 nginx 与 redis 缓存](docs/adr/0004-引入nginx与redis缓存.md)
- [0005 备份策略与保留期](docs/adr/0005-备份策略与保留期.md)
- [0006 HJ212 出口与北向适配](docs/adr/0006-HJ212出口与北向适配.md)
- [0007 P3 出口与传输方案](docs/adr/0007-P3出口与传输方案.md)
- [0008 多设备多实例路线](docs/adr/0008-多设备多实例路线.md)

**其他**
- [RELEASE v2.0.0 地基冻结（已归档）](docs/legacy/v2.0.0-地基冻结/RELEASE-v2.0.0-地基冻结.md)
- [docs 目录说明](docs/README.md)
- 旧版（6 容器 / 8 测点）资料归档于 `docs/legacy/v1.0-6容器-8测点/`

---

## 已知边界

以下为当前明确未覆盖/未验证的范围，**不作为已具备能力对外宣称**：

0. ⭐ **真机已接入，但只接了"温湿度"这一类**（2026-10-11）：一台 RS485 温湿度变送器接在**1 号炉**上，
   其余测点仍是仿真。⚠️ **真机的那两列（`Temp`/`Humidity`）承载的是环境温湿度、不是烟气参数**，
   本部署只作**数据链路演示**。边界与实测见 [`真机接入.md`](docs/runbooks/真机接入.md)。
1. **非现场联调**：**传输层未接真实监测平台 / 环保平台对拍**（无第三方参照仪器；`真机接入.md` 的 S3 判据
   "±0.5 degC 独立来源对照"**尚未做**）。
   ⚠️ **但要说清"对过什么"**：⭐ **展示层的报表版式与口径是对照真实环保平台导出的报表做的**
   （标题行+信息行+三行合并表头、标干值/折算值分列、监测时间按粒度、小时表叫"累计流量" —— 见「架构分层 · 展示层」），
   ⭐ **协议层也做过官方向量 + 开源实现对拍**（见 `docs/adr/0006` / `0007`）。
   ⇒ **"没对过"指的是传输层没换过真实对端，不是整个项目没看外部。**
2. ⚠️ **仿真测点仍是合成波形**：除 1 号炉温湿度外的 7 个测点仍为仿真器生成。
   ⭐ **但"真实仪表的异常行为"已部分验证**：实测发现该变送器**读不存在的寄存器返回全 0 而不报异常**、
   **功能码 06 被回显但值不生效**（见 `真机接入.md` S4）—— 这两条已反过来改变了设计。
3. **负载为合成**：高并发数据由 loadgen 生成并**直发 EMQX、绕过网关**，验证的是接入 + 存储层，不代表现场网关容量。
4. **单机部署**：所有容器在一台宿主上，未验证多机部署、集群与高可用（HA）。
5. **网关采集上限**：单网关实例节拍硬上限约 **1.05 条/秒**（采集周期从 5 s 压到 0.05 s 不再提升），
   扩容方式是增加网关实例，而非压单实例。
6. ⭐ **串口两种形态并存**：容器部署仍是"宿主辅助脚本"形态（`scripts/serial_bridge.py` 把 RS485/RTU 转成
   Modbus TCP，重启电脑后需重跑，或走 `scripts/up.ps1` 一条命令拉起）；设备进程与串口同宿主时可让 **device
   进程自己直读**（`REAL_DEVICE_SOURCE=serial`），已在 **x86 Linux（Mint）** 与 **ARM（树莓派 2B）** 的
   `/dev/ttyUSB0` 上各实测一次（原始帧与《物理层取证》逐字节一致）。默认值仍是 `tcp`，容器形态行为不变；
   取舍、越界值语义与未验证项见 [`直读串口.md`](docs/runbooks/直读串口.md)、[`边缘节点下板.md`](docs/runbooks/边缘节点下板.md)。
7. ⭐ **平台**：三处已收窄、三处未变。
   ✅ **应用镜像构建已在 Linux 本机验证**（`cems-pipeline:latest`，222 MB，容器以 compose 项目 `cems` 运行）；
   ✅ **南向两层已上 ARM 边缘节点**（树莓派 2B / Raspbian 12 armv7）：systemd 托管、`reboot` 自启、
   `kill -9` 自愈、停 broker 90 s 后缓存 25 条全部补传、485 真机直读，全部实测（见
   [`边缘节点下板.md`](docs/runbooks/边缘节点下板.md)）；
   ✅ **nginx 已在 x86 Linux 宿主以容器形态验过** 80 端口与 `/api/cache/*` 403 口径。
   ⚠️ **ARM 上只跑了南向两层**（device + 网关），web / 接入层 / 时序库仍在 x86，"整栈上 ARM"未验证；
   ⚠️ **看门狗只覆盖"循环卡住"**（`Type=notify` + `WatchdogSec=60`，采集循环每圈发 `WATCHDOG=1`）；`kill -STOP` 冻结进程 60 s 后被 systemd 杀掉重启已实测；`reboot` 后自动拉起也验过（硬断电未测）；
   ⚠️ **MQTT 已开认证但仍是明文**（无 TLS，跨不可信网络不行）；**单机，无 HA**。详见
   [`跨平台部署.md`](docs/runbooks/跨平台部署.md)、[`边缘节点下板.md`](docs/runbooks/边缘节点下板.md)。

---

## 源文件

- `docker-compose.yml` —— 平台侧 6 个服务；`--profile sim` 加 device/gateway = 8；再 `--profile plant2` 加 gateway2/subscriber2/web2 = 11；含端口、环境变量、nginx 403 口径
- `src/common/points.py` —— 9 个测点、量程、编码、限值与基准氧
- `README.md`（原有版本） —— 架构分层、启动命令、端口暴露、构建鉴权处理等正确内容
- `docs/runbooks/双设备启用与验证.md` —— 两个开关、逐值对比验收、回退
- `docs/runbooks/全链路验收设计.md` —— 覆盖率门限 0.75、已知边界、验收契约
- `docs/runbooks/恢复演练与对账口径.md` —— 备份落点、RTO 数字、备份行数与耗时
- `docs/reference/性能与可靠性指标.md` —— 延迟分位、完整率、补传成功率、断档判据
- `docs/reference/并发与负载指标.md` —— N=50 吞吐、网关 1.05 条/秒硬上限、loadgen 绕过网关
- `docs/adr/0008-多设备多实例路线.md` —— 双设备"已具备能力，配置即可"的口径
- `docs/runbooks/故障台账.md` —— healthy 与业务正常的区分、故障案例
- `docs/runbooks/容器重建与代码生效.md` —— 单构建入口、`pull_policy: never` 的表述
