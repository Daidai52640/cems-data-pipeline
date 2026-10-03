# CEMS 工业数据采集链路

一条端到端的工业烟气连续监测（CEMS）数据链路：**仿真设备 → 网关采集（Modbus TCP）→ MQTT 消息总线 → 接入层入库（TDengine）→ Web 实时大屏 / 报表**。
默认 **5 秒采集一次，单设备链路实测 12 条/分钟稳定入库**；支持双设备接入与高并发吞吐验证。

> 当前形态：**8 个基础容器 + 2 个双设备容器（profile）= 10 个容器**，**9 个测点**。
> 本项目在本地仿真环境中开发与验收，未接现场真实仪表，见文末「已知边界」。

---

## 架构分层

### 1. 设备层（Modbus TCP 仿真服务器）

- 纯 Python 实现的 Modbus TCP 从站，模拟一台 CEMS 监测设备，**9 个保持寄存器**对应 9 个测点：
  流量、颗粒物、SO2、NOx、O2、流速、温度、湿度、压力（定义见 `src/common/points.py`）。
- 寄存器值按正弦 + 日变化相位 + 小幅噪声生成，各测点有独立量程与编码（如 SO2 = `a21026`）。
- 支持多从站：`.env` 中 `SLAVE_IDS=1,2` 即同时仿真两台设备（unit id 严格分派，见下文「双设备 / 高并发」）。
- 端口 **5020**。

### 2. 网关层（数据采集 + 断网缓存 + 补传）

- 每 5 秒通过 Modbus TCP 轮询 9 个寄存器，按 HJ 212 因子顺序组装成一条 JSON 样本。
- 通过 MQTT（QoS 1）发布到 EMQX；发布前在本地 `data/cache.jsonl` 留一份待确认记录，收到 broker 确认后删除。
- **断网时自动缓存**（容量上限 64MB，超出丢弃最旧数据），恢复后按 30 秒间隔自动补传，保证数据不丢。
- 双设备时每台设备一个独立网关实例（第二套为 profile `plant2`，缓存目录 `data/plant2/`）。

### 3. 接入层（MQTT 订阅 + 入库 TDengine）

- 订阅 MQTT 主题，将样本批量写入 TDengine 超级表 `cems.cems_data`（按设备名分子表，`KEEP 365d`）。
- 告警判定（覆盖率门限 0.75、折算值严格大于限值判超标），事件落 `cems_alarm_event`、
  小时达标率落 `cems_hourly_verdict`、推送记录落 `cems_alarm_push`。
- 支持 MQTT Clean Session=0 + EMQX 持久会话，接入层重启期间消息由 broker 保留。
- 双设备时第二套接入实例独立消费、独立入库，互不影响。

### 4. 展示层（Flask + ECharts 实时大屏 / 报表）

- **实时大屏**（`/`，暗色主题）：ECharts 曲线展示全部测点；鼠标悬停 tooltip 对三个浓度测点
  （颗粒物、SO2、NOx）显示 **实测 / 折算 / 限值** 三行——
  折算值 = 实测值 × 15 / (21 − O2)；**O2 > 19% 时折算无意义，显示 "—"**。
- **超标标记**：以折算值与限值比较，**折算值 > 限值**的点在曲线上标红（等于限值算达标，不标）。
- **报表页**（`/report`）：分钟/日/月/自定义报表，覆盖率面板直接显示
  **整体覆盖率 overall、门限 threshold、数据不足窗口数 insufficient_windows、断档数 gap_count**；
  状态严格三态：**达标绿 / 超标红 / 数据不足灰**（缺数据绝不画成达标；窗口未结束另有"进行中"描边态）。
- Redis 查询缓存（TTL 300 秒），降低 TDengine 查询压力；`/api/cache/*` 经 nginx 对外 403。

---

## 快速开始

### 一键启动（Windows）

双击 **`一键启动.bat`**（内部调用同目录的 `start.ps1`），自动完成：环境检查 → 构建镜像 → 启动容器 → 轮询健康接口 → 打开浏览器。

| 启动方式 | 命令 | 说明 |
|---|---|---|
| 完整启动（默认） | `.\一键启动.bat` | 构建 + 启动 + 打开页面 |
| 快速重启（不构建） | `.\一键启动.bat -NoBuild` | 镜像已构建，跳过 build |
| 只启动不打开浏览器 | `.\一键启动.bat -NoBrowser` | 服务器启动后不自动开页面 |
| 本地模式（4 个窗口） | `.\一键启动.bat -Mode Local` | 不用 Docker，按层开 4 个 Python 窗口逐层看日志 |
| 只打印动作 | `.\一键启动.bat -DryRun` | 不构建不启动，只打印将要执行的命令 |
| 传统构建器 | `.\一键启动.bat -ClassicBuild` | 用 `DOCKER_BUILDKIT=0` 构建（BuildKit 报错时用） |
| 停止全部容器 | `.\一键启动.bat -Stop` | 等价 `docker compose down`，数据卷保留 |
| 查看帮助 | `.\一键启动.bat -?` | 显示所有参数说明 |

### Docker Compose（Linux/macOS 同样适用）

```bash
# 构建镜像并启动 8 个基础服务（后台运行）
docker compose up -d --build

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

### 访问入口

| 服务 | 地址 | 说明 |
|---|---|---|
| Nginx 统一入口 | http://localhost | 对外主入口，反代 Web 与 API |
| Web 直连后端 | http://localhost:5001 | 直连 web 容器（容器内端口 5000），对照 nginx 层差异 |
| EMQX Dashboard | http://localhost:18083 | MQTT 管理台，默认 admin/public |
| TDengine | 6041（REST）/ 6030（原生） | 默认 root/taosdata |
| Modbus 设备 | 5020 | 仿真设备 Modbus TCP 端口 |

### 本机方式（不用 Docker）

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
.\一键启动.bat -NoBuild    # 会自动拉起本机 EMQX/TDengine（若已安装）或用 Docker 只起中间件
```

### 端口暴露 / 安全

默认所有端口只绑定 `127.0.0.1`（本机访问）。如需局域网/外网访问：

1. 在 `.env` 中设置 `BIND_ADDR=0.0.0.0`（或具体网卡 IP）；
2. **务必修改默认口令**：EMQX（admin/public）、TDengine（root/taosdata）；
3. 不要把 5020（Modbus 设备）、6041（TDengine REST）暴露到公网；
4. 如需修改宿主机端口，设置 `NGINX_HOST_PORT`、`WEB_HOST_PORT` 等变量（见 `docker-compose.yml`）。

> **Docker Desktop 构建鉴权错误**：若 `docker compose up -d --build` 报
> `unauthorized: authentication required` / `not authorized`，是 Docker 客户端把本机构建请求
> 错误带上了 registry 凭证。处理：`docker logout`，或在 Docker Desktop 设置中登出 registry 账号后重试；
> 本项目镜像 `pull_policy: never`，构建不依赖任何 registry。

---

## 双设备 / 高并发

双设备能力**已具备，配置即可**：开关一在 `.env` 设 `SLAVE_IDS=1,2`（设备层双从站），
开关二用 `docker compose --profile plant2 up -d` 拉起第二套网关 + 接入实例（共 10 个容器）；
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
- **折算与超标展示**：tooltip 实测/折算/限值三行、曲线超标红点、报表三态状态列（见「展示层」）。
- **查询缓存**：Redis 缓存 TTL 300 秒，命中率/水位可经 5001 端口 `/api/cache/stats` 查看。
- **备份与恢复**：备份到仓库外 `F:\cems-backup`（逻辑 + 物理），双设备 18 570 行备份 4.42 s；
  灾难恢复 RTO（另起实例）P50 14.788 s，自定目标 RTO ≤ 30 分钟。
- **高并发**：N=50 时 52.59 条/秒、missing=0、duplicate_ts=0（loadgen 直发 EMQX，绕过网关）。

---

## 文档索引

**运维操作（runbooks）**
- [运维手册](docs/runbooks/运维手册.md) —— 启停、健康检查、巡检、备份恢复入口、日志、常见问题、阈值速查
- [双设备启用与验证](docs/runbooks/双设备启用与验证.md) —— 两个开关、启用步骤、逐值对比验收、回退
- [容器重建与代码生效](docs/runbooks/容器重建与代码生效.md) —— 改代码后让容器真正生效的正确命令
- [故障台账](docs/runbooks/故障台账.md) —— 历次故障的现象、排查、根因、修复与数据影响
- [恢复演练与对账口径](docs/runbooks/恢复演练与对账口径.md) —— 备份/恢复步骤、RTO 台账、对账恒等式
- [全链路验收设计](docs/runbooks/全链路验收设计.md) —— 启动顺序、C1–C19 验收契约、冻结常量表

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

1. **非现场联调**：仅在本地回环完成，对照手段为官方向量与开源实现对拍，未接真实监测平台/仪表/环保平台。
2. **接入层无入库副本**：TDengine 写失败时，该样本不在任何地方（接入层不持有本地队列，P1 未修）。
3. **仿真非真实工况**：设备为仿真器，数据为合成波形，未验证真实仪表的量程边界、异常码、时钟漂移等现场问题。
4. **负载为合成**：高并发数据由 loadgen 生成并**直发 EMQX、绕过网关**，验证的是接入 + 存储层，不代表现场网关容量。
5. **单机部署**：所有容器在一台宿主上，未验证多机部署、集群与高可用（HA）。
6. **网关采集上限**：单网关实例节拍硬上限约 **1.05 条/秒**（采集周期从 5 s 压到 0.05 s 不再提升），
   扩容方式是增加网关实例，而非压单实例。

---

## 源文件

- `docker-compose.yml` —— 8 基础 + 2 profile 共 10 个服务/容器、端口、环境变量、nginx 403 口径
- `src/common/points.py` —— 9 个测点、量程、编码、限值与基准氧
- `README.md`（原有版本） —— 架构分层、一键启动参数、本机方式、端口暴露、构建鉴权处理等正确内容
- `docs/runbooks/双设备启用与验证.md` —— 两个开关、逐值对比验收、回退
- `docs/runbooks/全链路验收设计.md` —— 覆盖率门限 0.75、6 条已知边界、验收契约
- `docs/runbooks/恢复演练与对账口径.md` —— 备份落点、RTO 数字、备份行数与耗时
- `docs/reference/性能与可靠性指标.md` —— 延迟分位、完整率、补传成功率、断档判据
- `docs/reference/并发与负载指标.md` —— N=50 吞吐、网关 1.05 条/秒硬上限、loadgen 绕过网关
- `docs/adr/0008-多设备多实例路线.md` —— 双设备"已具备能力，配置即可"的口径
- `docs/runbooks/故障台账.md` —— healthy 与业务正常的区分、故障案例
- `docs/runbooks/容器重建与代码生效.md` —— 单构建入口、`pull_policy: never` 的表述
