# `scripts/` 目录索引

本目录是 cems-data-pipeline 的**验收 / 度量 / 演练 / 演示工装 + 共用底座**：39 个**代码文件**（36 `.py` + 3 `.ps1`）**全部平铺**，外加本索引 `README.md`
一个脚本一件事，产出证据落到 `docs/evidence/`，结论被 `docs/adr/` 与 `docs/` 下的指标文档引用。

> 本文件是**纯索引**。脚本的功能、参数、退出码一律以脚本自身的 docstring / `--help` 为准。

---

## ⚠️ 为什么不许移动 / 重命名这里的文件

**扁平导入是本目录的既定约定，不是历史遗留。** 所有脚本都以「脚本所在目录」为 `sys.path[0]` 运行，
所以直接写 `from _td_ops import ...` / `from _device_scope import ...` 就能工作：

- `_device_scope.py`（第 37-38 行）把这条约定写进了 docstring：
  > 脚本都在 `scripts/` 下、以「脚本所在目录」在 `sys.path[0]` 运行，
  > 所以直接 `from _device_scope import ...` 即可（与既有的 `scripts/_td_ops.py` 同一约定）。
- 用 `from scripts._td_ops import ...` 形式调用的是**少数**（`backup_tdengine.py`、
  `restore_drill_*.py`、`drill_timezone_misconfig.py`、`verify_backup_selfcheck_scope.py`、
  `measure_report_coverage.py`）——它们要求仓库根在 `sys.path` 上，两种口径并存但都依赖当前路径。

**一旦移目录或改文件名，下面两类引用会永久失效：**

1. 脚本内的裸导入（`_device_scope` 被 6 个脚本直接 `from _device_scope import ...`）会立刻
   `ModuleNotFoundError`；
2. `docs/adr/0002`、`0003`、`0004`、`0007` 等**定稿不改写**的 ADR 大量按 `scripts/xxx.py` 逐字引用路径。

> 旁证：ADR-0007（状态＝**方案（待评审）**）里还引用了 `scripts/loopback_hj212_server.py` 与
> `scripts/verify_hj212_outlet.py` —— 这两个是它 **P3 阶段计划新增、至今未落地**的工装
> （`git log --all` 全历史 **0 提交**），**不是"曾经存在又消失"**。
> ⚠️ **ADR 不会跟着代码改名而回改**；而"计划文件"与"现行文件"的区别**只能靠读者自己核实**
> —— 本仓库就发生过把它误读成"已失效路径"的事，故此处写明状态。
> 所以**新增脚本请直接放在 `scripts/` 根下**，不要新建子目录。

---

## 目录约定

| 前缀 / 后缀 | 含义 |
|---|---|
| `verify_*` | 验收断言脚本，判 PASS/FAIL，退出码 `0` = 全通过 / `1` = 有失败 |
| `measure_*` | 度量脚本，量出数字并落原始数据，不做通过与否的判定 |
| `drill_*` / `restore_drill_*` / `backup_*` | 故障与灾备演练：制造故障、真恢复一次、按保留策略备份 |
| `*_demo.py` | 可独立跑通的最小演示，证明"接口/协议确实能用"（本地回环） |
| `_*.py` / `_*.ps1` | 共用底座与编排脚本，**不单独产生业务输出**，被别的脚本 import / 调用 |
| `*.ps1` | PowerShell 编排（镜像离线搬运、并发压测） |

通用习惯（写脚本时照做）：

- 控制台按 UTF-8 读输出：`$env:PYTHONIOENCODING="utf-8"`，并在仓库根目录执行
  `python scripts\<脚本>.py`。
- **设备维度**：`cems_data` 是多设备共用的超级表（TAG = `plant, device`）。凡涉及库里行的脚本都用
  `_device_scope.py` 的 `add_device_argument()` 提供 `--device`（默认 `device1`），**不带过滤会串设备**
  （完整率被撑到 >100%、断档被另一台填平）。
- 需要 Docker 的脚本会在 docstring 里写明（如"需要 8 个服务已在跑"）；标"只读"的脚本确实只有 SELECT。

---

## 一、验收类（17 个）

对"东西到底能不能用"下断言，全部有明确退出码，多数可直接进 CI。

> 「验收类」共 17 条：`acceptance_min.py`、`analyze_gateway_resend_lag.py`、`sim_trace_dump.py`、
> `verify_alarm_live.py`、`verify_alarm_offline.py`、`verify_alarm_rate.py`、
> `verify_atomic_handover.py`、`verify_backup_selfcheck_scope.py`、`verify_data_lineage.py`、
> `verify_pipeline_live.py`、`verify_reference_sentinel.py`、`verify_report_coverage.py`、
> `verify_sim_24h_sweep.py`、`verify_sim_characteristics.py`、`verify_sim_diurnal_local_peak.py`、
> `verify_sim_reproducible.py`、`verify_sim_spike_exceedance.py`。
> 其中 5 个 `verify_sim_*` 是仿真自检，`sim_trace_dump.py` 是被它们调用的子进程工具。

| 文件 | 一句话用途 | 关键参数 / 用法 |
|---|---|---|
| `acceptance_min.py` | 甲方口径最小验收：只验"出钱的人会在乎的 8 条"（A1~A8），每条给出可复算证据。 | `--only A1 A3` 只跑指定几条；`--json out.json` 落机读结果。只读，不构造场景。 |
| `analyze_gateway_resend_lag.py` | 从网关日志量"缓存→补传"滞后（单时钟口径），并统计走补传路径的条目占比。 | `--log-file`（不给就现场 `docker logs`）、`--container cems-gateway`、`--since`、`--tag resend_lag`。 |
| `sim_trace_dump.py` | 子进程工具（验收类的一员）：按固定假时钟输出一整段仿真读数（JSON 行），供 `verify_sim_reproducible.py` 做跨进程比对。 | `python scripts/sim_trace_dump.py [种子] [起始时刻] [点数]`。不单独使用。 |
| `verify_alarm_live.py` | 排放超标告警线上验收：在八服务运行时查真库，验 L1~L7（三张表、事件成对、判定快照、推送一一对应、幂等、小时结论、判据统计日志）。 | `--since "2026-10-02 18:26:00"`、`--device device1`。**只读**，可反复跑。 |
| `verify_alarm_offline.py` | 告警离线回放验收：纯逻辑边界（折算法/`>` 语义/`d2` 提醒/O2=21）+ 落库快照与幂等 + 小时结论三态。 | 无参数。离线，不连库不连 broker；隔离标签 `plant=verify/device=offline`。退出码 `0`/`1`。 |
| `verify_alarm_rate.py` | 告警事件率验收：`SIM_SPIKE_ENABLE=0` 一小时 0 个 start；默认参数一小时 start/end 成对且 ≤ 12 个/测点。 | **必须分两个进程**：`--mode off` 与 `--mode default`（`SPIKE_ENABLED` 是导入期读的环境变量）。 |
| `verify_atomic_handover.py` | 原子交接回归测试：`cache.jsonl` ↔ 在途批次文件（ADR-0007 §2 的 P1~P7 / N1 / D1 断言，排序关开各一遍）。 | `--sort-mode off\|on\|both`（默认 both）、`--verbose`、`--keep-tmp`。完全离线；退出码 `0/1/2/3`（`3` = 安全守卫拒绝运行）。 |
| `verify_backup_selfcheck_scope.py` | 备份自检口径复核：`reported_rows` 该与"整库所有 stable 之和"比，而非单张 `cems_data`，据此把"备份完整性"与"主表窗口单调性"分开判。 | `--backup <目录>`、`--tolerance 20`（判据①允许的绝对差）。对现网库**只有 SELECT**。 |
| `verify_data_lineage.py` | 验收 C19 数据血缘：同一条数据从寄存器走到北向出口，逐跳比对面值与条数（T1~T8），证明"发出去的就是入库的那条"。 | `--seconds 25`。需 Docker（起一次性临时链路 `TD_PLANT=trace`，跑完清理）；某跳工装不具备时打 SKIP，不伪造 PASS。 |
| `verify_pipeline_live.py` | 验收自检 4/4 全链路在线检查：库里新增数据是否"5 秒一条 + 不再乱跳"（采集节奏、新鲜度、跳变幅度）。 | `--minutes 5`、`--since`、`--device`。⚠️ `--since` 几乎必须给，否则回看窗口跨过改造时刻会被旧的随机数据带偏。 |
| `verify_reference_sentinel.py` | 验收无法折算时的传输哨兵值（HJ 212-2025 §8.1.1 d）：契约层 `9999.99`、折算算式逐位回归、接入层三个出站出口。 | 无参数，**不需要 Docker**，可直接进 CI。退出码 `0`/`1`。 |
| `verify_report_coverage.py` | 验收报表接口的"覆盖率 + 样本不足"标记：用库里真实断档做回归，逐条断言 A~H（断档必须出现、门限等于不算不足、契约只增不减、Excel 列布局未动）。 | `--base-url`、`--gap-start`、`--gap-end`。只读走 HTTP；`--device` 用来核对 web 实例 `device_scope`，不一致直接失败。 |
| `verify_sim_24h_sweep.py` | 验收自检 2/3：用**假时钟**扫 24 小时，验日周期形状（ASCII 曲线、峰谷与正弦拟合 R²、相位可复现），不必真等一天。 | 无参数，无需 Docker。退出码 `0`/`1`。 |
| `verify_sim_characteristics.py` | 验收自检 1/3：仿真信号的"工业特征"与改造前后对比（docstring 承诺跳变、量程合规、层次分离、负荷耦合、基线标定五段，代码实际只有前三段的函数）。 | 无参数，纯离线可跑。退出码 `0`/`1`。 |
| `verify_sim_diurnal_local_peak.py` | 日周期"当地时刻"复验：把 `verify_sim_24h_sweep.py` 的拟合峰值换算成当地钟点，并与"修复前按 UTC 小时"对照（差值应恒为 +8 h）。 | 无参数，无需 Docker。用于消除 24h sweep 横轴起点偏 8 小时的误读。 |
| `verify_sim_reproducible.py` | 验收自检 3/3：可复现性——同种子同时间参数必须逐值一致（含跨进程、不同 `PYTHONHASHSEED`、换种子必须变、缺省种子确定）。 | 无参数。起独立子进程跑 `sim_trace_dump.py` 比对。退出码 `0`/`1`。 |
| `verify_sim_spike_exceedance.py` | 验收自检 5/5：污染物偶发尖峰与"折算值超标"口径（触发概率、尖峰顶过折算限值、折算 vs 标干差异、`SIM_SPIKE_PROB` 可控）。 | 无参数，无需 Docker。退出码 `0`/`1`。 |

## 二、度量类（7 个）

只量数字、不判通过；原始数据落 `docs/evidence/`，引用口径与对应 ADR / 指标文档互相印证。

| 文件 | 一句话用途 | 关键参数 / 用法 |
|---|---|---|
| `measure_cache_invalidation.py` | 实测"数据更新后缓存多久刷新"：向一个未来空分钟注入 1 行哨兵，测完删除。 | `--timeout 120`、`--poll-interval 0.2`、`--sentinel-second 5`、`--out`。 |
| `measure_clock_consistency.py` | 跨容器 / 宿主机时钟一致性测量：容器间一致性、宿主机偏移时间序列、NTP 外部基准三部分。 | `--rounds 12`、`--round-gap 3.0`、`--seconds 60`、`--interval 1.0`、`--out`。 |
| `measure_completeness.py` | 数据完整率测量：实际入库条数 / 理论应有条数，含断档检测与窗口自动选取。 | `--since`、`--until now`、`--interval 5.0`（名义 `POLL_INTERVAL`）、`--gap-factor 3.0`、`--auto`、`--tag`。 |
| `measure_latency.py` | 测量端到端延迟（设备读数时间戳 → TDengine 可查），输出逐条原始数据 CSV。 | `--minutes 30`、`--poll 0.25`、`--limit 600`、`--recompute <csv>` 只重算不测、`--tag`。 |
| `measure_nginx_redis.py` | 测量 nginx 反代开销 + Redis 缓存前后对比，输出原始 JSON。 | `--phase both\|routes\|ab`、`--overhead-count 12`、`--tag`。A/B 需重启容器切换开关，故分两段跑。 |
| `measure_report_coverage.py` | 用**实测**周期分布与逐窗口条数分布标定报表覆盖率门限（给出覆盖率天花板与干净窗口下限），不拍脑袋定阈值。 | `--threshold 0.75`、`--device device1`、`--tag`、`--no-write`。只读；只按单台设备标定。 |
| `measure_resend_success.py` | 补传成功率演练：停 emqx → 恢复 → 按"该补条数"对账。 | `--outage 180`（断开秒数）、`--drain-timeout 420`、`--tag resend`、`--out`。 |

## 三、灾备 / 演练类（4 个）

制造故障或真恢复一次，记录 RTO 与保真度；`_td_ops.py` 是它们的共用底座。

| 文件 | 一句话用途 | 关键参数 / 用法 |
|---|---|---|
| `backup_tdengine.py` | TDengine `cems` 库全量逻辑备份（taosdump）+ 清单 + 校验 + 保留期清理，可选卷级 tar 与时间窗差量。 | `--tier hourly\|daily\|monthly\|manual`、`--since/--until`（差量）、`--physical`、`--prune`、`--prune-after`、`--list`、`--keep-*`、`--threads 4`。 |
| `drill_timezone_misconfig.py` | 时区错误演练（网关 `TZ=UTC`）：起一对一次性实例，记录墙上时钟偏移、入库 ts 与覆盖率/新鲜度判据的反应，并与正确时区对照。 | `--seconds`、`--tag tz_utc_run1`、`--keep-going`。隔离标签 `plant=tzdrill`；跑完 `docker rm -f` + `DROP TABLE` 复核。 |
| `restore_drill_dual_device.py` | 双设备恢复演练：逐设备对账 + RTO。 | `--backup <目录含 manifest.json>`、`--container`、`--threads`、`--timeout`、`--tag`、`--keep-going`。 |
| `restore_drill_tdengine.py` | TDengine 恢复演练：真恢复一次到独立环境 + 记录 RTO + 分段校验保真度。 | `--mode instance\|rename\|physical\|summary`、`--backup`（默认取最新）、`--backup-root`、`--keep-going`。 |

## 四、演示类（2 个）

本地回环跑通协议，证明"接口已就绪"；换成平台下发的地址 / MN / PW 即可对接真实平台。

| 文件 | 一句话用途 | 关键参数 / 用法 |
|---|---|---|
| `hj212_loopback_demo.py` | 最小 P4 演示：真数据 → 组 HJ212 包（ANSI CRC16，可选 SM4）→ **真实 TCP socket** → 本脚本内起的上位机分帧解码并回 `CN=9014` 应答。 | `--encrypt`（SM4 数据区）、`--from-db`（取 TDengine 最新一行，否则用内置样例）、`--resend`（写 `RF=1`）。 |
| `northbound_demo.py` | 最小 P5 演示：一份采样同时走 `Hj212Adapter`(TCP) 与 `HttpJsonAdapter`(HTTP) 两个出口，都被独立解码，证明"加出口 = 加一个类，上层不改"。 | `--from-db`、`--encrypt`。接收端由本脚本提供（本地回环）。 |

## 五、工具 / 库 / 编排（9 个）

不单独产生业务结论，被其他脚本 import 或调用；`_` 前缀表示"内部共用"。

| 文件 | 一句话用途 | 关键参数 / 用法 |
|---|---|---|
| `_atomic_handover.py` | 原子交接回归测试的共用工具：stub MQTT 客户端 + 临时目录 Sandbox（硬守卫，路径不指向临时目录就 `SystemExit(3)`）+ 断言/指纹助手。 | 无 CLI，不产生输出。被 `verify_atomic_handover.py` import。 |
| `_device_scope.py` | 设备维度 scope 的**唯一真源**：给测量/分析脚本提供「按哪台设备统计」的 `--device` 参数与 SQL 谓词。 | `from _device_scope import add_device_argument, resolve_device, device_predicate`；空值回落 env `TD_DEVICE`（默认 `device1`）。被 6 个脚本 import。 |
| `_loadtest_run.ps1` | 并发压测编排：起 N 个一次性接入实例（独立 client_id / 独立子表 `<plant>_lt<i>`）+ 调 `loadgen_devices.py` + 按固定间隔抓 `docker stats` 快照。 | `-Devices N -Duration S -Tag <tag>`（必填）、`-Interval 1.0`、`-ParallelPublishers 16`、`-OutDir docs/evidence/perf`。跑完**不自动清理**，用 `loadgen_devices.py --cleanup` 单独做。 |
| `_probe_mqtt_session.py` | 只做一次 MQTT 连接/订阅/发布的最小探针，用于定位 broker 是否在"新客户端接入"时出问题。 | 无参数：连 `emqx:1883`、订专属主题、发 3 条、等 5 秒退出。不写库、不建表。 |
| `_td_ops.py` | TDengine 备份/恢复演练的共用底座：一切 SQL 走容器内 `taos` CLI，时间边界统一取纪元毫秒，卷名从 `docker inspect` 读。 | 被 `backup_tdengine.py`、`restore_drill_*.py`、`drill_timezone_misconfig.py`、`verify_backup_selfcheck_scope.py`、`measure_report_coverage.py` import（`from scripts._td_ops import ...`）。**只读**，无任何写库语句。 |
| `export_images.ps1` | 把本项目全部镜像导出成可搬运的 tar 包（+ sha256），用于"给一台没网的干净机器让它跑起来"。 | `-Out dist`、`-DryRun`、`-SkipBuild`、`-Split`。⚠️ **一律按 tag 导出，绝不按 digest**：按 digest 导出的 tar `RepoTags=null`，`docker load` 后镜像无名，离线机上 `compose up` 直接失败。 |
| `import_images.ps1` | 在**干净 / 离线机器**上导入 `export_images.ps1` 产出的镜像包。 | `-From D:\dist`（必填）、`-WhatIf`。核对 5 个预期镜像（emqx 6.3.0 / tdengine 3.3.6.13 / redis 7.4.11-alpine / nginx 1.31.6-alpine / cems-pipeline:latest）。 |
| `loadgen_devices.py` | 负载发生器：模拟 N 台设备并发向 EMQX 上送，测吞吐/延迟，并可逐条对账入库是否丢数。 | `--devices N`、`--interval 5`、`--duration 60`、`--ingest-channels k`、`--no-verify`、`--cleanup`、`--dry-run`；`--listen-topic/--listen-out/--listen-seconds` 可只当旁听器量某条链路实际发布条数。 |
| `serial_bridge.py` | ⭐ **RS485（Modbus RTU）→ Modbus TCP 桥**：把 COM 口上的**真机**（温湿度变送器）暴露成 TCP 从站，扮演现场那台"**串口服务器**"。**必须跑在 Windows 宿主上** —— Docker 容器访问不到宿主 COM 口（`MODBUS_HOST=host.docker.internal` 可让容器读到它）。 | `--port COM3`、`--tcp-port 5021`（避开设备层 5020）、`--interval 1.0`、`--baud 4800`。对外 4 个保持寄存器：`0`=湿度×10、`1`=温度×10(**int16**)、⭐ `2`=桥状态（`0` 新鲜 / `1` 陈旧）、`3`=累计读失败次数。⚠️ **读失败时保留上次好值并把状态位置 1**，故意不重复真机"读错地址沉默返回 0"那个毛病（见 `docs/runbooks/真机接入.md` S4 发现 1）。 |

---

## 自查

**代码文件总数 39**（36 `.py` + 3 `.ps1`）—— ⚠️ **不含本索引 `README.md` 自己**；
因此 `Get-ChildItem scripts -File` 会输出 **40**（39 + 本索引），两者差 1 是正常的，不是漏登记。
每个**代码文件**在全文中**恰好登记一次**：

| 分组 | 个数 |
|---|---|
| 一、验收类（含子进程工具 `sim_trace_dump.py`） | 17 |
| 二、度量类 | 7 |
| 三、灾备 / 演练类 | 4 |
| 四、演示类 | 2 |
| 五、工具 / 库 / 编排 | 9 |
| **合计** | **39** |

组内按文件名排序（codepoint 序，`_` 开头者排在最前）；每个条目的一句话用途均取自脚本 docstring
或 argparse `help`，未臆测。
