# 机器证据（evidence）

本目录存放**实测原始数据**：测量脚本落盘的 csv / json / log、演练台账、日志导出。
文档正文（`../adr/`、`../runbooks/`、`../reference/`、`../test-reports/`）引用这些文件作为数字出处。

## 目录与归档判据

共 140 份（2026-10-03 清点），分布如下：

| 子目录 | 份数 | 放什么 | 判据（按文件名 + 内容双重确认） |
|---|---|---|---|
| `perf/` | 68 | 端到端延迟、数据完整率、容器时钟一致性、吞吐/背压、插入基准 | `latency_*`、`completeness_*`、`clock_consistency.json`、`loadgen_*`（初版 `_n<N>_` 与复测 `_acc_n<N>_` 各一组）、`backpressure_*`（含 `bp02s-30` / `slowdrain` 跟进实验）、`raw_insert_bench.json` |
| `drill/` | 42 | 补传 / 恢复 / 时区演练的逐次台账与汇总 | `resend_*`（对账台账 + 缓存快照，含 before/after）、`gateway_resend_*`（滞后与占比）、`tdengine_restore_drill_*`（单设备逐次 + 汇总）、`emqx_restart_*`（1A/1B 对账，**含 `_run{1,2,3}_logs_*.txt` 原始日志导出**）、`dual_device_*`、`gateway_preflight_*` |
| `cache/` | 8 | nginx + redis 缓存与一次真实数据错位的留档 | `nginx_redis_run1.json`、`cache_invalidation_drill.json`、`atomic_handover_probe.json`、`_incident_20261001_timestamp_shift_repair.py`、`_show_ab.py` |
| `backup/` | 17 | 备份自检标定 + **2026-10-03 双设备复验批次** | `backup_selfcheck_scope_*.json`（`scripts/verify_backup_selfcheck_scope.py` 的产出落点）、`dual_device_restore_drill_*`（双设备 RTO 复验 n=3）、`tdengine_restore_drill_*_dual_*`（改名/物理/整表级复验）、`tz_misconfig_drill_*`（时区演练）、`verify_sim_*_tzfix/diurnal_*`（时区修复后的仿真复验）、`emqx_status_watch_*.csv`（§10.6 broker 状态采样） |
| `coverage/` | 1 | 报表覆盖率标定 | `report_coverage_calibration.json` |
| `logs/` | 3 | 网关日志导出与演练运行日志 | `_gateway_log_dump.txt`、`resend_drill*_run.log` |

> 加上本 `README.md`，本目录共 141 个条目。

## ⚠️ 关于"运行时日志到底留不留"

本目录**有两类日志，处置相反**，不要一刀切：

| 类别 | 处置 | 例子 |
|---|---|---|
| **被文档引用为一手判据的日志导出** | **留**（删了结论就不可复核） | `logs/_gateway_log_dump.txt`（ADR-0003 / 性能与可靠性指标 §4.2 的复算输入）、`drill/emqx_restart_1a_run{1,2,3}_logs_*.txt`（TC-E2E-002 的"原始输出"）、`perf/loadgen_gateway_bp02s-30.log.drain` |
| **容器 stderr 与旁听端的逐行确认** | **不留**（体量最大、信息量最低） | 曾占 `perf/` 84% 的 stdout/stderr 转储，已清理且不再入库 |

## 三个现场脚本为什么留在这里

| 文件 | 结论 | 理由 |
|---|---|---|
| `cache/_incident_20261001_timestamp_shift_repair.py` | **保留** | 它是一次**真实数据损坏与还原**的原始留档（taosAdapter REST 的 `ts` 是 UTC，脚本一度把 11 行写到 10:38）。删掉脚本就只剩文字描述，事故无法复核。它是一次性修复脚本、不再复用，但与它修的那批数据同等重要，属"证据"不属"源码" |
| `cache/_show_ab.py` | **保留** | 它是 `nginx_redis_run1.json` 的**只读解析器**（52 行，只 print 不写盘）。AD 缓存实验的结论（命中率、查库下降、反代开销）都从这份 JSON 复算得来；把脚本留在同一目录，任何人拿到证据就能复现那张 A/B 表，不必重跑演练。它不被任何生产代码导入 |
| `backup/_probe_prod_after_load.py` | **保留** | 压测后的只读复核探针，被 `scripts/_loadtest_run.ps1` 编排调用（`docs/reference/并发与负载指标.md` §10.1）——**它是活依赖，不是留档**，移动它必须同步改编排脚本 |

> 这些脚本都不属于 `src/`，也不被 `requirements.txt` 依赖；`scripts/` 下是**可复跑**的测量工具，
> 这里放的是**跑完一次就不再复跑**的现场脚本，因此按"证据"归档而不是搬进 `scripts/`。

## 相关文档

- [ADR-0003 补传入队判定修正](../adr/0003-补传入队判定修正.md)
- [ADR-0005 备份策略、保留期与复制取向](../adr/0005-备份策略与保留期.md)
- [恢复演练与对账口径](../runbooks/恢复演练与对账口径.md)
- [性能与可靠性指标](../reference/性能与可靠性指标.md)
- [并发与负载指标](../reference/并发与负载指标.md)
- [容器时钟漂移](../reference/容器时钟漂移.md)
