# 机器证据（evidence）

本目录存放**实测原始数据**：测量脚本落盘的 csv / json / log、演练台账、日志导出。
文档正文（`../adr/`、`../runbooks/`、`../reference/`）引用这些文件作为数字出处。

## 目录与归档判据

| 子目录 | 放什么 | 判据（按文件名 + 内容双重确认） |
|---|---|---|
| `perf/` | 端到端延迟、数据完整率、容器时钟一致性 | `latency_*`（含逐样本 csv、`*.summary.json`、`*_run.log`）、`completeness_*`、`clock_consistency.json` |
| `drill/` | 补传/恢复演练台账与汇总 | `resend_*`（对账台账、缓存快照）、`gateway_resend_*`（滞后与占比）、`tdengine_restore_drill_*`（8 次恢复演练逐次 + 汇总） |
| `cache/` | nginx + redis 缓存与一次真实数据错位的留档 | `nginx_redis_run1.json`、`cache_invalidation_drill.json`、`_incident_20261001_timestamp_shift_repair.py`、`_show_ab.py` |
| `backup/` | 备份标定类证据 | **本仓库暂无**：备份/恢复的原始数据是 `tdengine_restore_drill_*`（属恢复演练，归 `drill/`），备份产物与台账落在仓库外 `F:\cems-backup\`（含 `logs\backup_log.jsonl`），体积原因不入库。保留此目录是为了让归档分类与 runbook 的叙述一一对应 |
| `coverage/` | 报表覆盖率标定 | `report_coverage_calibration.json` |
| `logs/` | 网关日志导出与演练运行日志 | `_gateway_log_dump.txt`、`resend_drill*_run.log` |

## 两个临时脚本为什么留在这里

| 文件 | 结论 | 理由 |
|---|---|---|
| `cache/_incident_20261001_timestamp_shift_repair.py` | **保留** | 它是一次**真实数据损坏与还原**的原始留档（taosAdapter REST 的 `ts` 是 UTC，脚本一度把 11 行写到 10:38）。删掉脚本就只剩文字描述，事故无法复核。它是一次性修复脚本、不再复用，但与它修的那批数据同等重要，属"证据"不属"源码" |
| `cache/_show_ab.py` | **保留** | 它是 `nginx_redis_run1.json` 的**只读解析器**（52 行，只 print 不写盘）。AD 缓存实验的结论（命中率、查库下降、反代开销）都从这份 JSON 复算得来；把脚本留在同一目录，任何人拿到证据就能复现那张 A/B 表，不必重跑演练。它不被任何生产代码导入 |

> 两个脚本都不属于 `src/`，也不被 `requirements.txt` 依赖；`scripts/` 下是**可复跑**的测量工具，
> 这里放的是**跑完一次就不再复跑**的现场脚本，因此按"证据"归档而不是搬进 `scripts/`。

## 相关文档

- [ADR-0005 备份策略、保留期与复制取向](../adr/0005-备份策略与保留期.md)
- [恢复演练与对账口径](../runbooks/恢复演练与对账口径.md)
- [性能与可靠性指标](../reference/性能与可靠性指标.md)
- [容器时钟漂移](../reference/容器时钟漂移.md)
