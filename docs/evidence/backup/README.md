# backup/ —— 备份标定与双设备复验批次

本目录放**两类**证据，都围绕"备份 / 恢复 / 时区"这条可靠性线：

## 1. 备份自检标定（活产出）

`scripts/verify_backup_selfcheck_scope.py` 的产出落点就是这个目录
（见该脚本的 `EVIDENCE_DIR`），文件形如 `backup_selfcheck_scope_<tag>.json`。
现存 `backup_selfcheck_scope_dual_dev_20261003.json` 是双设备那次。

⚠️ **不要把本脚本的产出搬家**：改目录要同步改脚本里的 `EVIDENCE_DIR`。

## 2. 2026-10-03 双设备复验批次

单设备时代的恢复演练逐次数据在 [`../drill/`](../drill/)（`tdengine_restore_drill_*.json`）；
**双设备复验**这一批按"同一批次、同一台机器、同一天"整体放在这里，便于与
[恢复演练与对账口径](../../runbooks/恢复演练与对账口径.md) 的双设备章节逐条对上：

| 文件 | 是什么 |
|---|---|
| `dual_device_restore_drill_dual_dev_run{1,2,3}.json` | 双设备 RTO 复验 n=3 |
| `dual_device_restore_drill_raw_20261003.txt` | 四轮演练的完整原始输出（trace） |
| `tdengine_restore_drill_instance_dual_20261003.json` | 整表级恢复复验 |
| `tdengine_restore_drill_rename_dual_20261003.json` | 改名路径恢复复验 |
| `tdengine_restore_drill_physical_dual_run{1,2}.json` | 卷级恢复复验 |
| `tz_misconfig_drill_tz_utc_run1.json` | 时区错（TZ=UTC）演练的逐条对账 |
| `verify_sim_24h_sweep_tzfix_20261003.txt` | 时区修复后的 24 h 仿真扫描 |
| `verify_sim_diurnal_local_peak_20261003.txt` | 时区修复后"日峰值落在当地午后"复验 |
| `emqx_status_watch_20261003.csv` | 该批次期间 EMQX 每分钟状态采样（被[并发与负载指标](../../reference/并发与负载指标.md) §10.6 引用） |
| `_probe_freshness.py` / `_probe_freshness2.py` | 一次性只读环境探针（量数据新鲜度与采集间隔），已跑完不再复用 |
| `_probe_prod_after_load.py` | 压测后的只读复核探针，**被 `scripts/_loadtest_run.ps1` 调用**——是活依赖，不是留档 |

## 相关文档

- [ADR-0005 备份策略与保留期](../../adr/0005-备份策略与保留期.md)
- [恢复演练与对账口径](../../runbooks/恢复演练与对账口径.md)
- [evidence 总说明](../README.md)
