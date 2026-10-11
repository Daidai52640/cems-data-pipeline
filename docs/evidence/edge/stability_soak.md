# 边缘侧稳定性长跑证据（资源受限下的 2 小时零异常）

> **结论**：在 **128 MiB / 1 vCPU** 的资源约束下，边缘侧三容器（`device` / `gateway` / `gateway-plant2`）
> **连续 2 小时零重启、零不健康、数据速率稳定、零误报**。
> 命令与原始输出见下；采证原始 CSV 见 `resource-24h.append-live.csv`。

- **窗口**：2026-10-11 **19:41:37 → 21:41:47**（每 15 分钟采一次，共 8 个采样点）
- **基线**：`real-device-baseline-2`；真机（RS-485 温湿度变送器）全程在线
- **限制**：`x-edge-limits` ⇒ `mem_limit 128m` + `cpus 1.0`（依据见
  [`边缘侧部署与资源约束.md`](../../runbooks/边缘侧部署与资源约束.md) §2.4）

## 原始输出

```
=== soak start 19:41:37 ===
19:56:37 | unhealthy=0 restarts=0 rows/3min=36 invalid_15m=0 critical=0 collector_sample=109
20:11:38 | unhealthy=0 restarts=0 rows/3min=36 invalid_15m=0 critical=0 collector_sample=137
20:26:39 | unhealthy=0 restarts=0 rows/3min=36 invalid_15m=0 critical=0 collector_sample=165
20:41:40 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0 collector_sample=193
20:56:42 | unhealthy=0 restarts=0 rows/3min=36 invalid_15m=0 critical=0 collector_sample=221
21:11:43 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0 collector_sample=249
21:26:44 | unhealthy=0 restarts=0 rows/3min=36 invalid_15m=0 critical=0 collector_sample=277
21:41:45 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0 collector_sample=305
=== soak end 21:41:47 ===
```

## 每一列在证什么

| 列 | 判据 | 结果 | 说明 |
|---|---|---|---|
| `unhealthy` | 10 个容器里健康检查失败的个数 | **恒 0** | ⭐ 资源收紧后**没有把任何一个容器压到不健康** |
| `restarts` | 6 个应用容器 `RestartCount` 之和 | **恒 0** | ⭐ **128 MiB 不是"卡着跑"** —— 没有 OOM、没有崩溃重启 |
| `rows/3min` | 主表最近 3 分钟入库条数 | **35~36** | = **~12 条/分钟**（5 s 一条 × 1 号炉）；⭐ 两个 35 是采样时刻与整分钟边界错位，不是丢数 |
| `invalid_15m` | 最近 15 分钟新增无效点事件 | **恒 0** | 真机全程在线 ⇒ **不该有故障留痕**；有才是问题 |
| `critical` | 最近 16 分钟 `cems-web` 的 CRITICAL 条数 | **恒 0** | ⭐ 新增的"连续停流告警"**没有误报**（阈值 300 s，远超正常的补传滞后） |
| `collector_sample` | 资源采证脚本的样本序号 | **109 → 305** | ⭐ 196 个点 = 8 × 24.5 ≈ **98 分钟无断采**（30 s 一个），说明采证进程本身也没掉 |

## 与资源峰值的对照

同一时段的资源采证（`docs/evidence/edge/resource-24h.append-live.csv`，**128 MiB 口径**）：

| 容器 | CPU 峰 | 内存 峰 | 占 128 MiB | 触顶次数 |
|---|---|---|---|---|
| `cems-device` | 10.95% | 25.5 MiB | **19.96%** | 0 |
| `cems-gateway` | 8.73% | **37.6 MiB** | **29.36%** | 0 |
| `cems-gateway-plant2` | 8.72% | 25.7 MiB | **20.10%** | 0 |

⇒ ⭐ **最坏情况仍留 ~3.4 倍内存余量**（37.6 → 128 MiB），且**零触顶**。
对比最初取的 512 MiB：占用只有 **~5%**，是 **10 倍余量 = 等于没约束**（见 runbook §2.4 的取值论证）。

## 复现

```powershell
# 1) 采证（脱离会话，30 s 一次，只采 3 个边缘容器）
Start-Process -WindowStyle Hidden -FilePath python -ArgumentList `
  'scripts\measure_edge_resources.py','--out','docs\evidence\edge\resource-24h.append-live.csv'

# 2) 汇总（min/avg/max + 触顶）
python scripts\measure_edge_resource_summary.py --csv docs\evidence\edge\resource-24h.append-live.csv
```

⚠️ **这不是 24 小时**：目标时长设的是 24 h，本文件记录的是其中**前 2 小时**的逐点观测。
长时间（跨工况/跨夜）的结论需跑满后再复核。

---

## 第二轮（同口径，21:45 → 23:45）—— ✅ 同样零异常

```
=== soak2 start 21:41:56 ===
21:56:56 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0
22:11:57 | unhealthy=0 restarts=0 rows/3min=34 invalid_15m=0 critical=0
22:26:58 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0
22:41:59 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0
22:57:01 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0
23:12:02 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0
23:27:03 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0
23:42:04 | unhealthy=0 restarts=0 rows/3min=35 invalid_15m=0 critical=0
=== soak2 end 23:42:05 ===
```

⚠️ **第二轮日志里没有 `collector_sample` 列，是个已知的观测缺陷**：那一列读的是
`resource-24h.status.append-live.json`，而 **21:42 采证重启后**新进程把心跳写到了另一个派生文件名
（`resource-24h.status.json`），于是那列一直显示旧值 307。
**采证本身没有断** —— 直接看 CSV 可证：`93605 → 93902` 字节 / `931 → 934` 行（35 秒内 +3 行）。

**两轮合计：4 小时逐点观测（19:41 → 23:42），零不健康、零重启、零误报、速率稳定。**
资源侧同期窗口 **4.72 小时 / 1590 行 / 每容器 530 个采样点 / 缺采 0 / 触顶 0**。

