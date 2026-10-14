# ADR-0012 · 南向第二种协议：OPC UA 采集（最小实现）

- 状态：已实现并单测（2026-10-14）；**asyncua 路径未在真实 OPC UA 服务器上验证**（见 §7）
- 关联：`docs/adr/0011-上游直读与契约指纹.md`（同一批"对标商业网关"复盘里发现的另一个空位）、
  `docs/adr/0009-接入层逐点质量位.md`（质量位语义的家底）、
  `docs/adr/0010-数据合理性判据.md`（「可疑 ≠ 无效」这条原则在本 ADR 里被复用到 Uncertain 上）
- 影响文件：`src/gateway/opcua_south.py`（新增）、`src/gateway/gateway.py`、
  `src/common/points.py`、`.env.example`、`docker-compose.yml`、`requirements.txt`

---

## 1. 背景

ADR-0011 复盘"对标商业网关"时发现了三个空位，补了两个（上游直读、心跳状态快照）。
剩下这个是南向：**网关照现在的实现只讲 Modbus**。

真实 CEMS 现场不是所有仪表都讲 Modbus。PLC、DCS、第三方数采软件常常只暴露
OPC UA 服务器。头部厂家的产品都是"南向多协议"的：

| 厂家/产品 | 南向协议 |
|---|---|
| 研华 WISE-6610 | Modbus/TCP、MQTT、BacNet、**OPC UA** 同框 |
| Moxa AIG-501 | Modbus RTU/ASCII/TCP master（+ Modbus TCP server） |
| Neuron（开源） | Modbus、**OPC UA**、S7、FINS、IEC61850、DL/T645……四十余种 |

所以补南向的第二条协议，是"能接真实现场"的必要条件之一。

---

## 2. 四个冲突与处置（本 ADR 的核心）

南向接第二种协议会撞上四个语义冲突。逐条给处置与**外部依据** —— 这些不是自创，
是查了开源网关与头部厂家做法之后的选择。

### 冲突 1：OPC UA 自带质量位，而本项目的质量位是接入层算的

**事实冲突**：OPC UA 每个值自带 `StatusCode`（Good / Uncertain / Bad + 数十个子码）。
本项目现在的质量位是接入层按量程与合理性算出来的 `invalid/missing/jump/frozen`。
两套东西语义重叠但不等价 —— 并存就有"以谁为准"的问题。

**Neuron 怎么做（关键证据）**：它的 MQTT 北向上行格式里，一个点位
**要么是 `value`，要么是 `error` 码，二选一**：

```json
tags-format:   {"tags":[{"name":"tag0","value":123},{"name":"tag1","error":2014}]}
values-format: {"values":{"tag0":123},"errors":{"tag1":2014},"metas":{}}
```
> TIP（原文）：当点位采集成功时，返回采集到的数据。当点位采集发生错误时，
> 返回错误码，**不再返回数值**。

也就是说：**行业做法是在驱动层把协议质量位收敛成"这个点有没有值"，北向不带质量字段。**

**本项目的处置**：采用同一个收敛方向，但**比 Neuron 多留一手**。

| OPC UA | 上送？ | 理由 |
|---|---|---|
| `Good` | 是 | — |
| `Uncertain`（大多数子码） | **是** | 复用 ADR-0010 的「可疑 ≠ 无效」：值与"可疑"是两件事，可疑的值照常入库 |
| `Uncertain` **`SubstituteValue`** | **否** | ⭐ 它是"值被替换/仿真出来的"，属**假数据**；项目原则「宁缺勿假」 |
| `Bad`（含最高两位 `10` 与 `11` 两种） | **否** | 值不可用。**不用 0、也不发越界哨兵** —— 越界哨兵是设备层表达"我读不到设备"的手段，与"服务端明确说这个值坏"是两种语义，混用会让事件表里的原因分不清 |

**比 Neuron 多留的一手**：原始 `StatusCode` 进日志与 `quality_report()` 计数。
Neuron 的格式里没有这个位置（收敛之后就没有了）。本项目有 `cems_point_invalid`
事件表这条"证据层"的传统，所以质量信息不丢在驱动里。

### 冲突 2：时间戳用谁的

**事实冲突**：OPC UA 的 `DataValue` 自带 `SourceTimestamp`（源端时间）与
`ServerTimestamp`（服务端时间），而本项目的主时间戳是**网关采集时刻**
（`gateway.build_payload` 里的 `time.strftime`）。

**Neuron 怎么做**：其上行格式的 `timestamp` 定义为「**数据采集时的** UNIX 时间戳」
—— 是采集侧时刻，不是源端时间戳。

**本项目的处置**：**主时间戳仍用网关采集时刻**。两条独立理由：
1. 改主键时间戳会动 `(子表, ts)` 的幂等语义（QoS 1 重投靠它去重）；
2. 源端时钟未经验证 —— 本项目自己记录过容器时钟漂移与一次被推翻的"8 小时时区 bug"。

`SourceTimestamp` 本版**不取**。要取也只能当证据字段，不能当主键。

### 冲突 3：OPC UA 支持订阅推送，本项目是 5 秒轮询

**事实冲突**：OPC UA 有 `Subscription` / `MonitoredItem`，服务端可以主动推变化。
本项目的整套节拍模型是固定 5 秒轮询。

**Neuron 怎么做（关键证据）**：Neuron 对**所有**南向驱动都是按组轮询 —— 文档原文
「Running: Started, issuing read commands at **each group's polling interval**」。
而且它有运行指标直接盯这件事：
> `group_last_timer_ms` — Duration of one group timer cycle, in milliseconds.
> **Approaching or exceeding the group's polling interval** means the group holds
> too many tags; split it or widen the interval.

连专业网关都不默认上订阅。

**本项目的处置**：**保持轮询**。还有一层本项目独有的理由：整套验收体系
（覆盖率分母按 5 秒算、单网关节拍上限 1.05 条/秒、12.93 h 长跑）都建立在
"固定节拍"上；换成事件驱动推送，这些数字**全部作废要重测**。

**顺手抄了 Neuron 的那条告警**：单轮读取耗时超过轮询周期时打 WARNING
（`opcua_south._poll_loop` 末尾），语义与 `group_last_timer_ms` 完全一致。

> 附：ThingWorx OPC Aggregator 把这件事做成可配置的"**扫描模式**"
> （为发送到订阅客户端的更新指定在设备中扫描标记的方式），即订阅与轮询**两种都提供**。
> 所以"只轮询"不是唯一正确答案，但是**最小实现**下的正确选择 —— 与项目现有节拍体系一致。

### 冲突 4：量纲 —— OPC UA 的值已经是工程量，本项目契约是"寄存器整数 + scale"

**事实冲突**：本项目的 `Point` 有 `scale`（`raw = round(value × scale)`），
那是 Modbus 保持寄存器的**线格式**；OPC UA 的值直接就是带工程单位的浮点。

**Neuron 怎么做**：它的架构文档写「Neuron 设计了**统一的数据类型**，例如，
Modbus 数据类型需要在服务节点内**转换为 Neuron 统一数据类型**，然后转换为 JSON」。
即：**协议私有的表示留在驱动内，跨驱动的统一表示是工程量。**

**本项目的处置**：OPC UA 路径**不做 `/scale`**，值原样当工程量用。
本项目的"统一表示"就是报文里的工程量，所以两条南向路径在**驱动内**收敛到同一形状：

```
Modbus:  read_holding_registers → raw / scale ─┐
                                               ├→ {测点名: 工程量} → build_payload → MQTT
OPC UA:  read_data_value        → float      ─┘
```

`Point.scale` 在 `points.py` 里已注明是 Modbus 专用的线格式字段。

---

## 3. 最小实现的范围

### 做了什么

| 项 | 内容 |
|---|---|
| 契约 | `Point.opcua_node_id`（默认 `""` = 该测点不走 OPC UA）+ `OPCUA_NODE_IDS` 只读视图 + `parse_node_id()` 纯函数 |
| 采集 | `src/gateway/opcua_south.py`：后台线程维持连接、按 `POLL_INTERVAL` 轮询、产出工程量快照 |
| 质量 | `classify_status_code()` / `decide_quality()`（纯函数，扩展点）、`QualityReport` |
| 网关接线 | `SOUTH_PROTOCOL` 二选一；心跳的 `modbus_ok` 正名为 `south_ok` 并加 `south_protocol` |
| 配置 | `.env.example` + compose 两个实例的注入（plant2 可用 `OPCUA_ENDPOINT2` 连另一台服务器） |
| 测试 | `tests/gateway/test_opcua_south.py` **46 条**（质量映射 / NodeId 解析 / 快照与陈旧语义 / 契约） |

### 刻意没做

| 没做 | 为什么不 |
|---|---|
| **证书登录** | Neuron 的 OPC UA 连接策略有 4 种组合（匿名 / 用户名密码 / 证书+匿名 / 证书+用户名密码），证书要走信任列表（DER 格式、SAN 里带 `URI:urn:...`）。现场配置成本另算，不是"最小实现"该背的 |
| **Subscription 订阅** | 见冲突 3 |
| **地址空间浏览** | Neuron 把"浏览 OPC UA 地址空间"做成**独立一节**（配置期工具），不在采集路径里。本模块同理：NodeId 由人在契约里写死 |
| **写点位** | CEMS 现场不需要反向控制；加写能力要另做权限与确认设计 |
| **`SourceTimestamp` 入库** | 见冲突 2 |
| **质量位进库** | 报文里没有质量字段，Uncertain 只到日志与计数。要做成"可在库里查"得给报文加字段 + 改库表 —— 那是下一步 |

---

## 4. 关键设计点

### 4.1 为什么是后台线程，不是主循环里同步读

| | Modbus 路径 | OPC UA 路径 |
|---|---|---|
| 形态 | `read_device()` 同步读，失败返回 `None` | 后台线程持续刷新，主循环只取 `snapshot()` |
| 理由 | 一次读写毫秒级，同步最简单 | 建连/握手是秒级，放主循环会直接撑爆采集节拍 |

这套"后台线程 + 快照"**照抄本仓库已有的 `RealDeviceReader`**
（设备层读真机温湿度用的就是它），所以不是新发明。

### 4.2 陈旧不冒充新鲜

`snapshot(max_age=POLL_INTERVAL × 3)` 超过时限返回 `None`，网关跳过本轮不发。
于是这一轮不进库、**覆盖率会掉** —— 那正是"这段时间真没采到"的诚实信号。
把旧值当新值发出去才是本项目的禁忌。

### 4.3 `SubstituteValue` 那条例外是刻意的

`UncertainSubstituteValue`（`0x40940000`）的含义是"值是被替换/仿真出来的"。
其他 Uncertain 只是"可能不准"，而它是**已知的假**。项目原则是「宁缺勿假」，
所以这一档按 Bad 处理。这是本 ADR 里唯一一处**偏离"Uncertain 照常上送"**的判断，
有专门测试盯着（`test_substitute_value_is_the_deliberate_exception`）。

### 4.4 三档判定不能用阈值比较

OPC UA 规范里 `StatusCode` 的**最高两位**表达质量：`00`=Good、`01`=Uncertain、
`10`=Bad、`11`=Bad。所以 `code >= 0x80000000` 这种写法会**漏掉 `0xC0000000` 这一类**。
实现用 `(code >> 30) & 0b11`，并有两条测试专门盯这个
（`test_classify_status_code` 的 `0xC0000000` 用例 + `test_classify_does_not_use_a_threshold_comparison`）。

---

## 5. 代价与边界

| 代价/边界 | 说明 |
|---|---|
| **配置前置条件两条** | 改 `SOUTH_PROTOCOL=opcua` 前必须先填 `opcua_node_id` **且**打开 `requirements.txt` 里的 `asyncua`。任一条没做，网关**拒绝启动**（故意的：配置错不该表现成"healthy 但不出数"） |
| **拒绝启动 vs 退避重连** | 配置错与依赖缺失 → `SystemExit(1)`；"OPC UA 服务器暂时没起来" → 后台线程指数退避重连（1→30 秒），不退出 |
| **只在**容器外**能用** | `asyncua` 默认不装（见 §6），所以容器里要真用，必须重建镜像 |
| **质量信息没进库** | Uncertain 只到日志 + `quality_report()` 计数，库里查不到 |
| **两套质量位的边界要说清** | 驱动层只回答"这个点有没有值"；"这个值可不可信"仍然由接入层按量程与合理性判。**两者不是替代关系**：OPC UA 说 Bad → 驱动拦掉 → 下游判 missing；OPC UA 说 Good 但值超量程 → 接入层照样判 invalid。同一列上两种判据都会留下痕迹，这是对的 |
| **`south_ok` 的含义变了** | 心跳里的 `modbus_ok` 改名为 `south_ok` 并新增 `south_protocol` —— 现在有两种南向协议，字段名不该绑定其中一种 |

---

## 6. 依赖为什么是注释掉的

`requirements.txt` 里 `asyncua` 那行**刻意注释掉**，两条理由：

1. **默认部署（Modbus）不该为用不到的可选能力承担镜像体积与依赖风险** ——
   这样 `pip install -r requirements.txt` 的行为与改造前一致。
2. **asyncua 路径还没在真实 OPC UA 服务器上跑过**（见 §7），不适合把一个
   未验证的版本约束写进默认安装。

代码侧做了对应兼容：`asyncua` 缺失时 `OpcUaReader.start()` 返回 `False` 并打 CRITICAL；
纯逻辑（质量映射 / NodeId 解析 / 快照语义）**不依赖它**，测试照常跑。

---

## 7. 诚实清单（没做 / 没验的）

1. ⭐ **asyncua 路径没有在真实 OPC UA 服务器上验证过。** 本机没装 `asyncua`，
   也没有可连的 OPC UA 服务器（Prosys Simulation Server / KEPServerEX / UaExpert 都没跑过）。
   **测到的是纯逻辑**（质量映射、NodeId 解析、快照与陈旧语义、契约），
   **没测到的是真连接**（建连、节点读取、用户名密码登录、断线退避重连）。
   这是本 ADR 最大的缺口，写在最前面。
2. **`asyncua` 的版本约束是估的**（`>=1.1,<2.0` 且被注释），没实测过任何版本。
3. **没验过非数值类型的节点**。代码里对 `bool` 与非 `(int, float)` 的值做了拦截
   （记为 skipped），但没在真服务器上见过这种点位。
4. **`SourceTimestamp` / `ServerTimestamp` 一概没读**，所以"源端时钟对不对"
   这件事在本模块里无从判断（这是刻意不做，不是遗漏）。
5. **`quality_report()` 没有接进心跳快照**。计数目前只在对象里，没写进
   `.gateway_alive`，也没进任何接口 —— 要看得自己加。
6. **只在单点粒度做了质量映射，没做"整台服务器级别的质量"**
   （比如服务器 `ServerStatus` 变 Bad 时所有点该怎么处置）。
7. **plant2 的 `OPCUA_ENDPOINT2` 只是配置就位**，没验证过"两个网关实例连两台
   OPC UA 服务器"这条路径。
8. **`parse_node_id` 没有覆盖 NodeId 的第三种写法**（`nsu=` 命名空间 URI 形式，
   OPC UA 1.04 起支持）。当前只认 `ns=<索引>`。

---

## 8. 备选方案与为什么没选

| 方案 | 为什么不选 |
|---|---|
| 把 NodeId 放在环境变量里，不动 `points.py` | 那会让同一个测点有两份"地址"（寄存器地址在契约、节点地址在环境），正是 `dtype`/`endian`/`addr_notation` 当初加进契约要避免的分裂 |
| 用 `Subscription` 订阅代替轮询 | 见冲突 3：整套验收数字建立在固定节拍上，换模型要全部重测 |
| 主循环里同步读 OPC UA | 建连秒级，会撑爆采集节拍；且与 `RealDeviceReader` 的既有形态不一致 |
| Bad 的值发越界哨兵，让接入层判 `invalid` | 越界哨兵是设备层"我读不到设备"的表达手段；服务端明确说值坏是另一回事。混用会让事件表里的原因分不清。**上送假值也违反宁缺勿假** |
| 所有 Uncertain 一律拦掉 | 那等于把"可能不准"当"不可用"，与 ADR-0010 的「可疑 ≠ 无效」相反。只有 `SubstituteValue`（已知的假）该拦 |
| 把质量位塞进报文（加字段） | 那要同步改接入层解析与 TDengine 表结构。Neuron 的先例说明"驱动层收敛"是可行且更简单的路；本项目先把角色补上，加字段留作下一步 |

---

## 9. 实测与验证

| 项 | 结果 |
|---|---|
| 新增单测 | `tests/gateway/test_opcua_south.py`，**46 条** |
| 全量回归 | **629 passed**（改造前 583，本 ADR 新增 46） |
| StatusCode 三档 | 参数化 8 个码，含 `0xC0000000`（阈值写法会漏的那个） |
| 上送决策 | Good 送 / Bad 三种码不送 / 普通 Uncertain 送 / `SubstituteValue` 不送 |
| NodeId 解析 | 5 种合法形态通过；11 种畸形输入逐个断言拒绝 |
| 快照语义 | 从未读到→None；时限内→值；超时→None；返回的是拷贝不是内部字典 |
| 契约 | `OPCUA_NODE_IDS` 默认空、每点字段默认空、`opcua_node_id` 进指纹 |
| 默认行为 | `SOUTH_PROTOCOL == "modbus"`、`OPCUA_READER is None`、陈旧判据 = 轮询×3 |
| compose | YAML 解析通过，两个网关实例都注入了协议变量，plant2 端点可独立覆盖 |

---

## 10. 影响面

- **不改**：Modbus 路径的行为、报文格式、入库路径、告警判据、既有 583 条测试的行为。
- **新增**：1 个模块、1 个契约字段（默认空）、5 个配置项 2 个必填前置、1 个只读视图、
  1 个纯函数；`requirements.txt` 里一行**被注释掉**的可选依赖。
- **默认不变**：不设 `SOUTH_PROTOCOL` 时一切与改造前一致 —— 有测试断言这一点。
