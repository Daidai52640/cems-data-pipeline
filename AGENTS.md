# AGENTS.md — CEMS 数据采集项目协作规则

> 适用范围：本仓库全部代码（`src/`、`data/`、`docs/`）。
> 详细依据：`skills/`（通用规范）+ `rules/`（评审/流程规范）。本文件是**精简入口**，只保留与本项目相关的规则；需要细节时再按 §8 索引加载对应文件。

---

## 1. 项目上下文

- 语言/版本：Python 3.12，依赖见 `requirements.txt`
- 链路分层（数据流：设备 → 网关 → MQTT → 平台 → 时序库 → Web）：
  - 设备层 `src/device/modbus_server.py` — pymodbus，Modbus TCP 仿真，端口 5020
  - 网关层 `src/gateway/gateway.py` — paho-mqtt，轮询读寄存器 → ÷10 还原 → 发布 `cems/plant1/data`
  - 平台层 `src/platform/subscriber_to_td.py` — taospy，订阅 → 写入 TDengine 超级表 `cems.cems_data`
  - 展示层 `src/web/web_dashboard.py` — Flask + ECharts，`/api/data`、`/api/health`
- Web 框架：**Flask（仅此一处）**；本项目没有 Django、DRF、FastAPI
- 外部系统：Modbus TCP、MQTT（EMQX）、TDengine 3.x（REST 6041）
- 数据约定：**9 个测点**（顺序即 HJ 212 上传值序：`Flow, Dust, SO2, NOx, O2, Velocity, Temp, Humidity, Pressure`），
  单位与超标限值以 `src/common/points.py` 为唯一真源；设备寄存器**按测点各自的比例系数**放大存整数（见 `Point.scale`），网关负责还原
- 当前项目**不存在的**能力（引入前先确认，别默认存在）：ORM / 数据库迁移层、测试框架、CI、`pyproject.toml`、类型检查配置

---

## 2. 代码风格 code-style

详细：`skills/common/code-style.md`（48KB，按需读；章节索引见 `skills/common/code-style-index.md`）

必须：

- 4 空格缩进，禁止 Tab；行宽 ≤ 100
- 每个 `.py` 顶部：中文模块 docstring → `from __future__ import annotations` → 标准库 / 第三方 / 本地 三组 import（组间空行，禁止一行多模块、禁止星号导入）
- 所有函数写类型注解；可调参数集中在文件顶部「配置区」，用模块级 `Final[...]` + `os.getenv("KEY", 默认值)`（沿用现有写法）
- 字符串用 f-string；日志用 `%s` 惰性占位。禁止 `%` 拼接和 `.format()`
- 命名：模块/函数 `snake_case`，类 `PascalCase`，常量 `UPPER_SNAKE_CASE`，布尔量 `is_/has_/can_` 前缀
- 禁止：可变默认参数、用 `type()` 判类型（用 `isinstance`）、在 `finally` 里 `return`、用异常消息做流程控制
- 注释/日志/文档用中文，技术术语保留英文；注释解释「为什么」，不复述代码

不要做（本项目未配置，别单方面引入）：

- 不要为了「符合规范」就新增 `pyproject.toml`、引 ruff/mypy 并对现有文件做大范围重排；确需引入时先提出并说明理由

---

## 3. 错误处理 error-handling

详细：`skills/common/error-handling.md`

- 只捕获能处理的具体异常（`except OSError`、`except ValueError`）；禁止裸 `except:`
- try 块保持最小，只包住真正可能抛错的那一步
- 跨系统调用失败（Modbus / MQTT / TDengine）要包装后记录，**不让第三方异常直接泄漏**给调用方
- 保留现场用 `LOGGER.exception(...)`（带堆栈）；仅补上下文用 `LOGGER.error("...: %s", exc)`
- **禁止吞异常**：记录后必须 重试 / 降级 / `raise` 三选一
- 长驻循环（网关轮询、设备寄存器刷新）里单次失败不能让线程退出；重试/重连要限速，避免忙等和日志刷屏
- 清理类失败（`close()`、断开连接）可降级到 `LOGGER.debug(... 忽略)`，沿用现有写法
- 不要拦截需要放行的中断：`KeyboardInterrupt` / `SystemExit`

---

## 4. 日志 logging

详细：`skills/common/logging.md`

- 用标准库 `logging`（本项目现状）；**不要 `print()`** 做运行日志
- 每个模块声明自己的 logger：`LOGGER: Final[logging.Logger] = logging.getLogger("层.模块")`（如 `web.dashboard`、`gateway`、`device.modbus_server`）；不要跨模块传递 logger 实例
- 统一由模块内 `setup_logging()` 初始化：`logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, datefmt=LOG_DATEFMT)`，格式 `%(asctime)s [%(levelname)s] %(name)s: %(message)s`
- 级别约定：
  - DEBUG — 逐次轮询、寄存器原始值、可忽略的清理失败
  - INFO — 启动、连接成功、订阅成功、建库建表就绪
  - WARNING — 可恢复异常：重试、降级、配置回落
  - ERROR — 影响一次采集/入库/接口请求的失败
  - CRITICAL — 进程无法继续
- 参数化输出 `LOGGER.info("连接 %s:%d", host, port)`，不要先拼字符串
- 记录异常用 `LOGGER.exception(...)` 或 `exc_info=True`
- **绝不记录**：`TD_PASS` 等口令、token、含凭据的完整连接串；必须记录时先脱敏

---

## 5. 安全 security

详细：`skills/common/security.md`

- 不新增硬编码密钥/口令；连接参数一律走环境变量（沿用 `TD_URL/TD_USER/TD_PASS/TD_DB/...` 模式）
- 现有 demo 默认值（root/taosdata、localhost）**仅限本机演示**；对外部署必须用环境变量覆盖，并补 `.env.example`（只列键名与说明，不放真实值）
- SQL 注入：TDengine 的查询/建表/写入不得把外部输入（MQTT payload、URL 参数）拼进 SQL；必须参数化或用严格白名单校验
- 接口入参：`/api/data` 等接口的参数要做类型与范围校验，非法输入返回 4xx
- 错误响应不暴露内部细节（SQL、连接串、堆栈）；堆栈只进日志
- CORS 用显式白名单，禁止 `*`；5020 / 5000 / 1883 / 6041 等端口默认仅内网可达
- 依赖：`requirements.txt` 固定最低版本，定期 `pip-audit -r requirements.txt`；新增依赖需说明用途
- 反序列化：`json.loads` 后的字段先校验类型/范围，再参与计算与入库

---

## 6. 代码评审 code-review

详细：`rules/code-review.md`

- 评论分级：`🔴 BLOCKER` / `🟠 MUST FIX` / `🟡 SUGGESTION` / `🟢 NIT` / `💬 QUESTION` / `👍 PRAISE`；每次评审至少留一条 `👍 PRAISE`
- 检查项（结合本项目）：
  - 正确性 — 边界与空值、寄存器换算、并发/线程安全（多线程刷新 vs 读取）
  - 安全 — 见 §5
  - 性能 — 轮询/查询频率是否合理、入库是否批量、有无忙等或 N+1 式重复连接
  - 可维护性 — 函数 < 30 行、单一职责、无死代码与注释掉的代码块、公共接口有类型注解
  - 契约 — 主题名、超级表列名、接口字段名变更需前后端同步
- 反馈要可执行：问题 + 建议做法 + 依据（指向 `skills/...` 或 `rules/...`）；避免「这样不对」「我会换个写法」这类无法落地的评论
- 提交前先自查 diff；变更尽量小（< 400 行），描述写清 what / why / how to test

---

## 7. Flask 专项

详细：`skills/flask/SKILL.md`（只取与本项目相关的部分）

保留并遵循：

- 路由只做「参数校验 → 调服务/查询函数 → 组装 JSON 响应」，数据访问逻辑放独立函数，不堆在视图里
- 用 `@app.errorhandler` 统一处理异常并返回结构化 JSON；沿用现有「放行 `HTTPException`、其余记 `LOGGER.exception` 后返回 500」的写法
- 连接类资源在请求内创建、在 `finally` 中关闭；不引入全局共享的可变状态
- 配置集中在文件顶部（`Final` 常量 + `os.getenv`），容器化时由 `docker-compose.yml` 注入
- 响应字段名与前端 ECharts 保持一致；改字段属于契约变更

不适用（本项目没有，别照搬）：

- SQLAlchemy / Flask-SQLAlchemy、Flask-Migrate、marshmallow/pydantic 校验层、Flask-JWT-Extended、Celery、应用工厂 + 蓝图拆分
- 存储是 TDengine，不是关系库；不要引入 ORM 或数据库迁移工具

---

## 8. 硬性红线

1. 不修改 `src/` 下既有业务逻辑，除非任务明确要求；规则类改动只新增文件
2. 不在代码中写死密钥/口令
3. 不用 `print()` 充当生产日志
4. 不写裸 `except:`
5. 不静默吞掉异常
6. 主题名（`cems/plant1/data`）、超级表/列名、接口字段属于契约，改动需全链路同步
7. 新增第三方依赖必须同步写入 `requirements.txt`

---

## 9. 文档结构 doc-structure

**入口**：`docs/README.md`（导航；AI 与他人各走哪条路都写在那里）

| 目录 | 类型 | 只写什么 |
|---|---|---|
| `docs/adr/` | 架构决策记录 | **决策与理由**（背景 → 备选 → 决策 → 后果含代价）。定稿后不改写，决策变了新增一份 |
| `docs/runbooks/` | 运维手册 / 台账 | **怎么做 / 做过什么**（命令可复现；台账按 现象→排查→根因→修复→数据影响） |
| `docs/reference/` | 事实与实测 | **约束与数字**（必须写测法、原始数据位置、误差来源） |
| `docs/evidence/` | 机器证据 | **只读归档**原始产物，分子目录（`perf/ drill/ cache/ backup/ coverage/ logs/`），**不在此写结论** |
| `docs/test-reports/` | 验收用例 | 测试目标 / 基线 / 通过标准 / 结论 |
| `docs/diagrams/` | 图形产物 | 生成物与其源数据一起放，便于重生成 |

**三条硬规矩**：

1. **结论必须能追溯到证据**——每个数字后面给命令或 `evidence/` 文件名，不许"约为/大概"。
2. **区分"决策"与"事实"**：选型与取舍 → `adr/`；实测数字 → `reference/`；发现的约束（如时钟会漂）→ `reference/`。
   ⚠️ 把事实写进 ADR、或把决策写进 reference，都会让人找不到东西。
3. **凡有实测，必单列"局限 / 没做到"一节**——不写局限的数字不可信。

**新增文档的判定**：影响后续设计的选择 → 新增 ADR；摸清一条新约束 → 新增 `reference/`；
真做了一次演练 → 往 `runbooks/故障台账.md` 追加；发现现有文档写错 → 就地修正并在提交信息里写明。

---

## 10. 规范文件索引（按需加载）

| 主题 | 路径 | 何时加载 |
|---|---|---|
| 代码风格 | `skills/common/code-style.md`（索引：`skills/common/code-style-index.md`） | 写任何 Python |
| 错误处理 | `skills/common/error-handling.md` | 写异常/重试逻辑 |
| 日志 | `skills/common/logging.md` | 加日志、调级别 |
| 安全 | `skills/common/security.md` | 密钥、输入校验、SQL、接口 |
| 代码评审 | `rules/code-review.md` | 评审代码 |
| Flask | `skills/flask/SKILL.md` | 改 Web 层 |
| **文档目录规范** | **`skills/common/folder-structure.md`** | **新增/移动 `docs/` 下任何文档** |

其余文件（`skills/django/`、`skills/drf/`、`skills/fastapi/`、`skills/common/microservices.md`、`db-design.md`、`data-migrations.md`、`api-auth.md`、`observability.md`、`feature-flags.md`、`llm-patterns.md`、`ci-cd.md`、`async-patterns.md`、`dependency-management.md`、`deployment.md`、`testing.md`、`performance.md`、`rules/api-design.md`、`rules/git-workflow.md`）与本项目技术栈无关，**本文件不引用、默认不加载**。
