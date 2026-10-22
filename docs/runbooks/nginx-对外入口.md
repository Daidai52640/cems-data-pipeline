# nginx 对外入口：在 Linux 宿主上把 80 端口跑起来（宿主机 venv 形态）

> 目的：把仓库里现成的 `deploy/nginx/` 这套配置在 Linux 宿主上真正启动，并留下可照着复现的步骤与一手证据。
> 结果：`cems-nginx` 在 `0.0.0.0:80` 对外反代到宿主机 venv 的 web（5000），6 项验收全部实测通过；`restart: unless-stopped` 由 docker 守护进程生效。
> 取证日期：2026-10-18 · 证据见 `../evidence/nginx-20261018/`
> 本文收口的是 `跨平台部署.md` §六 边界 4「nginx 未启动」。

---

## 一、为什么需要这一层

不是"计划里写了要加 nginx"，是四件具体的事（详细推演在 `deploy/nginx/nginx.conf` 顶部注释，决策记录见 `../adr/0004-引入nginx与redis缓存.md`）：

1. **并发模型不匹配。** 后端是 waitress，`WEB_THREADS=8`，是进程内线程池：第 9 个并发请求只能在内核 accept 队列里排队，慢客户端（弱网手机、休眠后唤醒的大屏机）会一直占着一个线程。nginx 是事件驱动，用很小的内存把慢连接"挂住"，只在后端真能处理时才转发。
2. **静态资源不该走 Python。** `/static/echarts.min.js` 实测 **1 024 695 字节**，走 waitress 时每个字节都要过一次 WSGI 循环；nginx 用 sendfile 由内核直接从页缓存拷到 socket，并支持 gzip 与 `If-None-Match`（304）。
3. **TLS 终结位置要提前定。** 将来上 HTTPS 只在这一层加证书，后端继续跑明文 HTTP；否则证书要装进业务镜像，轮换就得重建镜像。
4. **后端端口从对外契约里消失。** 对外只暴露 80，"后端在哪个端口"不再是外部调用方要知道的事。

**这一层不做什么**（先划清，免得被当成它没做的事）：

- **不做认证 / 授权**：`/` 与 `/api/*` 对能访问到 80 的人一律放行；它只挡 `/api/cache/*` 这类内部接口（见 §六）。
- **不缓存动态接口**：缓存是 Redis 层的事（`src/web/cache.py`），nginx 只做静态资源的一年期缓存头。
- **不判后端健康**：`/healthz` 只回答"nginx 进程能不能正常收并发响应"，不探后端；后端挂了是 web 服务 healthcheck 的事，混在一起会让两层同时红、排障时分不清是哪一层。
- **本次没终止 TLS**：只在注释里预留了位置，没装证书（见 §九）。

---

## 二、目标环境与本次形态（实测）

| 项 | 值 |
|---|---|
| 宿主 | Linux Mint 22.3（Ubuntu 24.04 基础），内核 6.14.0-37-generic，x86_64 |
| Docker | 29.1.3（`docker.io` 包） |
| Compose | 2.40.3 |
| nginx 镜像 | `nginx:1.31.6-alpine`，`docker image inspect` 实测 digest `sha256:df221db836e1754089190208cee7eeda94f233197056426eda74a43ab1abeac2`（前 12 位与 `docker-compose.yml` 注释里记的一致） |
| 容器内 nginx | 1.31.6 |
| 现有栈 | `cems-redis` / `tdengine` / `emqx` 三个基础设施容器（healthy），网络 `cems-net` |
| 业务进程 | 4 个由宿主机 venv + `nohup` 跑的 Python 进程；web 监听 `0.0.0.0:5000` |
| 本次新增容器 | `cems-nginx`（对外 80）、`cems-nginx-web`（`web` 名占位转发，见 §三） |

> 容器内端口、网络成员、镜像 digest 的原始输出见 `../evidence/nginx-20261018/00-environment.txt`。

### 与 compose 全栈形态的差异（一句话版）

compose 全栈形态里 `web` 是**真实容器**，由 Docker 内置 DNS 解析，**不需要任何替身**：`docker compose up -d nginx` 一条命令即可。
本次是**宿主机 venv 形态**：`web` 是宿主进程，Docker DNS 里没有这个名字，所以要补一个"占位转发容器"——差异就这一处，`deploy/nginx/nginx.conf` 与 `conf.d/cems.conf` 与 compose 形态**同一份、一行未改**。

---

## 三、为什么宿主机形态需要一个 `web` 替身（一手记录）

`conf.d/cems.conf` 是这么反代的：

```nginx
set $cems_web "http://web:5000";
proxy_pass $cems_web;
```

变量形式的 `proxy_pass` 里出现域名时，nginx **只走 `resolver`**；`nginx.conf` 里配的是 Docker 内置 DNS `127.0.0.11`。而 **nginx 不会去读容器的 `/etc/hosts`**。

这带来一个反直觉的失败：容器跑起来了、`/healthz` 正常，但**凡是需要反代的请求全部 502**。用 `--network host --add-host web:127.0.0.1` 试过一次（`/etc/hosts` 里确实写进了 `127.0.0.1 web`），实测结果：

```
$ docker exec nginx-hostnet-probe cat /etc/hosts
127.0.0.1	localhost
...
127.0.0.1	web

$ curl -sS -o /dev/null -w "status=%{http_code}" http://127.0.0.1/healthz
status=200

$ curl -sS -o /dev/null -w "status=%{http_code} time=%{time_total}" http://127.0.0.1/
status=502 time=3.003669

$ docker logs nginx-hostnet-probe
2026/10/18 14:58:04 [error] 21#21: recv() failed (111: Connection refused) while resolving, resolver: 127.0.0.11:53
2026/10/18 14:58:07 [error] 21#21: *2 web could not be resolved (110: Operation timed out), client: 127.0.0.1, server: _, request: "GET / HTTP/1.1", host: "127.0.0.1"
```

即：**`/etc/hosts` 里写了也没用，因为它根本不查 `/etc/hosts`**；`--network host` 下又没有 `127.0.0.11`，于是解析超时、每请求 3 秒后 502。
完整原始输出见 `../evidence/nginx-20261018/08-why-web-name-required.txt`。

> 也试过反向的"绕开 resolver"：补一个同名 upstream 组 `upstream web { server 127.0.0.1:5000; }`（`nginx -t` 通过），但请求仍然走 resolver → 同样 502。**不可用**，已放弃。

### 采用的做法：让 `web` 在 Docker DNS 里真实存在

起一个容器专门占住 `web` 这个名字，用镜像自带的 `stream` 模块做**纯 TCP 转发**（`5000` → `host.docker.internal:5000`，即宿主机上真实跑的 `src/web/web_dashboard.py`）：

- 它**不解析 HTTP**，只转发字节流，所以 `X-Real-IP` 等头不会被它改写，外层 nginx 仍按"连接的真实对端"写入（实测见 §五 第 6 项）。
- 它只占 `cems-net` 内部的 5000 端口，**不新增宿主监听端口**。
- 两份文件都在 `deploy/nginx/host/` 下，只有宿主机形态才用；compose 形态不需要、也不应加载（否则 `web` 会指向 nginx 容器自己的 `127.0.0.1:5000`）：

| 文件 | 作用 |
|---|---|
| `deploy/nginx/host/web-forward.conf` | 转发器本体：`stream { server { listen 5000; proxy_pass host.docker.internal:5000; } }` |
| `deploy/nginx/host/web-forward.compose.yml` | 独立 compose 文件：`external` 引用现有 `cems-net`、`extra_hosts: host.docker.internal:host-gateway`、固定容器名 `cems-nginx-web`、`restart: unless-stopped` |

> 为什么不用 `docker run` 手写：独立 compose 文件把"容器名 / 网络别名 / 重启策略 / 额外 hosts"固定成可 diff 的声明，起和拆各一条命令；主 `docker-compose.yml` **一行没动**。

---

## 四、这次怎么起（两条命令）

前置：整条栈已在跑（`cems-net` 已存在）、`nginx:1.31.6-alpine` 已在本地镜像里。

```bash
cd /home/ddf/cems-data-pipeline

# 1) 起 `web` 占位转发（先起：nginx 起来就要能解析到它）
docker compose -f deploy/nginx/host/web-forward.compose.yml up -d

# 2) 起 nginx 本体：用仓库 compose 里现成的 nginx 服务定义，跳过依赖
BIND_ADDR=0.0.0.0 docker compose up -d --no-deps nginx
```

两条命令实测输出（原始输出见 `../evidence/nginx-20261018/09-teardown-and-restart.txt`）：

```
 Container cems-nginx-web  Creating
 Container cems-nginx-web  Created
 Container cems-nginx-web  Starting
 Container cems-nginx-web  Started
 Container cems-nginx  Creating
 Container cems-nginx  Created
 Container cems-nginx  Starting
 Container cems-nginx  Started
```

两个参数都是**有原因的**：

- **`--no-deps`**：compose 里 nginx 服务写了 `depends_on: web: {condition: service_healthy}`（避免 nginx 在后端还没监听 5000 时对外全是 502）。本次 `web` 不是 compose 服务，所以跳过依赖、改由上面第 1 条命令先起替身。实测 `--no-deps` **没有报错**，因此不需要退回手写 `docker run`。
- **`BIND_ADDR=0.0.0.0`**：compose 里端口写的是 `${BIND_ADDR:-127.0.0.1}:${NGINX_HOST_PORT:-80}:80`，默认**只绑本机**。本次要从另一台机器验收（§五 第 7 项），故**临时放开**；**默认值不改**，`docker-compose.yml` 里仍是 `${BIND_ADDR:-127.0.0.1}`。

起完的自检（应输出 `200` / `ok`）：

```bash
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1/healthz    # 200
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1/           # 200
docker ps --filter name=cems-nginx --format '{{.Names}} {{.Status}}'
```

---

## 五、验收结果（全部实测）

| # | 项 | 结果 | 证据 |
|---|---|---|---|
| 1 | `/healthz` | ✅ 200，`Content-Type: text/plain; charset=utf-8`，body 逐字节为 `o k \n`（3 字节） | `02-healthz.txt` |
| 2 | `/` 经反代 | ✅ 200，**16 737 字节**，body 是大屏 HTML | `03-proxy-root-apidata.txt` |
| 3 | 任一 `/api/cache/*` | ✅ **403**，由本层 `return 403` 直接返回（不代理到后端）；同路径直连 5000 是 200 | `04-403-api-cache.txt` |
| 4 | `/static/echarts.min.js` | ✅ 200，**1 024 695 字节**；`Cache-Control: public, max-age=31536000, immutable`、`Expires` 为一年后；带 ETag 二次请求 **304**；带 `Accept-Encoding: gzip` 返回 `Content-Encoding: gzip` | `05-static-cache-headers.txt` |
| 5 | `nginx -t` | ✅ `syntax is ok` / `test is successful`（生效配置 284 行） | `01-nginx-t.txt` |
| 6 | 反代头（`X-Real-IP`） | ✅ 后端看到的是**连接的真实对端**：局域网客户端 `192.168.31.76` 故意伪造 `X-Real-IP: 1.2.3.4` 后，后端回显仍是 `192.168.31.76` | `07-x-real-ip-probe-from-lan.txt` |
| 7 | 跨机访问 | ✅ 从 Windows（`192.168.31.76`）经 `192.168.31.18:80`：`/healthz` 200（`text/plain`）、`/` 200（16 737 字节）、`/api/data` 200、`/api/health` 200、`/api/cache/stats` 403 | `10-lan-client-final.txt` |
| 8 | 容器健康 | ✅ `cems-nginx` healthcheck `healthy`（每 10 s 一次，5 次连续 ExitCode 0） | `06-restart-policy-and-health.txt` |

补充两条与"数字"有关的说明：

- `/api/data` 的响应大小随查询窗口滑动，实测出现过 **7 797** 与 **7 820** 字节两个值；第 2 项记的是与首次验收同一时刻的 `/` 大小（16 737 字节）。
- 第 6 项的做法：把 `web` 临时指到一个"回显探针"上（一次性容器，不在仓库里），由它把外层 nginx 送来的头原样回显；业务进程与仓库配置都没改。探针窗口内 `/` 返回的是探针文本（133 字节），**不是**大屏页面，别拿那一行当"页面大小"。

---

## 六、403 口径：本层直接返回，不代理

`conf.d/cems.conf` 里：

```nginx
location /api/cache/ {
    access_log off;
    return 403;
}
```

- **为什么挡**：`/api/cache/*` 是测量脚本与排障用的内部接口，会吐出内部计数、缓存水位、缓存键数量。它能被直连 5000 取到（测量脚本正是这么做的），但不该经对外入口暴露。
- **口径**：命中这条 `location` 的全部路径（本层任何 IP、任何方法）都由 **nginx 自己 `return 403`**，请求**不会**出现在后端日志里、也不产生 `upstream_response_time`；这一条 `location` 还写了 `access_log off`，所以连 nginx 访问日志里都不留行。
- **实测**（`../evidence/nginx-20261018/04-403-api-cache.txt`）：

| 请求 | 结果 |
|---|---|
| `curl -i http://127.0.0.1/api/cache/stats` | `HTTP/1.1 403 Forbidden`，body 为 nginx 自带 146 字节 HTML，`Server: nginx` |
| `curl -i http://127.0.0.1/api/cache/anything` | 同上（整个前缀都被挡） |
| `curl -o /dev/null -w '%{http_code}' http://127.0.0.1:5000/api/cache/stats` | **200**（直连后端可用 —— 对照用） |

---

## 七、与 compose 全栈形态的差异

| 项 | compose 全栈形态 | 本次宿主机 venv 形态 |
|---|---|---|
| `web` 后端 | `cems-web` 容器，Docker DNS 按服务名解析 | **替身容器** `cems-nginx-web`（别名 `web`）+ TCP 转发到宿主 5000 |
| 起法 | `docker compose up -d nginx`（依赖 `web` healthy） | 先 `docker compose -f deploy/nginx/host/web-forward.compose.yml up -d`，再 `BIND_ADDR=0.0.0.0 docker compose up -d --no-deps nginx` |
| 端口绑定 | `${BIND_ADDR:-127.0.0.1}:80` | 同上，本次验收临时 `BIND_ADDR=0.0.0.0` |
| 依赖顺序 | 由 `depends_on` 保证 | 手工先起替身（顺序反了会短暂 502；nginx 侧 resolver 缓存 `valid=10s`，本次实验是等 13 秒后确认恢复，见 §八末注） |
| 额外文件 | 无 | `deploy/nginx/host/web-forward.conf` + `web-forward.compose.yml`（仅宿主形态） |
| 应用镜像 | 需要 `cems-pipeline:latest`（本环境里是空的，见 `跨平台部署.md` §六 边界 1） | 不需要，业务进程用宿主 venv |
| `nginx.conf` / `cems.conf` | 同一份 | **同一份，一行未改** |

**容器全栈形态不需要替身。** 反过来说：哪天 web 也进了容器（应用镜像构建路径打通后），把 `deploy/nginx/host/` 这套替身删掉、`docker compose up -d nginx` 一条命令即可——差异就是一个替身文件对。

> ⚠️ **2026-10-22 实测补记（这条必须做）**：web 后来真的进了容器（应用镜像已在 Linux 本机构建），
> 但替身当时没拆，结果 `cems-web` 与 `cems-nginx-web` **同时在 `cems-net` 上持别名字 `web`**，
> Docker DNS 交替返回两个地址。从 nginx 容器里连测 `http://web:5000/` 12 次：**5 次成功、7 次失败**
> （失败的是替身 → 转发到宿主 5000，那里已经没有进程）。外层 nginx 连测 30 次恰好都命中真 web，
> 是因为 resolver 缓存 `valid=10s`——容器一重建就可能变成整段 502。
> ⇒ **web 容器化后第一件事就是拆替身**：`docker compose -f deploy/nginx/host/web-forward.compose.yml down`。
> 拆除后实测：`web` 只解析到一个地址，12/12 成功、20/20 200。

---

## 八、拆除与回退

```bash
cd /home/ddf/cems-data-pipeline

# 只停 nginx 层（不碰 emqx / tdengine / redis / 业务进程）
docker compose stop nginx

# 连容器一起删掉（下次用 §四 第 2 条命令重建）
docker compose rm -f nginx

# 拆掉 `web` 替身
docker compose -f deploy/nginx/host/web-forward.compose.yml down

# 回到 compose 默认绑定（只绑 127.0.0.1，本机可访问、外机不可）
docker compose up -d --no-deps nginx
```

⚠️ **不要用 `docker compose down`**：那是整个项目的 `down`，会带走 `emqx` / `tdengine` / `redis` 这批基础设施容器与数据卷编排。本次所有操作都是"显式服务名"或"独立 compose 文件"。

实测（`../evidence/nginx-20261018/09-teardown-and-restart.txt`）：

- `docker compose stop nginx` → 80 端口释放（`ss -ltn` 无 `:80`），`curl` 连接被拒；
- 替身 `down` → 容器移除；再 `up -d` + `up -d --no-deps nginx` → `cems-nginx` 恢复、`/` 回到 200；
- **注**：更换 `web` 指向后，nginx 侧 DNS 缓存有 `valid=10s`，切换后在 10 秒内仍可能命中旧地址；本次实测等待 13 秒后恢复（探针实验里就是这么做的）。

---

## 九、边界（本次**没有**覆盖的）

| # | 边界 | 说明 |
|---|---|---|
| 1 | ⚠️ **systemd 未接管** | 容器由 docker 守护进程按 `restart: unless-stopped` 拉起，**不等于** systemd 托管：没有 unit、没有依赖排序、没有统一 journald 日志。实测 `systemctl list-units --type=service --all \| grep -i cems` → 无输出；宿主机 pid 1 是 systemd（证据 `06-restart-policy-and-health.txt`）。**systemd 化推迟到树莓派阶段**，本次没做 |
| 2 | ⚠️ **TLS 未配置** | `nginx.conf` 只预留了"TLS 终结放这一层"的位置，**没装证书、没验 HTTPS / HTTP/2** |
| 3 | ⚠️ **限流未压测** | `limit_req 40r/s`、`limit_conn 32`、`limit_req_status/conn_status 429` 是**配置值**；本次只做了单请求功能验证，**没做并发压测，也没验证 429 实际触发** |
| 4 | ⚠️ **慢客户端行为未测** | §一 第 1 条的收益是机制性的（事件驱动 vs 线程池）；本次未做慢连接挂住、大屏休眠唤醒这类场景的实测 |
| 5 | ⚠️ **应用镜像构建路径仍未验证** | 本次完全没碰 `Dockerfile` / 应用镜像；`cems-pipeline:latest` 仍是空镜像（`跨平台部署.md` §六 边界 1 未变） |
| 6 | ⚠️ **掉电 / 重启自愈未测** | 没有 reboot 这台共享机器（会打断另外 4 个业务进程与 3 个容器） |
| 7 | ⚠️ **无 HA / 多机** | 单机单入口，多机、集群未涉及（与 `README.md` 已知边界一致） |
| 8 | ⚠️ **未接真实环保平台** | 与 `README.md` 已知边界第 1 条一致，本次未改变 |
| 9 | ⚠️ **替身是宿主形态的权宜之计** | Docker DNS 里的 `web` 只能指向容器；web 一旦也容器化，替身必须删除，否则 `web` 会指向错的目标 |
| 10 | ⚠️ **日志轮转未配置** | 访问日志走容器 stdout（实测容器内 `/var/log/nginx/access.log -> /dev/stdout`、`error.log -> /dev/stderr`，所以要用 `docker logs` 读；在容器内对 access.log 做 `tail` 会一直阻塞），由 docker 默认 json-file 驱动管理；**没设 `max-size`/`max-file`**，长跑会涨 |
| 11 | ⚠️ **只验了 IPv4** | `cems.conf` 里有 `listen [::]:80`，本次访问全走 IPv4，**IPv6 未验证** |
| 12 | ⚠️ **未做 nginx 升级 / 证书轮换演练** | 镜像固定在 `1.31.6`，升级路径未涉及 |

> 一条与证据阅读有关的提醒：`08-why-web-name-required.txt` 里的日志时间是 **UTC**（那次实验的容器没设 `TZ`），其余证据里的容器由 compose 定义了 `TZ=Asia/Shanghai`，日志是 `+08:00`。两边差 8 小时，不是数据错误。

---

## 十、复现命令（从零到跑通）

前置：4 个业务进程已在跑（`0.0.0.0:5000` 有响应）、`cems-net` 已存在、`nginx:1.31.6-alpine` 已 `docker load`。

```bash
cd /home/ddf/cems-data-pipeline

# 0) 确认前置：栈在跑、web 在监听、80 空闲
docker network ls | grep cems-net
curl -sS -o /dev/null -w 'web_direct=%{http_code}\n' http://127.0.0.1:5000/
ss -ltn | grep -E ':80 ' || echo 'port 80 free'

# 1) 起 `web` 替身（宿主形态专用）
docker compose -f deploy/nginx/host/web-forward.compose.yml up -d

# 2) 起 nginx（仓库 compose 定义；跨机验收才需要 BIND_ADDR=0.0.0.0）
BIND_ADDR=0.0.0.0 docker compose up -d --no-deps nginx

# 3) 等健康
docker ps --filter name=cems-nginx --format '{{.Names}} {{.Status}}'

# 4) 六项验收
docker exec cems-nginx nginx -t
curl -sS -i http://127.0.0.1/healthz
curl -sS -o /dev/null -w '/=%{http_code} bytes=%{size_download}\n' http://127.0.0.1/
curl -sS -i http://127.0.0.1/api/cache/stats
curl -sS -D - -o /dev/null http://127.0.0.1/static/echarts.min.js
curl -sS -o /dev/null -w 'api_data=%{http_code}\n' http://127.0.0.1/api/data
```

**预期**：`nginx -t` 两行 ok；`/healthz` 200 且 body 是 `ok`；`/` 200（大屏 HTML 约 16 KB）；`/api/cache/stats` 403；静态资源 200 且 `Cache-Control: public, max-age=31536000, immutable`；`/api/data` 200。

### 反代头探针的复现（一次性，验证 `X-Real-IP`）

```bash
# 1) 把 `web` 停掉，换成回显探针（探针配置见 07 号证据文件开头）
docker compose -f deploy/nginx/host/web-forward.compose.yml stop
docker run -d --name nginx-echo-probe --network cems-net --network-alias web \
  -v /tmp/probedir/probe-echo.conf:/etc/nginx/probe.conf:ro \
  nginx:1.31.6-alpine nginx -c /etc/nginx/probe.conf -g 'daemon off;'
sleep 13   # nginx 侧 resolver 缓存 valid=10s

# 2) 从另一台机器故意伪造 X-Real-IP，看后端回显
curl -sS -H 'X-Real-IP: 1.2.3.4' -H 'X-Forwarded-For: 1.2.3.4' http://192.168.31.18/probe-from-lan

# 3) 复原
docker rm -f nginx-echo-probe
docker compose -f deploy/nginx/host/web-forward.compose.yml up -d
```

**预期**：回显里 `x_real_ip` / `x_forwarded_for` 都是**客户端真实地址**（本次实测 `192.168.31.76`），而不是伪造的 `1.2.3.4`。

### 证据文件清单（`../evidence/nginx-20261018/`）

| 文件 | 内容 |
|---|---|
| `00-environment.txt` | 宿主 / Docker / Compose / nginx 版本、镜像 digest、容器与端口快照、`cems-net` 成员 |
| `01-nginx-t.txt` | `nginx -t` 原始输出、生效配置行数 |
| `02-healthz.txt` | `/healthz` 响应头 + body 逐字节 dump、替身容器未收到该请求 |
| `03-proxy-root-apidata.txt` | `/`（含头部与 HTML 前 200 字节）、`/api/data`、`/api/health`，以及直连 5000 的对照 |
| `04-403-api-cache.txt` | `/api/cache/*` 的 **403 原始响应头与 body**、直连 5000 同路径 200 的对照、访问日志无该路径 |
| `05-static-cache-headers.txt` | 静态资源的完整响应头、304（If-None-Match）、gzip（Content-Encoding） |
| `06-restart-policy-and-health.txt` | 两个容器的 `RestartPolicy`、healthcheck 明细、无 systemd unit、pid 1 |
| `07-x-real-ip-probe-from-lan.txt` | 回显探针配置、局域网客户端伪造头后后端实际看到的 `X-Real-IP`、访问日志 |
| `08-why-web-name-required.txt` | **失败形态的一手记录**：`/etc/hosts` 写了 `web` 仍 502、`resolver: 127.0.0.11` 连不上、`web could not be resolved` |
| `09-teardown-and-restart.txt` | 停 / 拆 / 重建两条命令的实测输出、80 端口释放与恢复 |
| `10-lan-client-final.txt` | 终态下从 Windows 经 80 端口的逐项状态码与 403 原始响应头 |

---

## 十一、这次收窄了哪条边界

`跨平台部署.md` §六 原有一条：

> **4. ⚠️ nginx 未启动**（compose 中的 nginx 服务未参与本次验证）

本次已在 Linux 宿主上把它**真正启动起来并逐项验证**，这条边界可以收窄为：

> **nginx 已在 Linux 宿主启动（容器形态），对外 80 反代到宿主机 venv 的 web；但采"独立替身容器 + `--no-deps`"的形态，且 systemd 化、TLS、限流压测、掉电自愈均未验证（见 §九）。**

⚠️ 措辞上不夸大：这不等于"compose 全栈形态的 nginx 已验证"——应用镜像仍未构建（边界 1），替身是宿主形态的权宜（边界 9）。

---

## 十二、后续进展（2026-10-22，本文件写完之后）

| 本文当时的边界 | 现状 |
|---|---|
| §九 边界 1：systemd 未接管 | **已在边缘节点上做完**：树莓派 2B 的 device + 双网关由 systemd 托管，`Type=notify` + `WatchdogSec=60`，`kill -STOP` 验证（见 `边缘节点下板.md`）。平台侧的 nginx/web 仍由容器 `restart: unless-stopped` 拉起，**未** systemd 化 |
| §九 边界 5：应用镜像构建路径未验证 | **已验证**：`cems-pipeline:latest` 在 Linux 本机离线构建成功（222 MB），容器以 compose 项目 `cems` 运行（见 `边缘节点下板.md` §五、`evidence/edge-arm-20261022/08-platform-image.txt`） |
| §七：web 容器化后必须拆占位替身 | 已拆（2026-10-22），并补了实测记录：不拆时 `web:5000` 12 次中 7 次打到死端口 |

⚠️ 本文其余内容（如 `BIND_ADDR=0.0.0.0` 的跨机验收写法）保持不变；当前默认部署里 nginx 只绑回环，跨机验收要么临时放开 `BIND_ADDR`，要么走 SSH 隧道。