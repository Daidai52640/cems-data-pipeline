# systemd 单元（边缘节点）

三个服务对应边缘节点的南向两层：

| unit | 角色 | 端口 / 依赖 |
|---|---|---|
| `cems-device.service` | Modbus TCP 从站（:5020），1 号炉 Temp/Humidity 由 `/dev/ttyUSB0` 真机直读 | 需要串口 |
| `cems-gateway-plant1.service` | 读从站 1 → MQTT（断网缓存/补传） | 依赖 `cems-device` |
| `cems-gateway-plant2.service` | 读从站 2（纯仿真）→ 独立主题与缓存目录 | 依赖 `cems-device` |

## 安装

```bash
sudo bash install.sh --repo /home/ddf/cems --user ddf --platform-host 192.168.31.18
```

`install.sh` 只替换三处：代码路径、运行用户、broker 地址。其它环境变量（端口、从站号、缓存目录）都是显式的。

## 卸载

```bash
sudo bash uninstall.sh      # 单元文件移到 /var/backups/cems-systemd/
```

## 边界

- ⚠️ **看门狗只覆盖"采集循环卡住"**：`Type=notify` + `WatchdogSec=60`，应用在采集循环每圈发 `WATCHDOG=1`；循环卡住（读请求挂死、线程死锁）会被 systemd 杀掉重启。**不覆盖**"循环在转但数据是假的"这类语义问题。
- ⚠️ **掉电自愈未验证**：`reboot` 已验证（三服务自动起），但断电（非正常关机）未测。
- ⚠️ **MQTT 无认证**：broker 依赖网络边界（见顶层 `README.md` §端口暴露 / 安全）。
- 服务日志走 journald，`journalctl -u cems-device -f` 可看。

完整背景、实测记录与复现步骤见 `docs/runbooks/边缘节点下板.md`。
