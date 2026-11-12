# 1000 并发连接压测

2026-11-06，对正式入口 `https://nineee.xyz/danmu/` 做了 WebSocket 并发压测。干净的一轮里，1000 个连接全部建立，保持约 120 秒，没有掉线。结论按这次运行（结果目录 `n1000d`）为准。

压测的是**同时在线的连接数**，不是弹幕库存条数。循环库上限 `LIBRARY_MAX = 250` 与这次测试无关。

## 结果

| 指标 | n1000d | 通过线 |
|---|---|---|
| 连接 | 1000 / 1000，失败率 0% | 失败率不超过 1% |
| 掉线 | 0 | 0 |
| 建连耗时 | p50 598 ms，p95 3115 ms | — |
| 上屏延迟 | p50 1.7 s，p95 9.4 s，最大 18.2 s（206 条样本） | p95 低于 10 s |
| 发送回执 | p50 312 ms，p95 1.9 s（208 条） | — |
| 容器 CPU 峰值 | 19.0% | 低于 60% |
| 8088 已建立连接峰值 | 1004 | — |
| 公网出口 `eth0` 峰值 | 9.5 Mbit/s | 机器带宽需高于此值 |
| 热队列 `pending` 峰值 | 208 | — |
| 观众收到的 `library` 消息 | 0 | 库列表只推管理端 |
| 入站流量 | 约 68.7 MB | — |

`report.py` 打印 `CONTINUE`。测完删除 208 条压测弹幕，库回到压测前的 22 条，热队列为 0。

同一次改造后还跑过 500 连接（490 观众 + 10 发送者，保持 90 秒）：500 / 500 成功，掉线 0，CPU 峰值 8.2%，上屏 p95 3.3 秒，8088 连接峰值 503。1500 没有跑。

## 测什么

混合场景：绝大多数连接只看，少数连接按间隔发弹幕。

- 观众 980，发送者 20，合计 1000。
- 建连速率 25 个/秒，6 个进程分摊。
- 全部连上后再保持 120 秒。
- 每个发送者大约每 15 秒发一条，实际间隔在 7.5–22.5 秒之间随机。
- 发送者昵称 `LT-<tag>`，正文 `LT-<tag>-<进程号>-<序号>`，便于事后按前缀删除。
- 地址：`wss://nineee.xyz/danmu/ws`。页面、静态资源和接口走 `https://nineee.xyz/danmu/`。

入口是服务器上的 Nginx 反代到 `danmu-api`，不是 Cloudflare 临时隧道。临时隧道在约 200 条连接时开始拒绝新连接（关闭码 1012）。那次对照：尝试 420，成功 392，失败率 6.67%，服务器上 8088 连接停在 201，容器 CPU 只有 5.1%。CPU 没有打满，卡住的是隧道连接数。

## 环境

- 服务器：2 vCPU，约 1.8 GB 内存。`danmu-api` 单进程 uvicorn。Nginx `worker_connections 8192`，文件句柄上限 65535，`/danmu/ws` 的 `proxy_read_timeout` 为 3600 秒。
- 压测机：Windows，Python 3.11，6 个逻辑核。依赖 `pip install websockets`。
- 采样机与压测机是同一台。采样通过 SSH 执行服务器上的脚本，密钥为本地 `id_ed25519_39`，主机 `root@39.108.164.203`。

上屏变慢的主要原因是热队列排队，不是 CPU 或带宽。新弹幕会优先播出 4 次（`HOT_BOOST`），20 个发送者持续写入时队列会积压。现场发言没有这么密时，延迟会更低。

## 脚本

都在仓库 `loadtest/`。原始 jsonl、`server.csv` 和 `sent_ids.txt` 只留在压测机，不进仓库。

| 文件 | 在哪跑 | 做什么 |
|---|---|---|
| `ws_load.py` | 压测机 | 多进程 asyncio。观众只收，发送者发弹幕。每个连接一条 jsonl。 |
| `report.py` | 压测机 | 汇总 `proc-*.jsonl` 和 `server.csv`，写 `report.txt`，并打印 `CONTINUE` 或 `STOP`。 |
| `cleanup.py` | 压测机 | 按 `sent_ids.txt` 删除；回执没写上的，再按昵称 `LT-` 前缀删。 |
| `server_monitor.ps1` | 压测机 | 每 5 秒 SSH 一次，把结果追加到 CSV。 |
| `sample_once.sh` | 服务器 `/tmp/danmu-sample-once.sh` | 采一次：容器 CPU/内存、8088 连接数、cloudflared、`/api/health`、网卡发送字节。 |
| `net_probe.sh` | 服务器 `/tmp/net_probe.sh` | 每 2 秒打印 `eth0` 出流量、443/8088 发送队列和 health。 |

`ws_load.py` 参数：

| 参数 | 1000 这轮 | 含义 |
|---|---|---|
| `--viewers` | 980 | 只接收的连接数 |
| `--senders` | 20 | 会发弹幕的连接数 |
| `--ramp-rate` | 25 | 全局每秒新建连接数。每个进程睡眠 `进程数 / ramp-rate` |
| `--hold` | 120 | 爬坡结束后再保持的秒数 |
| `--send-interval` | 15 | 发送间隔中值（秒），实际为 0.5–1.5 倍随机 |
| `--procs` | 6 | 进程数 |
| `--url` | `wss://nineee.xyz/danmu/ws` | WebSocket 地址 |
| `--tag` | `n1000d` | 写入昵称和正文，区分每一轮 |
| `--out` | `results/n1000d` | 日志目录 |

每个连接的日志事件：

- `ok`：收到 `hello`，附带建连毫秒数。
- `fail`：没连上或没收到 `hello`。
- `drop`：中途断开，记下关闭码。
- `fire_gap`：相邻两条 `fire` 的间隔。
- `ack`：发送者收到 `sent` 的耗时，并记下弹幕 id。
- `screen`：从发出到自己收到对应 `fire` 的耗时，按正文字符串匹配。
- `summary`：该连接收到的 `fire` 次数、`library` 次数和字节数。

`report.py` 自己的停止线比上面的通过线松：失败率超过 2%，或掉线率超过 1%，或上屏 p95 超过 60 秒，才打印 `STOP`。1000 这轮用的是更严的通过线，两项都过了。

## 流程

同一时间只跑一轮。上一轮的 `LT-` 弹幕没删干净时，热队列会把下一轮的上屏延迟抬高。

1. 看压测前的库大小，测完要回到这个数。

   ```powershell
   curl.exe -s https://nineee.xyz/danmu/api/health
   ```

2. 把采样脚本放到服务器（只需一次）：

   ```powershell
   scp -i $env:USERPROFILE\.ssh\id_ed25519_39 loadtest\sample_once.sh root@39.108.164.203:/tmp/danmu-sample-once.sh
   scp -i $env:USERPROFILE\.ssh\id_ed25519_39 loadtest\net_probe.sh root@39.108.164.203:/tmp/net_probe.sh
   ssh -i $env:USERPROFILE\.ssh\id_ed25519_39 root@39.108.164.203 "sed -i 's/\r$//' /tmp/danmu-sample-once.sh /tmp/net_probe.sh"
   ```

3. 在服务器上开始网卡采样，预计略长于爬坡加保持。1000 连接、25 个/秒、保持 120 秒时，采样 230 秒。

   ```bash
   nohup bash /tmp/net_probe.sh 230 > /tmp/net_probe.log 2>&1 &
   ```

4. 在压测机上开始容器采样，并启动客户端。工作目录是仓库根目录。

   ```powershell
   New-Item -ItemType Directory -Force loadtest\results\n1000 | Out-Null
   $mon = Start-Process powershell -ArgumentList "-NoProfile -File loadtest\server_monitor.ps1 -Seconds 220 -OutFile loadtest\results\n1000\server.csv" -PassThru -WindowStyle Hidden
   python loadtest\ws_load.py --viewers 980 --senders 20 --ramp-rate 25 --hold 120 --send-interval 15 --procs 6 --url wss://nineee.xyz/danmu/ws --tag n1000 --out loadtest\results\n1000
   Wait-Process -Id $mon.Id -Timeout 60 -ErrorAction SilentlyContinue
   ```

5. 汇总，并取回网卡日志：

   ```powershell
   python loadtest\report.py loadtest\results\n1000
   scp -i $env:USERPROFILE\.ssh\id_ed25519_39 root@39.108.164.203:/tmp/net_probe.log loadtest\results\n1000\net_probe.log
   ```

   看 `report.txt` 的失败率、掉线、CPU、上屏 p95，以及 `net_probe.log` 里 `eth0_tx=` 和 `"pending"` 的峰值。

6. 删除压测数据。若设置了 `DANMU_ADMIN_TOKEN`，先写入环境变量；当时这台机器未设置口令，删除接口不校验。

   ```powershell
   python loadtest\cleanup.py loadtest\results\n1000 --base https://nineee.xyz/danmu
   curl.exe -s https://nineee.xyz/danmu/api/health
   ```

   输出里 `LT-` 残留应为 0，`count` 应回到第 1 步看到的数字。

## 不要拿来当结论的一轮

`n1000c` 打印了 `STOP`：尝试 2000、掉线 557（关闭码 1006 和 1011）。原因是上一轮客户端没有退出，两轮叠在同一台服务器上，不是 1000 连接本身失败。有效结论用重叠消失之后的 `n1000d`。

## 复现时注意

- 正式地址用 `wss://nineee.xyz/danmu/ws`。临时隧道地址仍会在大约 200 连接处失败。
- 发送者太密时，上屏 p95 会先碰到 10 秒，CPU 仍远低于 60%。这是热队列积压，不是连接数不够。
- 1000 连接时出口大约 10 Mbit/s。公网带宽低于这个数时，延迟会先变差。
- `cleanup.py` 只删昵称以 `LT-` 开头的记录，不会动现场弹幕。不要用别的前缀发压测弹幕。
