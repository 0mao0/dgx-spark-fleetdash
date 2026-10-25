# dgx-spark-fleetdash — 多机 GPU 推理车队轻量监控

单页面板 + 企业微信告警，专为「几台常年跑推理服务的机器」设计。
零依赖（Python 标准库）、零外网、单次采集 0.13s CPU / 21 MiB 内存 / 0 GPU 占用。

覆盖能力：机器温度/功耗/SM 时钟/统一内存/磁盘/网口的历史曲线，**每个服务**的
显存、TPS、请求级 decode 速度、TTFT、排队、并发、请求速率，加失联/服务掉线/
"脑死"（端口在但 HTTP 不应答）实时告警与每日仪表盘日报。

## 架构

```
每台机器 collector.py ──30s──> 本机 sqlite ──/api/agg──┐
                                                       ▼
      观察者 edge (任意一台常开机器/VPS) ──拉取聚合──> 单页 HTML(60s 自刷新)
        │ 连续 4 次拉不通判失联(缓存续渲染+横幅)
        └─> 企业微信 webhook: 失联/恢复/停滞/服务掉线/脑死/时钟/硬件/日报图
DGX 侧 server.py --role slave 只出 /api/agg；master/edge 才渲染页面。
```

## 快速开始

### 1. 被监控机（每台）

```bash
git clone https://github.com/0mao0/dgx-spark-fleetdash.git && cd dgx-spark-fleetdash
./deploy.sh dgx <主机标识> master|slave [对端=URL ...]
# 例: ./deploy.sh dgx node2 slave
# 例: ./deploy.sh dgx node1 master "node2=http://100.x.x.x:9100"
```

装两个 systemd **user** 服务（collector + server :9100），脚本会提示
`loginctl enable-linger` 以支持未登录常驻。

服务清单**不用改代码**：写 `~/fleetdash/services.json`（格式见
`services.example.json`）。主机标识取 `--host`，缺省按 hostname 自动匹配。

### 2. 观察者 edge（一台即可，建议公网可达的 VPS）

```bash
./deploy.sh edge            # 或 PYTHON=python3.11 bash install-edge.sh ./server.py
```

peers 经环境变量或 `--peers` 指定；edge 支持 `/etc/fleetdash/peers.json`
热加载（mtime 轮询，加机器不重启）。告警推送需在 edge 机放
`/etc/fleetdash/webhook`（600，企业微信群机器人 URL）。

### 3. 电源操作与远程唤醒（可选）

页面「关机/重启/唤醒」按钮走 token 门控 + 企微留痕（聊天记录即操作台账）。
被监控机需 `power-sudoers` 进 `/etc/sudoers.d/`、token 放 `~/fleetdash/power.token`
(600)、`fleetdash-wol.service` 开机重装 WoL（magic packet 由同网段在线 peer 代发）。

## 服务分类与字段规范（统计口径）

服务清单每条 = `端口: (服务名, 类型, 模型显示名[, 真实后端端口])`。
`类型` 共 8 类；`./collector.py --print-schema` 可打印机器可读版。

所有服务的落库列结构**全局一致**（model 表 17 列），分类只决定哪些列有真数据，
取不到一律 NULL（页面显"—"，统计请跳过 NULL 而不是当 0）：

| 类别 | 含义 | 基础字段 | 引擎字段（有 /metrics 才有） |
|---|---|---|---|
| llm | 对话/生成引擎（vLLM、TensorFold） | 显存/存活/探活/请求速率 | tps·dec_tps·ttft_ms·queue_ms·req_run·req_wait·kv_use·mtp_acc |
| embed | 向量化（vLLM pooling） | 同上 | req_run·req_wait·kv_use |
| rerank | 重排序（vLLM pooling） | 同上 | req_run·req_wait·kv_use |
| tts | 语音合成 | 同上 | —（无 /metrics） |
| asr | 语音识别 | 同上 | — |
| parse | 文档解析 | 同上 | — |
| face | 人脸服务 | 同上 | — |
| proxy | 限流代理（探活+转发计数） | 同上 | — |

- 基础字段：`vram`（MiB，GB10 统一内存口径）、`alive`（端口/容器在）、
  `probe_ok`（真发 HTTP /health 的探活，脑死判据）、`act_rate`+`act_kind`
  （请求速率：vLLM `http_requests_total` 差分=精确，退化日志行数≈近似）。
- embed/rerank 归入「引擎类」：与 llm 同一套 /metrics 抓取路径，但 pooling
  引擎没有生成/时长直方图，tps、dec_tps、ttft、queue 恒 NULL 属正常。

## 文件

| 文件 | 作用 |
|---|---|
| collector.py | 采集器：sqlite 单写者，30s 对齐时间槽 |
| server.py | 渲染+聚合+告警状态机（role = master/slave/edge） |
| deploy.sh | 一键部署入口（dgx 子命令 / edge 子命令） |
| install.sh | DGX 侧 systemd user 服务安装（deploy.sh 内部调用） |
| install-edge.sh | VPS/观察者侧 root 系统服务安装（幂等可重跑升级） |
| services.example.json | 服务表外部配置样例 |
| peers.example.json | edge 热加载 peers 样例 |
| power-sudoers / fleetdash-wol.service | 电源操作与 WoL 配套 |
| render-test.py / report-drill.py | 日报出图演练（不发群）/ 手动推送 |
| test_*.py | 能耗积分、渲染、请求速率三组回归 |

## 安全

- 仓库永不存密钥：引擎 key 走 `~/fleetdash/vllm.key`(600) 或
  `FLEETDASH_VLLM_KEY`；推送 webhook 走 edge 机 `/etc/fleetdash/webhook`(600)；
  电源 token 走各机 `~/fleetdash/power.token`(600)。文件缺失 = 对应功能关闭。
- `/api/power` 连错 3 次锁 5 分钟；所有推送走磁盘闩锁防重启重推。
- 页面自身无鉴权，公网访问请挂在反代 + basic auth 之后。

## 已知边界

- GB10 显存只能走 `nvidia-smi --query-compute-apps`（`memory.used` 恒 N/A）。
- 容器名 ≠ 服务名时要在服务表 `containers` 里写别名，否则显存恒 0。
- 限流 proxy 的服务要写第 5 位真实后端端口，否则显存/指标归因到 0。
- 请求级指标是 30s 窗口均值（"这个窗口里的请求平均多快"），非逐请求散点。

## License

MIT
