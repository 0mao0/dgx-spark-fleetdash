#!/usr/bin/env python3
"""fleetdash collector — 采样本机 GPU/CPU 温度、SM 时钟、显存、各模型占用与 vLLM TPS,
写入 SQLite。

实测开销: 单次 0.13s CPU / 21 MiB RSS / 0 GPU。默认 30s 间隔 -> 单核 0.43%。
只用标准库, 无第三方依赖。设计为 systemd service 常驻。

定位: 只观察、只记录, 绝不干预机器 (与 vram_guardian / wecom_watchdog 同一哲学)。
告警推送由 wecom-watchdog 负责, 本采集器不发送任何通知。

    ./collector.py --once          # 采一次就退出 (调试)
    ./collector.py                 # 常驻循环
    ./collector.py --interval 60   # 覆盖采样间隔
    ./collector.py --print-schema  # 打印分类字段规范 (统计分析口径)

外部化配置(给非本车队的部署者): 服务表放 ~/fleetdash/services.json 即覆盖内置
SERVICES; 引擎 key 放 ~/fleetdash/vllm.key (600) 或 env FLEETDASH_VLLM_KEY。
"""
import argparse
import glob
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------- config
# 每台机器只需改这里。端口 -> (服务名, 类型, venv 提示不存在时为 None)
# 类型: llm/embed/rerank 走引擎 /metrics 拿 TPS (vLLM 或 TensorFold); asr/tts/face/parse 只有占用+健康。
SERVICES = {
    "4c47": {
        "hostname": "spark-4c47",
        # 8004/8005 是限流 proxy, 真正的 vLLM 在 18004/18005 -> 显存必须按真实端口归因,
        # 否则 embed/rerank 的 VRAM 永远读到 0 (proxy 进程不占显存)。
        # 10-04: 8000 生产 LLM 引擎由 vLLM(llm-api, 已停用留作回滚) 换为 TensorFold
        # (docker 容器 tf_qwen36_35b, systemd unit tf-qwen36)。svc 名即 journal 兜底
        # 活跃度查的 unit 名, 容器名≠unit 名, 显存另走 containers 别名。
        "ports": {
            8000: ("tf-qwen36", "llm", "qwen3.6-35b·TF"),
            8001: ("asr-api", "asr", None),
            # 8003 cosy-api 2026-10-25 卸载(停于 10-23, TTS 收敛到 dgx2 Ming :8101)
            8004: ("embed-api", "embed", "qwen3-embedding", 18004),
            8005: ("rerank-api", "rerank", "qwen3-reranker", 18005),
            8006: ("face-api", "face", None),
            # 8007 sage 转正服务 2026-10-25: 容器 sage-api(原 sage-tf-test :18892 测试实例),
            # restart=unless-stopped, 与服务同名免别名; 无 systemd 单元, 活体靠端口探。
            8007: ("sage-api", "llm", "sage-a1.5·TF"),
        },
        # TensorFold 容器实名≠服务名: 显存按容器名认领 + 进程榜反查都要走这层别名
        "containers": {"tf-qwen36": "tf_qwen36_35b"},
    },
    "53fa": {
        "hostname": "spark-53fa",
        "ports": {
            # fqtts×3 已全线下线（9-27 20:02 最后运行, 10-04 摘除 8012-8014）。
            # 10-23 起 TTS = Ming-3.0-TTS（vllm-omni :8094 + nginx 流式代理 :8101）;
            # cosy-api 已 stop+disable, 10-25 从监控摘除。
            8101: ("ming3", "tts", None),
            8007: ("mineru-api", "parse", None),
            8008: ("popo-api", "proxy", None),
            8010: ("popo-engine", "llm", "Popo"),
        },
    },
    "dbdd": {
        "hostname": "spark-dbdd",
        # 生产机: 仅 Qwen3.8-Flash-Next (Mia TP1 配方, docker 容器, :8888)
        "ports": {
            8888: ("qwen38-flash", "llm", "qwen3.8-flash-next"),
        },
        # 容器名≠服务名: 显存按「容器名==服务名」认领, 不加别名则 qwen38-flash 显存恒 0。
        # 10-05 起引擎为 TensorFold(docker 容器 qwen38-flash-next-tf, :8888);
        # 9-25 的 vLLM 容器 vllm-fn-tp1 已退役, 别名 10-11 起跟换。
        "containers": {"qwen38-flash": "qwen38-flash-next-tf"},
    },
}
# 元组第 5 位可选: 真实后端端口 (当监听端口是 proxy 时)。缺省 = 监听端口即后端。
def svc_spec(entry):
    """把配置元组规范化成 (svc, kind, model, listen_port_note, backend_port_or_None)。"""
    if len(entry) == 4:
        return entry[0], entry[1], entry[2], None, entry[3]
    return entry[0], entry[1], entry[2], None, None


# ---------------------------------------------------------------- kinds
# 服务分类与字段规范 (统计分析口径, 与 server.py 的 KIND_META 显示层同源)。
# 落库列结构全局统一: 任何 kind 的每个服务都写整行(model 表 17 列), 不存在"按类
# 换表"。本表只回答一个问题 —— 每类哪些字段有真数据、哪些必为 NULL:
#   base   = 所有类都有: vram(显存归因) / alive(端口或容器在) / probe_ok(真 HTTP 探活)
#            / act_rate + act_kind(活跃度: http 精确差分 或 log 行数兜底)
#   engine = 有 Prometheus /metrics 的引擎 (vLLM 或 TensorFold) 额外可填:
#            tps / dec_tps / ttft_ms / queue_ms / req_run / req_wait / kv_use / mtp_acc
# 约定: 取不到 = NULL (页面降级"—", 统计侧应跳过 NULL, 不得当 0 参与均值)。
KIND_SCHEMA = {
    "llm":    {"cn": "LLM 对话",   "engine": True,
               "fields": ["tps", "dec_tps", "ttft_ms", "queue_ms",
                          "req_run", "req_wait", "kv_use", "mtp_acc"]},
    "embed":  {"cn": "Embedding",  "engine": True,
               "fields": ["req_run", "req_wait", "kv_use"]},
    "rerank": {"cn": "Rerank", "engine": True,
               "fields": ["req_run", "req_wait", "kv_use"]},
    "tts":    {"cn": "语音合成",   "engine": False, "fields": []},
    "asr":    {"cn": "语音识别",   "engine": False, "fields": []},
    "face":   {"cn": "人脸",       "engine": False, "fields": []},
    "parse":  {"cn": "PDF 解析",   "engine": False, "fields": []},
    "proxy":  {"cn": "代理",       "engine": False, "fields": []},
}

DEFAULT_INTERVAL = 30


def _load_vllm_key():
    """本机引擎 API key, 只从本机取, 代码里永不存真值(公开仓安全且四端 md5 可全同)。
    优先顺序: env FLEETDASH_VLLM_KEY > ~/fleetdash/vllm.key(600) > ~/config.token > 空。
    空 key 只是不带 Authorization 头: /metrics 与 /health 均无鉴权, 不受影响。"""
    v = os.environ.get("FLEETDASH_VLLM_KEY")
    if v:
        return v.strip()
    for p in (os.path.expanduser("~/fleetdash/vllm.key"),
              os.path.expanduser("~/config.token")):
        try:
            with open(p) as f:
                return f.read().strip()
        except OSError:
            continue
    return ""


VLLM_KEY = _load_vllm_key()   # 仅用于 127.0.0.1 本机查询

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS sample (
    ts        INTEGER PRIMARY KEY,
    gpu_temp  REAL, gpu_util REAL, gpu_pw REAL,
    cpu_temp  REAL, cpu_max  REAL,
    mem_used  REAL, mem_total REAL,
    load1     REAL,
    disk_used REAL, disk_total REAL,
    net_rx_mbs REAL, net_tx_mbs REAL
);
CREATE TABLE IF NOT EXISTS model (
    ts     INTEGER NOT NULL,
    svc    TEXT    NOT NULL,
    kind   TEXT,
    model  TEXT,
    vram   REAL,
    alive  INTEGER,
    tps    REAL,
    req_run INTEGER,
    req_wait INTEGER,
    kv_use  REAL,
    act_rate REAL, act_kind TEXT,
    PRIMARY KEY (ts, svc)
);
CREATE INDEX IF NOT EXISTS idx_model_svc_ts ON model(svc, ts);
CREATE INDEX IF NOT EXISTS idx_sample_ts ON sample(ts);
"""

# v2 增列: GB10 特有的两种"吞吐塌陷"故障(不触发 OOM, 只看显存看不出来)。
#   sm_mhz        — SM 时钟。util 高但时钟掉到 1200 以下 = 时钟卡死, 需拔电冷启。
#   power_cap_us  — SW Power Capping 累计微秒(单调递增), 差分后判断"持续封顶"。
#   cap_delta_us  — 单次采样增量; 跨重启(计数归零)时置 NULL 而非负数。
# 用 ALTER TABLE 幂等加列, 老库老行新列为 NULL, 页面按 NULL 降级。
MIGRATIONS = [
    "ALTER TABLE sample ADD COLUMN sm_mhz REAL",
    "ALTER TABLE sample ADD COLUMN power_cap_us INTEGER",
    "ALTER TABLE sample ADD COLUMN cap_delta_us INTEGER",
    # v3 增列 (2026-09-14): 磁盘 / 网络吞吐 / 每服务活跃度。
    #   act_rate — 请求速率(次/秒): vLLM 服务取 http_requests_total 差分(act_kind=http,
    #              精确), 其余服务退化为日志行数差分(act_kind=log, 近似"活跃度")。
    #   net_*    — 物理网卡字节差分/秒(MB/s), 已剔除 lo/tailscale/docker 虚接口,
    #              避免与 tailnet 流量重复计数。
    "ALTER TABLE sample ADD COLUMN disk_used REAL",
    "ALTER TABLE sample ADD COLUMN disk_total REAL",
    "ALTER TABLE sample ADD COLUMN net_rx_mbs REAL",
    "ALTER TABLE sample ADD COLUMN net_tx_mbs REAL",
    "ALTER TABLE model ADD COLUMN act_rate REAL",
    "ALTER TABLE model ADD COLUMN act_kind TEXT",
    # v4 增列 (2026-09-15): 请求级速度指标。vLLM 直方图 _sum/_count 相邻采样做差,
    # Δsum/Δcount = 该窗口内所有已完成请求的"每请求平均"值(token 加权), 非逐请求散点。
    #   dec_tps  — Δgeneration_tokens_sum / Δdecode_time_sum: 平均 decode tok/s
    #   ttft_ms  — Δtime_to_first_token_sum / Δcount * 1000: 平均首 token 延迟
    #   queue_ms — Δrequest_queue_time_sum / Δcount * 1000: 平均排队时长(诊断拥堵)
    # 窗口内无完成请求(Δcount=0)时不出数(NULL), 图表上留空档。
    "ALTER TABLE model ADD COLUMN dec_tps REAL",
    "ALTER TABLE model ADD COLUMN ttft_ms REAL",
    "ALTER TABLE model ADD COLUMN queue_ms REAL",
    # v5 增列 (2026-09-15): 机器运行时长(秒, /proc/uptime), 日报显示"运行 X 天 Y 小时"。
    #   存原始秒而非格式化串: 渲染层可按需展示, 历史行也能事后换算。
    "ALTER TABLE sample ADD COLUMN uptime_s REAL",
    # v6 增列 (2026-09-15): 真 HTTP 探活 + 时钟巡检。
    #   probe_ok  — 对该服务真发 HTTP: GET /health, 404 时退回 /;
    #               连接拒绝/超时/状态>=500 = 0(死或脑死), 其余 = 1。
    #               与 alive(端口监听) 区分: 端口在但事件循环卡死的"脑死"只有探活能抓。
    #   clock_off_ms — 本周期内 墙钟前进量 - 单调钟前进量 (ms)。正常运行 |x|<100;
    #               NTP 硬跳/改时间/时钟卡死会产生秒级偏差。
    #   ntp_sync  — timedatectl NTPSynchronized (1/0, 10 分钟刷新一次)。
    "ALTER TABLE model ADD COLUMN probe_ok INTEGER",
    "ALTER TABLE sample ADD COLUMN clock_off_ms REAL",
    "ALTER TABLE sample ADD COLUMN ntp_sync INTEGER",
    # v7 增列 (2026-09-16): 周期累计量(月能耗/当日 token)。本机没有能量计数器
    # (hwmon 无 energy 文件, nvidia-smi 无 energy.total 字段), 只能由 collector
    # 按 rate·Δt 积分; 累计值持久化在 meta 表里(重启不丢)。
    #   month_wh — 截至该采样的当月 Wh (电费)
    #   day_tok  — 截至该采样的当日已生成 token 数 (Σ 各 vLLM 服务 tps × Δt)
    "ALTER TABLE sample ADD COLUMN month_wh REAL",
    "ALTER TABLE sample ADD COLUMN day_tok REAL",
    "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)",
    # 突变归因 (2026-09-23): server.py 渲染时按 ts 范围查活跃服务, 无此索引会全表扫
    "CREATE INDEX IF NOT EXISTS idx_model_ts ON model(ts)",
    # v8 增列 (2026-10-06): MTP 接受率(%) = Δ接受草稿 / Δ草稿。TF 是
    # mtp_accepted/mtp_drafted_total, vLLM 是 spec_decode_num_{accepted,draft}_tokens_total;
    # 窗口内无草稿(Δdrafted=0)不出数。同版补齐 TF 的 dec_tps: 引擎无 decode 直方图,
    # 用 e2e_sum − ttft_sum 反推(两个口径同为"已完成请求", count 一致才出数)。
    "ALTER TABLE model ADD COLUMN mtp_acc REAL",
]


def sh(cmd, timeout=8):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ""


def _auth_headers():
    return {"Authorization": f"Bearer {VLLM_KEY}"} if VLLM_KEY else {}


def http_get(url, timeout=3):
    try:
        req = urllib.request.Request(url, headers=_auth_headers())
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


def http_code(url, timeout=3):
    """HTTP 状态码; 连接失败/超时返回 None。4xx 表示服务器在应答, 不是故障。"""
    try:
        req = urllib.request.Request(url, headers=_auth_headers())
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return None


def probe_svc(listen_port, backend):
    """真探活: HTTP 服务器是否真的在应答、引擎是否还活着。

    /health 优先(实测: vLLM 系与 mineru/popo 返回 200; asr/cosy/face 无该路由
    返回 404 -> 退回 /, 任何 <500 都说明 HTTP 栈活着)。>=500 = 引擎死(vLLM
    /health 会转 500); 超时 = 事件循环卡死(脑死), 与端口在不在无关。
    proxy 入口(监听 != 后端, 如 8004->18004)要求两个端口都应答, 否则算死。
    单请求超时 3s: 全部服务脑死挂满的极端情况 6 服务 x 2 请求 x 3s 会拖过
    30s 周期, collector 会跳槽而不是补跑, 由 edge 的 STALE 兜底告警。
    """
    ports = [backend] if backend == listen_port else [backend, listen_port]
    for p in ports:
        code = http_code(f"http://127.0.0.1:{p}/health")
        if code is None:
            return 0
        if code == 404:
            code = http_code(f"http://127.0.0.1:{p}/")
            if code is None:
                return 0
        if code >= 500:
            return 0
    return 1


# ---------------------------------------------------------------- sensors
def gpu_stats():
    """返回 (temp, util, power, sm_mhz)。

    GB10 上 memory.* 恒为 N/A, 显存只能走 compute-apps。
    clocks.sm 与其它三项合并在同一条 --query-gpu 里, 不额外增加 nvidia-smi 调用。
    """
    out = sh("nvidia-smi --query-gpu=temperature.gpu,utilization.gpu,power.draw,clocks.sm "
             "--format=csv,noheader,nounits")
    if not out:
        return None, None, None, None
    parts = [p.strip() for p in out.splitlines()[0].split(",")]
    def num(i):
        try:
            return float(parts[i])
        except (ValueError, IndexError):
            return None
    return num(0), num(1), num(2), num(3)


def power_cap_us():
    """SW Power Capping 累计微秒。GB10 上该计数单调递增, 差分即"被封顶的时长"。

    这是社区 t/370304 描述的病理: 功率封顶持续累积时吞吐会塌陷, 但显存/温度都正常,
    光看显存完全发现不了。解析方式与 wecom-watchdog 一致。
    """
    out = sh("nvidia-smi -q -d PERFORMANCE", timeout=6)
    for ln in out.splitlines():
        if "SW Power Capping" in ln and ":" in ln:
            try:
                return int(float(ln.split(":")[-1].replace("us", "").strip()))
            except ValueError:
                return None
    return None


def gpu_procs():
    """{pid: used_mib}。GB10 上这是唯一可靠的显存来源。"""
    out = sh("nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader,nounits")
    procs = {}
    for line in out.splitlines():
        p = [x.strip() for x in line.split(",")]
        if len(p) >= 2:
            try:
                procs[int(p[0])] = float(p[1])
            except ValueError:
                pass
    return procs


def cpu_temps():
    """DGX Spark 上 acpitz 有 7 路, 取平均与最高。"""
    vals = []
    for z in glob.glob("/sys/class/thermal/thermal_zone*/temp"):
        try:
            with open(z) as f:
                v = float(f.read().strip())
            if v > 0:
                vals.append(v / 1000.0)
        except Exception:
            pass
    if not vals:
        return None, None
    return round(sum(vals) / len(vals), 1), round(max(vals), 1)


def mem_info():
    total = avail = None
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    total = float(line.split()[1]) / 1048576
                elif line.startswith("MemAvailable:"):
                    avail = float(line.split()[1]) / 1048576
    except Exception:
        pass
    if total is None or avail is None:
        return None, None
    return round(total - avail, 1), round(total, 1)


def load1():
    try:
        with open("/proc/loadavg") as f:
            return float(f.read().split()[0])
    except Exception:
        return None


def uptime_s():
    """机器连续运行秒数 (/proc/uptime 第一字段)。"""
    try:
        with open("/proc/uptime") as f:
            return round(float(f.read().split()[0]), 1)
    except Exception:
        return None


def disk_info():
    """根分区 used/total (GiB)。df 不可用时返回 None,None。"""
    out = sh("df -B1 --output=size,used / | tail -1")
    try:
        size, used = [float(x) for x in out.split()[:2]]
        return round(used / 1073741824, 1), round(size / 1073741824, 1)
    except (ValueError, IndexError):
        return None, None


# 虚拟/隧道接口前缀: 不计入"物理网络吞吐"(tailscale 流量会与其物理承载重复计数)
_NET_SKIP_PREFIXES = ("lo", "tailscale", "docker", "br-", "veth", "virbr",
                      "tap", "kube", "wg", "zt")


def net_bytes():
    """(rx_bytes, tx_bytes), 仅物理网卡。无数据返回 None,None。"""
    rx = tx = None
    try:
        with open("/proc/net/dev") as f:
            for line in f:
                name, sep, rest = line.partition(":")
                if not sep:
                    continue
                name = name.strip().lower()
                if name.startswith(_NET_SKIP_PREFIXES):
                    continue
                cols = rest.split()
                r_, t_ = int(cols[0]), int(cols[8])
                rx = (rx or 0) + r_
                tx = (tx or 0) + t_
    except Exception:
        return None, None
    return rx, tx


# ------------------------------------------------------- port -> pid -> svc
def listening_pids():
    """{port: pid}。无 ss 权限时退化到 /proc/net/tcp 解析。"""
    out = sh("ss -ltnp 2>/dev/null")
    found = {}
    for line in out.splitlines():
        m = re.search(r":(\d+)\s", line)
        p = re.search(r"pid=(\d+)", line)
        if m and p:
            found[int(m.group(1))] = int(p.group(1))
    return found


def pid_service_map(ports):
    """把端口对应的 pid 归因到某条 vLLM 进程及其 EngineCore 子进程。

    vLLM 结构: 父进程 (api server, 监听端口) -> EngineCore 子进程 (持有显存)。
    两者显存都在 nvidia-smi 里, 需一起算, 否则会漏掉一半。
    """
    listeners = listening_pids()
    result = {}   # svc -> {pids:set, vram:float, alive:bool}
    procs = gpu_procs()

    for port, entry in ports.items():
        svc, kind, _model, _, backend = svc_spec(entry)
        # 显存归因用真实后端端口 (proxy 端口不占显存)
        pid = listeners.get(backend or port)
        info = {"pids": set(), "vram": 0.0, "alive": listeners.get(port) is not None}
        if pid:
            info["alive"] = True
            info["pids"].add(pid)
            # 找 EngineCore 子进程 (它的 ppid == 父进程, 且占大显存)
            childs = sh(f"pgrep -P {pid} 2>/dev/null")
            for c in childs.split():
                info["pids"].add(int(c))
        for p in list(info["pids"]):
            if p in procs:
                info["vram"] += procs[p]
        result[svc] = info
    return result, procs


def docker_attribution():
    """53fa 场景: 显存挂在 docker 里, 端口不一定监听。用 cgroup 反查容器名。

    返回 {container_name: total_vram_mib}、已被容器认领的 pid 集合、
    {pid: vram_mib}, 以及 {pid: 容器名}(供进程 top 榜归因服务)。
    """
    procs = gpu_procs()
    by_container = {}
    claimed = set()
    pid2cont = {}
    for pid, mem in procs.items():
        try:
            with open(f"/proc/{pid}/cgroup") as f:
                cg = f.read()
        except Exception:
            continue
        m = re.search(r"[0-9a-f]{64}", cg)
        if not m:
            continue
        cid = m.group(0)
        name = sh(f"docker inspect --format '{{{{.Name}}}}' {cid} 2>/dev/null", timeout=5)
        if name:
            name = name.lstrip("/")
            by_container[name] = by_container.get(name, 0.0) + mem
            claimed.add(pid)
            pid2cont[pid] = name
    return by_container, claimed, procs, pid2cont


def proc_top(vram_map, p2svc, limit=12):
    """进程 top 榜: GB10 统一内存下 显存+常驻页 同池, 按两者之和排序。

    vram_map: {pid: mib}(nvidia-smi compute-apps); p2svc: {pid: 服务名}。
    查询瞬间进程已死(读不到 /proc)则跳过, 不进榜。
    """
    out = []
    for pid in sorted(set(vram_map) | set(p2svc)):
        name = rss = None
        try:
            with open(f"/proc/{pid}/comm") as f:
                name = f.read().strip()
        except Exception:
            pass
        try:
            with open(f"/proc/{pid}/status") as f:
                for ln in f:
                    if ln.startswith("VmRSS:"):
                        rss = float(ln.split()[1]) / 1024.0   # kB -> MiB
                        break
        except Exception:
            pass
        if name is None and rss is None:
            continue
        vram = vram_map.get(pid, 0.0)
        out.append({"pid": pid, "name": name or "?",
                    "svc": p2svc.get(pid) or "",
                    "vram": round(vram, 1),
                    "rss": round(rss, 1) if rss is not None else None})
    out.sort(key=lambda p: -((p["vram"] or 0.0) + (p["rss"] or 0.0)))
    return out[:limit]


# ---------------------------------------------------------------- vLLM TPS
def vllm_metrics(port):
    """解析 vLLM /metrics 里我们要的标量。"""
    txt = http_get(f"http://127.0.0.1:{port}/metrics")
    if not txt:
        return None
    out = {}
    http_total = None
    for line in txt.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        # vLLM 内置 FastAPI instrumentator 的 HTTP 计数: 精确请求数。
        # 要排除 /metrics 自身(我们每 30s 拉一次会虚增)与健康探针端点。
        if line.startswith("http_requests_total{"):
            handler = re.search(r'handler="([^"]*)"', line)
            try:
                val = float(line.rsplit(None, 1)[1])
            except (ValueError, IndexError):
                continue
            h = handler.group(1) if handler else ""
            # handler="none" = 未匹配路由的探测 404(监控噪声), 同样排除
            if (h in ("", "none", "/metrics", "/health", "/healthz", "/stats")
                    or h.startswith("/v1/models")):
                continue
            http_total = (http_total or 0.0) + val
            continue
        # 两家引擎共用一张表: vLLM 是 vllm:* (都带标签块), TensorFold 是
        # tensorfold:* (标签可选, 如 kv 带 stream="N"). TensorFold 没有
        # decode/queue 时长直方图, 对应字段自然缺省 (卡片显示 —, 不是 0)。
        m = re.match(r"^(vllm:|tensorfold:)([\w:]+)(?:\{[^}]*\})?\s+([\d.eE+\-]+)$", line)
        if not m:
            continue
        eng, name, val = m.group(1)[:-1], m.group(2), m.group(3)
        try:
            v = float(val)
        except ValueError:
            continue
        if eng == "tensorfold":
            if name == "generation_tokens_total":
                out["gen_tokens"] = v
            elif name == "num_requests_running":
                out["running"] = v
            elif name == "num_requests_waiting":
                out["waiting"] = v
            elif name == "kv_cache_usage_perc":
                # 每条 stream 一条序列, 面板 KV 水位取最满的一条
                out["kv"] = max(out.get("kv", 0.0), v)
            elif name == "prompt_tokens_total":
                out["prompt_tokens"] = v
            elif name == "time_to_first_token_seconds_sum":
                out["ttft_sum"] = v
            elif name == "time_to_first_token_seconds_count":
                out["ttft_cnt"] = v
            elif name == "e2e_request_latency_seconds_count":
                # TF 无 http_requests_total, 用已完成请求计数做精确请求速率
                out["http_reqs"] = v
            elif name == "e2e_request_latency_seconds_sum":
                out["e2e_sum"] = v
            elif name == "mtp_accepted_total":
                out["mtp_acc"] = v
            elif name == "mtp_drafted_total":
                out["mtp_drf"] = v
            continue
        name = f"vllm:{name}"
        if name == "vllm:generation_tokens_total":
            out["gen_tokens"] = v
        elif name == "vllm:num_requests_running":
            out["running"] = v
        elif name == "vllm:num_requests_waiting":
            out["waiting"] = v
        elif name == "vllm:kv_cache_usage_perc":
            out["kv"] = v
        elif name == "vllm:prompt_tokens_total":
            out["prompt_tokens"] = v
        elif name == "vllm:request_generation_tokens_sum":
            out["gen_tok_sum"] = v
        elif name == "vllm:request_decode_time_seconds_sum":
            out["dec_sum"] = v
        elif name == "vllm:time_to_first_token_seconds_sum":
            out["ttft_sum"] = v
        elif name == "vllm:time_to_first_token_seconds_count":
            out["ttft_cnt"] = v
        elif name == "vllm:request_queue_time_seconds_sum":
            out["q_sum"] = v
        elif name == "vllm:request_queue_time_seconds_count":
            out["q_cnt"] = v
        elif name == "vllm:spec_decode_num_accepted_tokens_total":
            out["mtp_acc"] = v
        elif name == "vllm:spec_decode_num_draft_tokens_total":
            out["mtp_drf"] = v
    if http_total is not None:
        out["http_reqs"] = http_total
    return out or None


class TpsTracker:
    """TPS = Δgeneration_tokens / Δt。首次采样建立基线, 不出数。
    http_rps = Δhttp_requests_total / Δt (vLLM 有 HTTP 计数时给精确请求速率)。"""

    def __init__(self):
        self.prev = {}     # svc -> (ts, gen_tokens)
        self.prev_http = {}  # svc -> (ts, http_reqs)
        self.prev_ratio = {}  # (svc, key) -> (ts, num, den) 直方图均值差分基线
        self.prev_tf = {}  # svc -> (ts, gen, e2e_sum, e2e_cnt, ttft_sum, ttft_cnt)

    def _tf_decode(self, svc, now, gen, e2s, e2c, tts, ttc):
        """TF 引擎没有 decode 时长直方图, 但 e2e 与 TTFT 都是"已完成请求"口径:
        decode 时长 = e2e − TTFT(逐请求恒等, 窗口内求和仍成立)。
        dec_tps = Δgeneration_tokens / (Δe2e_sum − Δttft_sum)。
        仅当同窗完成数一致(Δe2e_cnt == Δttft_cnt > 0)且分母 > 0 时出数。"""
        if gen is None or None in (e2s, e2c, tts, ttc):
            return None
        p = self.prev_tf.get(svc)
        r = None
        if p:
            pts, pgen, pe2s, pe2c, ptts, pttc = p
            n = e2c - pe2c
            den = (e2s - pe2s) - (tts - ptts)
            if (now - pts >= 3 and gen >= pgen and e2c >= pe2c and ttc >= pttc
                    and n > 0 and n == ttc - pttc and den > 0):
                r = round((gen - pgen) / den, 2)
        self.prev_tf[svc] = (now, gen, e2s, e2c, tts, ttc)
        return r

    def _ratio(self, svc, key, now, num, den, scale=1.0):
        """Δnum/Δden: 直方图 _sum/_count 差分比值 = 本窗口已完成请求的每请求平均值。
        首次建基线不出数; 计数器回退、窗口过短、窗口内无请求(Δden=0)均不出数。"""
        if num is None or den is None:
            return None
        p = self.prev_ratio.get((svc, key))
        r = None
        if p:
            pts, pnum, pden = p
            if now - pts >= 3 and num >= pnum and den >= pden and den > pden:
                r = round((num - pnum) / (den - pden) * scale, 2)
        self.prev_ratio[(svc, key)] = (now, num, den)
        return r

    def compute(self, svc, port, now):
        m = vllm_metrics(port)
        if not m:
            return None
        tps = None
        cur = m.get("gen_tokens")
        if cur is not None:
            p = self.prev.get(svc)
            if p and now > p[0] and cur >= p[1]:
                dt = now - p[0]
                if dt >= 3:                       # 太短的窗口噪声大
                    tps = round((cur - p[1]) / dt, 2)
            if cur is not None:
                self.prev[svc] = (now, cur)
        rps = None
        curh = m.get("http_reqs")
        if curh is not None:
            p = self.prev_http.get(svc)
            if p and now > p[0] and curh >= p[1]:
                dt = now - p[0]
                if dt >= 3:
                    rps = round((curh - p[1]) / dt, 3)
            self.prev_http[svc] = (now, curh)
        # 请求级 decode 速度: vLLM 有 decode 直方图直接差分; TF 没有则用
        # e2e−TTFT 恒等反推(见 _tf_decode)。e2e 计数即 http_reqs(同一计数器,
        # TF 分支已映射), 不必重复提取。
        dec = self._ratio(svc, "dec", now, m.get("gen_tok_sum"), m.get("dec_sum"))
        if dec is None:
            dec = self._tf_decode(svc, now, cur, m.get("e2e_sum"), m.get("http_reqs"),
                                  m.get("ttft_sum"), m.get("ttft_cnt"))
        return {
            "tps": tps,
            "http_rps": rps,
            "running": m.get("running"),
            "waiting": m.get("waiting"),
            "kv": m.get("kv"),
            "dec_tps": dec,
            "ttft_ms": self._ratio(svc, "ttft", now,
                                   m.get("ttft_sum"), m.get("ttft_cnt"), 1000.0),
            "queue_ms": self._ratio(svc, "queue", now,
                                    m.get("q_sum"), m.get("q_cnt"), 1000.0),
            "mtp_acc": self._ratio(svc, "mtp", now,
                                   m.get("mtp_acc"), m.get("mtp_drf"), 100.0),
        }


# ---------------------------------------------------------------- collect
class CapTracker:
    """SW Power Capping 计数器差分。计数跨重启会归零, 此时不出数而不是给负数。"""

    def __init__(self):
        self.prev = None
        self.accum_today = 0
        self.day = None

    def delta(self, cur, now):
        d = None
        if cur is not None:
            if self.prev is not None and cur >= self.prev:
                d = cur - self.prev
                day = time.strftime("%Y-%m-%d", time.localtime(now))
                if day != self.day:           # 跨天重置累计
                    self.day = day
                    self.accum_today = 0
                self.accum_today += d
            self.prev = cur
        return d


class NetTracker:
    """网卡字节计数差分 -> MB/s。接口增减(重启/插拔)导致计数回退时不出数。"""

    def __init__(self):
        self.prev = None

    def rate(self, now):
        rx, tx = net_bytes()
        if rx is None:
            return None, None
        out = (None, None)
        if self.prev is not None:
            (prx, ptx), pts = self.prev
            dt = now - pts
            if dt >= 3 and rx >= prx and tx >= ptx:
                out = (round((rx - prx) / dt / 1048576, 3),
                       round((tx - ptx) / dt / 1048576, 3))
        self.prev = ((rx, tx), now)
        return out


class ClockTracker:
    """墙钟与单调钟的周期间偏差。单调钟永不跳变; 墙钟比单调钟走快/走慢
    = NTP 硬跳、手动改时间或时钟异常。正常抖动 |x|<100ms, 秒级即异常
    (edge 侧判阈值告警)。NTPSynchronized 查询有进程开销, 10 分钟缓存。"""

    def __init__(self):
        self.prev = None
        self._ntp = None
        self._ntp_at = 0.0

    def drift_ms(self):
        mono = time.clock_gettime(time.CLOCK_MONOTONIC)
        wall = time.time()
        out = None
        if self.prev:
            pm, pw = self.prev
            out = round(((wall - pw) - (mono - pm)) * 1000.0, 1)
        self.prev = (mono, wall)
        return out

    def ntp_synced(self):
        if self._ntp is None or time.time() - self._ntp_at > 600:
            v = sh("timedatectl show -p NTPSynchronized --value").strip()
            self._ntp = 1 if v == "yes" else (0 if v == "no" else None)
            self._ntp_at = time.time()
        return self._ntp


class LogRateTracker:
    """非 vLLM 服务的"活跃度" = 日志行数/秒(近似)。
    4c47 裸金属服务: journalctl -u <unit>; 53fa docker 服务: docker logs --since。
    注意: journalctl 对非属主 unit 需要 systemd-journal 组; 没权限时得到 0 行,
    与"空闲"不可区分 —— 部署时给 faye 补组, 并在演练中用 busy 服务验证非 0。"""

    def __init__(self, use_docker):
        self.use_docker = use_docker
        self.prev_ts = {}
        self.containers = None

    def _docker_names(self):
        if self.containers is None:
            out = sh("docker ps --format '{{.Names}}' 2>/dev/null")
            self.containers = set(out.split()) if out else set()
        return self.containers

    def rate(self, svc, now):
        p = self.prev_ts.get(svc)
        self.prev_ts[svc] = now
        if p is None:
            return None
        dt = now - p
        if dt < 3:
            return None
        if self.use_docker:
            if svc not in self._docker_names():
                return None
            iso = time.strftime("%Y-%m-%dT%H:%M:%S.000000000Z", time.gmtime(p))
            iso_end = time.strftime("%Y-%m-%dT%H:%M:%S.000000000Z", time.gmtime(now))
            out = sh(f"docker logs --since {iso} --until {iso_end} {svc} 2>&1 "
                     "| wc -l", timeout=10)
        else:
            # --since @<epoch>: journalctl 接受 unix 时间; -q --no-pager 保证纯行数可数
            out = sh(f"journalctl -u {svc} --since @{p} --until @{now} -q --no-pager "
                     "2>/dev/null | wc -l", timeout=10)
        try:
            n = int(out.split()[-1])
            return round(n / dt, 3)
        except (ValueError, IndexError):
            return None


class PeriodAccum:
    """周期累计量(月能耗 Wh / 当日 token), 用 rate·Δt 积分。

    - 机器无能量计数器, 只能自己攒; Δt 取相邻两次采样差, 采样对齐整秒槽,
      30s 周期误差可忽略。Δt>MAX_DT(5min) 视为采集器停过/机器关机, 那段无法
      佐证实际量, 不计入(宁可少算, 不外推); 重启后首采只记基线。
    - 累计值与上次 ts 落 meta 表(pfx / pfx_v / pfx_last), 进程重启不丢。
    - 翻篇(fmt 变了)把上一周期存成 prev_pfx / prev_pfx_v(留一档历史)再清零。
    """
    MAX_DT = 300

    def __init__(self, con, pfx, fmt, now):
        self.pfx, self.fmt = pfx, fmt
        self.period = self._get(con, pfx) or time.strftime(fmt, time.localtime(now))
        try:
            self.val = float(self._get(con, pfx + "_v") or 0.0)
        except (TypeError, ValueError):
            self.val = 0.0
        try:
            self.last = int(self._get(con, pfx + "_last") or 0)
        except (TypeError, ValueError):
            self.last = 0
        self.rolled = None

    @staticmethod
    def _get(con, k):
        r = con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return r[0] if r else None

    def add_rate(self, rate, now, scale=1.0):
        if rate is not None and self.last:
            dt = now - self.last
            if 0 < dt <= self.MAX_DT:
                self.val += rate * dt * scale
        p = time.strftime(self.fmt, time.localtime(now))
        if p != self.period:
            self.rolled = (self.period, self.val)
            self.period, self.val = p, 0.0
        self.last = now

    def persist(self, con):
        kv = [(self.pfx, self.period), (self.pfx + "_v", f"{self.val:.3f}"),
              (self.pfx + "_last", str(self.last))]
        if self.rolled:
            kv += [("prev_" + self.pfx, self.rolled[0]),
                   ("prev_" + self.pfx + "_v", f"{self.rolled[1]:.3f}")]
            self.rolled = None
        con.executemany("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", kv)


def collect(cfg, tracker, cap_tracker, net_tracker, log_tracker, clock_tracker, now):
    gpu_temp, gpu_util, gpu_pw, sm_mhz = gpu_stats()
    cpu_avg, cpu_max = cpu_temps()
    mem_used, mem_total = mem_info()
    cap = power_cap_us()
    cap_d = cap_tracker.delta(cap, now)
    disk_used, disk_total = disk_info()
    net_rx, net_tx = net_tracker.rate(now)

    sample = {
        "ts": now, "gpu_temp": gpu_temp, "gpu_util": gpu_util, "gpu_pw": gpu_pw,
        "cpu_temp": cpu_avg, "cpu_max": cpu_max,
        "mem_used": mem_used, "mem_total": mem_total, "load1": load1(),
        "sm_mhz": sm_mhz, "power_cap_us": cap, "cap_delta_us": cap_d,
        "disk_used": disk_used, "disk_total": disk_total,
        "net_rx_mbs": net_rx, "net_tx_mbs": net_tx,
        "uptime_s": uptime_s(),
        "clock_off_ms": clock_tracker.drift_ms(),
        "ntp_sync": clock_tracker.ntp_synced(),
    }

    svc_info, vram_map = pid_service_map(cfg["ports"])

    # 53fa 场景: 显存挂在容器里, 端口由 docker-proxy 占着。这里只为"端口在但
    # 显存归因不到"的服务按容器名补 vram。注意: 绝不能无条件置 alive=True ——
    # 那会让 systemctl stop 后的服务永远显示存活(v6 演练时发现的历史 bug),
    # 只有真找到同名容器才算"端口没监听但容器在跑"。
    by_container, claimed, _, pid2cont = docker_attribution()
    alias = cfg.get("containers", {})
    for port, entry in cfg["ports"].items():
        svc, kind, _model, _, backend = svc_spec(entry)
        info = svc_info.get(svc, {})
        if info.get("alive") and info.get("vram", 0) > 0:
            continue
        cvram = by_container.get(alias.get(svc, svc))
        if info.get("alive") or cvram is not None:
            cur = svc_info.setdefault(svc, {"pids": set(), "vram": 0.0, "alive": False})
            if cvram is not None:
                cur["alive"] = True
                cur["vram"] = cvram

    rows = []
    for port, entry in cfg["ports"].items():
        svc, kind, model_hint, _, backend = svc_spec(entry)
        info = svc_info.get(svc, {"vram": 0.0, "alive": False})
        # 探活是最强存活信号: docker DNAT 端口没有宿主 socket、proxy 不占显存,
        # 只有真发 HTTP 能确认它在应答。alive = 端口/容器在 或 探活通过;
        # 服务真停时两者同时为 0。脑死 = alive=1 且 probe_ok=0 (edge 侧判)。
        probe_ok = probe_svc(port, backend or port)
        alive = bool(info.get("alive")) or probe_ok == 1
        stats = {}
        if kind in ("llm", "embed", "rerank"):
            stats = tracker.compute(svc, backend or port, now) or {}
        # 活跃度: vLLM 有 HTTP 计数器时用精确请求速率, 否则日志行数兜底
        act, act_kind = stats.get("http_rps"), "http"
        if act is None:
            act, act_kind = log_tracker.rate(svc, now), "log"
        rows.append({
            "ts": now, "svc": svc, "kind": kind,
            "model": model_hint, "vram": round(info.get("vram", 0.0), 1),
            "alive": 1 if alive else 0,
            "tps": stats.get("tps"), "req_run": stats.get("running"),
            "req_wait": stats.get("waiting"), "kv_use": stats.get("kv"),
            "dec_tps": stats.get("dec_tps"), "ttft_ms": stats.get("ttft_ms"),
            "queue_ms": stats.get("queue_ms"),
            "mtp_acc": stats.get("mtp_acc"),
            "act_rate": act, "act_kind": act_kind if act is not None else None,
            "probe_ok": probe_ok,
        })
    # 进程 top 榜: 端口 pid + EngineCore 子进程归因到服务, docker 容器内进程
    # 用容器名归因(容器名==服务名, 或经 containers 别名反查回服务名);
    # 都归不上的进榜显示空。
    p2svc = {}
    for svc, info in svc_info.items():
        for pid in info.get("pids") or ():
            p2svc[pid] = svc
    rev_alias = {v: k for k, v in alias.items()}
    for pid, cname in pid2cont.items():
        sname = rev_alias.get(cname, cname)
        p2svc.setdefault(pid, sname if sname in svc_info else "")
    return sample, rows, proc_top(vram_map, p2svc)


def ensure_db(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript(DB_SCHEMA)
    # 幂等加列: 已存在会抛 OperationalError, 忽略即可。老行新列为 NULL。
    for sql in MIGRATIONS:
        try:
            con.execute(sql)
        except sqlite3.OperationalError:
            pass
    con.execute("PRAGMA journal_mode=WAL")
    con.commit()
    return con


def _proc_btime():
    """/proc/stat 的 btime(开机时刻, epoch 秒), 每次现读——机器重启后基线自动跟换。"""
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("btime "):
                return int(line.split()[1])
    raise OSError("btime not found")


def _proc_starttime(pid):
    """进程诞生时刻(epoch 秒)。= /proc/stat btime + /proc/<pid>/stat 第 22 列
    (starttime, 自开机秒数)。comm 字段含括号/空格时按最后一个 ") " 切分防偏移;
    字段序号按 man proc(5) fields_after_state 第 18 项。"""
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(") ", 1)[1].split()
    return _proc_btime() + int(fields[18]) / os.sysconf("SC_CLK_TCK")


def _lock_owner_alive(lock_path):
    """锁文件里的 pid 是否仍是写下它的那个进程。

    只 os.kill(pid, 0) 判活不够: PID 会被系统回收后分给无关进程
    (2026-10-11 4c47 实证——pipewire-pulse 复用了崩溃 collector 的 pid 2264,
    新 collector 每 10s 起一次都被"已有 collector 在运行"挡回, crash-loop 数小时)。

    验法: 锁内容 = "<pid> <持锁者诞生时刻>"(btime+starttime, 同基
    CLOCK_REALTIME, 每次启动现读不缓存)。pid 活着且诞生时刻吻合(±2s) =
    同一个进程, 真互斥; 时刻对不上 = 锁里的 pid 已被无关进程复用(它出生时刻
    是复用者自己的), 判陈旧清掉重来; pid 已死同样陈旧。
    不用 cmdline grep: 持锁进程的命令行未必含 collector.py 字样(改名/软链/-c 皆可)。"""
    try:
        with open(lock_path) as f:
            parts = f.read().split()
        old = int(parts[0])
        born_at = float(parts[1])
        return abs(_proc_starttime(old) - born_at) <= 2.0   # pid 不存在 → FileNotFoundError
    except (OSError, ValueError, IndexError):
        return False


def acquire_lock(db_path):
    """单写者锁。重复启动第二个 collector 时立刻退出, 而不是两个进程交错写入
    (那会让采样间隔变成 14/16s 抖动, 并污染 TPS 基线)。"""
    lock = db_path + ".lock"

    def take():
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"{os.getpid()} {int(_proc_starttime(os.getpid()))}".encode())
        os.close(fd)
        return lock

    for _ in range(1):
        try:
            return take()
        except FileExistsError:
            # 锁存在: 持有进程必须"还活着且诞生时刻与锁记录吻合", 否则清掉重来
            if _lock_owner_alive(lock):
                with open(lock) as f:
                    old = f.read().split()[0]
                print(f"[fleetdash] 已有 collector 在运行 (pid {old})，退出。"
                      f"如确认是残留锁，删除 {lock} 后重试。", file=sys.stderr)
                return None
            os.unlink(lock)          # 陈旧锁(死 pid 或 PID 被无关进程复用)
            return take()
    return None


def write(con, sample, rows):
    con.execute(
        "INSERT OR REPLACE INTO sample (ts,gpu_temp,gpu_util,gpu_pw,cpu_temp,cpu_max,"
        "mem_used,mem_total,load1,sm_mhz,power_cap_us,cap_delta_us,disk_used,"
        "disk_total,net_rx_mbs,net_tx_mbs,uptime_s,clock_off_ms,ntp_sync,"
        "month_wh,day_tok) VALUES "
        "(:ts,:gpu_temp,:gpu_util,:gpu_pw,:cpu_temp,:cpu_max,:mem_used,:mem_total,"
        ":load1,:sm_mhz,:power_cap_us,:cap_delta_us,:disk_used,:disk_total,"
        ":net_rx_mbs,:net_tx_mbs,:uptime_s,:clock_off_ms,:ntp_sync,:month_wh,"
        ":day_tok)", sample)
    con.executemany(
        "INSERT OR REPLACE INTO model (ts,svc,kind,model,vram,alive,tps,req_run,"
        "req_wait,kv_use,act_rate,act_kind,dec_tps,ttft_ms,queue_ms,mtp_acc,probe_ok) VALUES "
        "(:ts,:svc,:kind,:model,:vram,:alive,:tps,:req_run,:req_wait,:kv_use,"
        ":act_rate,:act_kind,:dec_tps,:ttft_ms,:queue_ms,:mtp_acc,:probe_ok)", rows)
    con.commit()


def prune(con, keep_days):
    """保留 N 天。秒级分辨率下 30s 采样约 2880 行/天, 很轻。"""
    cut = int(time.time()) - keep_days * 86400
    con.execute("DELETE FROM sample WHERE ts < ?", (cut,))
    con.execute("DELETE FROM model  WHERE ts < ?", (cut,))
    con.commit()


def load_services(path):
    """从 JSON 读外部服务表(格式 = SERVICES 字面量, ports 键为字符串端口号)。
    给"别人也能用"的部署: 不改代码, 只改 ~/fleetdash/services.json。"""
    with open(path) as f:
        raw = json.load(f)
    out = {}
    for key, cfg in raw.items():
        ports = {}
        for p, entry in cfg.get("ports", {}).items():
            ports[int(p)] = tuple(entry)
        out[key] = {"hostname": cfg.get("hostname", key), "ports": ports}
        if cfg.get("containers"):
            out[key]["containers"] = cfg["containers"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=None, help="主机标识 (4c47/53fa/dbdd), 默认按 hostname 猜")
    ap.add_argument("--db", default=os.path.expanduser("~/fleetdash/fleet.db"))
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL)
    ap.add_argument("--keep-days", type=int, default=14)
    ap.add_argument("--services", default=os.path.expanduser("~/fleetdash/services.json"),
                    help="外部服务表 JSON; 文件存在则优先于内置 SERVICES")
    ap.add_argument("--print-schema", action="store_true",
                    help="打印分类字段规范 (kind -> 有真数据的字段) 后退出")
    ap.add_argument("--once", action="store_true", help="采一次退出")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    if args.print_schema:
        base = ["vram", "alive", "probe_ok", "act_rate", "act_kind"]
        print(json.dumps({"base_fields": base, "kinds": KIND_SCHEMA},
                         ensure_ascii=False, indent=2))
        return 0

    services = SERVICES
    if args.services and os.path.exists(args.services):
        try:
            services = load_services(args.services)
        except (OSError, ValueError, TypeError) as e:
            print(f"服务表 {args.services} 解析失败, 回退内置表: {e}", file=sys.stderr)

    key = args.host
    if not key:
        hn = sh("hostname")
        key = next((k for k, v in services.items()
                    if v["hostname"] in hn or k in hn), None)
    if key not in services:
        print(f"未知主机 {key!r}; 用 --host 指定 ({'/'.join(services)})", file=sys.stderr)
        return 1
    cfg = services[key]

    con = ensure_db(args.db)
    lock = acquire_lock(args.db)
    if lock is None:
        return 1
    tracker = TpsTracker()
    cap_tracker = CapTracker()
    net_tracker = NetTracker()
    # 53fa 的服务跑在 docker 里, 日志走 docker logs; 4c47 是裸 systemd unit
    log_tracker = LogRateTracker(use_docker=(key == "53fa"))
    clock_tracker = ClockTracker()
    energy = PeriodAccum(con, "month", "%Y-%m", int(time.time()))
    tok = PeriodAccum(con, "day", "%Y-%m-%d", int(time.time()))
    print(f"[fleetdash] {key} -> {args.db}, interval={args.interval}s "
          f"(pid {os.getpid()})", flush=True)

    last_prune = 0
    # 对齐到 interval 的整数倍时间槽 (epoch 基准), 消除累积漂移与 14/16s 抖动
    next_tick = (int(time.time()) // args.interval + 1) * args.interval
    while True:
        t0 = time.time()
        try:
            now = int(t0)
            sample, rows, procs = collect(cfg, tracker, cap_tracker,
                                          net_tracker, log_tracker,
                                          clock_tracker, now)
            energy.add_rate(sample.get("gpu_pw"), now, 1.0 / 3600.0)
            sample["month_wh"] = round(energy.val, 1)
            # 当日 token: tps 本身就是 Δgeneration_tokens/Δt, 求和后积分恒等于
            # 各 vLLM 服务 generation_tokens_total 的本日增量(空闲服务记 0)
            tok.add_rate(sum(r["tps"] or 0.0 for r in rows), now)
            sample["day_tok"] = int(tok.val)
            write(con, sample, rows)
            # 进程 top 榜只留"最新一帧"(排障看现值), 存 meta 不进 sample,
            # 避免 14 天历史里每 30s 存一份 JSON 白白撑大库
            try:
                con.execute("INSERT OR REPLACE INTO meta (k,v) VALUES ('procs',?)",
                            (json.dumps(procs, ensure_ascii=False),))
                con.commit()
            except sqlite3.Error:
                pass
            energy.persist(con)
            tok.persist(con)
            if t0 - last_prune > 3600:
                prune(con, args.keep_days)
                last_prune = t0
            if args.verbose or args.once:
                print(json.dumps({"sample": sample, "models": rows}, ensure_ascii=False))
        except Exception as e:
            print(f"[fleetdash] collect error: {e}", file=sys.stderr, flush=True)
        if args.once:
            break
        now_t = time.time()
        # 若采集耗时超过一个周期, 跳过错过的槽位而不是补跑
        while next_tick <= now_t:
            next_tick += args.interval
        time.sleep(max(0.5, next_tick - now_t))
        next_tick += args.interval
    con.close()
    try:
        os.unlink(lock)
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
