# -*- coding: utf-8 -*-
"""fleetdash 全局配置与运行期状态。

main() 负责在启动时改写本模块字段（ROLE/DB/PEERS...）；其余模块一律
`from . import config as cfg` 后按 cfg.X 读取 —— 模块属性是唯一真相。
容器型状态（EDGE/SVC_STATE/...）只原地改写，从不重新赋值。
"""
import os
import threading

DB = os.path.expanduser("~/fleetdash/fleet.db")
LABEL = {}          # host_key -> 显示名
PEERS = {}          # {host_key: base_url}
ROLE = "master"

# 服务显示元数据: kind -> (中文名, 颜色)
KIND_META = {
    "llm":    ("LLM 对话", "#e8590c"),
    "embed":  ("Embedding", "#1c7ed6"),
    "rerank": ("Rerank", "#7048e8"),
    "asr":    ("语音识别", "#0ca678"),
    "tts":    ("语音合成", "#f59f00"),
    "face":   ("人脸", "#868e96"),
    "parse":  ("PDF 解析", "#c2255c"),
    "proxy":  ("代理", "#adb5bd"),
}
WINDOWS = {"3h": 3 * 3600, "1d": 86400, "3d": 3 * 86400, "7d": 7 * 86400}
MAXPTS = 240        # 每条曲线降采样上限, 保证 SVG 轻量

# 电费单价(元/kWh), 只用于"约等于"展示; 部署机可在 systemd 里覆盖。
ELEC_PRICE = float(os.environ.get("ELEC_PRICE", "0.6"))

# 阈值口径与 wecom-watchdog 对齐(对账用), 改这里必须同步 watchdog。
CLK_TRAP_UTIL = 90      # util 高但 SM 时钟 < CLK_TRAP_MHZ -> 社区 t/370304 病征
CLK_TRAP_MHZ = 1200
TEMP_WARN_C = 85
HOT_C = 80
MEM_HIGH_GIB = 112.0

# 曲线突变归因(spec 2026-09-23): 阈值 = max(物理下限, 窗口量程 12%)。
SPIKE_BASE = {"gpu_temp": 8, "gpu_util": 25, "sm_mhz": 250,
              "mem_used": 1.5, "net": 0.5}
SPIKE_FRAC = 0.12
SPIKE_MAX = 40

# edge 运行参数(默认值; main 里被 argparse 覆盖)
POLL_SEC = 30           # 与 collector 采样对齐
FAIL_TH = 4             # 连续失败判 DOWN(容忍 DERP 抖动)
FETCH_TIMEOUT = 8
STALE_SEC = 300         # 拉通但快照超龄 -> collector 死了
POLL_WIN = "3d"         # poller 固定维护的窗口; 页面选其他窗口按需拉取
WEBHOOK_FILE = "/etc/fleetdash/webhook"
WEBHOOK = ""
CACHE_DIR = ""          # edge 好帧目录; main 里按 db 同级推导
REPORT_AT = "09:00"     # 日报时间 HH:MM, 空串关闭
PEERS_FILE = os.environ.get("FLEETDASH_PEERS_FILE", "/etc/fleetdash/peers.json")
STATE_PATH = ""         # 告警闩锁落盘(main 里按 DB 同级设定)

# 运行期状态(身份是对象, 原地改写)
EDGE = {}               # key -> {state,fails,last_err,last_ok,last_agg,down_since,stale}
EDGE_LOCK = threading.Lock()
EDGE_WIN_CACHE = {}     # (key, win) -> (ts, payload)
WIN_CACHE_TTL = 30
SVC_STATE = {}          # (key, svc) -> {"bad":bool,"fails":int,"since":ts,"reason":str}
HW_FIRED = {}           # host key -> {cat: True, "cat@day": "YYYY-MM-DD"}
LAST_REPORT_DAY = None

# ---------------------------------------------------------------- 电源操作
# 页面「关机/重启/唤醒」-> POST /api/power {action, token[, host]}。
# token 文件缺失 = 端点关闭; 全部机器共用同一个随机 token(部署生成一次)。
POWER_TOKEN_FILE_EDGE = "/etc/fleetdash/power.token"
POWER_OK = ("poweroff", "reboot", "wake")
POWER_VERB = {"poweroff": "关机", "reboot": "重启", "wake": "唤醒"}
PWR_FAIL_TH = 3         # 暴破护栏: 连错 3 次锁 5 分钟
PWR_LOCK_SEC = 300

# Wake-on-LAN: magic packet 必须从目标同二层广播域发出, 所以「唤醒」由
# 另一台同网段在线 DGX 代发; 全部关机时无人可代发(除非路由器自带 WoL)。
# 本机 MAC 读 /sys(换网卡不过期), 对端 MAC 只能写死在这。
WOL_MACS = {"4c47": "4c:bb:47:7d:4c:47", "53fa": "4c:bb:47:7d:53:fa",
            "dbdd": "4c:bb:47:7e:db:dd"}
WOL_IFACE = "enP7s7"    # 三台唯一的有线上行(10GbE RJ45)
WOL_BCAST = ("192.168.1.255", "255.255.255.255")
WOL_PORTS = (9, 7)      # 标准 9, 部分固件认 7
