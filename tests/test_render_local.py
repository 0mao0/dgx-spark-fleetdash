# 本地数据契约仿真: 临时 sqlite(新 schema+新列) -> agg()/page_payload()。
# 验证三件事:
#  1) 每服务时序带 req_run/act_rate(并发), heat 带 tok(token 增量);
#  2) 老行(NULL 列)静默降级为空序列, 不炸;
#  3) /api/page 只输出页面需要的 slim 键。
import os
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import collector as C
from fleetdash import config as cfg
from fleetdash.alerts import _uptime_cont
from fleetdash.data import agg
from fleetdash.webapp import PAGE_KEYS, page_payload

db = os.path.join(tempfile.mkdtemp(), "fleet.db").replace("\\", "/")
con = sqlite3.connect(db)
con.executescript(C.DB_SCHEMA)
for sql in C.MIGRATIONS:
    try:
        con.execute(sql)
    except sqlite3.OperationalError:
        pass
now = int(time.time())
con.execute("INSERT INTO sample (ts,gpu_temp,gpu_util,gpu_pw,cpu_temp,cpu_max,"
            "mem_used,mem_total,load1,day_tok) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (now, 70, 99, 120, 60, 75, 60, 121, 8, 1500))
con.execute("INSERT INTO sample (ts,gpu_temp,gpu_util,gpu_pw,cpu_temp,cpu_max,"
            "mem_used,mem_total,load1,day_tok) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (now - 30, 65, 50, 80, 55, 70, 55, 121, 4, 500))
con.executemany(
    "INSERT INTO model (ts,svc,kind,model,vram,alive,tps,req_run,req_wait,kv_use,"
    "act_rate,act_kind,dec_tps,ttft_ms,queue_ms,mtp_acc) VALUES "
    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
        # llm-api: 有新指标, 排队 1200ms, MTP 接受率 87.5%
        (now, "llm-api", "llm", "qwen3.6-35b", 51421, 1, 66.7, 2, 0, 0.3,
         0.5, "http", 43.8, 850.0, 1200.0, 87.5),
        # asr-api: 老数据形态, 新列 NULL -> 不得抛异常
        (now, "asr-api", "asr", "sensevoice", 1282, 1, None, 0, 0, None,
         None, None, None, None, None, None),
        # 30s 前的第二个点, 让曲线有两个采样
        (now - 30, "llm-api", "llm", "qwen3.6-35b", 51421, 1, 60.0, 1, 0, 0.2,
         0.4, "http", 42.1, 900.0, 10.0, 86.0),
    ])
con.commit()
con.close()

cfg.DB = db
cfg.ROLE = "master"
cfg.PEERS = {}
os.environ["FLEETDASH_HOST"] = "4c47"

a = agg(3600)
snap = a["snapshot"]
by = {m["svc"]: m for m in snap["models"]}
assert by["llm-api"]["dec_tps"] == 43.8 and by["llm-api"]["ttft_ms"] == 850.0
assert by["asr-api"]["dec_tps"] is None
assert a["series"]["models"]["asr-api"]["dec_tps"] == []    # NULL 降级为空序列
assert len(a["series"]["models"]["llm-api"]["dec_tps"]) == 2
# 并发/请求速率进时序(B 栏曲线的数据源)
assert [v for _, v in a["series"]["models"]["llm-api"]["req_run"]] == [1, 2]
assert [v for _, v in a["series"]["models"]["llm-api"]["act_rate"]] == [0.4, 0.5]
# token 增量曲线(A 栏): 1500-500=1000, 只在最新点
assert a["series"]["heat"]["tok"] == [[now, 1000]], a["series"]["heat"]["tok"]
assert snap["day_tok"] == 1500 and snap["hist_tok"] == 1500

pp = page_payload("3h")
assert pp["role"] == "master" and len(pp["hosts"]) == 1
h = pp["hosts"][0]
assert set(h.keys()) <= set(PAGE_KEYS), set(h.keys()) - set(PAGE_KEYS)
assert h["label"] == "4c47"
assert h["snapshot"]["models"][0]["req_run"] == 2

# 三档 uptime 格式 + None(告警文案用)
assert _uptime_cont(5 * 86400 + 3 * 3600) == "5 天 3 小时"
assert _uptime_cont(7 * 3600 + 12 * 60) == "7 小时 12 分"
assert _uptime_cont(42 * 60) == "42 分钟"
assert _uptime_cont(None) is None
print("RENDER_SIM_OK  page_host_keys=", sorted(h.keys()))
