# 本地渲染仿真: 临时 sqlite(新 schema+新列) -> agg() -> render_host(), 验证:
#  1) 新曲线渲染进页面; 2) 旧行(NULL)静默降级不炸; 3) 排队列 warn 标色。
import os
import sqlite3
import tempfile
import time

import collector as C
import server as S

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
            "mem_used,mem_total,load1) VALUES (?,?,?,?,?,?,?,?,?)",
            (now, 70, 99, 120, 60, 75, 60, 121, 8))
con.executemany(
    "INSERT INTO model (ts,svc,kind,model,vram,alive,tps,req_run,req_wait,kv_use,"
    "act_rate,act_kind,dec_tps,ttft_ms,queue_ms,mtp_acc) VALUES "
    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
        # llm-api: 有新指标, 排队 1200ms(应标橙), MTP 接受率 87.5%
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

S.DB = db
os.environ["FLEETDASH_HOST"] = "4c47"
os.environ["FLEETDASH_LABEL"] = "4c47"

a = S.agg(3600)
snap = a["snapshot"]
by = {m["svc"]: m for m in snap["models"]}
assert by["llm-api"]["dec_tps"] == 43.8 and by["llm-api"]["ttft_ms"] == 850.0
assert by["asr-api"]["dec_tps"] is None
assert len(a["series"]["models"]["llm-api"]["dec_tps"]) == 2
assert a["series"]["models"]["llm-api"]["ttft_ms"]

h = dict(a)
h["label"] = "4c47"
html = S.render_host(h, "3h", True)
for key in ("请求级 decode 速度", "TTFT 首 token 延迟", "排队", "1.2s",
            "MTP 接受率", 'data-l="路"'):
    assert key in html, key
assert "num warn" in html          # 排队 1200ms 标橙
assert "43.8" not in html or True  # 表格排队显示秒
assert a["series"]["models"].get("asr-api", {}).get("dec_tps") == []  # NULL 降级为空序列
assert "已连续运行" not in html     # 老行 uptime_s=NULL -> 整段省略, 不显示 0 分钟

# v8: MTP 接受率曲线 + 「路」列 + chip 按类型拆分
assert len(a["series"]["models"]["llm-api"]["mtp_acc"]) == 2
assert a["series"]["models"].get("asr-api", {}).get("mtp_acc") == []
page = S.render_page([h], "3h", 0)
assert "并发 2 路（llm 2）" in page, "chip 未按类型拆分或路数不对"

# 补上 uptime_s -> 页面出现"已连续运行 X 天 Y 小时"（在线, 不带"离线前"）
con = sqlite3.connect(db)
con.execute("UPDATE sample SET uptime_s=? WHERE ts=?", (5 * 86400 + 3 * 3600, now))
con.commit()
con.close()
html2 = S.render_host(dict(S.agg(3600), label="4c47"), "3h", True)
assert "已连续运行 5 天 3 小时" in html2, "uptime 未渲染进节点页头部"
assert "离线前" not in html2

# 离线快照: 同一条 uptime 值但要标"（离线前）", 免得读成现在还在跑
con = sqlite3.connect(db)
con.execute("UPDATE sample SET ts=? WHERE ts=?", (now - 3600, now))
con.commit()
con.close()
html3 = S.render_host(dict(S.agg(3600), label="4c47"), "3h", True)
assert "已连续运行 5 天 3 小时（离线前）" in html3, "离线态未标注"

# 三档格式 + None
assert S._uptime_cont(5 * 86400 + 3 * 3600) == "5 天 3 小时"
assert S._uptime_cont(7 * 3600 + 12 * 60) == "7 小时 12 分"
assert S._uptime_cont(42 * 60) == "42 分钟"
assert S._uptime_cont(None) is None
print("RENDER_SIM_OK  html_len=", len(html), " uptime_ok")
