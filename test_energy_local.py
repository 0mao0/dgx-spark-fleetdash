# 周期累计量仿真: PeriodAccum 积分数学 / MAX_DT 空洞剔除 / 翻篇归档 /
# 重启续算(meta 持久化), 功耗卡(有数/NULL)、token 卡与并发 chip 渲染。
import os
import sqlite3
import tempfile
import time

import collector as C
import server as S

db = os.path.join(tempfile.mkdtemp(), "fleet.db").replace("\\", "/")
con = C.ensure_db(db)
t0 = int(time.time()) // 30 * 30

# --- 1) 能耗积分: 恒定 120W。一步 30s = 120*30/3600 = 1.0 Wh
me = C.PeriodAccum(con, "month", "%Y-%m", t0)
for i in range(1, 12):                    # i=1 只记基线, 2..11 共 10 步积分
    me.add_rate(120.0, t0 + i * 30, 1.0 / 3600.0)
assert abs(me.val - 10.0) < 1e-6, me.val
me.persist(con)

# --- 2) 空洞剔除: 正常一步后断 1 小时(>MAX_DT), 断口不计
me.add_rate(120.0, t0 + 12 * 30, 1.0 / 3600.0)      # +1.0 -> 11.0
me.add_rate(120.0, t0 + 12 * 30 + 3600, 1.0 / 3600.0)  # 断口不积分
assert abs(me.val - 11.0) < 1e-6, me.val

# --- 3) 重启续算: 新实例从 meta 读回 11.0, 下一步继续累
me.persist(con)
me2 = C.PeriodAccum(con, "month", "%Y-%m", t0 + 12 * 30 + 3600)
assert abs(me2.val - 11.0) < 1e-6, me2.val
me2.add_rate(60.0, t0 + 12 * 30 + 3630, 1.0 / 3600.0)  # dt=30s -> +0.5 Wh
assert abs(me2.val - 11.5) < 1e-6, me2.val

# --- 4) 月翻篇归档
now = int(time.time())
e = C.PeriodAccum(con, "month", "%Y-%m", now)
e.last = 0
e.add_rate(1000.0, now, 1.0 / 3600.0)
e.add_rate(1000.0, now + e.MAX_DT, 1.0 / 3600.0)     # +83.33 Wh
m_now = time.strftime("%Y-%m", time.localtime(now))
ny = time.localtime(now)
nxt = (ny.tm_year + (ny.tm_mon == 12), ny.tm_mon % 12 + 1)
next_first = time.mktime((nxt[0], nxt[1], 1, 0, 0, 30, 0, 0, -1))
prev_wh = e.val
e.add_rate(100.0, int(next_first))
assert e.period != m_now and e.val == 0.0, (e.period, e.val)
assert e.rolled == (m_now, prev_wh)
e.persist(con)
got = dict(con.execute("SELECT k,v FROM meta").fetchall())
assert got["month"] == e.period
assert abs(float(got["prev_month_v"]) - prev_wh) < 1e-2, got  # meta 存 3 位小数
assert got["prev_month"] == m_now

# --- 5) 当日 token: Σtps×Δt 积分 + 独立 key 与月互不干扰
tk = C.PeriodAccum(con, "day", "%Y-%m-%d", t0)
tk.add_rate(50.0, t0 + 30)                 # 基线
tk.add_rate(50.0, t0 + 60)                 # +50*30 = 1500 tok
tk.persist(con)
tk2 = C.PeriodAccum(con, "day", "%Y-%m-%d", t0 + 60)
assert abs(tk2.val - 1500.0) < 1e-6, tk2.val
assert abs(tk2.last - (t0 + 60)) <= 0
got2 = dict(con.execute("SELECT k,v FROM meta").fetchall())
assert abs(float(got2["day_v"]) - 1500.0) < 1e-6 and got2["day"] == \
    time.strftime("%Y-%m-%d", time.localtime(t0 + 60))
# 月累计不受 day 键污染
me3 = C.PeriodAccum(con, "month", "%Y-%m", now)
assert me3.period == e.period and me3.val == 0.0

# --- 6) 页面渲染: 两卡有数 + 老数据 NULL + 并发 chip
S.DB = db
os.environ["FLEETDASH_HOST"] = "4c47"
os.environ["FLEETDASH_LABEL"] = "4c47"
now = int(time.time())

def render_page_with(month_wh, day_tok, tps, req_run):
    con.execute("INSERT OR REPLACE INTO sample (ts,gpu_temp,gpu_util,gpu_pw,"
                "cpu_temp,cpu_max,mem_used,mem_total,load1,month_wh,day_tok) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (now, 52, 0, 12.9, 54, 60, 92.4, 122, 0.11, month_wh, day_tok))
    con.execute("DELETE FROM model WHERE ts=?", (now,))
    con.execute("INSERT INTO model (ts,svc,kind,model,vram,alive,tps,req_run,"
                "req_wait,kv_use) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (now, "llm-api", "llm", "qwen3.6-35b", 51435, 1, tps, req_run, 0, 0.1))
    con.commit()
    a = S.agg(3600)
    h = dict(a); h["label"] = "4c47"
    host_html = S.render_host(h, "3h", True)
    page = S.render_page([h], "3h", 0)
    return host_html, page

host, page = render_page_with(3200.0, 1234567, 137.4, 3)
# 断言跟页面现版卡片对齐: 大字=聚合速率 "137 tok/s", 副行="今日 1.2M tok"
assert "137" in host and "tok/s" in host
assert "今日 1.2M tok" in host
assert "本月 3.2 kWh ≈ ¥1.9" in host
assert "并发 3 路" in page and "class=\"chip\"" in page
# 老 collector 形态: NULL 列与无 tps 降级
host, page = render_page_with(None, None, None, None)
assert "待采集器更新" in host and "— tok/s" not in host  # 速率显示 0
assert "并发 0 路" in page
print("PERIOD_SIM_OK")
