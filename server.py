#!/usr/bin/env python3
"""fleetdash server — 单页聚合三台 DGX 的实时状态与 3 天曲线。

架构: 每台 DGX 各跑一份(master/slave), 另在 VPS 跑一份 edge(观察者)。

    ./server.py --role master          # 4c47: 渲染页面 + 聚合(内网备用入口)
    ./server.py --role slave           # 53fa: 只提供 /api/agg
    ./server.py --role master --peers 53fa=http://100.107.85.39:9100
    ./server.py --role edge --peers 4c47=http://100.125.117.85:9100 \n                --peers 53fa=http://100.107.85.39:9100

edge 角色(2026-09-14, 融合 sparkDash 的"失联可见" + watchdog 的"主动推送"):
- 独立 30s 轮询每台 peer 的 /api/agg, 缓存最后成功快照(内存+落盘 edge-cache/)。
- 渲染整页; peer DOWN 时用最后快照渲染 + 失联横幅 => 页面不再吊在 B1 一根绳上。
- 车队级推送: 失联 CRIT / 恢复 INFO / 数据停滞 WARN, 冷却 1800s, 走企业微信 webhook。
  硬件阈值告警(时钟陷阱/高温...)仍归 wecom-watchdog, 零重叠。
- 每日 09:00 跨机日报。webhook 密钥: env FLEETDASH_WEBHOOK 或 /etc/fleetdash/webhook(600)。
"""
import argparse
import glob
import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DB = os.path.expanduser("~/fleetdash/fleet.db")
LABEL = {}          # 由 --label 填充: host_key -> 显示名
PEERS = {}          # {host_key: base_url}

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


# ---------------------------------------------------------------- db
def q(sql, args=()):
    if not os.path.exists(DB):
        return []
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        return con.execute(sql, args).fetchall()
    except sqlite3.Error:
        # 老库缺新列(尚未跑 collector 迁移)时降级为"无数据", 而不是 500
        return []
    finally:
        con.close()


def latest_snapshot():
    """当前状态: 最近一条 sample + 该时刻各模型占用。"""
    rows = q("SELECT ts,gpu_temp,gpu_util,gpu_pw,cpu_temp,cpu_max,mem_used,mem_total,"
             "load1,sm_mhz,power_cap_us,cap_delta_us,disk_used,disk_total,"
             "net_rx_mbs,net_tx_mbs,uptime_s,clock_off_ms,ntp_sync,month_wh,day_tok "
             "FROM sample ORDER BY ts DESC LIMIT 1")
    if len(rows) < 1:
        return None
    r = rows[0]
    (ts, gt, gu, gp, ct, cm, mu, mt, ld, sm, cap, capd,
     dk_u, dk_t, n_rx, n_tx, up, clk_off, ntp, mwh, dtok) = r
    models = q("SELECT svc,kind,model,vram,alive,tps,req_run,req_wait,kv_use,"
               "act_rate,act_kind,dec_tps,ttft_ms,queue_ms,mtp_acc,probe_ok "
               "FROM model WHERE ts=? ORDER BY vram DESC", (ts,))
    # 进程 top 榜: collector 每采样覆盖写 meta('procs'), 只存最新一帧。
    # 老 collector 没有这行 -> 空列表, 页面显示"待采集器升级"。
    procs = []
    try:
        pr = q("SELECT v FROM meta WHERE k='procs'")
        procs = json.loads(pr[0][0]) if pr else []
    except Exception:
        procs = []
    # 历史 token 累计: 保留期内每天末尾 day_tok(=当日终值)求和。day_tok 列
    # 是 09-16 才加的、且 prune 按 keep-days 滚动 —— 更早的量不在此数内。
    hist = q("SELECT sum(mx) FROM (SELECT max(day_tok) mx FROM sample "
             "WHERE day_tok IS NOT NULL GROUP BY date(ts,'unixepoch','localtime'))")
    hist_tok = hist[0][0] if hist and hist[0][0] is not None else None
    return {
        "procs": procs,
        "ts": ts,
        "hist_tok": hist_tok,
        "gpu_temp": gt, "gpu_util": gu, "gpu_pw": gp,
        "cpu_temp": ct, "cpu_max": cm,
        "mem_used": mu, "mem_total": mt, "load1": ld,
        "sm_mhz": sm, "power_cap_us": cap, "cap_delta_us": capd,
        "disk_used": dk_u, "disk_total": dk_t,
        "net_rx_mbs": n_rx, "net_tx_mbs": n_tx,
        "uptime_s": up,
        "clock_off_ms": clk_off, "ntp_sync": ntp,
        "month_wh": mwh, "day_tok": dtok,
        # probe: None = 老数据无此列(不参与判死), 0 = 端口在但 HTTP 探活失败(脑死)
        "models": [{"svc": s, "kind": k, "model": m, "vram": v, "alive": a,
                    "tps": t, "req_run": rr, "req_wait": rw, "kv": kv,
                    "act": ac, "act_kind": ak,
                    "dec_tps": dct, "ttft_ms": ttft, "queue_ms": qms,
                    "mtp_acc": macc, "probe": pr}
                   for s, k, m, v, a, t, rr, rw, kv, ac, ak,
                   dct, ttft, qms, macc, pr in models],
    }


# 电费单价(元/kWh), 只用于"约等于"展示; 部署机可在 .env/systemd 里覆盖。
ELEC_PRICE = float(os.environ.get("ELEC_PRICE", "0.6"))


def tok_txt(n, dash="—"):
    """token 量缩写: 1.2M / 34.5K / 931。"""
    if n is None:
        return dash
    if n >= 1e9:
        return f"{n / 1e9:.1f}G"
    if n >= 1e6:
        return f"{n / 1e6:.1f}M"
    if n >= 1e3:
        return f"{n / 1e3:.1f}K"
    return f"{n:.0f}"


def month_cost_txt(mwh):
    """功耗卡副行: 本月累计 kWh ≈ 电费。老数据/未部署 collector 时给占位。"""
    if mwh is None:
        return "本月 —（待采集器更新）"
    kwh = mwh / 1000.0
    yuan = kwh * ELEC_PRICE
    k = f"{kwh:.1f}" if kwh < 100 else f"{kwh:.0f}"
    y = f"{yuan:.1f}" if yuan < 100 else f"{yuan:.0f}"
    return f"本月 {k} kWh ≈ ¥{y}"


# util 高但 SM 时钟掉到 1200 MHz 以下 -> 社区 t/370304 病征, 需拔电冷启。
CLK_TRAP_UTIL = 90
CLK_TRAP_MHZ = 1200
TEMP_WARN_C = 85
HOT_C = 80
MEM_HIGH_GIB = 112.0


def health_stats(seconds):
    """近 N 秒的异常命中统计。纯 SQL, 走 idx_sample_ts。

    这些阈值与 wecom-watchdog 完全对齐, 便于和微信告警对账。
    本函数只统计不推送 —— 推送仍由 watchdog 负责。
    """
    since = int(time.time()) - seconds
    rows = q("""SELECT
        SUM(CASE WHEN gpu_util>=? AND sm_mhz IS NOT NULL AND sm_mhz<? THEN 1 ELSE 0 END),
        SUM(CASE WHEN cap_delta_us IS NOT NULL AND cap_delta_us>=? THEN 1 ELSE 0 END),
        SUM(CASE WHEN gpu_temp>=? THEN 1 ELSE 0 END),
        SUM(CASE WHEN gpu_temp>=? THEN 1 ELSE 0 END),
        SUM(CASE WHEN mem_used>=? THEN 1 ELSE 0 END),
        COUNT(*),
        SUM(CASE WHEN cap_delta_us IS NOT NULL THEN cap_delta_us ELSE 0 END)
      FROM sample WHERE ts>=?""",
        # 封顶阈值 = 单个采样周期满额(0.9×间隔)。此前误用窗口秒数(如 3d
        # 时阈值 233e9 us), 单条 cap_delta_us 最大才 3e7, 计数恒为 0。
        (CLK_TRAP_UTIL, CLK_TRAP_MHZ, int(sample_interval() * 0.9 * 1e6),
         TEMP_WARN_C, HOT_C, MEM_HIGH_GIB, since))
    # edge 本机没有 sample 库(或老库无表)时 q 返回 [] —— 降级为全 0, 不 500
    (trap, cap, hot85, hot80, mem, total, cap_us) = rows[0] if rows else (None,) * 7
    return {
        "clock_trap": trap or 0, "power_cap": cap or 0,
        "hot85": hot85 or 0, "hot80": hot80 or 0, "mem_high": mem or 0,
        "total": total or 0,
        "cap_seconds": round((cap_us or 0) / 1e6, 1),
    }


def downsample(rows, maxpts=MAXPTS):
    """等距抽稀, 保留首尾。rows 已按 ts 升序。"""
    n = len(rows)
    if n <= maxpts:
        return rows
    step = n / maxpts
    out = [rows[int(i * step)] for i in range(maxpts)]
    if out[-1] != rows[-1]:
        out.append(rows[-1])
    return out


def series(seconds):
    """返回 {metric: [[ts,val],...]} 与每模型 TPS 序列。"""
    now = int(time.time())
    since = now - seconds
    srows = q("SELECT ts,gpu_temp,cpu_max,gpu_util,mem_used,gpu_pw,sm_mhz,cap_delta_us,"
              "disk_used,net_rx_mbs,net_tx_mbs FROM sample WHERE ts>=? ORDER BY ts",
              (since,))
    srows = downsample(srows)
    heat = {
        "gpu_temp": [[r[0], r[1]] for r in srows],
        "cpu_max":  [[r[0], r[2]] for r in srows],
        "gpu_util": [[r[0], r[3]] for r in srows],
        "mem_used": [[r[0], r[4]] for r in srows],
        "gpu_pw":   [[r[0], r[5]] for r in srows],
        # None 要保留: 老数据没有该列, 曲线上应断开而不是补 0
        "sm_mhz":   [[r[0], r[6]] for r in srows if r[6] is not None],
        "cap_delta": [[r[0], (r[7] or 0) / 1e6] for r in srows if r[7] is not None],
        "disk_used": [[r[0], r[8]] for r in srows if r[8] is not None],
        "net_rx":   [[r[0], r[9]] for r in srows if r[9] is not None],
        "net_tx":   [[r[0], r[10]] for r in srows if r[10] is not None],
    }
    # TPS: 只取有 tps 的模型, 按 svc 分组
    mrows = q("SELECT ts,svc,tps,vram,dec_tps,ttft_ms,queue_ms,mtp_acc "
              "FROM model WHERE ts>=? ORDER BY ts", (since,))
    by_svc = {}
    for ts, svc, tps, vram, dct, ttft, qms, macc in mrows:
        by_svc.setdefault(svc, {"tps": [], "vram": [],
                                "dec_tps": [], "ttft_ms": [], "queue_ms": [],
                                "mtp_acc": []})
        for key, val in (("tps", tps), ("vram", vram), ("dec_tps", dct),
                         ("ttft_ms", ttft), ("queue_ms", qms),
                         ("mtp_acc", macc)):
            if val is not None:
                by_svc[svc][key].append([ts, val])
    for svc in by_svc:
        for key in ("tps", "vram", "dec_tps", "ttft_ms", "queue_ms", "mtp_acc"):
            by_svc[svc][key] = downsample(by_svc[svc][key])
    return {"heat": heat, "models": by_svc}


# ------------------------------------------------- 突变归因 (spec 2026-09-23)
# 曲线跳变起点标一个黄点, hover 显示当时是谁在用。判定与渲染同一份判定逻辑,
# 在 peer 侧 agg() 算好随 /api/agg JSON 透传给 edge。
#   阈值 = max(物理下限, 窗口量程 12%) —— 3h 细看与 7d 概览都要有意义。
#   同一方向连续超限只记起点 1 个事件 (方波一次上升沿 = 1 个事件)。
SPIKE_BASE = {"gpu_temp": 8, "gpu_util": 25, "sm_mhz": 250,
              "mem_used": 1.5, "net": 0.5}
SPIKE_FRAC = 0.12
SPIKE_MAX = 40               # 每图事件数上限(留跳变最大的), 防极端噪声窗口爆查询


def _who_active(t):
    """t ±35s 内活跃服务(任一采样 req_run>0 或 tps>0), 按活跃度取前 2。"""
    rows = q("SELECT svc, MAX(req_run), MAX(tps) FROM model"
             " WHERE ts>=? AND ts<=? GROUP BY svc", (t - 35, t + 35))
    if not rows:
        return "归因不可用"
    act = [r for r in rows if (r[1] or 0) > 0 or (r[2] or 0) > 0]
    if not act:
        return "无服务活跃"
    act.sort(key=lambda r: (-(r[1] or 0), -(r[2] or 0)))
    s, rr, tp = act[0]
    if (rr or 0) > 0:
        lab = f"{s}(req_run {rr}" + (f" · tps {tp:.1f})" if tp else ")")
    elif tp:
        lab = f"{s}(tps {tp:.1f})"
    else:
        lab = s
    if len(act) > 1:
        lab += f" +{act[1][0]}"
    return lab


def _who_loaded(t):
    """内存台阶归因: 与前一采样比显存变化量最大的服务 (负值=卸载)。"""
    cur = {}
    for svc, vr in q("SELECT svc, vram FROM model WHERE ts>=? AND ts<=?",
                     (t - 35, t + 35)):
        cur[svc] = max(cur.get(svc, 0), vr or 0)
    if not cur:
        return "归因不可用"
    best, best_d = None, 500.0   # vram 单位 MiB; 0.5G 以下不算加载/卸载
    for svc, vr in cur.items():
        prev = q("SELECT vram FROM model WHERE svc=? AND ts<?"
                 " ORDER BY ts DESC LIMIT 1", (svc, t - 35))
        d = vr - (prev[0][0] if prev and prev[0][0] is not None else 0)
        if abs(d) > abs(best_d):
            best, best_d = svc, d
    if not best:
        return "无显存变化"
    return f"{best} 显存 {best_d/1000:+.1f}G"


def spike_events(heat):
    """{chart_key: [[t, v, who], ...]} —— 各图主序列的突变起点 + 归因。"""
    out = {}
    cfgs = [("gpu_temp", heat.get("gpu_temp", []), _who_active),
            ("gpu_util", heat.get("gpu_util", []), _who_active),
            ("sm_mhz",   heat.get("sm_mhz", []),   _who_active),
            ("mem_used", heat.get("mem_used", []), _who_loaded),
            ("net",      heat.get("net_rx", []),   _who_active)]
    for key, pts, who_fn in cfgs:
        if len(pts) < 3:
            continue
        vals = [p[1] for p in pts]
        rng = max(vals) - min(vals)
        if rng <= 0:
            continue
        thr = max(SPIKE_BASE[key], rng * SPIKE_FRAC)
        raw, run_sign = [], 0
        for i in range(1, len(pts)):
            d = pts[i][1] - pts[i - 1][1]
            if abs(d) < thr:
                run_sign = 0
                continue
            s = 1 if d > 0 else -1
            if s != run_sign:                     # 新的一段跳变, 只记起点
                raw.append((abs(d), pts[i][0]))
            run_sign = s
        if not raw:
            continue
        raw.sort(key=lambda x: -x[0])
        evs = []
        for _, t in sorted(raw[:SPIKE_MAX], key=lambda x: x[1]):
            v = next(p[1] for p in pts if p[0] == t)
            evs.append([t, v, who_fn(t)])
        out[key] = evs
    return out


def daily_stats(seconds):
    """车队日报用的原料聚合, 由被观察机(peer)侧计算, edge 只负责排版。

    reqs 用 AVG(act_rate)×span 估算: http 类是 vLLM 计数器差分(准),
    log 类是日志行数差分(≈), 日报统一加 ≈ 前缀。
    drops = alive 1→0 的翻转次数(服务掉线次数), 窗口内 LAG 差分。
    """
    since = int(time.time()) - seconds
    rows = q("SELECT MIN(ts),MAX(ts),COUNT(*),MAX(gpu_temp),MAX(gpu_pw),"
             "MAX(mem_used),MAX(net_rx_mbs),MAX(net_tx_mbs)"
             " FROM sample WHERE ts>=?", (since,))
    if not rows or not rows[0][0]:
        return {}
    t0, t1, n, gpu_pk, pw_pk, mem_pk, nrx_pk, ntx_pk = rows[0]
    span = t1 - t0
    interval = round(span / (n - 1)) if n > 1 else None
    pk = q("SELECT ts FROM sample WHERE ts>=? AND gpu_temp IS NOT NULL"
           " ORDER BY gpu_temp DESC LIMIT 1", (since,))
    reqs = {}
    for svc, avg_r, kind in q(
            "SELECT svc, AVG(act_rate), MIN(act_kind) FROM model"
            " WHERE ts>=? AND act_rate IS NOT NULL GROUP BY svc", (since,)):
        reqs[svc] = {"n": int(round((avg_r or 0) * span)), "kind": kind}
    drops = {}
    for svc, cnt in q(
            "SELECT svc, SUM(CASE WHEN prev_alive=1 AND alive=0 THEN 1 ELSE 0 END)"
            " FROM (SELECT svc, alive, LAG(alive) OVER"
            " (PARTITION BY svc ORDER BY ts) prev_alive FROM model WHERE ts>=?)"
            " GROUP BY svc", (since,)):
        if cnt:
            drops[svc] = int(cnt)
    # 聚合 TPS 日峰(F4 性能退化哨兵的原料): 每服务 今天/昨天 的 tps
    # (Δgeneration_tokens/Δt, 全并发合计的聚合吞吐)最大值。不用 dec_tps:
    # 并发×N 会把每请求速度÷N, 拿它比日峰=每晚误报(2026-09-22/23 复盘)。
    # d0n = 今日 tps>0 的样本数, edge 用它挡"刚零点峰值未长成"的结构性低峰。
    # 用窗口内的行, 按本地日切分; 空 = 该日无请求。
    tps_pk = {}
    for svc, d0, d1, n0 in q(
            "SELECT svc,"
            " MAX(CASE WHEN date(ts,'unixepoch','localtime')"
            "      = date('now','localtime') THEN tps END),"
            " MAX(CASE WHEN date(ts,'unixepoch','localtime')"
            "      = date('now','-1 day','localtime') THEN tps END),"
            " SUM(CASE WHEN date(ts,'unixepoch','localtime')"
            "      = date('now','localtime') AND tps>0 THEN 1 ELSE 0 END)"
            " FROM model WHERE ts>=? AND tps IS NOT NULL"
            " GROUP BY svc", (since,)):
        tps_pk[svc] = {"d0": d0, "d1": d1, "d0n": int(n0 or 0)}
    return {
        "span_s": span, "interval_s": interval, "samples": n,
        "gpu_peak": gpu_pk, "gpu_peak_ts": pk[0][0] if pk else None,
        "pw_peak": pw_pk, "mem_peak": mem_pk,
        "net_rx_peak": nrx_pk, "net_tx_peak": ntx_pk,
        "reqs": reqs, "drops": drops, "tps_pk": tps_pk,
    }


WD_STATE = "/var/lib/wecom-watchdog/state.json"
WD_ALERT_ZH = {
    "hot_freq": "高温振荡", "trap": "时钟卡死", "cap": "功耗封顶",
    "temp": "高温", "mem_high": "内存紧张", "power": "功耗",
}


def wd_alert_name(k):
    if k.startswith("down:"):
        return "服务掉线 " + k[5:]
    if k.startswith("probe:"):
        return "探活异常 " + k[6:]
    if k.startswith("unit:"):
        return "单元异常 " + k[5:]
    return WD_ALERT_ZH.get(k, k)


def watchdog_digest():
    """读 wecom-watchdog 的状态文件(644, faye 可读)拿"当前活动告警 +
    guardian 状态 + 热力累计"。与 watchdog 自己日报同源, 数字可直接对账;
    文件读不到时各项返回 None, 日报降级为"未知", 不影响其余行。"""
    out = {"alerts": None, "guardian": None, "heat": None}
    try:
        with open(WD_STATE) as f:
            d = json.load(f)
    except Exception:
        return out
    # 时效检查: watchdog 停止后 state.json 会变成"能读到但全是旧值"的僵尸
    # 文件(比读不到更阴险)。超 15 分钟未更新 = 按不可读降级。
    try:
        if time.time() - os.path.getmtime(WD_STATE) > 900:
            return out
    except OSError:
        pass
    act = []
    for k, v in (d.get("alerts") or {}).items():
        if isinstance(v, dict) and v.get("active"):
            act.append([k, v.get("since")])
    out["alerts"] = act
    if d.get("guardian_state"):
        out["guardian"] = {"state": d["guardian_state"]}
    dd = d.get("daily") or {}
    if dd.get("heat"):
        out["heat"] = dd["heat"]
    return out


def guardian_events(hours):
    """近 N 小时 vram-guardian 的状态翻转次数(journal state= 序列)。
    faye 在 adm 组可读 journal; 读不到(非 adm/无该 unit)时返回 None。"""
    try:
        r = subprocess.run(
            f"journalctl -u vram-guardian --since '-{int(hours)}h' "
            "-q -o cat 2>/dev/null | grep -oE 'state=[A-Z]+'",
            shell=True, capture_output=True, text=True, timeout=8)
        seq = [ln.split("=", 1)[1] for ln in r.stdout.split() if "=" in ln]
    except Exception:
        return None
    if not seq:
        return None
    flips = sum(1 for a, b in zip(seq, seq[1:]) if a != b)
    return flips + (0 if seq[0] == "NORMAL" else 1)


def sample_interval():
    """实际采样间隔(秒): 取最近两条采样的 ts 差, 异常时回退 30。"""
    r = q("SELECT ts FROM sample ORDER BY ts DESC LIMIT 2")
    if len(r) == 2 and 5 <= r[0][0] - r[1][0] <= 300:
        return r[0][0] - r[1][0]
    return 30


def hw_run():
    """硬件类异常的"当前连续段"(从最新采样往回数)。2026-09-16 从
    wecom-watchdog 移植判定所需的原料, 阈值语义与其 check() 对齐:
    时钟卡死/功率封顶满额/高温持续/≥80°C 连续段与当日上穿/内存高企。
    edge 只做时长判定, 数据全部来自本库 sample 表。"""
    rows = q("SELECT ts,gpu_temp,gpu_util,sm_mhz,cap_delta_us,mem_used"
             " FROM sample ORDER BY ts DESC LIMIT 120")
    if not rows:
        return {}
    iv = sample_interval()
    capfull = iv * 0.9 * 1e6

    def _run(pred):
        n = 0
        for r in rows:
            if pred(r):
                n += 1
            else:
                break
        return n

    t_now = rows[0][1]
    out = {
        "interval_s": iv,
        "temp_now": t_now, "util_now": rows[0][2], "sm_now": rows[0][3],
        "mem_now": rows[0][5],
        "trap_min": round(_run(
            lambda r: r[2] is not None and r[3] is not None
            and r[2] >= CLK_TRAP_UTIL and r[3] < CLK_TRAP_MHZ) * iv / 60, 1),
        "cap_run": _run(lambda r: r[4] is not None and r[4] >= capfull),
        "hot85_run": _run(lambda r: r[1] is not None and r[1] >= TEMP_WARN_C),
        "hot80_min": round(_run(
            lambda r: r[1] is not None and r[1] >= HOT_C) * iv / 60, 1),
    }
    # 当日 ≥80°C 上穿次数(LAG 找冷->热跳变) —— watchdog hot_freq 告警的原料。
    # 同时给出"何时/多高/什么形态": 告警只说次数、页面默认又是 3h 窗口时,
    # 两者会对不上(2026-09-17 实测: 39 次全发生在 01:06-02:33, 页面看 06:33-09:33
    # 自然一片正常, 读起来像误报)。
    lt = time.localtime()
    today0 = int(time.time()) - (lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec)
    hot = q("SELECT ts,gpu_temp,gpu_util FROM sample WHERE ts>=? AND gpu_temp>=?"
            " ORDER BY ts", (today0, HOT_C))
    if hot:
        out["hot_first_ts"], out["hot_last_ts"] = hot[0][0], hot[-1][0]
        pk = max(hot, key=lambda r: r[1])
        out["hot_peak"], out["hot_peak_ts"] = pk[1], pk[0]
        us = [r[2] for r in hot if r[2] is not None]
        # 高温期的平均 GPU 利用率: 高 = 持续满载贴线(散热/功耗问题),
        # 低 = 真正的反复启停(把批量合并成长窗口才有用)。
        out["hot_util_mean"] = round(sum(us) / len(us), 1) if us else None
        # 与采样间隔无关的累计量: 上穿次数会随采样加密而翻倍(同一段贴线抖动,
        # 30s 采样数出 39 次、60s 只有 19 次), 累计分钟数不会 —— 判据用它,
        # 次数只作参考(2026-09-17 两个系统就是因为这个口径差吵起来的)。
        out["hot80_min_today"] = round(len(hot) * sample_interval() / 60, 1)
    cross = q("""SELECT SUM(c) FROM (
        SELECT CASE WHEN gpu_temp>=? AND (LAG(gpu_temp) OVER (ORDER BY ts) IS NULL
               OR LAG(gpu_temp) OVER (ORDER BY ts)<?) THEN 1 ELSE 0 END AS c, ts
        FROM sample) WHERE ts>=?""", (HOT_C, HOT_C, today0))
    out["cross80"] = (cross[0][0] if cross and cross[0][0] else 0)
    return out


def guardian_now():
    """vram-guardian 当前状态: journal 直读(与 watchdog 同源但不经它转抄)。
    返回 NORMAL/DEGRADED/CRITICAL/... 或 None(journal 无记录/不可读)。"""
    try:
        r = subprocess.run(
            "journalctl -u vram-guardian -n 60 -q -o cat 2>/dev/null"
            " | grep -oE 'state=[A-Z]+'",
            shell=True, capture_output=True, text=True, timeout=8)
        seq = [ln.split("=", 1)[1] for ln in r.stdout.split() if "=" in ln]
    except Exception:
        return None
    return seq[-1] if seq else None


# systemd 单元活体清单 —— 镜像 watchdog 的 UNITS(4c47 默认清单)与其
# /etc/wecom-watchdog/units.conf(53fa)。补的是 collector 探活看不见的三类:
# 无端口 unit 的单元死(embed/rerank 后端)、vram-guardian 自身死、
# fqtts 三兄弟(此前无人探)。53fa 的 cosy 不是 systemd 单元(端口探活已覆盖)。
UNITS = {
    # 10-04: llm-api(vLLM) 停用留作回滚, 生产 LLM 换 tf-qwen36(TensorFold systemd 常驻)
    "4c47": ["tf-qwen36", "embed-api", "embed-proxy", "rerank-api",
             "rerank-proxy", "asr-api", "face-api",
             "vram-guardian"],
    # cosy-api 2026-10-25 卸载摘除(停于 10-23, TTS 收敛到 dgx2 Ming :8101)
    "53fa": ["mineru-api", "popo-api", "popo-engine", "vram-guardian",
             "fqtts-api", "fqtts-api2", "fqtts-api3"],
    # dbdd: supervisor/maintenance 是 user unit, 系统级 is-active 查不到;
    # flash 容器死活由 8888 端口探活(collector)覆盖, 这里只盯 guardian。
    "dbdd": ["vram-guardian"],
}
# watchdog WECOM_WATCHDOG_SHEDDABLE: guardian 卸载期间这些 unit 掉线属预期
SHEDDABLE = {"fqtts-api", "fqtts-api2", "fqtts-api3"}


def units_now():
    """systemctl is-active 批量查询(faye 非 root 也可查询单元状态)。
    返回 {unit: active/inactive/...} 或 {}(本机不在清单/查询失败)。"""
    import socket
    hn = socket.gethostname()
    names = UNITS.get("4c47" if "4c47" in hn else "53fa" if "53fa" in hn
                      else host_key()) or []
    if not names:
        return {}
    try:
        r = subprocess.run(["systemctl", "is-active", *names],
                           capture_output=True, text=True, timeout=8)
    except Exception:
        return {}
    lines = r.stdout.split()
    # is-active 退出码非 0 仅表示"有单元不在 active", 输出仍逐行对齐
    return dict(zip(names, lines)) if len(lines) == len(names) else {}


def agg(seconds):
    ser = series(seconds)
    return {"host": host_key(), "snapshot": latest_snapshot(),
            "series": ser,
            "events": spike_events(ser["heat"]),
            "health": health_stats(seconds),
            "daily": daily_stats(seconds),
            "wd": watchdog_digest(),
            "gd": {"events": guardian_events(max(1, seconds // 3600))},
            "hw_run": hw_run(), "guardian_now": guardian_now(),
            "units": units_now()}


def host_key():
    return os.environ.get("FLEETDASH_HOST") or "host"


# ---------------------------------------------------------------- edge(观察者)
# 轮询/推送参数。POLL_SEC 与 collector 采样对齐; FAIL_TH 容忍 DERP 中继抖动
# (4 次 x 8s 超时下界, 约 2 分钟连续失败才判 DOWN, 防误报)。
POLL_SEC = 30
FAIL_TH = 4
FETCH_TIMEOUT = 8
STALE_SEC = 300            # 拉通但 collector 死了: 快照 age 超此值 -> WARN(一次/冷却)
PUSH_COOLDOWN = 1800       # 同类告警冷却, 与 watchdog 对齐
POLL_WIN = "3d"            # poller 固定维护的窗口; 页面选其他窗口时按需拉取
WEBHOOK_FILE = "/etc/fleetdash/webhook"

EDGE = {}                  # key -> {state,fails,last_err,last_ok,last_agg,down_since,stale}
EDGE_LOCK = threading.Lock()
EDGE_WIN_CACHE = {}        # (key, win) -> (ts, payload)  页面切窗口时少重复拉取
WIN_CACHE_TTL = 30
PUSH_STATE = {}            # category -> 上次推送 ts (冷却)
LAST_REPORT_DAY = None
WEBHOOK = ""
CACHE_DIR = ""
REPORT_AT = "09:00"


def resolve_webhook(path):
    url = os.environ.get("FLEETDASH_WEBHOOK", "").strip()
    if not url:
        try:
            with open(path) as f:
                url = f.read().strip()
        except OSError:
            url = ""
    return url


def push_wecom(title, lines):
    if not WEBHOOK:
        print("[edge] webhook 未配置, 丢弃推送: " + title, file=sys.stderr, flush=True)
        return False
    NL = chr(10)
    content = ("### " + title + NL) + NL.join(lines) + NL + \
        "> fleetdash-edge @ " + time.strftime("%Y-%m-%d %H:%M:%S")
    body = json.dumps({"msgtype": "markdown",
                       "markdown": {"content": content}}).encode("utf-8")
    req = urllib.request.Request(WEBHOOK, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            resp = json.loads(r.read().decode("utf-8", "replace") or "{}")
        ok = resp.get("errcode", 0) == 0
        if not ok:
            print(f"[edge] 推送被拒: {resp}", file=sys.stderr, flush=True)
        return ok
    except Exception as e:
        print(f"[edge] 推送失败: {e}", file=sys.stderr, flush=True)
        return False


def push_cool(cat):
    """同类告警冷却。True = 现在可以推。"""
    now = time.time()
    if PUSH_STATE.get(cat, 0) > now - PUSH_COOLDOWN:
        return False
    PUSH_STATE[cat] = now
    state_save()
    return True


# ---------------------------------------------------------------- 闩锁落盘
# 告警闩锁("当日只推一次"/冷却)原来只存内存 —— 服务重启即清零, 于是重启后的
# 第一次轮询会把几小时前的旧事件当新事件重推。2026-09-17 实测: 部署重启 6 次,
# 凌晨 01:5x 那次高温告警被重推成 09:27~09:39 的连续多条, 读起来像"刚出事故"。
STATE_PATH = ""            # main() 里按角色设定
HW_FIRED = {}              # host key -> {cat: True, "cat@day": "YYYY-MM-DD"}(与 EDGE[key]["hw"] 同一个对象)
TPS_FIRED = {}             # host key -> {svc: "YYYY-MM-DD"} F4 性能退化当日闩锁


def state_save():
    if not STATE_PATH:
        return
    try:
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"push": PUSH_STATE, "hw": HW_FIRED, "tps": TPS_FIRED}, f)
        os.replace(tmp, STATE_PATH)
    except OSError:
        pass


def state_load():
    if not STATE_PATH:
        return
    try:
        with open(STATE_PATH) as f:
            d = json.load(f)
    except (OSError, ValueError):
        return
    PUSH_STATE.update(d.get("push") or {})
    HW_FIRED.update(d.get("hw") or {})
    TPS_FIRED.update(d.get("tps") or {})
    print("[fleetdash] 载入告警闩锁 %d 项 / 冷却 %d 类（重启不重推）"
          % (sum(len(v) for v in HW_FIRED.values()), len(PUSH_STATE)), flush=True)


def _fmt_age(sec):
    if sec is None:
        return "未知"
    m = sec // 60
    return f"{m} 分钟" if m < 60 else f"{m // 60} 小时 {m % 60} 分"


def _fmt_uptime(sec):
    """运行时长按"天+小时"呈现: 338h -> 14 天 2 小时。"""
    if sec is None:
        return "运行未知"
    d, rem = divmod(int(sec), 86400)
    h = rem // 3600
    if d:
        return f"运行 {d} 天 {h} 小时"
    m = rem // 60
    return f"运行 {h} 小时" if h else f"运行 {m} 分钟"


def _uptime_cont(sec):
    """节点页「已连续运行」正文: 5 天 3 小时 / 7 小时 12 分 / 42 分钟。

    和 _fmt_uptime 分开是因为这里要的是"连续"这个语义: 机器 reboot 即归零,
    看的人据此判断有没有非计划重启。老 collector 的行没有 uptime_s, 返回 None
    由调用方整段省略, 而不是显示 0 分钟的假数。
    """
    if sec is None:
        return None
    d, rem = divmod(int(sec), 86400)
    h, m = rem // 3600, rem % 3600 // 60
    if d:
        return f"{d} 天 {h} 小时"
    if h:
        return f"{h} 小时 {m} 分"
    return f"{m} 分钟"


def notify_down(key, st):
    if not push_cool("down:" + key):
        return
    snap = (st.get("last_agg") or {}).get("snapshot") or {}
    ago = int(time.time()) - snap["ts"] if snap.get("ts") else None
    nm = LABEL.get(key, key)
    lines = [f"DGX **<font color=\"warning\">{nm} 失联</font>**：edge 连续 "
             f"{FAIL_TH} 次拉取失败（约 {_fmt_age((FAIL_TH + 1) * POLL_SEC)}）。"]
    if ago is not None:
        lines.append(f"- 最后一次成功采集：{_fmt_age(ago)}前（collector 视角）")
    else:
        lines.append("- 无缓存快照，edge 启动后从未拉通过")
    lines.append(f"- 最后错误：`{(st.get('last_err') or '')[:150]}`")
    lines.append("- 页面仍可看失联前最后曲线：https://dgx.angineer.cn/fleet/")
    push_wecom("🔴 车队告警：节点失联", lines)


def notify_up(key, st):
    if not push_cool("up:" + key):
        return
    nm = LABEL.get(key, key)
    # poll_once 会先把 down_since 置 None 再调本函数, 所以必须用 or 兜底,
    # 不能依赖 st.get("down_since", 默认值)(键存在但值为 None)。
    gap = int(time.time()) - (st.get("down_since") or int(time.time()))
    push_wecom("🟢 车队恢复：节点上线",
               [f"DGX **{nm}** 已恢复（失联约 {_fmt_age(gap)}），数据重新入库。"])


def notify_stale(key, st):
    if not push_cool("stale:" + key):
        return
    nm = LABEL.get(key, key)
    snap = (st.get("last_agg") or {}).get("snapshot") or {}
    ago = int(time.time()) - snap["ts"] if snap.get("ts") else None
    push_wecom("🟠 车队告警：数据停滞",
               [f"DGX **{nm}** 的 fleetdash-server 还活着，但 **collector 已 "
                f"{_fmt_age(ago)}没有新数据**（fleetdash-collector 服务可能挂了）。",
                "- 处置：`systemctl --user status fleetdash-collector`"])


def peer_ping(url):
    """失联前的二次确认: ping 对端 tailscale IP。通 = 机器活着。"""
    host = url.split("//", 1)[-1].split("/", 1)[0].split(":", 1)[0]
    if not host:
        return False
    try:
        r = subprocess.run(["ping", "-c", "2", "-W", "2", host],
                           capture_output=True, timeout=8)
        return r.returncode == 0
    except Exception:
        return False


def notify_svr_unresp(key, st):
    """tailscale ping 通但 HTTP 拉不通: 机器在线, 是 fleetdash-server
    挂了/卡了。级别低于失联(不翻转 DOWN 状态, 页面仍按缓存渲染)。"""
    if not push_cool("svrunresp:" + key):
        return
    nm = LABEL.get(key, key)
    push_wecom("🟠 车队告警：监控服务无响应",
               [f"DGX **{nm}** tailscale ping 通（机器在线），但 "
                f"fleetdash-server 连续 {st.get('fails', 0)} 次拉取失败。",
                "- 最后错误：`" + (st.get("last_err") or "")[:150] + "`",
                "- 处置：`systemctl --user restart fleetdash-server`"])


# F4 性能退化哨兵: 今天聚合 TPS 日峰跌破昨天日峰的 70% 即推。口径用聚合
# (Δtokens/Δt)而非 dec_tps: 夜间批 3~4 并发把每请求速度÷N, 旧口径凌晨必误报
# (2026-09-22/23 复盘; 聚合峰 122~175 tok/s 日间稳定, 且与夜间批在否解耦)。
# 刚零点的"今日峰"只含零星深夜慢请求+窗口闲置稀释, 属结构性低峰, 所以要求
# 今日 tps>0 样本 ≥ TPS_MIN_N 才评估: 夜间真退化只是推迟到批开跑后补报,
# 不漏报; 老 peer 无 d0n 字段时退回旧行为(不设门槛)。两侧峰值都要 >=
# TPS_MIN 才比较, 防止空闲服务/低量日误报。日峰是全日单调量, 条件一旦成立全
# 日成立, 所以按 服务+日期 闩锁只推一次(TPS_FIRED 落盘, 重启不重推); 曾只靠
# push_cool 30min 冷却, 结果 72/110 这种状态刷了一整天(2026-09-18 实测)。
TPS_MIN = 10.0
TPS_DROP_TH = 0.7
TPS_MIN_N = 20


def tps_check(key, d):
    pk = ((d.get("daily") or {}).get("tps_pk")) or {}
    nm = LABEL.get(key, key)
    today = time.strftime("%Y-%m-%d")
    fired = TPS_FIRED.setdefault(key, {})
    for svc in [s for s, v in fired.items() if s not in pk]:
        del fired[svc]                      # 服务下线即清, 防状态文件虚胖
    for svc, v in sorted(pk.items()):
        d0, d1 = v.get("d0"), v.get("d1")
        n0 = v.get("d0n")
        if n0 is not None and n0 < TPS_MIN_N:
            continue                        # 今日峰未长成, 下一轮再看
        if d1 is None or d1 < TPS_MIN or d0 is None or d0 < TPS_MIN:
            continue
        if d0 >= TPS_DROP_TH * d1:
            continue
        if fired.get(svc) == today:          # 当日已推过
            continue
        if not push_cool(f"tpsdrop:{key}:{svc}"):
            continue
        fired[svc] = today
        state_save()
        push_wecom("🟠 车队告警：推理性能退化", [
            f"DGX **{nm}** 的 **{svc}** 今日聚合 TPS 日峰 **{d0:.0f} tok/s**，"
            f"仅为昨日（{d1:.0f}）的 **{d0 / d1 * 100:.0f}%**。",
            "- 常见原因：功耗封顶/高温降频、模型切换、显存紧张触发抢占。",
            "- 排查：页面「各模型 TPS」曲线 + 健康检查「功耗封顶」计数"])


# ---------------------------------------------------- 服务级实时告警(down/脑死)
# collector 每次采样真发 HTTP 探活(见 collector.probe_svc), edge 在连续 SVC_TH
# 个采样(约 1 分钟)确认坏才推, 恢复即推。判据: alive=0 -> 端口掉线;
# alive=1 且 probe=0 -> "端口在但服务脑死"(HTTP 超时/5xx)。probe=None(老数据)
# 不参与判死, 避免 edge 升级当天误报。冷却按 机器/服务 独立, 与整机失联分开。
SVC_TH = 2
SVC_STATE = {}             # (key, svc) -> {"bad":bool,"fails":int,"since":ts,"reason":str}
CLOCK_DRIFT_MS = 2000      # 墙钟周期间偏差超此值视为跳变


def notify_svc_down(key, svc, reason, gd=None):
    if not push_cool(f"svcdown:{key}:{svc}"):
        return
    nm = LABEL.get(key, key)
    crit = "**<font color=\"warning\">脑死</font>**" if "脑" in reason \
        else "**<font color=\"warning\">掉线</font>**"
    lines = [f"DGX **{nm}** 的 **{svc}** {crit}（{reason}，"
             f"连续 {SVC_TH} 个采样确认，约 {SVC_TH * POLL_SEC}s）。"]
    if gd and gd != "NORMAL":
        # watchdog 对可 shed 的 unit 在卸载期间直接抑制告警; fleetdash 不知道
        # 每台机的 sheddable 清单, 不抑制只注解 —— 宁可多报不可漏报。
        lines.append(f"- 注意：vram-guardian 当前 **{gd}**（内存腾位卸载中，"
                     "本服务掉线可能是预期行为，先看 guardian 是否随后恢复）。")
    lines.append(f"- 处置：`systemctl status {svc}` 或 `docker restart {svc}`")
    lines.append("- 页面：https://dgx.angineer.cn/fleet/")
    push_wecom("🔴 车队告警：服务不可用", lines)


def notify_svc_up(key, svc, stv):
    if not push_cool(f"svcup:{key}:{svc}"):
        return
    nm = LABEL.get(key, key)
    gap = int(time.time()) - (stv.get("since") or int(time.time()))
    push_wecom("🟢 服务恢复",
               [f"DGX **{nm}** 的 **{svc}** 已恢复探活（不可用约 {_fmt_age(gap)}）。"])


def svc_check(key, d):
    """每次成功拉到新鲜快照后跑一遍。在 EDGE_LOCK 内调用。
    防抖: 快照比当前轮询时刻旧超过一个周期时直接跳过 —— 堵住
    "edge 重启/双进程后不再轮询, 却拿启动缓存里的旧状态推恢复"
    的误报路径(2026-10-04 4c47 asr-api "恢复探活"误报复盘)。"""
    snap = d.get("snapshot") or {}
    if snap.get("ts") and int(time.time()) - int(snap["ts"]) > 2 * POLL_SEC:
        return
    seen = set()
    for m in (snap.get("models") or []):
        k = (key, m["svc"])
        seen.add(k)
        alive = bool(m.get("alive"))
        probe = m.get("probe")
        bad = (not alive) or (probe is not None and probe == 0)
        stv = SVC_STATE.setdefault(k, {"bad": False, "fails": 0,
                                       "since": None, "reason": ""})
        if bad:
            stv["fails"] += 1
            if not stv["bad"] and stv["fails"] >= SVC_TH:
                stv["bad"] = True
                stv["since"] = int(time.time())
                stv["reason"] = ("端口未监听" if not alive
                                 else "端口在但 HTTP 探活失败（脑死）")
                notify_svc_down(key, m["svc"], stv["reason"],
                                d.get("guardian_now"))
        else:
            if stv["bad"]:
                stv["bad"] = False
                notify_svc_up(key, m["svc"], stv)
            stv["fails"] = 0
    # 服务从配置里消失(改名/下线)时清掉陈旧状态, 防止误判恢复
    for k in [k for k in SVC_STATE if k[0] == key and k not in seen]:
        del SVC_STATE[k]


def clock_check(key, d):
    """时钟巡检: NTP 脱同步 或 墙钟相对单调钟跳变 >= CLOCK_DRIFT_MS。"""
    snap = d.get("snapshot") or {}
    nm = LABEL.get(key, key)
    issues = []
    if snap.get("ntp_sync") == 0:
        issues.append("NTP 同步丢失")
    off = snap.get("clock_off_ms")
    if off is not None and abs(off) >= CLOCK_DRIFT_MS:
        issues.append(f"墙钟跳变 {off:+.0f}ms")
    if not issues:
        return
    if not push_cool("clock:" + key):
        return
    push_wecom("🕐 车队告警：时钟异常",
               [f"DGX **{nm}**：" + " · ".join(issues) + "。",
                "- 处置：`timedatectl` 查看（跳变本身通常无害，"
                "但 GB10 时钟卡死病常伴随时间异常）"])


# ------------------------------------------------ 硬件类告警(移植 wecom-watchdog)
# 2026-09-16 把 watchdog 的 1/2/3/3b/4/7 类判定搬进 edge: 原料全部来自
# peer 侧 hw_run()/guardian_now()(同一 sqlite + journal 直读), edge 只做
# "时长/状态 -> 推送"。fired 闩锁记在 EDGE[key]["hw"]: 越过阈值首刻推一次,
# 恢复再推一次; 冷却 1800s/类/机 兜底防刷屏。阈值与 watchdog check() 对齐。
TEMP_CRIT_C = 90
HW_TRAP_MIN = 3      # 时钟卡死持续分钟(=watchdog 3 个 60s 采样)
HW_CAP_RUN = 5       # 封顶满额连续采样数
HW_HOT_SUSTAIN_MIN = 30
# 「高频冲击」判据: 当日 ≥80°C 累计 ≥HW_HOT_FREQ_MIN 分钟 **且** 上穿 ≥HW_HOT_FREQ 次。
# 次数上限压得低(8)是为了跨采样间隔可比: 60s 采样数出的次数约为 30s 的一半,
# 阈值取 24 时两个系统会对同一次事件给出"响/不响"两种结论。
HW_HOT_FREQ = 8
HW_HOT_FREQ_MIN = 20


def hw_check(key, d):
    """每次成功拉到新鲜快照后跑一遍。在 EDGE_LOCK 内调用。"""
    st = EDGE.get(key)
    if st is None:
        return
    # 闩锁挂在落盘那份 dict 上(HW_FIRED), 服务重启不清零 —— 否则重启后的第一次
    # 轮询会把旧事件当新事件重推(2026-09-17 踩过: 部署重启 6 次 = 重推 6 条)。
    hw = st.setdefault("hw", None)
    if hw is None:
        hw = st["hw"] = HW_FIRED.setdefault(key, {})
    run = d.get("hw_run") or {}
    nm = LABEL.get(key, key)
    today = time.strftime("%F")

    def fire(cat, title, lines, once_day=False):
        if hw.get(cat) or (once_day and hw.get(cat + "@day") == today):
            return
        if not push_cool("hw:%s:%s" % (cat, key)):
            return
        hw[cat] = True
        if once_day:
            hw[cat + "@day"] = today
        state_save()
        push_wecom(title, lines)

    def clear(cat, title, lines):
        if not hw.pop(cat, None):
            return
        state_save()
        if not push_cool("hw:%s:ok:%s" % (cat, key)):
            return
        push_wecom(title, lines)

    iv = run.get("interval_s") or 30
    t = run.get("temp_now")
    sm = run.get("sm_now")

    # 1) 时钟卡死: util>=90% 且 SM<1200 持续 HW_TRAP_MIN 分钟 -> CRIT
    tm = run.get("trap_min") or 0
    if tm >= HW_TRAP_MIN:
        if t is not None and t >= 70:
            hint = ("当前 %.0f°C，偏热保护方向：优先降载/通风。" % t)
        elif t is not None:
            hint = ("当前 %.0f°C，低温卡死=固件态：整机+电源适配器拔墙断电 "
                    "30s 再上电（软重启无效）。" % t)
        else:
            hint = "处理按冷断电流程。"
        fire("trap", "🔴 车队告警：GPU 时钟卡死", [
            "DGX **<font color=\"warning\">%s</font>** util≥%d%% 但 SM 卡在 "
            "**%.0f MHz**（正常应≥2200）已 **%g 分钟**，性能约剩 25%%。"
            % (nm, CLK_TRAP_UTIL, sm or 0, tm),
            "- " + hint,
            "- 反复出现：`sudo nvidia-smi -lgc 2200,2200` 锁频作保险"
            "（`-rgc` 撤销）"])
    elif sm is not None and sm >= CLK_TRAP_MHZ:
        clear("trap", "🟢 车队恢复：GPU 时钟正常",
              [f"DGX **{nm}** SM 已回到 {sm:.0f} MHz。"])

    # 2) 功率封顶: cap_delta_us 连续满额 HW_CAP_RUN 个采样 -> WARN
    cr = run.get("cap_run") or 0
    if cr >= HW_CAP_RUN:
        cap_s = (d.get("health") or {}).get("cap_seconds") or 0
        fire("cap", "🟠 车队告警：GPU 持续功率封顶", [
            f"DGX **{nm}** SW Power Capping 计数器已连续 {cr} 个采样"
            f"（约 {cr * iv / 60:.1f} 分钟）满额增长，窗口内封顶累计 "
            f"{cap_s:.0f}s。",
            "- 社区 t/370304：若伴随吞吐骤降按冷断电流程处理；页面"
            "「SM 时钟/功耗」曲线可核对。"])
    elif cr == 0:
        clear("cap", "🟢 功率封顶已解除",
              [f"DGX **{nm}** 封顶计数器停止增长（当前 "
               f"{run.get('sm_now') or 0:.0f} MHz）。"])

    # 3) 温度: ≥85 连续 2 个采样(≥90 升 CRIT) -> 推; 回落推恢复
    if (run.get("hot85_run") or 0) >= 2 and t is not None:
        lvl = "🔴" if t >= TEMP_CRIT_C else "🟠"
        fire("hot", f"{lvl} 车队告警：GPU 温度过高", [
            f"DGX **{nm}** 当前 **{t:.0f}°C**（连续 {run['hot85_run']} 个采样 "
            f"≥{TEMP_WARN_C}°C）。社区断电事故多发生在 86–97°C。",
            "- 建议：检查风扇/环境，必要时锁频 `nvidia-smi -lgc 2200,2200`。"])
    elif t is not None and t < TEMP_WARN_C:
        clear("hot", "🟢 GPU 温度回落", [f"DGX **{nm}** 当前 {t:.0f}°C。"])

    # 3b) 持续高温 / 高频冲击(时钟卡死前兆): 各每天推一次, 段结束后重开闩
    if (run.get("hot80_min") or 0) >= HW_HOT_SUSTAIN_MIN:
        hs = d.get("health") or {}
        fire("sust", "🟠 车队告警：GPU 持续高温", [
            f"DGX **{nm}** ≥{HOT_C}°C 已连续 **{run['hot80_min']:g} 分钟**"
            f"（窗口内 ≥{TEMP_WARN_C}°C 命中 {hs.get('hot85', 0)} 采样）。",
            "- 长期高温+满载是社区时钟卡死前兆：压夜间批量并发/长批，"
            "或锁频 2200，检查通风。"], once_day=True)
    elif t is not None and t <= HOT_C - 2:
        hw.pop("sust", None)
    cross_n = run.get("cross80") or 0
    hot_min = run.get("hot80_min_today") or 0
    if cross_n >= HW_HOT_FREQ and hot_min >= HW_HOT_FREQ_MIN:
        # 次数必须带"何时 + 多高 + 什么形态", 否则和 3h 窗口的页面对不上。
        t0, t1 = run.get("hot_first_ts"), run.get("hot_last_ts")
        win = ("，发生在 %s-%s" % (time.strftime("%H:%M", time.localtime(t0)),
                                 time.strftime("%H:%M", time.localtime(t1)))
               ) if t0 and t1 else ""
        pk, pkt = run.get("hot_peak"), run.get("hot_peak_ts")
        if win and pk and pkt:
            win += "，峰值 %g°C@%s" % (pk, time.strftime("%H:%M", time.localtime(pkt)))
        um = run.get("hot_util_mean")
        if um is not None and um >= 80:
            tip = ("- 高温期 GPU 利用率均值 **%g%%**：这是**持续满载贴线**，不是反复启停"
                   "——关注进风/积灰与功耗策略，必要时锁频；单纯拉长窗口不会更凉。" % um)
        elif um is not None:
            tip = ("- 高温期 GPU 利用率均值 %g%%：属**反复启停**型冷热冲击，"
                   "把批量任务合并成更少长窗口可减少轮次。" % um)
        else:
            tip = "- 建议把批量任务合并成更少长窗口，减少每日满载启停轮次。"
        fire("freq", "🟠 车队告警：GPU 温度高频冲击", [
            "DGX **%s** 当日 ≥%d°C 累计 **%g 分钟**%s（上穿 %d 次，本机 30s 采样）。"
            % (nm, HOT_C, hot_min, win, cross_n),
            tip,
            "- 台账：[面板 1d 窗口 · %s](https://dgx.angineer.cn/fleet/?w=1d&hn=%s)"
            % (nm, nm)], once_day=True)

    # 4) 统一内存高企(≥112G): 即时推, guardian 状态一并附上
    mm = run.get("mem_now")
    if mm is not None and mm >= MEM_HIGH_GIB:
        fire("mem", "🔴 车队告警：统一内存高企", [
            "DGX **<font color=\"warning\">%s</font>** 已用 **%.1f GiB** "
            "≥ %.0f（guardian=%s）。" % (nm, mm, MEM_HIGH_GIB,
                                        d.get("guardian_now") or "?"),
            "- 若 guardian 未自动卸载，检查 `systemctl status vram-guardian`。"])
    elif mm is not None:
        clear("mem", "🟢 内存回落", [f"DGX **{nm}** 已用 {mm:.1f} GiB。"])

    # 7) vram-guardian 状态翻转(journal 直读): 变化即推一条
    g = d.get("guardian_now")
    if g and hw.get("gd") and g != hw["gd"]:
        if push_cool("gd:" + key):
            if g == "NORMAL":
                push_wecom("🟢 vram-guardian 恢复",
                           [f"DGX **{nm}** guardian 状态 "
                            f"{hw['gd']} → NORMAL（内存压力解除）。"])
            else:
                push_wecom("🟠 vram-guardian 卸载中", [
                    f"DGX **{nm}** guardian 状态 {hw['gd']} → **{g}**"
                    "（统一内存逼近上限，正在卸载 uncapped 服务腾内存）。",
                    "- 期间相关服务显示掉线属预期，恢复前不必人工干预。"])
    if g:
        hw["gd"] = g


# ------------------------------------------- systemd 单元看护(镜像 watchdog down:*)
# 端口探活抓不到"单元被 systemctl stop 且端口随进程消失"以外的单元态,
# 更抓不到无端口单元(embed/rerank 后端)与看护者自身死。3 连确认同 SVC_TH。
UNIT_STATE = {}


def notify_unit_down(key, u, stv):
    nm = LABEL.get(key, key)
    st = stv.get("state", "inactive")
    if u == "vram-guardian":
        if not push_cool("unitdown:vram-guardian:" + key):
            return
        push_wecom("🔴 车队告警：vram-guardian 看护者自身停止", [
            f"DGX **<font color=\"warning\">{nm}</font>** 的 **vram-guardian**"
            f" 单元非 active（is-active={st}，连续 {SVC_TH} 次确认）。",
            "- 统一内存自动卸载失效：内存高企只能人工处置，"
            "fqtts 类可卸载服务也不再受保护。",
            f"- 处置：`sudo systemctl status vram-guardian && "
            f"sudo systemctl start vram-guardian`"])
        return
    if not push_cool(f"unitdown:{key}:{u}"):
        return
    push_wecom("🔴 车队告警：systemd 单元停止",
               [f"DGX **{nm}** 的单元 **{u}** 非 active"
                f"（is-active={st}，连续 {SVC_TH} 个采样确认，"
                f"约 {SVC_TH * POLL_SEC}s）。",
                f"- 处置：`systemctl status {u}` 或重启该单元",
                "- 页面：https://dgx.angineer.cn/fleet/"])


def notify_unit_up(key, u, stv):
    if not push_cool(f"unitup:{key}:{u}"):
        return
    nm = LABEL.get(key, key)
    gap = int(time.time()) - (stv.get("since") or int(time.time()))
    push_wecom("🟢 systemd 单元恢复",
               [f"DGX **{nm}** 的单元 **{u}** 已回到 active"
                f"（停了约 {_fmt_age(gap)}）。"])


def unit_check(key, d):
    """每次成功拉到新鲜快照后跑一遍。在 EDGE_LOCK 内调用。
    guardian 卸载期间 SHEDDABLE 单元不计失败(与 watchdog 同语义:
    卸载=预期掉线, 告警只会是噪音)。"""
    units = d.get("units")
    if not units:
        return
    gd = d.get("guardian_now")
    shedding = bool(gd) and gd != "NORMAL"
    seen = set()
    for u, state in units.items():
        k = (key, u)
        seen.add(k)
        stv = UNIT_STATE.setdefault(k, {"bad": False, "fails": 0, "since": None})
        if state == "active":
            if stv["bad"]:
                stv["bad"] = False
                notify_unit_up(key, u, stv)
            stv["fails"] = 0
            continue
        if u in SHEDDABLE and shedding:
            stv["fails"] = 0
            continue
        stv["fails"] += 1
        if not stv["bad"] and stv["fails"] >= SVC_TH:
            stv["bad"] = True
            stv["since"] = int(time.time())
            stv["state"] = state
            notify_unit_down(key, u, stv)
    for k in [k for k in UNIT_STATE if k[0] == key and k not in seen]:
        del UNIT_STATE[k]


def fetch_agg(key, url, win=POLL_WIN, timeout=FETCH_TIMEOUT):
    with urllib.request.urlopen(f"{url}/api/agg?w={win}", timeout=timeout) as r:
        d = json.loads(r.read().decode("utf-8", "replace"))
    d["label"] = LABEL.get(key, key)
    return d


def cache_path(key):
    return os.path.join(CACHE_DIR, key + ".json")


def save_cache(key, d):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = cache_path(key) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(d, f, ensure_ascii=False, default=str)
        os.replace(tmp, cache_path(key))
    except OSError:
        pass


def load_caches():
    for key in PEERS:
        try:
            with open(cache_path(key)) as f:
                d = json.load(f)
            EDGE[key]["last_agg"] = d
            # 缓存新鲜 => 视为 UP, 这样 edge 重启后 peer 若仍是死的, 能正常走
            # UP->DOWN 翻转并再次推送(冷却期内不重复); 陈旧缓存不复活状态, 避免误报"恢复"
            snap = d.get("snapshot") or {}
            if snap.get("ts") and int(time.time()) - snap["ts"] < STALE_SEC * 2:
                EDGE[key]["state"] = "UP"
            print(f"[edge] 载入磁盘缓存 {key} (上次快照 "
                  f"{time.strftime('%m-%d %H:%M', time.localtime(snap['ts']))})"
                  if snap.get("ts") else f"[edge] 载入磁盘缓存 {key}", flush=True)
        except Exception:
            pass


def poll_once():
    for key, url in PEERS.items():
        st = EDGE[key]
        try:
            d = fetch_agg(key, url)
        except Exception as e:
            # F2 失联降噪: 达到阈值先 ping 对端 tailscale IP 二次确认。
            # ping 通 = 机器活着、只是 fleetdash-server/HTTP 链路坏了 ——
            # 不推"整机失联"(那是误报), 改推级别较低的"服务无响应"。
            ping = None
            if st["state"] == "UP" and st["fails"] + 1 >= FAIL_TH:
                ping = peer_ping(url)
            with EDGE_LOCK:
                st["fails"] += 1
                st["last_err"] = str(e)
                if st["state"] == "UP" and st["fails"] >= FAIL_TH:
                    if ping:
                        notify_svr_unresp(key, st)
                    else:
                        st["state"] = "DOWN"
                        st["down_since"] = int(time.time())
                        st["stale"] = False
                        notify_down(key, st)
            continue
        with EDGE_LOCK:
            was_down = st["state"] == "DOWN"
            st.update(fails=0, last_err="", last_ok=int(time.time()), last_agg=d)
            snap = d.get("snapshot") or {}
            st["stale"] = (bool(snap.get("ts"))
                           and int(time.time()) - snap["ts"] > STALE_SEC)
            if st["state"] != "UP":
                st["state"] = "UP"
                st["down_since"] = None
                if was_down:
                    notify_up(key, st)
            elif st["stale"]:
                notify_stale(key, st)
            # 数据停滞时快照是陈旧的, 服务/时钟判定只在新鲜数据上做,
            # 否则 collector 挂 5 分钟后会把最后一帧里的状态当现值重推
            if not st["stale"]:
                svc_check(key, d)
                clock_check(key, d)
                tps_check(key, d)
                hw_check(key, d)
                unit_check(key, d)
        save_cache(key, d)


def _kfmt(n):
    return f"{n / 1000:.1f}k" if n >= 1000 else str(int(n))


# ------------------------------------------------- 日报图片渲染(PIL, 仅 edge 有依赖)
# 企业微信机器人支持 msgtype=image: 把日报排成一张仪表盘(状态点/条形图/进度条),
# 比 markdown 文字版可读得多。Pillow+雅黑只装在 VPS, DGX 上这些函数不会被调用;
# 渲染任何一步失败(缺依赖/字体)都回退到 markdown 文本版, 日报永不误删。
FONT_DIR = "/opt/fleetdash/fonts"
IMG_W = 1200
IMG = {"bg": "#eef1f5", "card": "#ffffff", "line": "#e3e6ea",
       "text": "#1f2430", "mut": "#8a93a3", "ok": "#2f9e44",
       "warn": "#e8590c", "bad": "#e03131", "bar": "#4c6ef5",
       "barbg": "#e3e7ee", "head": "#101826", "headmut": "#98a4b5"}

_fonts_cache = {}


def _f(size, bold=False):
    from PIL import ImageFont
    key = (size, bold)
    if key not in _fonts_cache:
        path = os.path.join(FONT_DIR, "msyhbd.ttc" if bold else "msyh.ttc")
        _fonts_cache[key] = ImageFont.truetype(path, size)
    return _fonts_cache[key]


def _tw(txt, size, bold=False):
    f = _f(size, bold)
    b = f.getbbox(txt)
    return b[2] - b[0]


_SYM = {"✓": "√", "✗": "×", "⚠": "!"}


def _sym(t):
    """msyh 无 ✓✗⚠ 字形(渲染成豆腐块), 换成同义且有的字符。"""
    return "".join(_SYM.get(c, c) for c in t)


def _fit(d, x, y, txt, size, bold, fill, maxw):
    """截断超宽文本(加 …), 防止字段溢出卡片。"""
    txt = _sym(txt)
    f = _f(size, bold)
    while _tw(txt, size, bold) > maxw and len(txt) > 2:
        txt = txt[:-1]
    if _tw(txt, size, bold) > maxw:
        return
    d.text((x, y), txt, font=f, fill=fill)


def _dash(d, x0, y, x1, col):
    x = x0
    while x < x1:
        d.line([x, y, min(x + 8, x1), y], fill=col, width=2)
        x += 14


def _lohi(pts, fixed=None):
    if fixed:
        return fixed
    vals = [p[1] for p in pts if p[1] is not None]
    lo, hi = min(vals), max(vals)
    if hi - lo < 1:
        hi = lo + 1
    pad = (hi - lo) * .15
    return (max(lo - pad, 0) if lo >= 0 else lo - pad), hi + pad


def _chart_panel(d, cx, xr, px0, px1, y, label, cap, series, t0, t1,
                 days, ths=()):
    """卡片内嵌小曲线面板。series: [(名称, pts, 颜色, "L"/"R")]，左右轴各自定标，
    轴刻度数字用对应曲线颜色标色。返回面板底部 y。"""
    PT, PLOT, GAP = 34, 132, 16
    d.text((cx, y), label, font=_f(26, True), fill=IMG["text"])
    lx = cx + 40 + _tw(label, 26, True) + 24   # 颜色图例紧跟面板名
    for name, pts, col, sc in series:
        if pts:
            d.line([lx, y + 17, lx + 24, y + 17], fill=col, width=3)
            d.text((lx + 30, y + 6), name, font=_f(21), fill=IMG["mut"])
            lx += 30 + _tw(name, 21) + 18
    if cap:
        capx = xr - _tw(cap, 22)               # 说明右对齐, 但不得压图例
        if capx < lx + 16:
            _fit(d, lx + 16, y + 6, cap, 22, False, IMG["mut"],
                 max(xr - lx - 16, 0))
        else:
            _fit(d, capx, y + 6, cap, 22, False, IMG["mut"], xr - capx)
    top, bot = y + PT, y + PT + PLOT
    lw = 46
    scales = {}
    for _, pts, _, sc in series:
        if sc not in scales and pts:
            scales[sc] = _lohi(pts)
    for fr in (0, .5, 1):
        yy = bot - (bot - top) * fr
        d.line([px0, yy, px1, yy], fill=IMG["line"], width=1)
        for sc in ("L", "R"):
            if sc in scales:
                lo, hi = scales[sc]
                st = f"{lo + (hi - lo) * fr:.0f}"
                col = next(c for _, _, c, s in series if s == sc)
                if sc == "L":
                    d.text((cx + lw - _tw(st, 20) - 8, yy - 11), st,
                           font=_f(20), fill=col)
                else:
                    d.text((px1 + 8, yy - 11), st, font=_f(20), fill=col)
    for tday in days:
        if t0 <= tday <= t1 and px1 > px0:
            x = px0 + (px1 - px0) * (tday - t0) / (t1 - t0)
            d.line([x, top, x, bot], fill=IMG["line"], width=1)
    if "L" in scales:
        lo, hi = scales["L"]
        for tv, tc in ths:
            if lo <= tv <= hi:
                _dash(d, px0, bot - (bot - top) * (tv - lo) / (hi - lo),
                      px1, tc)
    # 先画右轴序列, 左轴(温度)最后画, 重叠时不被盖住
    for _, pts, col, sc in sorted(series, key=lambda s: s[3] == "L"):
        if not pts or sc not in scales or px1 <= px0:
            continue
        lo, hi = scales[sc]
        seg = []
        for t, v in pts:
            if v is None:
                if len(seg) > 1:
                    d.line(seg, fill=col, width=3, joint="curve")
                seg = []
                continue
            seg.append((px0 + (px1 - px0) * (t - t0) / (t1 - t0),
                        bot - (bot - top)
                        * (min(max(v, lo), hi) - lo) / (hi - lo)))
        if len(seg) > 1:
            d.line(seg, fill=col, width=3, joint="curve")
    return bot + GAP


def _chart_h(m):
    """内嵌曲线块高度: 2 面板 + 时间轴; 无序列数据时退化为两条文本行。"""
    s = m.get("series") or {}
    if s.get("gpu_temp") or s.get("mem_used"):
        return 2 * (34 + 132 + 16) + 30
    return 56 * 2


def render_report_png(date_str, m, ts_str):
    """一台 DGX 一张图, 标题"DGX-<名>运维日报"。返回 PNG bytes。"""
    from PIL import Image, ImageDraw
    machines = [m]   # 下面按卡片列表排版
    CPAD, ROW, REQH, W = 30, 56, 48, IMG_W
    PAD = 24
    head_h, foot_h = 92, 52
    # 先量每张卡的高度, 再定画布
    chs = []
    for m in machines:
        chs.append(CPAD + 66 + 18 + ROW * 8 + REQH * len(m["reqs"])
                   + _chart_h(m) + CPAD - 2)
    H = head_h + sum(ch + PAD for ch in chs) + foot_h
    im = Image.new("RGB", (W, H), IMG["bg"])
    d = ImageDraw.Draw(im)
    d.rectangle([0, 0, W, head_h], fill=IMG["head"])
    d.text((PAD + 8, 24), f"DGX-{m['name']}运维日报",
           font=_f(40, True), fill="#ffffff")
    fw = _tw(date_str, 26)
    d.text((W - PAD - 8 - fw, 34), date_str, font=_f(26), fill=IMG["headmut"])

    y = head_h + PAD - 6
    for m, ch in zip(machines, chs):
        x0, x1 = PAD, W - PAD
        d.rounded_rectangle([x0, y, x1, y + ch], radius=16, fill=IMG["card"],
                            outline=IMG["line"], width=1)
        cx = x0 + CPAD
        # 机头: 状态点 + 名 + 右侧 meta
        dot_c = IMG["ok"] if m["ok"] else IMG["bad"]
        d.ellipse([cx, y + CPAD + 8, cx + 26, y + CPAD + 34], fill=dot_c)
        d.text((cx + 40, y + CPAD), m["name"], font=_f(38, True), fill=IMG["text"])
        _fit(d, W - PAD - CPAD - _tw(m["meta"], 25), y + CPAD + 10,
             m["meta"], 25, False, IMG["mut"], 800)
        yy = y + CPAD + 66
        d.line([cx, yy, x1 - CPAD, yy], fill=IMG["line"], width=1)
        yy += 18
        xlab = cx
        xval = cx + 140
        avail = x1 - CPAD - xval

        def row(label, txt, color=None, warn=False):
            nonlocal yy
            d.text((xlab, yy + 10), label, font=_f(26), fill=IMG["mut"])
            _fit(d, xval, yy + 10, txt, 27, False,
                 color or (IMG["warn"] if warn else IMG["text"]), avail)
            yy += ROW

        # 服务: 每服务状态点+名, 横向排开
        d.text((xlab, yy + 10), "服务", font=_f(26), fill=IMG["mut"])
        sx = xval
        for svc, stt in m["services"]:
            col = (IMG["ok"], IMG["bad"], IMG["warn"])[stt]
            d.ellipse([sx, yy + 19, sx + 16, yy + 35], fill=col)
            lab = svc + {0: "", 1: " 掉线", 2: " 脑死"}[stt]
            d.text((sx + 24, yy + 11), lab, font=_f(26),
                   fill=IMG["text"] if stt == 0 else col)
            sx += 24 + _tw(lab, 26) + 26
        yy += ROW
        # 请求量: 水平条形图, 按最大值归一
        if m["reqs"]:
            d.text((xlab, yy + 10), "请求量", font=_f(26), fill=IMG["mut"])
            nmax = max(n for _, n in m["reqs"]) or 1
            bar_x, cnt_w = xval + 150, 110
            bar_w = x1 - CPAD - bar_x - cnt_w
            for svc, n in m["reqs"]:
                d.text((xval, yy + 12), svc, font=_f(24), fill=IMG["mut"])
                frac = max(n / nmax, 0.02)
                d.rounded_rectangle([bar_x, yy + 14, bar_x + bar_w, yy + 34],
                                    radius=8, fill=IMG["barbg"])
                if frac < 1:
                    d.rounded_rectangle([bar_x, yy + 14, bar_x + bar_w * frac,
                                         yy + 34], radius=8, fill=IMG["bar"])
                else:   # 满格时圆角会叠歪, 直接画满
                    d.rounded_rectangle([bar_x, yy + 14, bar_x + bar_w, yy + 34],
                                        radius=8, fill=IMG["bar"])
                d.text((bar_x + bar_w + 14, yy + 13), _kfmt(n), font=_f(25),
                       fill=IMG["text"])
                yy += REQH
            yy += ROW - REQH
        # GPU·内存曲线(近 3 天, 与网页曲线同源); 无序列数据退化为两行文本
        srs = m.get("series") or {}
        gp = srs.get("gpu_temp") or []
        pw2 = srs.get("gpu_pw") or []
        mem = srs.get("mem_used") or []
        allt = [p[0] for q2 in (gp, pw2, mem) for p in q2]
        if allt and max(allt) - min(allt) > 60:
            t0, t1 = min(allt), max(allt)
            days = []
            td = int(t0) // 86400 * 86400
            while td <= t1:
                days.append(td)
                td += 86400
            px0, px1 = cx + 46, x1 - CPAD - 46
            yy = _chart_panel(d, cx, x1 - CPAD, px0, px1, yy, "GPU",
                              m["gpu_txt"],
                              [("温度°C", gp, "#e8590c", "L"),
                               ("功耗W", pw2, "#7048e8", "R")],
                              t0, t1, days,
                              [(80, "#f08c00"), (85, IMG["bad"])])
            yy = _chart_panel(d, cx, x1 - CPAD, px0, px1, yy, "内存",
                              m["mem_txt"],
                              [("已用GiB", mem, "#2f9e44", "L")],
                              t0, t1, days, [(112, "#f08c00")])
            for tt in (t0, (t0 + t1) / 2, t1):
                s_ = time.strftime("%m-%d %H:%M", time.localtime(tt))
                d.text((min(max(px0 + (px1 - px0) * (tt - t0) / (t1 - t0)
                                - _tw(s_, 22) / 2, cx), x1 - CPAD - _tw(s_, 22)),
                        yy + 4), s_, font=_f(22), fill=IMG["mut"])
            yy += 30
        else:
            row("内存", m["mem_txt"])
            row("GPU", m["gpu_txt"])
        # 磁盘进度条
        d.text((xlab, yy + 10), "磁盘", font=_f(26), fill=IMG["mut"])
        u, t = m["disk"]
        if u and t:
            pct = u / t
            bx, bw = xval, 340
            d.rounded_rectangle([bx, yy + 14, bx + bw, yy + 34], radius=8,
                                fill=IMG["barbg"])
            bc = IMG["bad"] if pct >= .92 else (IMG["warn"] if pct >= .85 else IMG["bar"])
            d.rounded_rectangle([bx, yy + 14, bx + max(bw * pct, 8), yy + 34],
                                radius=8, fill=bc)
            d.text((bx + bw + 16, yy + 11),
                   f"{fmt(u, 0)}/{fmt(t, 0)} G ({pct * 100:.0f}%)", font=_f(26),
                   fill=IMG["warn"] if pct >= .85 else IMG["text"])
        yy += ROW
        row("热力", m["heat_txt"], warn=m["heat_warn"])
        row("网络", m["net_txt"])
        row("TPS 日峰", m.get("tps_txt", "—") + "（今/昨）",
            warn=m.get("tps_warn"))
        row("巡检", m["insp_txt"], warn=m["insp_warn"])
        row("本期告警", m["alert_txt"], warn=m["alert_warn"])
        row("guardian", m["gd_txt"])
        y += ch + PAD
    fw2 = _tw("fleetdash-edge @ " + ts_str, 22)
    d.rectangle([0, H - foot_h, W, H], fill=IMG["bg"])
    d.text(((W - fw2) / 2, H - foot_h + 14), "fleetdash-edge @ " + ts_str,
           font=_f(22), fill=IMG["mut"])
    import io
    buf = io.BytesIO()
    im.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def push_wecom_image(png):
    import base64
    import hashlib
    if not WEBHOOK:
        return False
    body = json.dumps({"msgtype": "image", "image": {
        "base64": base64.b64encode(png).decode("ascii"),
        "md5": hashlib.md5(png).hexdigest()}}).encode("utf-8")
    req = urllib.request.Request(WEBHOOK, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read().decode("utf-8", "replace") or "{}")
        if resp.get("errcode", 0) != 0:
            print(f"[edge] 图片推送被拒: {resp}", file=sys.stderr, flush=True)
        return resp.get("errcode", 0) == 0
    except Exception as e:
        print(f"[edge] 图片推送失败: {e}", file=sys.stderr, flush=True)
        return False


def daily_report():
    """每日跨机日报, 每台一个 watchdog 同规格的多行块(峰值/时间戳/掉线次数)。

    数字全部来自 peer 侧 agg 的 snapshot/health/daily, edge 不碰数据库。
    优先推送仪表盘图片(PIL), 失败回退 markdown 文本。
    """
    lines = []
    machines = []
    for key in PEERS:
        if lines:
            lines.append("")   # 机与机之间空一行, 卡片上块状分明
        st = EDGE.get(key) or {}
        d = st.get("last_agg") or {}
        nm = LABEL.get(key, key)
        snap = d.get("snapshot") or {}
        hs = d.get("health") or {}
        dy = d.get("daily") or {}
        if not snap:
            lines.append(f"**{nm}**：无数据（{st.get('state') or '?'}）")
            machines.append({"name": nm, "ok": False,
                             "meta": st.get("state") or "?",
                             "services": [], "reqs": [], "mem_txt": "无数据",
                             "disk": (None, None), "gpu_txt": "—",
                             "heat_txt": "—", "heat_warn": False,
                             "net_txt": "—", "insp_txt": "无数据",
                             "insp_warn": True, "alert_txt": "未知",
                             "alert_warn": True, "gd_txt": "未知",
                             "tps_txt": "—", "tps_warn": False,
                             "series": {}})
            continue
        ago = int(time.time()) - snap["ts"]
        ok = st.get("state") == "UP" and ago < STALE_SEC
        dot = "🟢" if ok else "🔴"
        reqs = dy.get("reqs") or {}
        drops = dy.get("drops") or {}
        interval = dy.get("interval_s")
        span = dy.get("span_s") or 0
        days = round(span / 86400, 1) if span else None
        win = f"近{days:g}天" if days else "近3d"
        head = (f"{dot} **{nm}**（{_fmt_uptime(snap.get('uptime_s'))}"
                + (f" · 采样 {interval}s" if interval else "")
                + f" · {win}"
                + ("" if ok else f" · **数据{_fmt_age(ago)}前**") + "）")
        # 服务状态只报异常: 正常时一行带过, 掉线/脑死单独红字点名。
        # 请求量拆成独立行按量降序 —— 混在服务状态里读不动。
        models = snap.get("models") or []
        bads = [m["svc"] + ("掉线" if not m.get("alive") else "脑死")
                for m in models
                if not m.get("alive") or m.get("probe") == 0]
        if not models:
            svc_txt = "无服务数据"
        elif not bads:
            svc_txt = f"全部 {len(models)} 个探活✓"
        else:
            svc_txt = (f"{len(models) - len(bads)}/{len(models)} ✓ ｜ "
                       + " · ".join(f'**<font color="warning">{b}</font>**'
                                    for b in bads))
        req_txt = " · ".join(
            f"{k} {_kfmt(v['n'])}"
            for k, v in sorted(reqs.items(), key=lambda kv: -kv[1]["n"])
            if v["n"] > 0) or "无记录"
        clk_off, ntp = snap.get("clock_off_ms"), snap.get("ntp_sync")
        clk_txt = "NTP" + ("✓" if ntp == 1 else "✗" if ntp == 0 else "?")
        if clk_off is not None and abs(clk_off) >= CLOCK_DRIFT_MS:
            clk_txt += f"(跳变{clk_off:+.0f}ms)"
        pk_ts = dy.get("gpu_peak_ts")
        pk_at = (time.strftime("%m-%d %H:%M", time.localtime(pk_ts))
                 if pk_ts else "?")
        dk_u, dk_t = snap.get("disk_used"), snap.get("disk_total")
        dk_pct = (f"({dk_u / dk_t * 100:.0f}%)" if dk_u and dk_t else "")
        wd = d.get("wd") or {}
        hot = (wd.get("heat") or {})
        alerts = wd.get("alerts")
        # "本期" = 日报统计窗口。watchdog 的 active 标记可能从更早残留
        # (如 9-05 的 unit:tts-api), 窗口外的不算本期告警。
        if alerts is not None:
            wsec = span or 259200
            now_w = int(time.time())
            alerts = [a for a in alerts
                      if a[1] is None or now_w - a[1] <= wsec * 1.05]
        if alerts is None:
            alert_txt = "未知(watchdog 状态文件不可读)"
        elif not alerts:
            alert_txt = "无"
        else:
            alert_txt = " · ".join(
                wd_alert_name(k) + (f"({time.strftime('%m-%d %H:%M', time.localtime(s))}起)"
                                    if s else "")
                for k, s in alerts)
        # guardian 状态: journal 直读优先(不经 watchdog 转抄, watchdog 停了
        # 也准); 老 peer 无此键时退回 watchdog state.json。
        gd_state = d.get("guardian_now") \
            or (wd.get("guardian") or {}).get("state") or "未知"
        ev = (d.get("gd") or {}).get("events")
        # 零值即噪声: 无高温时段就一行说"无", 不再排四个 0。
        # 注意区分"watchdog 报回全 0"(真无) 与 "读不到 watchdog"(降级 SQL 估算),
        # 后者不能谎报"无"。
        hot_hit = bool(hot.get("ge80_min") or hot.get("ge85_min")
                       or hot.get("maxrun_min") or hot.get("crossings"))
        if hot and not hot_hit:
            heat_val = "无 ≥80°C 时段"
        elif hot:
            heat_val = (f"≥80°C 共 {hot.get('ge80_min',0)}min"
                        f"（最长段 {hot.get('maxrun_min',0)}min） · ≥85°C "
                        f"{hot.get('ge85_min',0)}min · 上穿80°C "
                        f"{hot.get('crossings',0)} 次")
        else:
            m80 = hs.get("hot80", 0) * (interval or 30) // 60
            heat_val = (f"≈{m80}min ≥80°C（SQL 估算）"
                        if hs.get("hot80") else "无 ≥80°C 时段")
        heat_txt = f"**热力**：{heat_val}"
        cap_s = hs.get("cap_seconds") or 0
        drop_plain = ("无掉线" if not drops else
                      "掉线 {}（{}）".format(
                          sum(drops.values()),
                          ", ".join(f"{k}×{v}" for k, v in drops.items())))
        drop_txt = drop_plain if not drops else \
            '<font color="warning">' + drop_plain + "</font>"
        insp = [drop_txt, f"SM卡死 {hs.get('clock_trap',0)}",
                (f"封顶 {hs.get('power_cap',0)}（累计 {fmt(cap_s,0)}s）"
                 if hs.get("power_cap") or cap_s else "封顶 0")]
        if hs.get("mem_high"):
            insp.append(f"内存≥112G {hs['mem_high']} 次")
        insp.append(clk_txt)
        insp_plain = [drop_plain, f"SM卡死 {hs.get('clock_trap',0)}",
                      (f"封顶 {hs.get('power_cap',0)}（累计 {fmt(cap_s,0)}s）"
                       if hs.get("power_cap") or cap_s else "封顶 0")]
        if hs.get("mem_high"):
            insp_plain.append(f"内存≥112G {hs['mem_high']} 次")
        insp_plain.append(clk_txt)
        insp_warn = bool(drops or hs.get("clock_trap") or hs.get("power_cap")
                         or hs.get("mem_high") or snap.get("ntp_sync") == 0
                         or (clk_off is not None
                             and abs(clk_off) >= CLOCK_DRIFT_MS))
        # 聚合 TPS 日峰(今天/昨天): 来自 peer 侧 daily.tps_pk; 两侧都有量且
        # 今日 < 昨日的 70% 时整行标橙(与 tps_check 同阈值)
        def _t1(v):
            return "—" if v is None else f"{v:.0f}"
        pk = dy.get("tps_pk") or {}
        tps_parts = [f"{svc} {_t1(v.get('d0'))}/{_t1(v.get('d1'))}"
                     for svc, v in sorted(pk.items())
                     if v.get("d0") is not None or v.get("d1") is not None]
        tps_txt = (" · ".join(tps_parts) + " tok/s") if tps_parts else "—"
        tps_warn = any(v.get("d0") and v.get("d1")
                       and v["d0"] < TPS_DROP_TH * v["d1"]
                       for v in pk.values())
        machines.append({
            "name": nm, "ok": ok,
            "meta": f"{_fmt_uptime(snap.get('uptime_s'))} · {win}"
                    + ("" if ok else f" · 数据{_fmt_age(ago)}前"),
            "services": [(m["svc"], 0 if m.get("alive") and m.get("probe") != 0
                          else (1 if not m.get("alive") else 2))
                         for m in models],
            "reqs": [(k, v["n"]) for k, v in
                     sorted(reqs.items(), key=lambda kv: -kv[1]["n"])
                     if v["n"] > 0],
            "mem_txt": (f"当前 {fmt(snap.get('mem_used'),1)} / 峰值 "
                        f"{fmt(dy.get('mem_peak'),1)} GiB"),
            "disk": (dk_u, dk_t),
            "gpu_txt": (f"{fmt(snap.get('gpu_temp'),0)}°C · util "
                        f"{fmt(snap.get('gpu_util'),0)}% · "
                        f"{fmt(snap.get('gpu_pw'),1)}W · SM "
                        f"{fmt(snap.get('sm_mhz'),0)}MHz（峰值 "
                        f"{fmt(dy.get('gpu_peak'),0)}°C@{pk_at}）"),
            "heat_txt": heat_val, "heat_warn": hot_hit,
            "net_txt": (f"峰值 下 {fmt(dy.get('net_rx_peak'),1)} · 上 "
                        f"{fmt(dy.get('net_tx_peak'),1)} MB/s"),
            "insp_txt": " · ".join(insp_plain), "insp_warn": insp_warn,
            "tps_txt": tps_txt, "tps_warn": tps_warn,
            "alert_txt": alert_txt, "alert_warn": bool(alerts),
            "gd_txt": gd_state + (f"（状态变化 {ev} 次）" if ev is not None
                                  else ""),
            # series 为 {heat, models} 嵌套, 曲线只用 heat 组
            "series": (d.get("series") or {}).get("heat") or {},
        })
        lines.extend([
            head,
            f"> **服务**：{svc_txt}",
            f"> **请求量**：{req_txt}（≈次）",
            f"> **内存**：当前 {fmt(snap.get('mem_used'),1)} / 峰值 "
            f"{fmt(dy.get('mem_peak'),1)} GiB；**磁盘** {fmt(dk_u,0)}/"
            f"{fmt(dk_t,0)} G {dk_pct}",
            f"> **GPU**：{fmt(snap.get('gpu_temp'),0)}°C · util "
            f"{fmt(snap.get('gpu_util'),0)}% · {fmt(snap.get('gpu_pw'),1)}W · "
            f"SM {fmt(snap.get('sm_mhz'),0)}MHz（峰值 {fmt(dy.get('gpu_peak'),0)}°C"
            f"@{pk_at}）",
            f"> {heat_txt}",
            f"> **网络**：峰值 下 {fmt(dy.get('net_rx_peak'),1)} · 上 "
            f"{fmt(dy.get('net_tx_peak'),1)} MB/s",
            (f"> **TPS 日峰**：{tps_txt}（今日/昨日）"),
            "> **巡检**：" + " · ".join(insp),
            f"> **本期告警**：{alert_txt}",
            f"> **guardian**：{gd_state}"
            + (f"（状态变化 {ev} 次）" if ev is not None else ""),
        ])
    if machines:
        try:
            all_ok = True
            for mm in machines:
                png = render_report_png(time.strftime("%Y-%m-%d %H:%M"), mm,
                                        time.strftime("%Y-%m-%d %H:%M:%S"))
                if not push_wecom_image(png):
                    all_ok = False
                    break
                time.sleep(1)  # 企微机器人限频, 连发留间隔
            if all_ok:
                return
            print("[edge] 图片推送失败, 回退文本版", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"[edge] 日报图片渲染失败, 回退文本: {type(e).__name__}: {e}",
                  file=sys.stderr, flush=True)
    if lines:
        push_wecom("📋 DGX 车队日报", lines)


def maybe_daily_report():
    global LAST_REPORT_DAY
    if not REPORT_AT or not WEBHOOK:
        return
    day = time.strftime("%Y-%m-%d")
    # 已推日落盘: LAST_REPORT_DAY 是内存量, 服务重启即清零,
    # 不持久化的话 09:00 后每次 restart 都会重推一遍日报(2026-09-16 实测)
    marker = os.path.join(CACHE_DIR, ".report-" + day) if CACHE_DIR else None
    if LAST_REPORT_DAY == day or (marker and os.path.exists(marker)):
        return
    if time.strftime("%H:%M") < REPORT_AT:
        return
    # 只有拿到过数据才推, 否则日报毫无意义
    if not any(st.get("last_agg") for st in EDGE.values()):
        return
    LAST_REPORT_DAY = day
    if marker:
        try:
            os.makedirs(CACHE_DIR, exist_ok=True)
            open(marker, "w").close()
        except OSError:
            pass
    daily_report()


# F5 机器清单热加载: edge 每轮轮询前查一次该文件的 mtime, 变了就增删 peer,
# 加机器不再需要动 systemd ExecStart。格式 {"4c47": {"url": "http://...",
# "label": "4c47"}, ...}(值也可以是裸 url 字符串)。--peers 仍是启动种子,
# 文件里的同 key 条目会覆盖它。
PEERS_FILE = os.environ.get("FLEETDASH_PEERS_FILE", "/etc/fleetdash/peers.json")
_PEERS_MT = 0.0


def reload_peers():
    global _PEERS_MT
    try:
        mt = os.path.getmtime(PEERS_FILE)
    except OSError:
        return
    if mt == _PEERS_MT:
        return
    try:
        with open(PEERS_FILE) as f:
            raw = json.load(f)
    except Exception as e:
        # 坏文件不炸轮询, 但记下 mtime 避免每轮重复读
        print(f"[edge] peers 文件解析失败(保持原清单): {e}",
              file=sys.stderr, flush=True)
        _PEERS_MT = mt
        return
    _PEERS_MT = mt
    spec = raw.get("peers") if isinstance(raw, dict) and "peers" in raw else raw
    if not isinstance(spec, dict):
        return
    for k, v in spec.items():
        url = ((v.get("url") if isinstance(v, dict) else v) or "").strip().rstrip("/")
        if not url:
            continue
        if PEERS.get(k) != url:
            PEERS[k] = url
            print(f"[edge] peer 热更新 {k} -> {url}", flush=True)
        if isinstance(v, dict) and v.get("label"):
            LABEL[k] = v["label"]
        EDGE.setdefault(k, {"state": None, "fails": 0, "last_err": "",
                            "last_ok": None, "last_agg": None,
                            "down_since": None, "stale": False})
    for k in [k for k in list(EDGE) if k not in PEERS]:
        EDGE.pop(k, None)
        print(f"[edge] peer 热移除 {k}", flush=True)


def poller_loop():
    while True:
        try:
            reload_peers()
        except Exception as e:
            print(f"[edge] peers reload error: {e}", file=sys.stderr, flush=True)
        try:
            poll_once()
        except Exception as e:
            print(f"[edge] poller error: {e}", file=sys.stderr, flush=True)
        try:
            maybe_daily_report()
        except Exception as e:
            print(f"[edge] report error: {e}", file=sys.stderr, flush=True)
        time.sleep(POLL_SEC)


def edge_hosts(win):
    """构造 hosts 列表: 优先新鲜数据; 失败/非默认窗口按需拉取; DOWN 回退缓存。
    返回的 dict 额外带 down(横幅文案) 标记。"""
    out = []
    now = time.time()
    for key, url in PEERS.items():
        st = EDGE[key]
        with EDGE_LOCK:
            cached = dict(st["last_agg"]) if st.get("last_agg") else None
            state = st["state"]
        d = None
        err = None
        c = EDGE_WIN_CACHE.get((key, win))
        if c and now - c[0] < WIN_CACHE_TTL:
            d = c[1]
        else:
            try:
                d = fetch_agg(key, url, win=win, timeout=5)
                EDGE_WIN_CACHE[(key, win)] = (now, d)
            except Exception as e:
                err = str(e)
        if d is not None:
            h = dict(d)
        elif cached is not None:
            h = dict(cached)          # DOWN: 用最后成功快照(窗口可能与所选不一致)
            h["down"] = err or "拉取失败"
        else:
            h = {"host": key, "label": LABEL.get(key, key), "snapshot": None,
                 "error": err or "尚无数据"}
        h.setdefault("label", LABEL.get(key, key))
        if state == "DOWN" and "down" not in h:
            h["down"] = st.get("last_err") or "拉取失败"
        out.append(h)
    return out


# ---------------------------------------------------------------- svg
def svg_lines(series_list, w=560, h=190, ymin=None, ymax=None, unit="",
              fill=True, labels=None, xmax=None, hlines=None, bands=None,
              events=None):
    """series_list: [(points, color, name)]。手写 SVG 折线, 无依赖。
    附带 hover 读数: 隐藏的 JSON 数据 + 一条跟随鼠标的竖线 (由页面底部的 JS 驱动)。
    events: [[t, v, who], ...] 突变归因点 (spike_events 产出)。画琥珀黄圆点
    (.edot, 半径由页面 JS 按缩放补偿), 并把 [[t, who]] 并入 hover payload。

    xmax: 时间轴右端 (通常传当前时间)。当数据跨度远小于窗口时, 若强行把轴拉到
    窗口末尾, 曲线只占左侧一小截, 看起来像断了。这里在"充满绘图区"与"反映真实
    时间跨度"之间取折中: 轴右端不早于最后一个数据点, 也不超过 xmax。

    hlines: [(value, color, label)] 水平参考线 (如 SM 时钟 1200MHz 危险线)。
    bands:  [(t_a, t_b)] 需要高亮的时段 (如时钟卡死区间), 画浅红底。
    """
    allpts = [p for pts, _, _ in series_list for p in pts]
    if not allpts:
        return f'<svg viewBox="0 0 {w} {h}" class="ch"></svg>'
    xs = [p[0] for p in allpts]
    ys = [p[1] for p in allpts]
    x0, xlast = min(xs), max(xs)
    # 数据跨度太小(刚启动)时, 限制右端最多比左端晚 2 倍跨度, 避免整条线挤在角落
    span = max(xlast - x0, 60)
    x1 = xlast + span * 0.15
    if xmax is not None:
        x1 = min(x1, max(xmax, xlast))
    x1 = max(x1, x0 + 1)
    lo = ymin if ymin is not None else min(ys)
    hi = ymax if ymax is not None else max(ys)
    if hi - lo < 1e-9:
        hi = lo + 1
    pad = (hi - lo) * 0.12
    lo -= pad
    hi += pad
    if ymin is not None:
        lo = ymin
    if ymax is not None:
        hi = ymax
    # 左右留白必须 >= 一个时间标签的宽度。viewBox 用 17px 字号时
    # "16:36:43" ≈ 73 单位, "09-12 16:36" ≈ 97 单位, 故留 78/100 偏保守的 80。
    # B 要同时容纳: 最低刻度标签(y=T+ph) + 时间轴(y=h-8), 两者需明显分离,
    # 否则 "46" 会和 "16:00:59" 叠在一起。故 B 取 46 并让刻度略上抬。
    L, R, T, B = 80, 80, 14, 46
    pw, ph = w - L - R, h - T - B

    def X(t):
        return L + (0 if x1 == x0 else (t - x0) / (x1 - x0)) * pw

    def Y(v):
        return T + (1 - (v - lo) / (hi - lo)) * ph

    # 不用 preserveAspectRatio="none": 那会把 viewBox 横向非等比拉伸,
    # 文字随之被拉宽变形且字号随容器宽度飘移。等比缩放 + CSS 控高更稳。
    parts = [f'<svg viewBox="0 0 {w} {h}" class="ch" '
             f'style="aspect-ratio:{w}/{h};" '
             f'data-x0="{x0}" data-x1="{x1}" data-lo="{lo:.4f}" data-hi="{hi:.4f}" '
             f'data-l="{L}" data-r="{R}" data-t="{T}" data-b="{B}" '
             f'data-unit="{unit}">']
    # 高亮时段 (如时钟卡死区间): 画在所有元素之前, 作为底色
    for tb in (bands or []):
        ta_, tb_ = tb[0], tb[1]
        xa, xb = X(ta_), X(tb_)
        if xb - xa < 1:                      # 单点区间给个最小可见宽度
            xb = xa + 1.5
        parts.append(f'<rect x="{xa:.1f}" y="{T}" width="{xb-xa:.1f}" height="{ph}" '
                     f'fill="#e03131" opacity="0.16"/>')
    # y 网格 3 条。刻度小数位随量程自适应: 网络吞吐常年 <1 MB/s、统一内存
    # 常年 91~92 之间, 固定 0 位会把三个刻度都印成同一个整数(全"0"/全"92")。
    dec = 0 if hi - lo >= 20 else (1 if hi - lo >= 2 else 2)
    for i in range(3):
        v = lo + (hi - lo) * i / 2
        y = Y(v)
        parts.append(f'<line x1="{L}" y1="{y:.1f}" x2="{w-R}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{L-4}" y="{y+3:.1f}" class="yl">{v:.{dec}f}</text>')
    # 水平参考线 (如 1200MHz 危险线)
    for hv, hc, hl in (hlines or []):
        if not (lo <= hv <= hi):
            continue
        y = Y(hv)
        parts.append(f'<line x1="{L}" y1="{y:.1f}" x2="{w-R}" y2="{y:.1f}" '
                     f'stroke="{hc}" stroke-width="1.2" stroke-dasharray="5 4" '
                     f'opacity="0.85"/>')
        parts.append(f'<text x="{w-R-2}" y="{y-3:.1f}" class="hlbl" '
                     f'text-anchor="end" fill="{hc}">{hl}</text>')
    # 曲线
    for pts, color, name in series_list:
        if not pts:
            continue
        d = " ".join(f"{X(t):.1f},{Y(v):.1f}" for t, v in pts)
        if fill and len(pts) > 1:
            area = f"{L},{T+ph} " + d + f" {X(pts[-1][0]):.1f},{T+ph}"
            parts.append(f'<polygon points="{area}" fill="{color}" opacity="0.10"/>')
        parts.append(f'<polyline points="{d}" fill="none" stroke="{color}" '
                     f'stroke-width="1.6" stroke-linejoin="round"/>')
    # 突变归因点: 琥珀黄小圈压在跳变起点上(半径由页面 JS 按缩放补偿)
    ev_js = []
    for t_, v_, who in (events or []):
        if t_ < x0 or t_ > x1:
            continue
        parts.append(f'<circle class="edot" cx="{X(t_):.1f}" cy="{Y(v_):.1f}" '
                     f'r="2.6" fill="#fab005"/>')
        ev_js.append([t_, who])
    # time 轴: 首/尾两个标签。三标签在 440px 宽的图表里会挤在一起,
    # 且字号放大后 "MM-DD HH:MM" 一定重叠, 故只留两端 + 按跨度自适应粒度。
    span_sec = x1 - x0
    if span_sec > 4 * 86400:
        tf = "%m-%d"
    elif span_sec > 36 * 3600:
        tf = "%m-%d %H:%M"
    elif span_sec > 2 * 3600:
        tf = "%H:%M"
    else:
        tf = "%H:%M:%S"
    for frac in (0, 1):
        t = x0 + span_sec * frac
        # 时间标签画在坐标文字行, 与数据区不重叠; ±R 的内缩让标签不压到曲线
        xx = L if frac == 0 else w - R
        parts.append(f'<text x="{xx:.1f}" y="{h-8}" class="xl" '
                     f'text-anchor="{"start" if frac==0 else "end"}">'
                     f'{time.strftime(tf, time.localtime(t))}</text>')
    # 单位标注: 放在绘图区内右上角。左下有最低刻度标签、左下角有单位抢位,
    # 顶部左侧又有最高刻度, 只有右上角是空的 (危险线标签会占右下)。
    parts.append(f'<text x="{w-R-4}" y="{T+22:.1f}" class="yu" '
                 f'text-anchor="end">{unit}</text>')
    # hover 层: 竖线 + 圆点标记
    parts.append(f'<line class="hx" x1="0" y1="{T}" x2="0" y2="{T+ph}" '
                 f'style="display:none;stroke:var(--mut);stroke-width:1;'
                 f'stroke-dasharray:2 2"/>')
    for i, (pts, color, name) in enumerate(series_list):
        parts.append(f'<circle class="hd" data-i="{i}" r="2.6" fill="{color}" '
                     f'style="display:none"/>')
    # 内嵌数据供 JS 读取 (避免额外请求)
    payload = [[[t, v] for t, v in pts] for pts, _, _ in series_list]
    names = [n or "" for _, _, n in series_list]
    colors = [c for _, c, _ in series_list]
    meta = json.dumps({"pts": payload, "names": names, "colors": colors,
                       "ev": ev_js},
                      separators=(",", ":"))
    parts.append(f'<script type="application/json" class="cdata">{meta}</script>')
    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------- html
def fmt(v, d=1, dash="—"):
    return dash if v is None else f"{v:.{d}f}"


def temp_cls(t):
    if t is None:
        return ""
    if t >= 85:
        return "crit"
    if t >= 80:
        return "warn"
    return ""


def clock_trap_bands(sm_pts, util_pts, gap=180):
    """找出「util 高 且 SM 时钟低于危险线」的连续时段, 用于在图上涂红底。

    两序列按时间对齐(取最近的 util 采样), 相邻命中点间隔 < gap 视为同一段。
    """
    if not sm_pts or not util_pts:
        return []
    ut = [p[0] for p in util_pts]
    uv = [p[1] for p in util_pts]
    import bisect
    bands, cur = [], None
    for t, sm in sm_pts:
        if sm is None or sm >= CLK_TRAP_MHZ:
            if cur:
                bands.append((cur[0], cur[1]))
                cur = None
            continue
        i = bisect.bisect_left(ut, t)
        cand = [j for j in (i - 1, i) if 0 <= j < len(uv)]
        u = max((uv[j] for j in cand), default=None)
        if u is not None and u >= CLK_TRAP_UTIL:
            if cur and t - cur[1] <= gap:
                cur[1] = t
            else:
                if cur:
                    bands.append((cur[0], cur[1]))
                cur = [t, t]
        elif cur:
            bands.append((cur[0], cur[1]))
            cur = None
    if cur:
        bands.append((cur[0], cur[1]))
    return bands


def render_host(h, win, primary):
    snap = h.get("snapshot")
    name = h.get("label") or h.get("host", "?")
    if not snap:
        # 没数据也必须能唤醒: 目标机正关着时 edge 冷启动后无缓存, 这张卡就是
        # 唯一的入口。关机/重启此时没意义(HTTP 到不了那台机器), 只给唤醒。
        wakebtn = (f'<button class="pbtn" data-h="{h.get("host")}" '
                   f'data-a="wake" title="由另一台在线机器广播 magic packet">'
                   f'唤醒</button>')
        return (f'<section class="host"><h2>{name} '
                f'<span class="badge off">无数据</span>'
                f'<span class="pwrs">{wakebtn}</span></h2>'
                f'<p class="muted">collector 未运行或数据库为空。'
                f'{h.get("error", "")}</p></section>')

    age = int(time.time()) - snap["ts"]
    online = age < 180
    badge = ('<span class="badge on">在线</span>' if online
             else f'<span class="badge off">离线 {age//60}m</span>')
    # 已连续运行 = 宿主机 uptime (/proc/uptime), reboot 归零。离线时快照停在
    # 最后一次成功采样, 标"（离线前）"免得把停机前的时长读成现在还在跑。
    _cont = _uptime_cont(snap.get("uptime_s"))
    cont_seg = (f"已连续运行 {_cont}" + ("" if online else "（离线前）")
                + " · ") if _cont else ""

    ser = h.get("series", {})
    heat = ser.get("heat", {})
    # 突变归因(老 peer 的 JSON 无此键 -> 空 dict, 全部图正常降级无记号)
    ev = h.get("events") or {}
    now_ts = int(time.time())

    # --- 实时卡片 (snap 老数据可能没有新键, 一律 .get)
    # 「已用/总量」的总量用 .tot 小字弱化 —— 大数字只留给已用量。
    dk_u, dk_t = snap.get("disk_used"), snap.get("disk_total")
    disk_cls = "crit" if (dk_t and dk_u / dk_t >= 0.92) else \
               ("warn" if (dk_t and dk_u / dk_t >= 0.85) else "")
    # 温度合成一张卡: GPU 是大数字(GB10 热敏感主角), CPU 进副行;
    # 告警色取两者较高者, 任一过热都会点亮整卡。
    t_hi = max((t for t in (snap["gpu_temp"], snap["cpu_temp"]) if t is not None),
               default=None)
    cards = [
        ("温度", f'GPU {fmt(snap["gpu_temp"], 0)}°', temp_cls(t_hi),
         f'CPU {fmt(snap["cpu_temp"], 1)}°'),
        # 利用率一张卡: GPU 使用率是大数字, CPU 负载进副行。成对看:
        # 一个看算力喂没喂饱, 一个看 CPU/IO 排没排队
        ("利用率", f'GPU {fmt(snap["gpu_util"], 0)}%', "",
         f'CPU 负载 {fmt(snap["load1"], 2)}'),
        ("功耗", fmt(snap["gpu_pw"], 1) + " W", "",
         month_cost_txt(snap.get("month_wh"))),
        # token 一张卡: 大字=实时速率(各 vLLM 聚合 tps 之和, 与 TPS 图同口径),
        # 副行=今日累计 + 保留期历史累计(collector 按 Σtps×Δt 积分持久化)。
        ("token",
         f'{fmt(sum(m.get("tps") or 0.0 for m in snap["models"]), 0)}'
         '<span class="tot"> tok/s</span>', "",
         (f'今日{tok_txt(snap.get("day_tok"))}，历史{tok_txt(snap.get("hist_tok"))}'
          if snap.get("day_tok") is not None else "待采集器更新")),
        ("统一内存", f'{fmt(snap["mem_used"],1)}'
         f'<span class="tot">/{fmt(snap["mem_total"],0)} G</span>', ""),
        ("磁盘", f"{fmt(dk_u,0)}<span class='tot'>/{fmt(dk_t,0)} G</span>",
         disk_cls),
    ]
    ch = []
    for item in cards:
        k, v, c = item[:3]
        sub = item[3] if len(item) > 3 else ""
        ch.append(f'<div class="card {c}"><div class="k">{k}</div>'
                  f'<div class="v">{v}</div>'
                  + (f'<div class="s">{sub}</div>' if sub else "") + "</div>")
    cardhtml = "".join(ch)

    # --- 温度曲线 (GPU + CPU 同图, 共用自适应 Y 轴)
    thtml = svg_lines([
        (heat.get("gpu_temp", []), "#e8590c", "GPU"),
        (heat.get("cpu_max", []), "#1c7ed6", "CPU"),
    ], unit="°C", ymin=None, xmax=now_ts, events=ev.get("gpu_temp"))
    # --- 利用率 / 内存 / SM 时钟 (GB10 时钟卡死前兆)
    uhtml = svg_lines([(heat.get("gpu_util", []), "#0ca678", "util")],
                      unit="%", ymin=0, ymax=100, xmax=now_ts,
                      events=ev.get("gpu_util"))
    mhtml = svg_lines([(heat.get("mem_used", []), "#7048e8", "mem")],
                      unit="G", xmax=now_ts, events=ev.get("mem_used"))
    sm_pts = heat.get("sm_mhz", [])
    sm_bands = clock_trap_bands(sm_pts, heat.get("gpu_util", []))
    # SM 时钟: 若强制 ymin=0, 2405~2411 的数据会被压成顶部一条平线,
    # 1200 危险线也永远贴底看不见。改为自适应 Y 轴, 但下限不高于危险线,
    # 这样"当前时钟离危险线有多远"始终可读。
    sm_lo = None
    if sm_pts:
        vals = [p[1] for p in sm_pts]
        sm_lo = min(min(vals) * 0.9, CLK_TRAP_MHZ - 100)
    smhtml = svg_lines(
        [(sm_pts, "#f59f00", "SM")], unit="MHz", ymin=sm_lo, xmax=now_ts,
        fill=False,
        hlines=[(CLK_TRAP_MHZ, "#e03131", f"{CLK_TRAP_MHZ} 危险线")],
        bands=sm_bands, events=ev.get("sm_mhz"))
    # --- 网络吞吐(物理网卡, 不含 lo/tailscale/docker 虚接口, 由 collector 侧过滤)
    nhtml = svg_lines([
        (heat.get("net_rx", []), "#1c7ed6", "下行"),
        (heat.get("net_tx", []), "#e8590c", "上行"),
    ], unit="MB/s", ymin=0, xmax=now_ts, events=ev.get("net"))

    # --- 每模型 TPS
    mser = ser.get("models", {})
    tps_blocks = []
    for svc, d in sorted(mser.items(), key=lambda kv: -max(
            [p[1] for p in kv[1]["tps"]] or [0])):
        pts = d.get("tps", [])
        if not pts:
            continue
        color = KIND_META.get(
            next((m["kind"] for m in snap["models"] if m["svc"] == svc), ""),
            ("", "#495057"))[1]
        top = max(p[1] for p in pts)
        # 全程为 0 说明服务空闲(非故障): 显式标注, 否则一条贴底直线看起来像坏了
        if top <= 0.0:
            tps_blocks.append(
                f'<div class="sp idle"><div class="sph"><b>{svc}</b>'
                f'<span class="peak">窗口内无生成（空闲）</span></div>'
                f'<div class="idleline">— 0 tok/s</div></div>')
            continue
        tps_blocks.append(
            f'<div class="sp"><div class="sph"><b>{svc}</b>'
            f'<span class="peak">peak {top:.1f} tok/s</span></div>'
            + svg_lines([(pts, color, svc)], h=112, unit="tok/s", ymin=0,
                        xmax=now_ts)
            + "</div>")
    if not tps_blocks:
        tps_blocks = ['<p class="muted">窗口内无 TPS 数据（vLLM 服务空闲或未暴露 '
                      '/metrics）。</p>']

    # --- 请求级速度（直方图 Δsum/Δcount 窗口平均, 每 svc 一条线）。
    #     dec_tps = 每请求 decode 速度(tok/s, 不含排队); ttft = 首 token 延迟(ms)。
    #     与上方聚合 TPS 的区别: 这里是"单个请求有多快", 那里是"整机吐字多快"。
    #     旧 edge 缓存可能没有新键, 一律 .get() 降级为无曲线。
    no_speed_hint = ('<p class="muted">暂无数据（collector 升级前的旧数据无此列；'
                     '窗口内需有已完成的请求）。</p>')
    dec_lines, ttft_lines = [], []
    for svc, d in sorted(mser.items()):
        color = KIND_META.get(
            next((m["kind"] for m in snap["models"] if m["svc"] == svc), ""),
            ("", "#495057"))[1]
        if d.get("dec_tps"):
            dec_lines.append((d["dec_tps"], color, svc))
        if d.get("ttft_ms"):
            ttft_lines.append((d["ttft_ms"], color, svc))
    dec_html = (svg_lines(dec_lines, unit="tok/s", ymin=0, xmax=now_ts)
                if dec_lines else no_speed_hint)
    ttft_html = (svg_lines(ttft_lines, unit="ms", ymin=0, xmax=now_ts)
                 if ttft_lines else no_speed_hint)
    # --- MTP 接受率(%): Δ接受草稿 / Δ草稿, 仅 TF/spec-decode 引擎有。
    #     掉得多 = 草稿质量或接受配比退化, decode 会跟着变慢。
    mtp_lines = []
    for svc, d in sorted(mser.items()):
        color = KIND_META.get(
            next((m["kind"] for m in snap["models"] if m["svc"] == svc), ""),
            ("", "#495057"))[1]
        if d.get("mtp_acc"):
            mtp_lines.append((d["mtp_acc"], color, svc))
    mtp_html = (svg_lines(mtp_lines, unit="%", ymin=0, ymax=100, xmax=now_ts)
                if mtp_lines else
                '<p class="muted">暂无数据（TF/spec-decode 引擎才有；窗口内需有'
                '带草稿的已完成请求）。</p>')

    # --- 模型占用表
    trows = []
    for m in snap["models"]:
        kind_cn, color = KIND_META.get(m["kind"], (m["kind"] or "?", "#495057"))
        # 三色状态: 红=端口掉线, 橙=脑死(端口在但 collector 真发 HTTP 探活失败),
        # 绿=探活通过。老数据 probe=None 时不判脑死。
        brain = bool(m["alive"]) and m.get("probe") == 0
        alive = ('<span class="dot off" title="端口未监听"></span>'
                 if not m["alive"] else
                 '<span class="dot warn" title="端口在但 HTTP 探活失败：'
                 '服务进程可能还活着，但已不响应请求（脑死）"></span>'
                 if brain else
                 '<span class="dot on" title="HTTP 探活通过"></span>')
        dead_tag = '<span class="dead">脑死</span>' if brain else ""
        tps = fmt(m["tps"], 1) if m["tps"] is not None else "—"
        # 活跃度: http=精确请求速率(vLLM 计数器差分), log=日志行数近似(兜底)
        act, ak = m.get("act"), m.get("act_kind")
        if act is None:
            act_html = "—"
            act_tip = ""
        elif ak == "http":
            act_html, act_tip = f"{act:.2f}/s", "精确: 引擎请求计数差分 (vLLM http_requests_total / TF e2e_count)"
        else:
            act_html, act_tip = f"≈{act:.1f}/s", "近似: 日志行数差分(无 HTTP 指标的服务)"
        # 请求级指标(旧数据无列为 None); 排队 >1s 说明算力饱和, 标黄
        qms = m.get("queue_ms")
        q_txt = "—"
        q_cls = "num"
        if qms is not None:
            q_txt = f"{qms/1000:.1f}s" if qms >= 1000 else f"{qms:.0f}ms"
            q_cls = "num warn" if qms >= 1000 else "num"
        # 窄屏(≤640px)下每行折叠成小卡, td::before 用 data-l 充当字段名
        trows.append(
            f'<tr class="{"" if m["alive"] else "dim"}">'
            f'<td class="c1">{alive} {m["svc"]}{dead_tag}</td>'
            f'<td class="c2"><span class="tag" style="--c:{color}">{kind_cn}</span></td>'
            f'<td class="c3 mono" data-l="模型">{m["model"] or "—"}</td>'
            f'<td class="c4 num" data-l="显存 MiB">{fmt(m["vram"],0)}</td>'
            f'<td class="c5 num" data-l="TPS">{tps}</td>'
            f'<td class="c6 num" data-l="活跃度" title="{act_tip}">{act_html}</td>'
            f'<td class="c7 num" data-l="KV">'
            f'{fmt(m["kv"],2) if m["kv"] is not None else "—"}</td>'
            f'<td class="c8 {q_cls}" data-l="排队" '
            f'title="窗口内已完成请求的平均排队(WAITING)时长">'
            f'{q_txt}</td>'
            f'<td class="c9 num" data-l="路" '
            f'title="运行中请求数(采样瞬时值; 短请求间歇期可能为 0)">'
            f'{fmt(m["req_run"], 0) if m.get("req_run") is not None else "—"}</td></tr>')
    table = ('<div class="tblwrap"><table class="mt"><thead><tr><th>服务</th>'
             '<th>类型</th><th>模型</th>'
             '<th class="num">显存 MiB</th><th class="num">TPS</th>'
             '<th class="num">活跃度</th><th class="num">KV</th>'
             '<th class="num">排队</th><th class="num">路</th></tr></thead><tbody>'
             + "".join(trows) + "</tbody></table></div>")

    # --- 健康卡片: 复现 watchdog 的阈值口径, 但只显示不推送
    #     (推送仍归 wecom-watchdog, 见 README「与 watchdog 的分工」)
    hs = h.get("health") or {}
    tot = hs.get("total") or 0
    sm_now = snap.get("sm_mhz")
    util_now = snap.get("gpu_util")
    trap_now = (sm_now is not None and util_now is not None
                and util_now >= CLK_TRAP_UTIL and sm_now < CLK_TRAP_MHZ)

    def hcell(label, val, level):
        return (f'<div class="hcell {level}"><div class="hk">{label}</div>'
                f'<div class="hv">{val}</div></div>')

    # 无新数据时(老行 NULL)显示 — 而不是误报"正常"
    no_sm = not sm_pts
    hcells = [
        hcell("时钟卡死", "命中中" if trap_now else (f'{hs.get("clock_trap",0)} 次'
              if not no_sm else "—"),
              "crit" if trap_now else ("warn" if hs.get("clock_trap") else "ok")),
        hcell("功耗封顶", (f'{hs.get("power_cap",0)} 次 · 累计 '
              f'{hs.get("cap_seconds",0):.0f}s') if hs.get("power_cap")
              else f'未发生 · 累计 {hs.get("cap_seconds",0):.0f}s',
              "warn" if hs.get("power_cap") else "ok"),
        hcell("高温 ≥85°", f'{hs.get("hot85",0)} 次',
              "crit" if hs.get("hot85") else "ok"),
        hcell("高温 ≥80°", f'{hs.get("hot80",0)} 次',
              "warn" if hs.get("hot80") else "ok"),
        hcell("内存 ≥112G", "命中" if hs.get("mem_high") else "正常",
              "crit" if hs.get("mem_high") else "ok"),
        hcell("SM 时钟", (f'{sm_now:.0f} MHz' if sm_now is not None else "—"),
              "crit" if trap_now else "ok"),
        hcell("NTP / 钟跳", (
                  ("同步" if snap.get("ntp_sync") == 1 else
                   "脱同步" if snap.get("ntp_sync") == 0 else "—")
                  + ("（无新列）" if snap.get("ntp_sync") is None else "")),
              "crit" if (snap.get("ntp_sync") == 0
                        or (snap.get("clock_off_ms") is not None
                            and abs(snap["clock_off_ms"]) >= CLOCK_DRIFT_MS))
              else "ok"),
    ]
    warn_n = sum(1 for c in hcells if 'class="hcell warn"' in c or 'class="hcell crit"' in c)
    health_html = (
        f'<div class="ct">健康检查 <span class="muted sm">窗口内 {tot} 个采样 · '
        f'{warn_n} 项命中 · 阈值与 wecom-watchdog 一致（只显示，推送见微信）</span></div>'
        f'<div class="hgrid">{"".join(hcells)}</div>')

    tot_vram = sum(m["vram"] or 0 for m in snap["models"])
    # edge 角色: peer 拉不通时用缓存渲染, 页顶挂红色横幅说明数据截止
    banner = ""
    if h.get("down"):
        banner = (f'<div class="downbanner">⚠ <b>本机当前失联</b>：edge 拉取失败'
                  f'（{(h.get("down") or "")[:120]}）。以下内容为最后一次成功拉取的缓存，'
                  f'采集时间 {age}s 前，不会更新。</div>')
    # 进程 top 榜(GB10 统一内存: 显存+常驻同池, 按合计排序)。数据来自
    # collector 每采样覆盖写的 meta('procs'); 老 collector 显示升级提示。
    procs = snap.get("procs") or []
    if procs:
        prows = []
        for p in procs:
            vr = f"{p['vram']/1024:.1f}G" if p.get("vram") else "—"
            rs = f"{p['rss']/1024:.1f}G" if p.get("rss") else "—"
            svc = (f'<span class="tag" style="--c:{KIND_META.get(next((m["kind"] for m in snap["models"] if m["svc"] == p["svc"]), ""), ("", "#868e96"))[1]}">{p["svc"]}</span>'
                   if p.get("svc") else '<span class="muted">—</span>')
            prows.append(
                f'<tr><td class="c1 mono" data-l="PID">{p["pid"]}</td>'
                f'<td class="mono" data-l="进程">{p["name"][:32]}</td>'
                f'<td data-l="归属">{svc}</td>'
                f'<td class="num" data-l="显存">{vr}</td>'
                f'<td class="num" data-l="常驻">{rs}</td></tr>')
        procs_html = ('<div class="tblwrap"><table class="mt"><thead><tr>'
                      '<th>PID</th><th>进程</th><th>归属服务</th>'
                      '<th class="num">显存</th><th class="num">常驻内存</th>'
                      '</tr></thead><tbody>' + "".join(prows) + "</tbody></table></div>")
    else:
        procs_html = ('<p class="muted">暂无数据（collector 升级前的旧数据无此栏；'
                      '升级后每个采样自动刷新）。</p>')

    # 电源按钮: 点击 -> 原生确认 -> 输入 token -> POST /api/power。
    # token 不落 localStorage, 每次手输, 误触成本=拒绝。
    # 唤醒由「另一台在线机器」代发 magic packet（本机若已关机收不到请求）。
    pbtn = (lambda a, t, cls, tip="":
            f'<button class="pbtn {cls}" data-h="{h.get("host")}" '
            f'data-a="{a}" title="{tip}">{t}</button>')
    power_btns = (f'<span class="pwrs">'
                  f'{pbtn("wake", "唤醒", "", "由另一台在线机器广播 magic packet；全部关机时无效")}'
                  f'{pbtn("reboot", "重启", "")}'
                  f'{pbtn("poweroff", "关机", "danger")}</span>')
    return f'''<section class="host">
  <h2 class="htitle">{name} {badge} <span class="muted sm">{cont_seg}采集 {age}s 前 · 模型显存合计 {tot_vram/1024:.1f} GiB</span>{power_btns}</h2>
  {banner}
  <div class="cards">{cardhtml}</div>
  <div class="charts">
    <div class="ch-wrap"><div class="ct">温度 GPU / CPU
      <span class="lg"><i style="background:#e8590c"></i>GPU
      <i style="background:#1c7ed6"></i>CPU</span></div>{thtml}</div>
    <div class="ch-wrap"><div class="ct">GPU 利用率</div>{uhtml}</div>
    <div class="ch-wrap"><div class="ct">SM 时钟
      <span class="lg">红线为 {CLK_TRAP_MHZ} MHz 危险线；红底=卡死区间</span></div>{smhtml}</div>
    <div class="ch-wrap"><div class="ct">统一内存占用</div>{mhtml}</div>
    <div class="ch-wrap"><div class="ct">网络吞吐
      <span class="lg"><i style="background:#1c7ed6"></i>下行
      <i style="background:#e8590c"></i>上行</span></div>{nhtml}</div>
  </div>
  {health_html}
  <div class="ct">各模型 TPS <span class="muted sm">聚合吞吐 = Δgeneration_tokens/Δt（全部并发请求合计）</span></div>
  <div class="tpsgrid">{"".join(tps_blocks)}</div>
    <div class="charts">
    <div class="ch-wrap"><div class="ct">请求级 decode 速度
      <span class="muted sm">每请求 tok/s · 直方图窗口均值 · 不含排队（TF 由 e2e−TTFT 反推）</span></div>{dec_html}</div>
    <div class="ch-wrap"><div class="ct">TTFT 首 token 延迟
      <span class="muted sm">ms · 直方图窗口均值</span></div>{ttft_html}</div>
    <div class="ch-wrap"><div class="ct">MTP 接受率
      <span class="muted sm">% · Δ接受/Δ草稿 · 掉落=草稿质量退化, decode 变慢</span></div>{mtp_html}</div>
  </div>
  <div class="ct">模型占用</div>
  {table}
  <div class="ct">进程 Top <span class="muted sm">GB10 统一内存：显存+常驻同池，按合计降序取前 12 ·
    归属=端口进程及其 EngineCore 子进程 / docker 容器名</span></div>
  {procs_html}
</section>'''


def render_page(hosts, win, sel=0):
    """hosts: 已聚合的节点列表; sel: 当前选中显示的节点下标。
    只渲染选中的那一台 —— 通过顶部「主机切换」而不是纵向瀑布流。"""
    if not hosts:
        hosts = [{"host": "?", "snapshot": None}]
    sel = max(0, min(sel, len(hosts) - 1))
    cur = hosts[sel]

    # 主机切换按钮: 显示名称 + 在线状态点, 点一下切到那台
    hbtns = []
    for i, h in enumerate(hosts):
        nm = h.get("label") or h.get("host", "?")
        snap = h.get("snapshot")
        on = bool(snap) and (int(time.time()) - snap["ts"] < 180)
        hbtns.append(
            f'<a class="hbtn {"act" if i == sel else ""}" href="?w={win}&amp;h={i}">'
            f'<span class="dot {"on" if on else "off"}"></span>{nm}</a>')
    htabs = "".join(hbtns)

    # 右上角「实时路数」: 当前选中主机各 vLLM 服务运行中请求数之和。
    # 恒渲染(离线显示 —), 否则 margin-left:auto 的推右效果会随 chip 有无漂移。
    # 并发 >0 时按类型拆分各是几路(采样瞬时值, 两个请求间空档会显示 0)。
    mdls = (cur.get("snapshot") or {}).get("models") or []
    by_kind = {}
    kinds_present = []
    for m in mdls:
        k = m.get("kind")
        if k in ("llm", "embed", "rerank") and k not in kinds_present:
            kinds_present.append(k)
        if m.get("req_run"):
            by_kind[k] = by_kind.get(k, 0) + m["req_run"]
    runs = sum(by_kind.values())
    if mdls:
        if runs:
            detail = " / ".join(f"{k} {by_kind.get(k, 0)}" for k in kinds_present)
            chip = f'<span class="chip">并发 {runs} 路（{detail}）</span>'
        else:
            chip = '<span class="chip">并发 0 路</span>'
    else:
        chip = '<span class="chip">并发 —</span>'

    wopts = "".join(
        f'<option value="{k}"{" selected" if k == win else ""}>{k}</option>'
        for k in WINDOWS)

    body = render_host(cur, win, True)

    return f'''<!DOCTYPE html><html lang="zh"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DGX Fleet</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='14' fill='%230f1622'/%3E%3Cpath d='M8 38h10l6-16 8 26 6-12 4 2h14' fill='none' stroke='%2337b24d' stroke-width='5' stroke-linecap='round' stroke-linejoin='round'/%3E%3Ccircle cx='50' cy='16' r='6' fill='%23e03131'/%3E%3C/svg%3E">
<meta http-equiv="refresh" content="60">
<style>
:root{{--bg:#f8f9fa;--fg:#212529;--mut:#767d86;--bd:#dee2e6;--card:#fff}}
@media (prefers-color-scheme:dark){{:root{{--bg:#16181c;--fg:#e9ecef;--mut:#9aa2ac;
--bd:#2b2f36;--card:#1d2025}}}}
*{{box-sizing:border-box}}
body{{margin:0;padding:20px 24px;background:var(--bg);color:var(--fg);
font:16px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans CJK SC",sans-serif}}
h1{{font-size:22px;margin:0 0 14px}}
.hint{{display:block;color:var(--mut);font-size:13px;margin:-6px 0 10px}}
h2{{font-size:19px;margin:0 0 14px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
.host{{background:var(--card);border:1px solid var(--bd);border-radius:12px;
padding:18px 22px;margin-bottom:14px}}
.htitle{{font-size:23px;margin:0 0 16px}}
/* 电源按钮: 默认低调(灰描边), hover 才显出危险色, 平时不诱导点击 */
.pwrs{{margin-left:auto;display:flex;gap:8px}}
.pbtn{{font-size:12.5px;padding:4px 12px;border:1px solid var(--bd);border-radius:8px;
background:var(--card);color:var(--mut);cursor:pointer}}
.pbtn:hover{{color:var(--fg);border-color:var(--mut)}}
.pbtn.danger:hover{{color:#fff;background:#e03131;border-color:#e03131}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;
margin-bottom:16px}}
.card{{background:var(--bg);border:1px solid var(--bd);border-radius:9px;padding:10px 13px}}
.card .k{{font-size:14px;color:var(--mut)}}
.card .v{{font-size:28px;font-weight:600;font-variant-numeric:tabular-nums;
margin-top:3px;letter-spacing:-.5px}}
.card.warn .v{{color:#e8590c}} .card.crit .v{{color:#e03131}}
.card .s{{font-size:12.5px;color:var(--mut);margin-top:2px;
font-variant-numeric:tabular-nums}}
.card .v .tot{{font-size:16px;font-weight:400;color:var(--mut)}}
/* 轨道上限 640: 图少时 (TPS 只有 1 个模型) auto-fit 会塌缩空列让 1fr 轨道
   撑满整行, 图被拉到 3~4 倍设计宽, 等比缩放的 SVG 高度同步爆长. 设上限后
   scale 锁在 0.68~1.15, 富余空间由 JS 字号补偿抹平. */
.charts{{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,640px));
gap:16px;justify-content:center}}
.ct{{font-size:15px;font-weight:600;color:var(--mut);margin:14px 0 6px}}
.ch{{width:100%;height:auto;display:block}}
.ch .grid{{stroke:var(--bd);stroke-width:.5;stroke-dasharray:2 3}}
/* viewBox 宽固定 560, 但网格轨道随屏宽变化 (auto-fit 塌缩空列 + 1fr 拉伸,
   scale=渲染宽/560 可远超 1), 字号不能写死。JS 实测每张图的 scale 后在 svg
   上设 --ch-fs* = 目标屏幕px/scale, 把字拉回设计尺寸; var 兜底为原值。 */
.ch .yl,.ch .xl,.ch .yu{{fill:var(--mut);font-size:var(--ch-fs,17px);text-anchor:end}}
.ch .xl{{text-anchor:middle}}
.ch .yu{{font-size:var(--ch-fs2,16px)}}
/* 描边同理: 用 non-scaling-stroke 让线宽按屏幕 px 渲染, 不随 viewBox 放大 */
.ch line,.ch polyline{{vector-effect:non-scaling-stroke}}
.tpsgrid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,640px));
gap:16px;justify-content:center}}
/* 健康卡片: 与 watchdog 阈值对齐的状态概览 */
.hgrid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(158px,1fr));gap:10px}}
.hcell{{border:1px solid var(--bd);border-radius:9px;padding:9px 13px;
background:var(--bg)}}
.hcell .hk{{font-size:13px;color:var(--mut)}}
.hcell .hv{{font-size:17px;font-weight:600;margin-top:3px;
font-variant-numeric:tabular-nums}}
.hcell.ok .hv{{color:#37b24d}}
.hcell.warn{{border-color:#e8590c55;background:#e8590c12}}
.hcell.warn .hv{{color:#e8590c}}
.hcell.crit{{border-color:#e0313155;background:#e0313112}}
.hcell.crit .hv{{color:#e03131}}
.ch .hlbl{{font-size:var(--ch-fs3,13px);font-weight:600}}
.sp{{border:1px solid var(--bd);border-radius:9px;padding:9px 12px;min-width:0}}
.sp .idleline{{height:112px;display:flex;align-items:center;justify-content:center;
color:var(--mut);font-size:15px;border-top:1px dashed var(--bd)}}
.sp.idle{{opacity:.75}}
.sph{{display:flex;justify-content:space-between;font-size:15px;margin-bottom:3px}}
.peak{{color:var(--mut);font-variant-numeric:tabular-nums}}
.tblwrap{{width:100%;overflow-x:auto}}
table.mt{{width:100%;border-collapse:collapse;font-size:15px;table-layout:fixed}}
table.mt th,table.mt td{{padding:9px 10px;border-bottom:1px solid var(--bd);
text-align:left;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
table.mt th{{color:var(--mut);font-weight:500;font-size:14px}}
table.mt .num{{text-align:right;font-variant-numeric:tabular-nums}}
table.mt .c1{{width:12%}} table.mt .c2{{width:10%}}
table.mt .c3{{width:14%}} table.mt .c4{{width:10%}}
table.mt .c5{{width:10%}} table.mt .c6{{width:12%}} table.mt .c7{{width:12%}}
table.mt .c8{{width:12%}} table.mt .c9{{width:8%}}
table.mt .num.warn{{color:#e8590c}}
table.mt th:nth-child(1){{width:14%}} table.mt th:nth-child(2){{width:10%}}
table.mt th:nth-child(3){{width:18%}} table.mt th:nth-child(4){{width:11%}}
table.mt th:nth-child(5){{width:10%}} table.mt th:nth-child(6){{width:12%}}
table.mt th:nth-child(7){{width:12%}} table.mt th:nth-child(8){{width:13%}}
.downbanner{{background:#e0313118;border:1px solid #e0313166;color:#c92a2a;
border-radius:9px;padding:9px 14px;margin:0 0 12px;font-size:15px}}
table.mt tr.dim{{opacity:.45}}
.mono{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:14px}}
.tag{{display:inline-block;padding:2px 9px;border-radius:11px;font-size:13px;
border:1px solid var(--c);color:var(--c)}}
.dot{{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:4px}}
.dot.on{{background:#37b24d}} .dot.off{{background:#e03131}}
.dot.warn{{background:#e8590c}}
.dead{{color:#e03131;font-size:12px;font-weight:600;margin-left:4px}}
/* 窄屏: 模型占用表 9 列折成「一行一张小卡」——服务名当卡头、类型 tag 右上角,
   模型名单占一行, 其余数字两两成对, 字段名由 td 的 data-l 经 ::before 补上 */
@media (max-width:640px){{
  body{{padding:12px}}
  .tblwrap{{overflow:visible}}
  table.mt,table.mt tbody{{display:block;width:100%}}
  table.mt thead{{display:none}}
  table.mt tr{{display:grid;grid-template-columns:1fr 1fr;gap:1px 14px;
    background:var(--bg);border:1px solid var(--bd);border-radius:10px;
    padding:9px 12px;margin-bottom:9px}}
  table.mt td{{display:flex;align-items:baseline;justify-content:space-between;
    border:0;padding:3px 0;font-size:13.5px;white-space:normal;overflow:visible}}
  /* 桌面列宽 .c1{{width:14%}} 等是 类+元素 特异性, 会压过上面的 td 规则,
     必须用同等或更高的「td.cX」选择器把百分比宽度清掉, 否则数字被裁没 */
  table.mt td.c1,table.mt td.c2,table.mt td.c3,table.mt td.c4,
  table.mt td.c5,table.mt td.c6,table.mt td.c7,table.mt td.c8,
  table.mt td.c9{{width:auto}}
  table.mt td.num{{text-align:right}}
  table.mt .tag{{flex:none}}
  table.mt td::before{{content:attr(data-l);color:var(--mut);
    font-size:12.5px;margin-right:10px;flex:none}}
  table.mt td.c1{{grid-column:1/2;font-weight:600;font-size:14.5px}}
  table.mt td.c1::before{{content:none}}
  table.mt td.c2{{grid-column:2/3;justify-content:flex-end}}
  table.mt td.c2::before{{content:none}}
  table.mt td.c3{{grid-column:1/-1;border-bottom:1px solid var(--bd);
    padding-bottom:6px;margin-bottom:2px}}
}}
.badge{{font-size:13px;padding:2px 10px;border-radius:11px;font-weight:400}}
.badge.on{{background:#37b24d22;color:#2b8a3e}}
.badge.off{{background:#e0313122;color:#c92a2a}}
.muted{{color:var(--mut);font-weight:400}} .sm{{font-size:14px}}
.tabs{{display:flex;gap:8px;margin:0 0 12px;flex-wrap:wrap}}
.tab{{padding:5px 15px;border:1px solid var(--bd);border-radius:8px;
text-decoration:none;color:var(--fg);font-size:15px;background:var(--card)}}
.tab.act{{background:#1c7ed6;color:#fff;border-color:#1c7ed6}}
.hbtn{{display:inline-flex;align-items:center;padding:7px 18px;border:1px solid var(--bd);
border-radius:9px;text-decoration:none;color:var(--fg);font-size:17px;font-weight:600;
background:var(--card)}}
.hbtn.act{{background:#1c7ed6;color:#fff;border-color:#1c7ed6}}
.hbtn.act .dot{{background:#fff}}
.hrow{{display:flex;gap:10px;margin:0 0 12px;flex-wrap:wrap}}
.wsel{{margin-left:8px;font:inherit;font-size:14px;color:var(--fg);
background:var(--card);border:1px solid var(--bd);border-radius:8px;
padding:5px 8px;cursor:pointer}}
.chip{{margin-left:auto;padding:6px 12px;font-size:13px;color:var(--mut);
background:var(--card);border:1px solid var(--bd);border-radius:999px;
font-variant-numeric:tabular-nums}}
.lg{{font-weight:400;color:var(--mut);margin-left:10px;font-size:14px}}
.lg i{{display:inline-block;width:12px;height:3px;vertical-align:middle;
margin:0 4px 0 10px;border-radius:2px}}
.tip{{position:fixed;pointer-events:none;background:var(--card);color:var(--fg);
border:1px solid var(--bd);border-radius:7px;padding:7px 11px;font-size:14px;
box-shadow:0 2px 12px #0003;display:none;z-index:99;white-space:nowrap;
font-variant-numeric:tabular-nums;line-height:1.5}}
</style></head><body>
<div class="hrow">{htabs}{chip}<select class="wsel" onchange="var m=location.search.match(/[?&]h=([0-9]+)/);location='?w='+this.value+(m?'&amp;h='+m[1]:'')">{wopts}</select></div>
{body}
<div class="tip" id="tip"></div>
<script>
// viewBox 内文字/圆点会随「渲染宽/560」整体缩放; 实测 scale 后把目标屏幕 px
// 换算回用户单位 (13/12/10px 对应设计稿在 ~425px 宽下的观感)。resize 时重算。
(function(){{
  function fit(){{
    document.querySelectorAll('svg.ch').forEach(function(svg){{
      var vb=svg.viewBox.baseVal;
      if(!vb || !vb.width) return;
      var k=svg.clientWidth/vb.width;
      if(!(k>0)) return;
      var t=function(px){{ return (px/k).toFixed(2)+'px'; }};
      svg.style.setProperty('--ch-fs', t(13));   // 刻度/时间标签
      svg.style.setProperty('--ch-fs2', t(12));  // 单位标注
      svg.style.setProperty('--ch-fs3', t(10));  // 参考线文字
      svg.querySelectorAll('.hd').forEach(function(c){{
        c.setAttribute('r', (2.6*0.76/k).toFixed(2));  // hover 圆点 ~2px
      }});
      svg.querySelectorAll('.edot').forEach(function(c){{
        c.setAttribute('r', (3.2/k).toFixed(2));       // 突变记号屏幕恒定 ~3.2px
      }});
    }});
  }}
  fit();
  addEventListener('resize', fit);
  // 容器变化 (侧栏折叠、缩放、字体迟到) 不一定伴随 window.resize, RO 兜底
  if (typeof ResizeObserver !== 'undefined') {{
    var raf = 0;
    var ro = new ResizeObserver(function(){{
      cancelAnimationFrame(raf); raf = requestAnimationFrame(fit);
    }});
    document.querySelectorAll('svg.ch').forEach(function(s){{ ro.observe(s); }});
  }}
}})();
(function(){{
  var tip = document.getElementById('tip');
  // 用归一化坐标 (viewBox 空间) 找最近数据点
  function nearest(pts, x){{
    var lo=0, hi=pts.length-1;
    while(hi-lo>1){{ var m=(lo+hi)>>1; if(pts[m][0]<x) lo=m; else hi=m; }}
    return (Math.abs(pts[lo][0]-x) <= Math.abs(pts[hi][0]-x)) ? pts[lo] : pts[hi];
  }}
  document.querySelectorAll('svg.ch').forEach(function(svg){{
    var meta=svg.querySelector('.cdata');
    if(!meta) return;
    var D; try{{ D=JSON.parse(meta.textContent); }}catch(e){{ return; }}
    if(!D.pts || !D.pts.length) return;
    var hx=svg.querySelector('.hx'), hds=svg.querySelectorAll('.hd');
    var vb=svg.viewBox.baseVal;
    var X0=+svg.dataset.x0, X1=+svg.dataset.x1, LO=+svg.dataset.lo, HI=+svg.dataset.hi;
    var L=+svg.dataset.l, R=+svg.dataset.r, T=+svg.dataset.t, B=+svg.dataset.b;
    var W=vb.width, H=vb.height, pw=W-L-R, ph=H-T-B, unit=svg.dataset.unit||'';
    if(X1<=X0) X1=X0+1;
    function fT(t){{ var d=new Date(t*1000);
      return String(d.getMonth()+1).padStart(2,'0')+'-'+
             String(d.getDate()).padStart(2,'0')+' '+
             String(d.getHours()).padStart(2,'0')+':'+
             String(d.getMinutes()).padStart(2,'0'); }}
    svg.addEventListener('mousemove', function(ev){{
      var r=svg.getBoundingClientRect();
      var px=(ev.clientX-r.left)/r.width*W;         // -> viewBox 空间
      if(px<L||px>W-R){{ svg.dispatchEvent(new Event('mouseleave')); return; }}
      var t=X0+(px-L)/pw*(X1-X0);
      var rows=[], best=null;
      D.pts.forEach(function(pts,i){{
        if(!pts.length) return;
        var p=nearest(pts,t);
        var y=T+(1-(p[1]-LO)/(HI-LO))*ph;
        best=best||p;
        rows.push('<span style="color:'+D.colors[i]+'">&#9632;</span> '+
                  (D.names[i]||'')+' '+p[1].toFixed(1)+unit);
        var d=hds[i]; if(d){{ d.setAttribute('cx', (L+(p[0]-X0)/(X1-X0)*pw).toFixed(1));
                             d.setAttribute('cy', y.toFixed(1)); d.style.display=''; }}
      }});
      // 鼠标靠近突变归因点(±6 viewBox 单位)时浮层追加「谁在用」
      if(D.ev){{ D.ev.forEach(function(e){{
        var ex=L+(e[0]-X0)/(X1-X0)*pw;
        if(Math.abs(px-ex)<6) rows.push('<span style="color:#fab005">&#9650;</span> '+e[1]);
      }}); }}
      if(!rows.length) return;
      hx.setAttribute('x1', (L+(best[0]-X0)/(X1-X0)*pw).toFixed(1));
      hx.setAttribute('x2', hx.getAttribute('x1'));
      hx.style.display='';
      tip.innerHTML=fT(best[0])+'<br>'+rows.join('<br>');
      tip.style.display='block';
      var tw=tip.offsetWidth;
      tip.style.left=Math.min(ev.clientX+12, innerWidth-tw-8)+'px';
      tip.style.top=(ev.clientY-8)+'px';
    }});
    svg.addEventListener('mouseleave', function(){{
      tip.style.display='none'; hx.style.display='none';
      hds.forEach(function(d){{ d.style.display='none'; }});
    }});
  }});
}})();
// 电源按钮: 确认 -> 手输 token -> POST /api/power(edge 转发; 唤醒由在线的那台代发)
(function(){{
  document.querySelectorAll('.pbtn').forEach(function(b){{
    b.addEventListener('click', function(){{
      var a = b.dataset.a, hk = b.dataset.h;
      var verb = a === 'reboot' ? '重启' : (a === 'wake' ? '唤醒' : '关机');
      var ask = a === 'wake'
        ? '确认唤醒 ' + hk + '？（由另一台在线机器广播唤醒包；全部关机时无效）'
        : '确认对 ' + hk + ' 执行' + verb + '？推理服务会立即中断。';
      if (!confirm(ask))
        return;
      var tok = prompt('输入电源操作 token（错误则拒绝，连输错 3 次会触发限频）：');
      if (tok === null || tok === '') return;
      b.disabled = true;
      // 相对路径: 页面挂在 nginx /fleet/ 前缀下, 写死 '/api/power' 会打到根域
      fetch('api/power', {{
        method: 'POST', headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify({{host: hk, action: a, token: tok}})
      }}).then(function(r){{
        return r.json().catch(function(){{ return {{}}; }});
      }}).then(function(j){{
        alert(j.err ? (verb + '失败：' + j.err)
                    : (a === 'wake' ? (j.note || '唤醒包已发出')
                                    : (verb + '已执行。' + (j.note || ''))));
      }}).catch(function(e){{ alert('请求失败：' + e); }})
      .finally(function(){{ b.disabled = false; }});
    }});
  }});
}})();
</script>
</body></html>'''


# ---------------------------------------------------------------- 电源操作
# 页面「关机/重启/唤醒」按钮 -> POST /api/power {action, token[, host]}。
# edge(VPS): 校验自己的 token 后把同样请求转发给 peer; peer(DGX): 校验
# ~/fleetdash/power.token 后 `sudo -n systemctl <action>` 延迟 1.2s 执行
# (先回 202 再断电, 让浏览器拿到结果)。token 文件缺失 = 端点关闭,
# 全部机器共用同一个随机 token(部署时 openssl rand -hex 16 生成一次)。
POWER_TOKEN_FILE_EDGE = "/etc/fleetdash/power.token"
POWER_OK = ("poweroff", "reboot", "wake")
POWER_VERB = {"poweroff": "关机", "reboot": "重启", "wake": "唤醒"}

# Wake-on-LAN: magic packet 必须从目标的同一二层广播域发出。edge 在公网,
# 跨不过 192.168.1.0/24, 所以「唤醒」由另一台同网段 DGX 代发:
#   页面 -> edge -> 在线的那台 peer -> 广播 magic packet -> 目标网卡。
# 全部关机时无人可代发, 唤醒不成立(除非路由器自带 WoL)。
# 本机 MAC 直接读 /sys(换网卡也不会过期), 对端 MAC 只能写死在这。
WOL_MACS = {"4c47": "4c:bb:47:7d:4c:47", "53fa": "4c:bb:47:7d:53:fa",
            "dbdd": "4c:bb:47:7e:db:dd"}
WOL_IFACE = "enP7s7"                       # 三台唯一的有线上行(10GbE RJ45)
WOL_BCAST = ("192.168.1.255", "255.255.255.255")
WOL_PORTS = (9, 7)                         # 标准 9, 部分固件认 7


def wol_mac(host):
    if host == host_key():
        try:
            with open(f"/sys/class/net/{WOL_IFACE}/address") as f:
                return f.read().strip()
        except OSError:
            pass
    return WOL_MACS.get(host, "")


def wol_send(mac):
    """广播 magic packet(6x FF + MAC x16)。纯 UDP 发包, 不需要 root。
    返回实际发出的包数(0 = 全部失败)。"""
    m = bytes.fromhex(mac.replace(":", "").replace("-", ""))
    if len(m) != 6:
        return 0
    pkt = b"\xff" * 6 + m * 16
    n = 0
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        for b in WOL_BCAST:
            for p in WOL_PORTS:
                for _ in range(2):        # 每目标发 2 遍, 防单包丢
                    try:
                        s.sendto(pkt, (b, p))
                        n += 1
                    except OSError:
                        pass
    finally:
        s.close()
    return n


def power_token_file():
    env = os.environ.get("FLEETDASH_POWER_TOKEN")
    if env:
        return env
    return (POWER_TOKEN_FILE_EDGE if ROLE == "edge"
            else os.path.expanduser("~/fleetdash/power.token"))


def power_token_now():
    try:
        with open(power_token_file()) as f:
            return f.read().strip()
    except OSError:
        return ""


# 暴破护栏: token 连错 3 次锁 5 分钟(进程级, edge/peer 各自计数)。
# 页面 JS 的提示文案与此对齐; 成功一次即清零。
PWR_FAIL_TH = 3
PWR_LOCK_SEC = 300
_pwr = {"fails": 0, "until": 0.0}
_pwr_lock = threading.Lock()


def pwr_locked():
    with _pwr_lock:
        return time.time() < _pwr["until"]


def pwr_fail():
    with _pwr_lock:
        _pwr["fails"] += 1
        if _pwr["fails"] >= PWR_FAIL_TH:
            _pwr["until"] = time.time() + PWR_LOCK_SEC
            _pwr["fails"] = 0
            return True     # 本次已触发锁定
    return False


def pwr_ok():
    with _pwr_lock:
        _pwr["fails"] = 0


# ---------------------------------------------------------------- http
def _sel_by_name(hosts, name):
    """?hn=4c47 —— 按主机名/标签定位到那一台(告警推送里的深链用)。
    找不到返回 None, 调用方保持原下标。"""
    if not name:
        return None
    for i, h in enumerate(hosts):
        if name in (h.get("host"), h.get("label")):
            return i
    return None


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        import hmac
        from urllib.parse import urlparse
        if urlparse(self.path).path != "/api/power":
            self._send(404, "not found", "text/plain")
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            self._send(400, json.dumps({"err": "bad json"}),
                       "application/json")
            return
        action = str(body.get("action") or "")
        if action not in POWER_OK:
            self._send(400, json.dumps({"err": f"bad action {action!r}"}),
                       "application/json")
            return
        verb = POWER_VERB[action]
        tok = str(body.get("token") or "")
        want = power_token_now()
        if not want:      # fail-closed: 没有 token 文件就没有该功能
            self._send(503, json.dumps({"err": "电源操作未启用（token 文件缺失）"}),
                       "application/json")
            return
        if pwr_locked():
            self._send(429, json.dumps({"err": "尝试过多，锁定 5 分钟"}),
                       "application/json")
            return
        if not hmac.compare_digest(tok, want):
            just_locked = pwr_fail()
            msg = "token 不对，锁定 5 分钟" if just_locked else "token 不对"
            print(f"[fleetdash] 电源 token 校验失败: {self.client_address[0]}",
                  file=sys.stderr, flush=True)
            self._send(403 if not just_locked else 429,
                       json.dumps({"err": msg}), "application/json")
            return
        pwr_ok()
        if action == "wake":
            # 唤醒不走 systemctl: 需要的是「同网段的一台机器广播 magic packet」。
            # 目标关机时它自己收不到 HTTP, 所以 edge 挑一台在线的同网段机器代发。
            tgt = str(body.get("target") or body.get("host") or "")
            if ROLE == "edge":
                if tgt not in PEERS:
                    self._send(400, json.dumps({"err": f"未知主机 {tgt}"}),
                               "application/json")
                    return
                with EDGE_LOCK:
                    tgt_st = (EDGE.get(tgt) or {}).get("state")
                    cand = [k for k in PEERS if k != tgt
                            and (EDGE.get(k) or {}).get("state") == "UP"]
                if tgt_st == "UP":
                    # 在线机器收到 magic packet 是空操作, 而且"卡死"也唤不醒。
                    # 直接拒绝, 免得推一条看着像事故的通知出去。
                    self._send(409, json.dumps(
                        {"err": f"{LABEL.get(tgt, tgt)} 当前在线，无需唤醒"
                                "（WoL 只对完全关机/休眠有效；若它其实已卡死，"
                                "唤醒无效，需现场处理）"}),
                        "application/json")
                    return
                if not cand:
                    self._send(502, json.dumps(
                        {"err": "局域网侧没有在线机器可代发（全部关机时无法远程唤醒）"}),
                        "application/json")
                    return
                sender = cand[0]
                req = urllib.request.Request(
                    PEERS[sender] + "/api/power",
                    data=json.dumps({"action": "wake", "token": tok,
                                     "target": tgt}).encode(),
                    headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(req, timeout=8) as r:
                        code, resp = r.getcode(), r.read().decode("utf-8", "replace")
                except urllib.error.HTTPError as e:
                    code, resp = e.code, e.read().decode("utf-8", "replace")
                except Exception as e:
                    self._send(502, json.dumps({"err": f"代发机不可达: {e}"}),
                               "application/json")
                    return
                if code == 202:
                    nm, snm = LABEL.get(tgt, tgt), LABEL.get(sender, sender)
                    print(f"[edge] 唤醒: {nm} 由 {snm} 代发 magic packet", flush=True)
                    # 文案先声明"这是操作留痕不是故障告警", 否则在企微里容易被读成事故。
                    push_wecom("🔆 手动唤醒（页面发起 · 操作留痕，非故障）",
                               [f"DGX **{nm}** 处于离线状态，页面发起了唤醒。",
                                f"- 唤醒包已由 **{snm}** 在其网段广播（8 包）。",
                                "- WoL 只对「完全关机/休眠」有效；2 分钟内仍未上线，"
                                "需现场按电源键。"])
                    self._send(200, json.dumps(
                        {"ok": True, "note": f"唤醒包已由 {snm} 发往 {nm}"}),
                        "application/json")
                else:
                    self._send(code, resp, "application/json")
                return
            # peer/master: 本机就在同一网段, 自己代发
            mac = wol_mac(tgt)
            if not mac:
                self._send(400, json.dumps({"err": f"没有 {tgt} 的 MAC 记录"}),
                           "application/json")
                return
            n = wol_send(mac)
            print(f"[fleetdash] 唤醒: 向 {tgt} ({mac}) 广播 {n} 个 magic packet",
                  flush=True)
            self._send(202 if n else 502,
                       json.dumps({"ok": bool(n),
                                   "note": f"已向 {tgt} 广播 {n} 个唤醒包"
                                           "（仅对完全关机/休眠有效）"}),
                       "application/json")
            return
        if ROLE == "edge":
            hk = str(body.get("host") or "")
            url = PEERS.get(hk)
            if not url:
                self._send(400, json.dumps({"err": f"未知主机 {hk}"}),
                           "application/json")
                return
            req = urllib.request.Request(
                url + "/api/power",
                data=json.dumps({"action": action, "token": tok}).encode(),
                headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=8) as r:
                    code, resp = r.getcode(), r.read().decode("utf-8", "replace")
            except urllib.error.HTTPError as e:
                code, resp = e.code, e.read().decode("utf-8", "replace")
            except Exception as e:
                self._send(502, json.dumps({"err": f"对端不可达: {e}"}),
                           "application/json")
                return
            if code == 202:
                nm = LABEL.get(hk, hk)
                # 审计留痕: 操作本身 + 企微推送(聊天记录就是操作台账)
                print(f"[edge] 电源操作: {nm} {verb} (页面发起)", flush=True)
                push_wecom("🔌 手动电源操作",
                           [f"DGX **{nm}** 已被页面发起 **{verb}**。",
                            "- 预期：该机随后进入失联；重启请注意恢复通知。"])
                self._send(200, json.dumps(
                    {"ok": True, "note": f"{nm} {verb}指令已下达"}),
                    "application/json")
            else:
                self._send(code, resp, "application/json")
            return
        # peer 侧(master/slave): 本机执行。延迟 1.2s, 先让响应发出去
        print(f"[fleetdash] 电源操作: 本机 {verb} (token 校验通过)", flush=True)

        def _go():
            time.sleep(1.2)
            try:
                subprocess.run(["sudo", "-n", "systemctl", action],
                               capture_output=True, timeout=30)
            except Exception as e:
                print(f"[fleetdash] 电源操作执行失败: {e}", file=sys.stderr,
                      flush=True)
        threading.Thread(target=_go, daemon=True).start()
        self._send(202, json.dumps({"ok": True, "note": f"{verb}指令已下达"}),
                   "application/json")

    def _send(self, code, body, ctype="text/html; charset=utf-8"):
        b = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        win = qs.get("w", ["3d"])[0]
        if win not in WINDOWS:
            win = "3d"
        try:
            sel = int(qs.get("h", ["0"])[0])
        except ValueError:
            sel = 0
        hn = qs.get("hn", [""])[0]      # 按主机名定位(告警深链 ?w=1d&hn=4c47)

        if u.path == "/api/agg":
            w = qs.get("w", ["3d"])[0]
            if w not in WINDOWS:
                w = "3d"
            self._send(200, json.dumps(agg(WINDOWS[w]), ensure_ascii=False, default=str),
                       "application/json")
            return
        if u.path == "/api/health":
            body = {"ok": True, "role": ROLE}
            if ROLE == "edge":
                body["peers"] = {
                    k: ((EDGE[k]["state"] or "INIT")
                        + ("+STALE" if EDGE[k].get("stale") else ""))
                    for k in EDGE}
                body["svc_bad"] = sorted(
                    f"{k}/{s}" for (k, s), v in SVC_STATE.items() if v.get("bad"))
            self._send(200, json.dumps(body), "application/json")
            return
        if u.path not in ("/", "/index.html"):
            self._send(404, "not found", "text/plain")
            return

        if ROLE == "edge":
            hosts = edge_hosts(win)
            i = _sel_by_name(hosts, hn)
            self._send(200, render_page(hosts, win, sel if i is None else i))
            return
        if ROLE != "master":
            self._send(200, "<h1>fleetdash slave</h1><p>用 <code>/api/agg</code> 取数；"
                            "页面由主节点渲染。</p>")
            return

        me = agg(WINDOWS[win])
        me["label"] = LABEL.get(host_key(), host_key())
        hosts = [me]
        for key, url in PEERS.items():
            try:
                with urllib.request.urlopen(f"{url}/api/agg?w={win}", timeout=4) as r:
                    d = json.loads(r.read().decode())
                d["label"] = LABEL.get(key, key)
                hosts.append(d)
            except Exception as e:
                hosts.append({"host": key, "label": LABEL.get(key, key),
                              "snapshot": None, "error": str(e)})
        i = _sel_by_name(hosts, hn)
        self._send(200, render_page(hosts, win, sel if i is None else i))


ROLE = "master"


def main():
    global ROLE, PEERS, LABEL, DB
    global POLL_SEC, FAIL_TH, STALE_SEC, CACHE_DIR, WEBHOOK, REPORT_AT, STATE_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=["master", "slave", "edge"], default="master")
    ap.add_argument("--label", default=None, help="本节点显示名")
    ap.add_argument("--peers", action="append", default=[],
                    help="key=url, 如 53fa=http://100.107.85.39:9100，可多次")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--bind", default=None)
    # edge 专属
    ap.add_argument("--poll", type=int, default=POLL_SEC, help="轮询秒数")
    ap.add_argument("--fail-th", type=int, default=FAIL_TH, help="连续失败多少次判 DOWN")
    ap.add_argument("--stale-sec", type=int, default=STALE_SEC,
                    help="拉通但快照超过此秒数 -> 数据停滞告警")
    ap.add_argument("--cache-dir", default=None, help="失联缓存目录, 默认 <db 同级>/edge-cache")
    ap.add_argument("--webhook-file", default=WEBHOOK_FILE)
    ap.add_argument("--report-at", default=REPORT_AT, help="日报时间 HH:MM, 空串关闭")
    ap.add_argument("--test-push", action="store_true", help="发一条测试推送后退出")
    a = ap.parse_args()

    ROLE = a.role
    DB = a.db
    # 告警闩锁落盘路径(所有角色通用, 与 DB 同目录): 重启不清零, 避免把旧事件
    # 当新事件重推 —— 这是 2026-09-17 那条"9:30 高温告警"的真正来源。
    STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(DB)),
                              "push-state.json")
    state_load()
    if a.port is None:
        a.port = 9101 if ROLE == "edge" else 9100
    if a.bind is None:
        # edge 只服务 VPS 本机 nginx; master/slave 需要 tailnet 内可达
        a.bind = "127.0.0.1" if ROLE == "edge" else "0.0.0.0"

    # 节点标识: 用 --label 或 hostname 匹配
    import socket
    hn = socket.gethostname()
    key = ("4c47" if "4c47" in hn else "53fa" if "53fa" in hn
           else "dbdd" if "dbdd" in hn else hn)
    LABEL = {key: a.label or key}
    for p in a.peers:
        k, _, v = p.partition("=")
        PEERS[k] = v.rstrip("/")
        LABEL.setdefault(k, k)

    if ROLE == "edge":
        if not PEERS:
            print("edge 需要至少一个 --peers key=http://...", file=sys.stderr)
            return 1
        POLL_SEC = a.poll
        FAIL_TH = a.fail_th
        STALE_SEC = a.stale_sec
        CACHE_DIR = a.cache_dir or os.path.join(
            os.path.dirname(os.path.abspath(DB)), "edge-cache")
        WEBHOOK = resolve_webhook(a.webhook_file)
        REPORT_AT = (a.report_at or "").strip()
        for k in PEERS:
            EDGE[k] = {"state": None, "fails": 0, "last_err": "",
                       "last_ok": None, "last_agg": None, "down_since": None,
                       "stale": False}
            # 闩锁接到落盘的那份 dict 上(见 STATE_PATH 注释)
            EDGE[k]["hw"] = HW_FIRED.setdefault(k, {})
        load_caches()
        if a.test_push:
            ok = push_wecom("🧪 fleetdash-edge 测试",
                            ["这是一条来自 edge 观察者的测试消息。",
                             f"- peers: {', '.join(PEERS)}",
                             f"- webhook 来源: "
                             f"{'env' if os.environ.get('FLEETDASH_WEBHOOK') else a.webhook_file}"])
            print("推送成功" if ok else "推送失败, 检查 webhook/网络", flush=True)
            return 0 if ok else 1
        threading.Thread(target=poller_loop, daemon=True).start()
        if not WEBHOOK:
            print("[edge] 警告: webhook 未配置, 告警只写日志不推送", file=sys.stderr)

    os.environ["FLEETDASH_HOST"] = key
    if a.bind in (None, "", "0.0.0.0", "::"):
        # 通配绑定 → 双栈: 一个 :: 套接字同时吃 v4/v6。
        # 否则浏览器把 localhost 解析成 ::1 时会直接连不上(Windows 尤甚)。
        class DualStack(ThreadingHTTPServer):
            address_family = socket.AF_INET6

            def server_bind(self):
                self.socket.setsockopt(socket.IPPROTO_IPV6,
                                       socket.IPV6_V6ONLY, 0)
                super().server_bind()
        srv = DualStack(("::", a.port), H)
        bind_note = f"*:{a.port} (v4+v6)"
    else:
        srv = ThreadingHTTPServer((a.bind, a.port), H)
        bind_note = f"{a.bind}:{a.port}"
    print(f"[fleetdash] role={ROLE} host={key} db={DB} "
          f"peers={list(PEERS)} bind={bind_note}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
