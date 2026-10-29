"""数据层：快照 / 时序 / 突变归因 / 统计 / 聚合（agg 即 /api/agg 载荷）。"""
import json
import os
import subprocess
import sys
import time

from . import config as cfg
from .db import q, sample_interval, host_key
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
    yuan = kwh * cfg.ELEC_PRICE
    k = f"{kwh:.1f}" if kwh < 100 else f"{kwh:.0f}"
    y = f"{yuan:.1f}" if yuan < 100 else f"{yuan:.0f}"
    return f"本月 {k} kWh ≈ ¥{y}"


# util 高但 SM 时钟掉到 1200 MHz 以下 -> 社区 t/370304 病征, 需拔电冷启。



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
        (cfg.CLK_TRAP_UTIL, cfg.CLK_TRAP_MHZ, int(sample_interval() * 0.9 * 1e6),
         cfg.TEMP_WARN_C, cfg.HOT_C, cfg.MEM_HIGH_GIB, since))
    # edge 本机没有 sample 库(或老库无表)时 q 返回 [] —— 降级为全 0, 不 500
    (trap, cap, hot85, hot80, mem, total, cap_us) = rows[0] if rows else (None,) * 7
    return {
        "clock_trap": trap or 0, "power_cap": cap or 0,
        "hot85": hot85 or 0, "hot80": hot80 or 0, "mem_high": mem or 0,
        "total": total or 0,
        "cap_seconds": round((cap_us or 0) / 1e6, 1),
    }





def downsample(rows, maxpts=cfg.MAXPTS):
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
    """返回 heat{metric: [[ts,val],...]} 与 models{svc: {metric: [...]}}。

    heat["tok"] = 每个采样周期内新增生成的 token 数(抽稀前差分)。
    models 里 req_run=并发, act_rate=请求速率。
    """
    now = int(time.time())
    since = now - seconds
    srows = q("SELECT ts,gpu_temp,cpu_max,gpu_util,mem_used,gpu_pw,sm_mhz,cap_delta_us,"
              "disk_used,net_rx_mbs,net_tx_mbs,day_tok,mem_total FROM sample WHERE ts>=?"
              " ORDER BY ts", (since,))
    # token 增量必须在抽稀前按相邻采样求差; 跨天 day_tok 归零(变小)按当日已生成计。
    tok_pts = []
    prev = None
    for r in srows:
        cur = r[11] if len(r) > 11 else None
        if cur is not None:
            if prev is not None:
                d = cur - prev
                tok_pts.append([r[0], cur if d < 0 else d])
            prev = cur
    mem_ts = {r[0]: r[4] for r in srows}     # ts -> 宿主内存 used(G)，堆叠残差列用
    mem_tot_ts = {r[0]: (r[12] if len(r) > 12 else None) for r in srows}
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
        "tok":      downsample(tok_pts),
    }
    # 每服务时序(含并发 req_run / 请求速率 act_rate), 按 svc 分组
    mrows = q("SELECT ts,svc,tps,vram,dec_tps,ttft_ms,queue_ms,mtp_acc,req_run,"
              "act_rate FROM model WHERE ts>=? ORDER BY ts", (since,))
    by_svc = {}
    MK = ("tps", "vram", "dec_tps", "ttft_ms", "queue_ms",
          "mtp_acc", "req_run", "act_rate")
    for ts, svc, tps, vram, dct, ttft, qms, macc, rr, act in mrows:
        by_svc.setdefault(svc, {k: [] for k in MK})
        for key, val in (("tps", tps), ("vram", vram), ("dec_tps", dct),
                         ("ttft_ms", ttft), ("queue_ms", qms),
                         ("mtp_acc", macc), ("req_run", rr), ("act_rate", act)):
            if val is not None:
                by_svc[svc][key].append([ts, val])
    for svc in by_svc:
        for key in MK:
            by_svc[svc][key] = downsample(by_svc[svc][key])

    # vram 堆叠时序(内存柱状图用): 按采样 ts 对齐, 整条时间线一次性抽稀。
    # 各 svc 时序是各自独立抽稀的, ts 可能对不上, 堆叠必须走这份对齐数据。
    # 单位统一成 GiB(vram 表里是 MiB); 残差列 = 宿主内存 used - 归因合计。
    vrows = q("SELECT ts, svc, vram FROM model WHERE ts>=? ORDER BY ts", (since,))
    ts_v = {}
    for ts, svc, v in vrows:
        ts_v.setdefault(ts, {})[svc] = (v or 0.0) / 1024.0
    ts_all = sorted(ts_v)
    stack_pts = []
    order = []
    if ts_all:
        lastv = ts_v[ts_all[-1]]
        order = sorted(lastv, key=lambda s: -lastv[s])
        for t in reversed(ts_all):            # 只在更早采样里露过面的服务补到尾部
            for s in ts_v[t]:
                if s not in order:
                    order.append(s)
        for p in downsample([[t, 0.0] for t in ts_all]):
            t = p[0]
            row = [round(ts_v[t].get(s, 0.0), 3) for s in order]
            used = mem_ts.get(t)
            if used is not None:
                row.append(max(0.0, round(used - sum(row), 3)))     # 系统/其他进程
                tot = mem_tot_ts.get(t)
                if tot:
                    row.append(max(0.0, round(tot - used, 3)))      # 空闲
            stack_pts.append([t] + row)
    n_seg = 1 + len(order)
    return {"heat": heat, "models": by_svc,
            "vram_stack": {"order": order, "pts": stack_pts,
                           "other": any(len(p) > n_seg for p in stack_pts),
                           "free": any(len(p) > n_seg + 1 for p in stack_pts)}}


# ------------------------------------------------- 突变归因 (spec 2026-09-23)
# 曲线跳变起点标一个黄点, hover 显示当时是谁在用。判定与渲染同一份判定逻辑,
# 在 peer 侧 agg() 算好随 /api/agg JSON 透传给 edge。
#   阈值 = max(物理下限, 窗口量程 12%) —— 3h 细看与 7d 概览都要有意义。
#   同一方向连续超限只记起点 1 个事件 (方波一次上升沿 = 1 个事件)。



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
        thr = max(cfg.SPIKE_BASE[key], rng * cfg.SPIKE_FRAC)
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
        for _, t in sorted(raw[:cfg.SPIKE_MAX], key=lambda x: x[1]):
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
            and r[2] >= cfg.CLK_TRAP_UTIL and r[3] < cfg.CLK_TRAP_MHZ) * iv / 60, 1),
        "cap_run": _run(lambda r: r[4] is not None and r[4] >= capfull),
        "hot85_run": _run(lambda r: r[1] is not None and r[1] >= cfg.TEMP_WARN_C),
        "hot80_min": round(_run(
            lambda r: r[1] is not None and r[1] >= cfg.HOT_C) * iv / 60, 1),
    }
    # 当日 ≥80°C 上穿次数(LAG 找冷->热跳变) —— watchdog hot_freq 告警的原料。
    # 同时给出"何时/多高/什么形态": 告警只说次数、页面默认又是 3h 窗口时,
    # 两者会对不上(2026-09-17 实测: 39 次全发生在 01:06-02:33, 页面看 06:33-09:33
    # 自然一片正常, 读起来像误报)。
    lt = time.localtime()
    today0 = int(time.time()) - (lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec)
    hot = q("SELECT ts,gpu_temp,gpu_util FROM sample WHERE ts>=? AND gpu_temp>=?"
            " ORDER BY ts", (today0, cfg.HOT_C))
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
        FROM sample) WHERE ts>=?""", (cfg.HOT_C, cfg.HOT_C, today0))
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



_UNITS_OVERRIDES = None   # {host_key: [unit, ...]}，来自 ~/fleetdash/units.json
_UNITS_MTIME = 0.0


def units_for(key):
    """户口名单：机上 units.json 按 host key 整组覆盖内置 UNITS。
    文件缺失/解析失败回退内置表；新集群不填 = 户口线自动关闭(端口线照跑)。
    坏文件只打日志不炸监控(与 edge peers 热加载同语义)。"""
    global _UNITS_OVERRIDES, _UNITS_MTIME
    path = os.environ.get("FLEETDASH_UNITS_FILE",
                          os.path.expanduser("~/fleetdash/units.json"))
    try:
        mt = os.path.getmtime(path)
    except OSError:
        mt = None
    if mt is None:
        _UNITS_OVERRIDES = {}
        _UNITS_MTIME = 0.0
    elif mt != _UNITS_MTIME or _UNITS_OVERRIDES is None:
        try:
            with open(path) as f:
                raw = json.load(f)
            _UNITS_OVERRIDES = {k: v for k, v in raw.items()
                                if not k.startswith("_") and isinstance(v, list)}
        except Exception as e:
            print(f"[fleetdash] units.json 解析失败, 用内置名单: {e}",
                  file=sys.stderr, flush=True)
            _UNITS_OVERRIDES = {}
        _UNITS_MTIME = mt
    return _UNITS_OVERRIDES.get(key) or UNITS.get(key) or []


def units_now():
    """systemctl is-active 批量查询(faye 非 root 也可查询单元状态)。
    返回 {unit: active/inactive/...} 或 {}(本机不在清单/查询失败)。"""
    import socket
    hn = socket.gethostname()
    key = "4c47" if "4c47" in hn else "53fa" if "53fa" in hn else host_key()
    names = units_for(key)
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





WD_STATE = "/var/lib/wecom-watchdog/state.json"



WD_ALERT_ZH = {
    "hot_freq": "高温振荡", "trap": "时钟卡死", "cap": "功耗封顶",
    "temp": "高温", "mem_high": "内存紧张", "power": "功耗",
}





UNITS = {
    # 10-04: llm-api(vLLM) 停用留作回滚, 生产 LLM 换 tf-qwen36(TensorFold systemd 常驻)
    # 10-25: embed/rerank 引擎 venv→docker(容器 embed/rerank, restart=unless-stopped,
    # 无 systemd 单元, 活体由 collector 端口探 8004/8005 覆盖); 代理单元保留在此。
    # 10-27: tf-qwen36 摘除 —— 10-23 起 TF 换容器 tf_qwen36_35b(Up, :8000 /v1/models
    # 实测正常), systemd 单元 dead 仅留回滚位; :8000 端口探活已覆盖活体。
    # 10-28: embed-proxy/rerank-proxy 摘除 —— 代理进 compose(容器 embed-proxy/rerank-proxy,
    # systemd 单元已 stop+disable 留一周回滚位); 活体=8004/8005 端口探活+容器归因。
    "4c47": ["asr-api", "face-api",
             "vram-guardian"],
    # cosy-api 2026-10-25 卸载摘除(停于 10-23, TTS 收敛到 dgx2 Ming :8101)
    # 10-27: fqtts-api×3 摘除 —— 9-27 下线、unit 文件 10-27 实证已删
    # (is-enabled=not-found), Ming 活体由 8101/8094 端口探活覆盖。
    "53fa": ["mineru-api", "popo-api", "popo-engine", "vram-guardian"],
    # dbdd: supervisor/maintenance 是 user unit, 系统级 is-active 查不到;
    # flash 容器死活由 8888 端口探活(collector)覆盖, 这里只盯 guardian。
    "dbdd": ["vram-guardian"],
}
# watchdog WECOM_WATCHDOG_SHEDDABLE: guardian 卸载期间这些 unit 掉线属预期
# 10-27: 清空 —— 原成员 fqtts×3 已全退役(见上), 在役单元卸载都属真告警。



SHEDDABLE = set()



