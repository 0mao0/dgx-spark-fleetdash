"""每日日报：统计 + PIL 出图 + 企微推送（与页面渲染无关）。"""
import json
import os
import sys
import time

from . import config as cfg
from . import alerts
from .alerts import push_wecom, _fmt_age, _fmt_uptime
from .data import wd_alert_name
def _kfmt(n):
    return f"{n / 1000:.1f}k" if n >= 1000 else str(int(n))


# ------------------------------------------------- 日报图片渲染(PIL, 仅 edge 有依赖)
# 企业微信机器人支持 msgtype=image: 把日报排成一张仪表盘(状态点/条形图/进度条),
# 比 markdown 文字版可读得多。Pillow+雅黑只装在 VPS, DGX 上这些函数不会被调用;
# 渲染任何一步失败(缺依赖/字体)都回退到 markdown 文本版, 日报永不误删。



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
    if not cfg.WEBHOOK:
        return False
    body = json.dumps({"msgtype": "image", "image": {
        "base64": base64.b64encode(png).decode("ascii"),
        "md5": hashlib.md5(png).hexdigest()}}).encode("utf-8")
    req = urllib.request.Request(cfg.WEBHOOK, data=body,
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
    for key in cfg.PEERS:
        if lines:
            lines.append("")   # 机与机之间空一行, 卡片上块状分明
        st = cfg.EDGE.get(key) or {}
        d = st.get("last_agg") or {}
        nm = cfg.LABEL.get(key, key)
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
        ok = st.get("state") == "UP" and ago < cfg.STALE_SEC
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
    if not cfg.REPORT_AT or not cfg.WEBHOOK:
        return
    day = time.strftime("%Y-%m-%d")
    # 已推日落盘: cfg.LAST_REPORT_DAY 是内存量, 服务重启即清零,
    # 不持久化的话 09:00 后每次 restart 都会重推一遍日报(2026-09-16 实测)
    marker = os.path.join(cfg.CACHE_DIR, ".report-" + day) if cfg.CACHE_DIR else None
    if cfg.LAST_REPORT_DAY == day or (marker and os.path.exists(marker)):
        return
    if time.strftime("%H:%M") < cfg.REPORT_AT:
        return
    # 只有拿到过数据才推, 否则日报毫无意义
    if not any(st.get("last_agg") for st in cfg.EDGE.values()):
        return
    cfg.LAST_REPORT_DAY = day
    if marker:
        try:
            os.makedirs(cfg.CACHE_DIR, exist_ok=True)
            open(marker, "w").close()
        except OSError:
            pass
    daily_report()


# F5 机器清单热加载: edge 每轮轮询前查一次该文件的 mtime, 变了就增删 peer,
# 加机器不再需要动 systemd ExecStart。格式 {"4c47": {"url": "http://...",
# "label": "4c47"}, ...}(值也可以是裸 url 字符串)。--peers 仍是启动种子,
# 文件里的同 key 条目会覆盖它。



FONT_DIR = "/opt/fleetdash/fonts"



IMG_W = 1200



IMG = {"bg": "#eef1f5", "card": "#ffffff", "line": "#e3e6ea",
       "text": "#1f2430", "mut": "#8a93a3", "ok": "#2f9e44",
       "warn": "#e8590c", "bad": "#e03131", "bar": "#4c6ef5",
       "barbg": "#e3e7ee", "head": "#101826", "headmut": "#98a4b5"}




_SYM = {"✓": "√", "✗": "×", "⚠": "!"}



