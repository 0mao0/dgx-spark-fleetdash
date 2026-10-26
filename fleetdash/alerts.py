"""告警：企微推送、闩锁状态机、各类 check（仅在 edge 轮询成功后调用）。"""
import json
import os
import sys
import urllib.error
import urllib.request

from . import config as cfg
from .db import q
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
    if not cfg.WEBHOOK:
        print("[edge] webhook 未配置, 丢弃推送: " + title, file=sys.stderr, flush=True)
        return False
    NL = chr(10)
    content = ("### " + title + NL) + NL.join(lines) + NL + \
        "> fleetdash-edge @ " + time.strftime("%Y-%m-%d %H:%M:%S")
    body = json.dumps({"msgtype": "markdown",
                       "markdown": {"content": content}}).encode("utf-8")
    req = urllib.request.Request(cfg.WEBHOOK, data=body,
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



def state_save():
    if not cfg.STATE_PATH:
        return
    try:
        tmp = cfg.STATE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"push": PUSH_STATE, "hw": cfg.HW_FIRED, "tps": TPS_FIRED}, f)
        os.replace(tmp, cfg.STATE_PATH)
    except OSError:
        pass





def state_load():
    if not cfg.STATE_PATH:
        return
    try:
        with open(cfg.STATE_PATH) as f:
            d = json.load(f)
    except (OSError, ValueError):
        return
    PUSH_STATE.update(d.get("push") or {})
    cfg.HW_FIRED.update(d.get("hw") or {})
    TPS_FIRED.update(d.get("tps") or {})
    print("[fleetdash] 载入告警闩锁 %d 项 / 冷却 %d 类（重启不重推）"
          % (sum(len(v) for v in cfg.HW_FIRED.values()), len(PUSH_STATE)), flush=True)





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
    nm = cfg.LABEL.get(key, key)
    lines = [f"DGX **<font color=\"warning\">{nm} 失联</font>**：edge 连续 "
             f"{cfg.FAIL_TH} 次拉取失败（约 {_fmt_age((cfg.FAIL_TH + 1) * cfg.POLL_SEC)}）。"]
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
    nm = cfg.LABEL.get(key, key)
    # poll_once 会先把 down_since 置 None 再调本函数, 所以必须用 or 兜底,
    # 不能依赖 st.get("down_since", 默认值)(键存在但值为 None)。
    gap = int(time.time()) - (st.get("down_since") or int(time.time()))
    push_wecom("🟢 车队恢复：节点上线",
               [f"DGX **{nm}** 已恢复（失联约 {_fmt_age(gap)}），数据重新入库。"])





def notify_stale(key, st):
    if not push_cool("stale:" + key):
        return
    nm = cfg.LABEL.get(key, key)
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
    nm = cfg.LABEL.get(key, key)
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



def tps_check(key, d):
    pk = ((d.get("daily") or {}).get("tps_pk")) or {}
    nm = cfg.LABEL.get(key, key)
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



def notify_svc_down(key, svc, reason, gd=None):
    if not push_cool(f"svcdown:{key}:{svc}"):
        return
    nm = cfg.LABEL.get(key, key)
    crit = "**<font color=\"warning\">脑死</font>**" if "脑" in reason \
        else "**<font color=\"warning\">掉线</font>**"
    lines = [f"DGX **{nm}** 的 **{svc}** {crit}（{reason}，"
             f"连续 {SVC_TH} 个采样确认，约 {SVC_TH * cfg.POLL_SEC}s）。"]
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
    nm = cfg.LABEL.get(key, key)
    gap = int(time.time()) - (stv.get("since") or int(time.time()))
    push_wecom("🟢 服务恢复",
               [f"DGX **{nm}** 的 **{svc}** 已恢复探活（不可用约 {_fmt_age(gap)}）。"])





def svc_check(key, d):
    """每次成功拉到新鲜快照后跑一遍。在 cfg.EDGE_LOCK 内调用。
    防抖: 快照比当前轮询时刻旧超过一个周期时直接跳过 —— 堵住
    "edge 重启/双进程后不再轮询, 却拿启动缓存里的旧状态推恢复"
    的误报路径(2026-10-04 4c47 asr-api "恢复探活"误报复盘)。"""
    snap = d.get("snapshot") or {}
    if snap.get("ts") and int(time.time()) - int(snap["ts"]) > 2 * cfg.POLL_SEC:
        return
    seen = set()
    for m in (snap.get("models") or []):
        k = (key, m["svc"])
        seen.add(k)
        alive = bool(m.get("alive"))
        probe = m.get("probe")
        bad = (not alive) or (probe is not None and probe == 0)
        stv = cfg.SVC_STATE.setdefault(k, {"bad": False, "fails": 0,
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
    for k in [k for k in cfg.SVC_STATE if k[0] == key and k not in seen]:
        del cfg.SVC_STATE[k]





def clock_check(key, d):
    """时钟巡检: NTP 脱同步 或 墙钟相对单调钟跳变 >= CLOCK_DRIFT_MS。"""
    snap = d.get("snapshot") or {}
    nm = cfg.LABEL.get(key, key)
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
# "时长/状态 -> 推送"。fired 闩锁记在 cfg.EDGE[key]["hw"]: 越过阈值首刻推一次,
# 恢复再推一次; 冷却 1800s/类/机 兜底防刷屏。阈值与 watchdog check() 对齐。



def hw_check(key, d):
    """每次成功拉到新鲜快照后跑一遍。在 cfg.EDGE_LOCK 内调用。"""
    st = cfg.EDGE.get(key)
    if st is None:
        return
    # 闩锁挂在落盘那份 dict 上(cfg.HW_FIRED), 服务重启不清零 —— 否则重启后的第一次
    # 轮询会把旧事件当新事件重推(2026-09-17 踩过: 部署重启 6 次 = 重推 6 条)。
    hw = st.setdefault("hw", None)
    if hw is None:
        hw = st["hw"] = cfg.HW_FIRED.setdefault(key, {})
    run = d.get("hw_run") or {}
    nm = cfg.LABEL.get(key, key)
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
            % (nm, cfg.CLK_TRAP_UTIL, sm or 0, tm),
            "- " + hint,
            "- 反复出现：`sudo nvidia-smi -lgc 2200,2200` 锁频作保险"
            "（`-rgc` 撤销）"])
    elif sm is not None and sm >= cfg.CLK_TRAP_MHZ:
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
            f"≥{cfg.TEMP_WARN_C}°C）。社区断电事故多发生在 86–97°C。",
            "- 建议：检查风扇/环境，必要时锁频 `nvidia-smi -lgc 2200,2200`。"])
    elif t is not None and t < cfg.TEMP_WARN_C:
        clear("hot", "🟢 GPU 温度回落", [f"DGX **{nm}** 当前 {t:.0f}°C。"])

    # 3b) 持续高温 / 高频冲击(时钟卡死前兆): 各每天推一次, 段结束后重开闩
    if (run.get("hot80_min") or 0) >= HW_HOT_SUSTAIN_MIN:
        hs = d.get("health") or {}
        fire("sust", "🟠 车队告警：GPU 持续高温", [
            f"DGX **{nm}** ≥{cfg.HOT_C}°C 已连续 **{run['hot80_min']:g} 分钟**"
            f"（窗口内 ≥{cfg.TEMP_WARN_C}°C 命中 {hs.get('hot85', 0)} 采样）。",
            "- 长期高温+满载是社区时钟卡死前兆：压夜间批量并发/长批，"
            "或锁频 2200，检查通风。"], once_day=True)
    elif t is not None and t <= cfg.HOT_C - 2:
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
            % (nm, cfg.HOT_C, hot_min, win, cross_n),
            tip,
            "- 台账：[面板 1d 窗口 · %s](https://dgx.angineer.cn/fleet/?w=1d&hn=%s)"
            % (nm, nm)], once_day=True)

    # 4) 统一内存高企(≥112G): 即时推, guardian 状态一并附上
    mm = run.get("mem_now")
    if mm is not None and mm >= cfg.MEM_HIGH_GIB:
        fire("mem", "🔴 车队告警：统一内存高企", [
            "DGX **<font color=\"warning\">%s</font>** 已用 **%.1f GiB** "
            "≥ %.0f（guardian=%s）。" % (nm, mm, cfg.MEM_HIGH_GIB,
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



def notify_unit_down(key, u, stv):
    nm = cfg.LABEL.get(key, key)
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
                f"约 {SVC_TH * cfg.POLL_SEC}s）。",
                f"- 处置：`systemctl status {u}` 或重启该单元",
                "- 页面：https://dgx.angineer.cn/fleet/"])





def notify_unit_up(key, u, stv):
    if not push_cool(f"unitup:{key}:{u}"):
        return
    nm = cfg.LABEL.get(key, key)
    gap = int(time.time()) - (stv.get("since") or int(time.time()))
    push_wecom("🟢 systemd 单元恢复",
               [f"DGX **{nm}** 的单元 **{u}** 已回到 active"
                f"（停了约 {_fmt_age(gap)}）。"])





def unit_check(key, d):
    """每次成功拉到新鲜快照后跑一遍。在 cfg.EDGE_LOCK 内调用。
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





PUSH_COOLDOWN = 1800       # 同类告警冷却, 与 watchdog 对齐



PUSH_STATE = {}            # category -> 上次推送 ts (冷却)



TPS_FIRED = {}             # host key -> {svc: "YYYY-MM-DD"} F4 性能退化当日闩锁





TPS_MIN = 10.0



TPS_DROP_TH = 0.7



TPS_MIN_N = 20





SVC_TH = 2



CLOCK_DRIFT_MS = 2000      # 墙钟周期间偏差超此值视为跳变





TEMP_CRIT_C = 90



HW_TRAP_MIN = 3      # 时钟卡死持续分钟(=watchdog 3 个 60s 采样)



HW_CAP_RUN = 5       # 封顶满额连续采样数



HW_HOT_SUSTAIN_MIN = 30
# 「高频冲击」判据: 当日 ≥80°C 累计 ≥HW_HOT_FREQ_MIN 分钟 **且** 上穿 ≥HW_HOT_FREQ 次。
# 次数上限压得低(8)是为了跨采样间隔可比: 60s 采样数出的次数约为 30s 的一半,
# 阈值取 24 时两个系统会对同一次事件给出"响/不响"两种结论。



HW_HOT_FREQ = 8



HW_HOT_FREQ_MIN = 20





UNIT_STATE = {}



