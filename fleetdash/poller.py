"""edge 轮询：拉各机 /api/agg、好帧缓存、peers 热加载、页面主机列表。"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

from . import config as cfg
from . import alerts
from . import report
from .alerts import (peer_ping, notify_svr_unresp, notify_down, notify_up,
                     notify_stale, svc_check, clock_check, tps_check,
                     hw_check, unit_check)
def fetch_agg(key, url, win=cfg.POLL_WIN, timeout=cfg.FETCH_TIMEOUT):
    with urllib.request.urlopen(f"{url}/api/agg?w={win}", timeout=timeout) as r:
        d = json.loads(r.read().decode("utf-8", "replace"))
    d["label"] = cfg.LABEL.get(key, key)
    return d





def cache_path(key):
    return os.path.join(cfg.CACHE_DIR, key + ".json")





def save_cache(key, d):
    try:
        os.makedirs(cfg.CACHE_DIR, exist_ok=True)
        tmp = cache_path(key) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(d, f, ensure_ascii=False, default=str)
        os.replace(tmp, cache_path(key))
    except OSError:
        pass





def load_caches():
    for key in cfg.PEERS:
        try:
            with open(cache_path(key)) as f:
                d = json.load(f)
            cfg.EDGE[key]["last_agg"] = d
            # 缓存新鲜 => 视为 UP, 这样 edge 重启后 peer 若仍是死的, 能正常走
            # UP->DOWN 翻转并再次推送(冷却期内不重复); 陈旧缓存不复活状态, 避免误报"恢复"
            snap = d.get("snapshot") or {}
            if snap.get("ts") and int(time.time()) - snap["ts"] < cfg.STALE_SEC * 2:
                cfg.EDGE[key]["state"] = "UP"
            print(f"[edge] 载入磁盘缓存 {key} (上次快照 "
                  f"{time.strftime('%m-%d %H:%M', time.localtime(snap['ts']))})"
                  if snap.get("ts") else f"[edge] 载入磁盘缓存 {key}", flush=True)
        except Exception:
            pass





def poll_once():
    for key, url in cfg.PEERS.items():
        st = cfg.EDGE[key]
        try:
            d = fetch_agg(key, url)
        except Exception as e:
            # F2 失联降噪: 达到阈值先 ping 对端 tailscale IP 二次确认。
            # ping 通 = 机器活着、只是 fleetdash-server/HTTP 链路坏了 ——
            # 不推"整机失联"(那是误报), 改推级别较低的"服务无响应"。
            ping = None
            if st["state"] == "UP" and st["fails"] + 1 >= cfg.FAIL_TH:
                ping = peer_ping(url)
            with cfg.EDGE_LOCK:
                st["fails"] += 1
                st["last_err"] = str(e)
                if st["state"] == "UP" and st["fails"] >= cfg.FAIL_TH:
                    if ping:
                        notify_svr_unresp(key, st)
                    else:
                        st["state"] = "DOWN"
                        st["down_since"] = int(time.time())
                        st["stale"] = False
                        notify_down(key, st)
            continue
        with cfg.EDGE_LOCK:
            was_down = st["state"] == "DOWN"
            st.update(fails=0, last_err="", last_ok=int(time.time()), last_agg=d)
            snap = d.get("snapshot") or {}
            st["stale"] = (bool(snap.get("ts"))
                           and int(time.time()) - snap["ts"] > cfg.STALE_SEC)
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





def reload_peers():
    global _PEERS_MT
    try:
        mt = os.path.getmtime(cfg.PEERS_FILE)
    except OSError:
        return
    if mt == _PEERS_MT:
        return
    try:
        with open(cfg.PEERS_FILE) as f:
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
        if cfg.PEERS.get(k) != url:
            cfg.PEERS[k] = url
            print(f"[edge] peer 热更新 {k} -> {url}", flush=True)
        if isinstance(v, dict) and v.get("label"):
            cfg.LABEL[k] = v["label"]
        cfg.EDGE.setdefault(k, {"state": None, "fails": 0, "last_err": "",
                            "last_ok": None, "last_agg": None,
                            "down_since": None, "stale": False})
    for k in [k for k in list(cfg.EDGE) if k not in cfg.PEERS]:
        cfg.EDGE.pop(k, None)
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
        time.sleep(cfg.POLL_SEC)





def edge_hosts(win):
    """构造 hosts 列表: 优先新鲜数据; 失败/非默认窗口按需拉取; DOWN 回退缓存。
    返回的 dict 额外带 down(横幅文案) 标记。"""
    out = []
    now = time.time()
    for key, url in cfg.PEERS.items():
        st = cfg.EDGE[key]
        with cfg.EDGE_LOCK:
            cached = dict(st["last_agg"]) if st.get("last_agg") else None
            state = st["state"]
        d = None
        err = None
        c = cfg.EDGE_WIN_CACHE.get((key, win))
        if c and now - c[0] < cfg.WIN_CACHE_TTL:
            d = c[1]
        else:
            try:
                d = fetch_agg(key, url, win=win, timeout=5)
                cfg.EDGE_WIN_CACHE[(key, win)] = (now, d)
            except Exception as e:
                err = str(e)
        if d is not None:
            h = dict(d)
        elif cached is not None:
            h = dict(cached)          # DOWN: 用最后成功快照(窗口可能与所选不一致)
            h["down"] = err or "拉取失败"
        else:
            h = {"host": key, "label": cfg.LABEL.get(key, key), "snapshot": None,
                 "error": err or "尚无数据"}
        h.setdefault("label", cfg.LABEL.get(key, key))
        if state == "DOWN" and "down" not in h:
            h["down"] = st.get("last_err") or "拉取失败"
        out.append(h)
    return out


# ---------------------------------------------------------------- svg



_PEERS_MT = 0.0



