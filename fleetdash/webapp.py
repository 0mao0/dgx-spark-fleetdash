# -*- coding: utf-8 -*-
"""HTTP 层：静态页面(/, /app.js, /style.css) + 数据端点(/api/page, /api/agg,
/api/health) + 电源端点(/api/power)。页面本体在 web/ 目录，逻辑全在浏览器
(app.js)，服务端不再拼 HTML。"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler

from . import config as cfg
from . import poller
from .alerts import push_wecom
from .data import agg, host_key

WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")

# 新页面只需要这些键；/api/agg 仍给全量(日报/兼容旧调用方)。
# series = {"heat": {指标: [[ts,v]..]}, "models": {svc: {字段: [[ts,v]..]}}}
PAGE_KEYS = ("host", "label", "snapshot", "series", "down", "error")


def page_payload(win):
    """按角色组装页面数据。edge=拉各机+好帧回退；master=本机+直连 peers；
    slave 不出页面。"""
    if cfg.ROLE == "edge":
        hosts = poller.edge_hosts(win)
    elif cfg.ROLE == "master":
        hosts = []
        me = agg(cfg.WINDOWS[win])
        me["label"] = cfg.LABEL.get(host_key(), host_key())
        hosts.append(me)
        for key, url in cfg.PEERS.items():
            try:
                import urllib.request as ur
                with ur.urlopen(f"{url}/api/agg?w={win}", timeout=4) as r:
                    d = json.loads(r.read().decode())
                d["label"] = cfg.LABEL.get(key, key)
                hosts.append(d)
            except Exception as e:
                hosts.append({"host": key, "label": cfg.LABEL.get(key, key),
                              "snapshot": None, "error": str(e)})
    else:
        return None
    slim = []
    for h in hosts:
        slim.append({k: h[k] for k in PAGE_KEYS if k in h})
    return {"role": cfg.ROLE, "win": win, "ts": int(time.time()),
            "hosts": slim}


# ---------------------------------------------------------------- 电源操作
# 页面「关机/重启/唤醒」按钮 -> POST /api/power {action, token[, host]}。
# edge(VPS): 校验自己的 token 后转发给 peer; peer(DGX): 校验
# ~/fleetdash/power.token 后 `sudo -n systemctl <action>` 延迟 1.2s 执行。
def wol_mac(host):
    if host == host_key():
        try:
            with open(f"/sys/class/net/{cfg.WOL_IFACE}/address") as f:
                return f.read().strip()
        except OSError:
            pass
    return cfg.WOL_MACS.get(host, "")


def wol_send(mac):
    """广播 magic packet(6x FF + MAC x16)。纯 UDP 发包, 不需要 root。"""
    import socket
    m = bytes.fromhex(mac.replace(":", "").replace("-", ""))
    if len(m) != 6:
        return 0
    pkt = b"\xff" * 6 + m * 16
    n = 0
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        for b in cfg.WOL_BCAST:
            for p in cfg.WOL_PORTS:
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
    return (cfg.POWER_TOKEN_FILE_EDGE if cfg.ROLE == "edge"
            else os.path.expanduser("~/fleetdash/power.token"))


def power_token_now():
    try:
        with open(power_token_file()) as f:
            return f.read().strip()
    except OSError:
        return ""


# 暴破护栏: token 连错 3 次锁 5 分钟(进程级)。成功一次即清零。
_pwr = {"fails": 0, "until": 0.0}
_pwr_lock = threading.Lock()


def pwr_locked():
    with _pwr_lock:
        return time.time() < _pwr["until"]


def pwr_fail():
    with _pwr_lock:
        _pwr["fails"] += 1
        if _pwr["fails"] >= cfg.PWR_FAIL_TH:
            _pwr["until"] = time.time() + cfg.PWR_LOCK_SEC
            _pwr["fails"] = 0
            return True
    return False


def pwr_ok():
    with _pwr_lock:
        _pwr["fails"] = 0


# ---------------------------------------------------------------- http
STATIC = {"/": ("index.html", "text/html; charset=utf-8"),
          "/index.html": ("index.html", "text/html; charset=utf-8"),
          "/app.js": ("app.js", "application/javascript; charset=utf-8"),
          "/style.css": ("style.css", "text/css; charset=utf-8")}


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
        if action not in cfg.POWER_OK:
            self._send(400, json.dumps({"err": f"bad action {action!r}"}),
                       "application/json")
            return
        verb = cfg.POWER_VERB[action]
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
            # 唤醒需要「同网段机器广播 magic packet」。目标关机时收不到 HTTP,
            # 所以 edge 挑一台在线同网段机器代发。
            tgt = str(body.get("target") or body.get("host") or "")
            if cfg.ROLE == "edge":
                if tgt not in cfg.PEERS:
                    self._send(400, json.dumps({"err": f"未知主机 {tgt}"}),
                               "application/json")
                    return
                with cfg.EDGE_LOCK:
                    tgt_st = (cfg.EDGE.get(tgt) or {}).get("state")
                    cand = [k for k in cfg.PEERS if k != tgt
                            and (cfg.EDGE.get(k) or {}).get("state") == "UP"]
                if tgt_st == "UP":
                    # 在线机器收到 magic packet 是空操作, 而且"卡死"也唤不醒。
                    self._send(409, json.dumps(
                        {"err": f"{cfg.LABEL.get(tgt, tgt)} 当前在线，无需唤醒"
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
                    cfg.PEERS[sender] + "/api/power",
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
                    nm, snm = cfg.LABEL.get(tgt, tgt), cfg.LABEL.get(sender, sender)
                    print(f"[edge] 唤醒: {nm} 由 {snm} 代发 magic packet", flush=True)
                    # 文案先声明"这是操作留痕不是故障告警"。
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
        if cfg.ROLE == "edge":
            hk = str(body.get("host") or "")
            url = cfg.PEERS.get(hk)
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
                nm = cfg.LABEL.get(hk, hk)
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

    def _static(self, name, ctype):
        try:
            with open(os.path.join(WEB_DIR, name), "rb") as f:
                self._send(200, f.read(), ctype)
        except OSError:
            self._send(404, "missing " + name, "text/plain")

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        u = urlparse(self.path)
        qs = parse_qs(u.query)

        if u.path == "/api/agg":
            w = qs.get("w", ["3d"])[0]
            if w not in cfg.WINDOWS:
                w = "3d"
            self._send(200, json.dumps(agg(cfg.WINDOWS[w]), ensure_ascii=False,
                                       default=str), "application/json")
            return
        if u.path == "/api/page":
            if cfg.ROLE == "slave":
                self._send(404, json.dumps({"err": "slave 只出 /api/agg"}),
                           "application/json")
                return
            w = qs.get("w", ["3d"])[0]
            if w not in cfg.WINDOWS:
                w = "3d"
            self._send(200, json.dumps(page_payload(w), ensure_ascii=False,
                                       default=str), "application/json")
            return
        if u.path == "/api/health":
            body = {"ok": True, "role": cfg.ROLE}
            if cfg.ROLE == "edge":
                body["peers"] = {
                    k: ((cfg.EDGE[k]["state"] or "INIT")
                        + ("+STALE" if cfg.EDGE[k].get("stale") else ""))
                        for k in cfg.EDGE}
                body["svc_bad"] = sorted(
                    f"{k}/{s}" for (k, s), v in cfg.SVC_STATE.items()
                    if v.get("bad"))
            self._send(200, json.dumps(body), "application/json")
            return
        if u.path in STATIC:
            if cfg.ROLE == "slave":
                self._send(200, "<h1>fleetdash slave</h1><p>用 <code>/api/agg</code>"
                                " 取数；页面由 master/edge 渲染。</p>")
                return
            self._static(*STATIC[u.path])
            return
        self._send(404, "not found", "text/plain")
