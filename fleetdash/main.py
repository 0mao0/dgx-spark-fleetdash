# -*- coding: utf-8 -*-
"""启动器：argparse -> 写 config -> 起轮询线程/日报 -> 起 HTTP。"""
import argparse
import json
import os
import socket
import sys
import threading
from http.server import ThreadingHTTPServer

from . import config as cfg
from .alerts import state_load, push_wecom, resolve_webhook
from .db import host_key
from .poller import poller_loop, load_caches
from .webapp import H


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=["master", "slave", "edge"], default="master")
    ap.add_argument("--label", default=None, help="本节点显示名")
    ap.add_argument("--peers", action="append", default=[],
                    help="key=url, 如 53fa=http://100.107.85.39:9100，可多次")
    ap.add_argument("--db", default=cfg.DB)
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--bind", default=None)
    # edge 专属
    ap.add_argument("--poll", type=int, default=cfg.POLL_SEC, help="轮询秒数")
    ap.add_argument("--fail-th", type=int, default=cfg.FAIL_TH,
                    help="连续失败多少次判 DOWN")
    ap.add_argument("--stale-sec", type=int, default=cfg.STALE_SEC,
                    help="拉通但快照超过此秒数 -> 数据停滞告警")
    ap.add_argument("--cache-dir", default=None,
                    help="失联缓存目录, 默认 <db 同级>/edge-cache")
    ap.add_argument("--webhook-file", default=cfg.WEBHOOK_FILE)
    ap.add_argument("--report-at", default=cfg.REPORT_AT,
                    help="日报时间 HH:MM, 空串关闭")
    ap.add_argument("--test-push", action="store_true",
                    help="发一条测试推送后退出")
    a = ap.parse_args()

    cfg.ROLE = a.role
    cfg.DB = a.db
    # 告警闩锁落盘路径(所有角色通用, 与 DB 同目录): 重启不清零, 避免把旧事件
    # 当新事件重推 —— 这是 2026-09-17 那条"9:30 高温告警"的真正来源。
    cfg.STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(cfg.DB)),
                                  "push-state.json")
    state_load()
    if a.port is None:
        a.port = 9101 if cfg.ROLE == "edge" else 9100
    if a.bind is None:
        # edge 只服务 VPS 本机 nginx; master/slave 需要 tailnet 内可达
        a.bind = "127.0.0.1" if cfg.ROLE == "edge" else "0.0.0.0"

    # 节点标识: 用 --label 或 hostname 匹配
    hn = socket.gethostname()
    key = ("4c47" if "4c47" in hn else "53fa" if "53fa" in hn
           else "dbdd" if "dbdd" in hn else hn)
    cfg.LABEL = {key: a.label or key}
    for p in a.peers:
        k, _, v = p.partition("=")
        cfg.PEERS[k] = v.rstrip("/")
        cfg.LABEL.setdefault(k, k)

    if cfg.ROLE == "edge":
        if not cfg.PEERS:
            print("edge 需要至少一个 --peers key=http://...", file=sys.stderr)
            return 1
        cfg.POLL_SEC = a.poll
        cfg.FAIL_TH = a.fail_th
        cfg.STALE_SEC = a.stale_sec
        cfg.CACHE_DIR = a.cache_dir or os.path.join(
            os.path.dirname(os.path.abspath(cfg.DB)), "edge-cache")
        cfg.WEBHOOK = resolve_webhook(a.webhook_file)
        cfg.REPORT_AT = (a.report_at or "").strip()
        for k in cfg.PEERS:
            cfg.EDGE[k] = {"state": None, "fails": 0, "last_err": "",
                           "last_ok": None, "last_agg": None, "down_since": None,
                           "stale": False}
            # 闩锁接到落盘的那份 dict 上(见 STATE_PATH 注释)
            cfg.EDGE[k]["hw"] = cfg.HW_FIRED.setdefault(k, {})
        load_caches()
        if a.test_push:
            ok = push_wecom("🧪 fleetdash-edge 测试",
                            ["这是一条来自 edge 观察者的测试消息。",
                             f"- peers: {', '.join(cfg.PEERS)}",
                             "- webhook 来源: "
                             + ("env" if os.environ.get("FLEETDASH_WEBHOOK")
                                else a.webhook_file)])
            print("推送成功" if ok else "推送失败, 检查 webhook/网络", flush=True)
            return 0 if ok else 1
        threading.Thread(target=poller_loop, daemon=True).start()
        if not cfg.WEBHOOK:
            print("[edge] 警告: webhook 未配置, 告警只写日志不推送",
                  file=sys.stderr)

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
    print(f"[fleetdash] role={cfg.ROLE} host={key} db={cfg.DB} "
          f"peers={list(cfg.PEERS)} bind={bind_note}", flush=True)
    srv.serve_forever()
