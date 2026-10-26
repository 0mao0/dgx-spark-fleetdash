# 一次性演练脚本: 手动触发一次车队日报推送(不占用定时日报的当日名额)
# 用法(VPS): cd /opt/fleetdash && python3.11 tests/report-drill.py
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fleetdash import config as cfg
from fleetdash import poller, report
from fleetdash.alerts import resolve_webhook

cfg.ROLE = "edge"
cfg.DB = os.environ.get("FLEETDASH_DB", "/opt/fleetdash/fleet.db")
cfg.STATE_PATH = os.path.join(os.path.dirname(cfg.DB), "push-state.json")
cfg.PEERS.update({"4c47": "http://100.125.117.85:9100",
                  "53fa": "http://100.107.85.39:9100",
                  "dbdd": "http://100.86.101.0:9100"})
cfg.LABEL.update({"4c47": "4c47", "53fa": "53fa", "dbdd": "dbdd"})
cfg.WEBHOOK = resolve_webhook("/etc/fleetdash/webhook")
for k, u in cfg.PEERS.items():
    st = {"state": "UP", "fails": 0, "last_err": "",
          "last_ok": int(time.time()), "last_agg": None,
          "down_since": None, "stale": False}
    try:
        st["last_agg"] = poller.fetch_agg(k, u)
    except Exception as e:
        st["last_err"] = str(e)
    cfg.EDGE[k] = st
report.daily_report()
print("done")
