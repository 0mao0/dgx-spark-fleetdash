# 一次性演练脚本: 手动触发一次车队日报推送(不占用定时日报的当日名额)
# 用法(VPS): python3.11 report-drill.py
import importlib.util
import time

spec = importlib.util.spec_from_file_location("fd", "/opt/fleetdash/server.py")
fd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fd)

fd.PEERS.update({"4c47": "http://100.125.117.85:9100",
                 "53fa": "http://100.107.85.39:9100"})
fd.LABEL.update({"4c47": "4c47", "53fa": "53fa"})
fd.WEBHOOK = fd.resolve_webhook("/etc/fleetdash/webhook")
for k, u in fd.PEERS.items():
    st = {"state": "UP", "fails": 0, "last_err": "",
          "last_ok": int(time.time()), "last_agg": None,
          "down_since": None, "stale": False}
    try:
        st["last_agg"] = fd.fetch_agg(k, u)
    except Exception as e:
        st["last_err"] = str(e)
    fd.EDGE[k] = st
fd.daily_report()
print("done")
