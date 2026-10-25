# 干渲染测试: 不发企微, 只出 PNG 供人工检视。用法: python3.11 render-test.py
import sys, importlib.util
spec = importlib.util.spec_from_file_location("srv", "/opt/fleetdash/server.py")
srv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(srv)

machines = [
    {"name": "4c47", "ok": True,
     "meta": "运行 6 天 4 小时 · 近3天",
     "services": [("vllm-qwen", 0), ("vllm-embed", 0), ("face-api", 0),
                  ("tts-api", 0), ("stt-api", 2)],
     "reqs": [("vllm-qwen", 12480), ("vllm-embed", 3021), ("face-api", 856),
              ("tts-api", 42)],
     "mem_txt": "当前 41.2 / 峰值 63.8 GiB",
     "disk": (118, 1863),
     "gpu_txt": "44°C · util 3% · 12.4W · SM 1005MHz（峰值 81°C@09-14 03:22）",
     "heat_txt": "≥80°C 共 12min（最长段 7min） · ≥85°C 0min · 上穿80°C 2 次",
     "heat_warn": True,
     "net_txt": "峰值 下 23.4 · 上 8.7 MB/s",
     "insp_txt": "服务掉线 0 · SM卡死 0 · 封顶 3 · 内存≥112G 0 · NTP✓",
     "insp_warn": False,
     "alert_txt": "高温(09-14 03:22起)", "alert_warn": True,
     "tps_txt": "llm-api 66/70 · embed —/120", "tps_warn": False,
     "gd_txt": "watchdog 正常（状态变化 2 次）"},
    {"name": "53fa", "ok": True,
     "meta": "运行 12 天 22 小时 · 近3天",
     "services": [("vllm-glm", 0), ("popo-api", 0), ("whisper", 0),
                  ("proxy", 1)],
     "reqs": [("vllm-glm", 8410), ("popo-api", 1902)],
     "mem_txt": "当前 58.7 / 峰值 96.1 GiB",
     "disk": (1712, 1863),
     "gpu_txt": "51°C · util 87% · 148.2W · SM 1965MHz（峰值 84°C@09-13 21:04）",
     "heat_txt": "≥80°C 共 41min（最长段 23min） · ≥85°C 0min · 上穿80°C 5 次",
     "heat_warn": True,
     "net_txt": "峰值 下 45.1 · 上 12.3 MB/s",
     "insp_txt": "服务掉线 1（proxy×1） · SM卡死 0 · 封顶 12 · 内存≥112G 1 · NTP✓",
     "insp_warn": True,
     "alert_txt": "无", "alert_warn": False,
     "tps_txt": "vllm-glm 41/64 · popo-api 12/13", "tps_warn": True,
     "gd_txt": "watchdog 正常（状态变化 0 次）"},
    {"name": "9xxx", "ok": False, "meta": "?",
     "services": [], "reqs": [], "mem_txt": "无数据",
     "disk": (None, None), "gpu_txt": "—", "heat_txt": "—",
     "heat_warn": False, "net_txt": "—", "insp_txt": "—",
     "insp_warn": False, "alert_txt": "未知(watchdog 状态文件不可读)",
     "alert_warn": True, "tps_txt": "—", "tps_warn": False,
     "gd_txt": "未知"},
]

# 合成 3 天曲线数据(10min 间隔), 验证趋势图排版
import math, time as _t
now = int(_t.time())
for m in machines[:2]:
    pts = []
    for k in range(432):
        ts = now - (432 - k) * 600
        pts.append([ts, 44 + 22 * math.sin(k / 14) + (6 if k % 97 == 0 else 0)])
    util = [[ts, max(0, min(100, 40 + 45 * math.sin(k / 9)))]
            for k, (ts, _) in enumerate(pts)]
    pw = [[ts, 18 + 60 * (1 + math.sin(k / 11 + 1.1)) / 2
           + (35 if k % 97 == 0 else 0)] for k, (ts, _) in enumerate(pts)]
    mem = [[ts, 41 + 18 * math.sin(k / 33) + (60 if 200 < k < 210 else 0)]
           for k, (ts, _) in enumerate(pts)]
    mem[150][1] = None   # 模拟一段数据空洞
    m["series"] = {"gpu_temp": pts, "gpu_util": util,
                   "gpu_pw": pw, "mem_used": mem}

for i, m in enumerate(machines):
    png = srv.render_report_png("2026-09-15 09:00", m,
                                "2026-09-15 09:00:07")
    open(f"/tmp/report-test-{i}.png", "wb").write(png)
    print(f"OK card={len(png)} -> /tmp/report-test-{i}.png")
