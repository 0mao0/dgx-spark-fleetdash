# 本地仿真: 用 4c47 真实抓取的 /metrics 文本验证 collector 新增解析与 _ratio 数学。
# 不入 systemd, 一次性跑: python tests/test_reqspeed_local.py
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import collector as C

PATH = os.environ.get("METRICS_PATH", "/tmp/vllm_metrics.txt")
txt1 = open(PATH, encoding="utf-8").read()

# 第二份样本: 在真实值基础上叠加已知增量(gen +2000 tok / decode +50s -> 40 tok/s;
# ttft +1.2s/2 请求 -> 600ms; queue +0.3s/2 请求 -> 150ms)
delta = {
    "vllm:request_generation_tokens_sum": 2000.0,
    "vllm:request_decode_time_seconds_sum": 50.0,
    "vllm:time_to_first_token_seconds_sum": 1.2,
    "vllm:time_to_first_token_seconds_count": 2.0,
    "vllm:request_queue_time_seconds_sum": 0.3,
    "vllm:request_queue_time_seconds_count": 2.0,
    "vllm:generation_tokens_total": 2000.0,
}

def bump(text):
    out = []
    for ln in text.splitlines():
        m = re.match(r"^(vllm:[\w:]+(?:_sum|_count|_total)?)(\{[^}]*\})\s+([\d.eE+\-]+)$", ln)
        if m and m.group(1) in delta:
            ln = f"{m.group(1)}{m.group(2)} {float(m.group(3)) + delta[m.group(1)]}"
        out.append(ln)
    return "\n".join(out)

txt2 = bump(txt1)
box = {"t": txt1}
C.http_get = lambda url, *a, **k: box["t"]

tr = C.TpsTracker()
r1 = tr.compute("llm-api", 8000, now=1000)
assert r1 and r1["dec_tps"] is None, "首采样应只建基线"
box["t"] = txt2
r2 = tr.compute("llm-api", 8000, now=1030)
print("r2 =", {k: r2[k] for k in ("tps", "dec_tps", "ttft_ms", "queue_ms", "kv")})
assert r2["dec_tps"] == 40.0, r2["dec_tps"]
assert r2["ttft_ms"] == 600.0, r2["ttft_ms"]
assert r2["queue_ms"] == 150.0, r2["queue_ms"]

# Δcount=0(窗口无完成请求)时应出 None: 原样再喂一次
box["t"] = txt2
r3 = tr.compute("llm-api", 8000, now=1060)
assert r3["dec_tps"] is None and r3["ttft_ms"] is None and r3["queue_ms"] is None, r3
print("COLLECTOR_SIM_OK")
