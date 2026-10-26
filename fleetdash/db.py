"""sqlite 只读访问 + 主机标识。"""
import os
import sqlite3
import time

from . import config as cfg
# ---------------------------------------------------------------- db
def q(sql, args=()):
    if not os.path.exists(cfg.DB):
        return []
    con = sqlite3.connect(f"file:{cfg.DB}?mode=ro", uri=True)
    try:
        return con.execute(sql, args).fetchall()
    except sqlite3.Error:
        # 老库缺新列(尚未跑 collector 迁移)时降级为"无数据", 而不是 500
        return []
    finally:
        con.close()





def sample_interval():
    """实际采样间隔(秒): 取最近两条采样的 ts 差, 异常时回退 30。"""
    r = q("SELECT ts FROM sample ORDER BY ts DESC LIMIT 2")
    if len(r) == 2 and 5 <= r[0][0] - r[1][0] <= 300:
        return r[0][0] - r[1][0]
    return 30





def host_key():
    return os.environ.get("FLEETDASH_HOST") or "host"


# ---------------------------------------------------------------- edge(观察者)
# 轮询/推送参数。cfg.POLL_SEC 与 collector 采样对齐; cfg.FAIL_TH 容忍 DERP 中继抖动
# (4 次 x 8s 超时下界, 约 2 分钟连续失败才判 DOWN, 防误报)。

