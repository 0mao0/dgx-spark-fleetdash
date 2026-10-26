#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fleetdash 入口(瘦包装)。实现已分层进 fleetdash/ 包:
    fleetdash/config.py   全局配置与运行期状态(唯一可变真相)
    fleetdash/db.py       sqlite 只读 + 主机标识
    fleetdash/data.py     快照/时序/归因/agg(/api/agg 载荷)
    fleetdash/alerts.py   企微推送 + 告警状态机
    fleetdash/poller.py   edge 轮询 + 好帧缓存 + peers 热加载
    fleetdash/report.py   每日日报(PIL 出图 + 推送)
    fleetdash/webapp.py   HTTP: web/ 静态页 + /api/page|agg|health|power
    fleetdash/main.py     启动器

用法不变:
    ./server.py --role slave
    ./server.py --role master --peers 53fa=http://...
    ./server.py --role edge --peers ...
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fleetdash.main import main   # noqa: E402

if __name__ == "__main__":
    sys.exit(main() or 0)
