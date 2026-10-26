# -*- coding: utf-8 -*-
"""生成 README 用架构图 assets/architecture.png。
重生成: python tools/make_arch.py  (依赖 matplotlib + Pillow, 仅开发机需要)"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle, FancyArrowPatch
from PIL import Image

plt.rcParams["font.family"] = ["Microsoft YaHei", "Noto Sans CJK SC", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

fig, ax = plt.subplots(figsize=(9.6, 5.3), dpi=200)
ax.set_xlim(0, 96); ax.set_ylim(0, 53); ax.axis("off")
fig.patch.set_facecolor("white")

INK, SUB, LINE, MUT = "#1A2027", "#5B6472", "#9AA3AE", "#B0B7C0"
TEAL, INDIGO, AMBER = "#0F766E", "#4338CA", "#B45309"

def card(x, ytop, w, title, sub, accent=TEAL, fs=9.2, sfs=7.6):
    h = 7.8
    ax.add_patch(FancyBboxPatch((x, ytop - h), w, h,
                                boxstyle="round,pad=0.35,rounding_size=0.7",
                                fc="#F6F8F9", ec="#E2E6EA", lw=0.9, zorder=2))
    ax.add_patch(FancyBboxPatch((x + 0.55, ytop - h + 1.05), 0.42, h - 2.1,
                                boxstyle="round,pad=0.05,rounding_size=0.2",
                                fc=accent, ec="none", zorder=3))
    ax.text(x + 1.7, ytop - 1.8, title, ha="left", va="top",
            fontsize=fs, fontweight="bold", color=INK, zorder=3)
    ax.text(x + 1.7, ytop - 4.6, sub, ha="left", va="top",
            fontsize=sfs, color=SUB, zorder=3)
    return ytop - h

def conn(x, y1, y2):
    ax.add_patch(FancyArrowPatch((x, y1), (x, y2), arrowstyle="-|>",
                                 mutation_scale=8, color=LINE, lw=1.0, zorder=1))

def flow(x1, y1, x2, y2, col=LINE, ls="-", lw=1.2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>",
                                 mutation_scale=10, color=col, lw=lw,
                                 linestyle=ls, zorder=1))

def band(x, y0, w, h, name, rest, col):
    ax.add_patch(FancyBboxPatch((x, y0), w, h,
                                boxstyle="round,pad=0.6,rounding_size=1.4",
                                fc="#F2F5F6", ec="none", zorder=0))
    ax.text(x + 1.5, y0 + h - 1.2, name, ha="left", va="top",
            fontsize=8.6, fontweight="bold", color=col)
    name_w = sum(1.06 if ord(c) > 0x2E80 else 0.62 for c in name)
    ax.text(x + 1.5 + name_w + 0.9, y0 + h - 1.32, rest, ha="left",
            va="top", fontsize=7.0, color=SUB)

# ── 标题（左上, 编辑式）──
ax.text(2, 50.8, "fleetdash 数据流", ha="left", va="top",
        fontsize=13.5, fontweight="bold", color=INK)
ax.text(2, 47.2, "数据不出本机 · VPS 只做观察者（只拉不存）", ha="left",
        va="top", fontsize=8.6, color=SUB)

# ── 列 1: 每台 DGX ──
band(1.5, 6.2, 29.5, 37.8, "每台 DGX", "× N 台 · systemd user 服务", TEAL)
a1 = card(4, 40.8, 24.5, "collector.py", "每 30s 采一次 · 仅 Python 标准库")
conn(16.2, a1 - 0.3, 30.0)
a2 = card(4, 29.5, 24.5, "sqlite（fleet.db）", "唯一历史存放处 · 留 14 天")
conn(16.2, a2 - 0.3, 18.7)
a3 = card(4, 18.2, 24.5, "server.py :9100", "读本机库出 /api/agg（快照+曲线）")

# ── 拉取（方向: VPS→机器）──
flow(28.8, 15.0, 42.5, 36.8, col=LINE, ls=(0, (3, 2)))
ax.text(35.6, 28.5, "每 30s 拉\n/api/agg", ha="center", va="center",
        fontsize=7.8, color=AMBER)
ax.text(35.6, 25.2, "方向：VPS → 机器\n机器从不外推", ha="center", va="top",
        fontsize=6.8, color=SUB)

# ── 列 2: VPS ──
band(40.5, 6.2, 33.5, 37.8, "VPS / 观察者机", "fleetdash-edge", INDIGO)
b1 = card(43, 40.8, 28.5, "edge 轮询", "拉 N 台 · 连挂 4 次判失联（先 ping 确认）", INDIGO)
conn(57.2, b1 - 0.3, 30.0)
b2 = card(43, 29.5, 28.5, "单页面板", "60s 自刷新 · 曲线现拉现算，不建集中库")
conn(57.2, b2 - 0.3, 18.7)
b3 = card(43, 18.2, 28.5, "告警状态机 + 日报", "失联/掉线/脑死/时钟 · 每日 09:00 日报图", AMBER)

# ── 列 3: 出口 ──
c1 = card(79.5, 32.2, 14.8, "浏览器", "nginx 反代 + basic auth")
c2 = card(79.5, 19.8, 14.8, "企业微信", "告警 + 日报（留痕即台账）", AMBER)
flow(71.8, 27.0, 79.2, 28.6)
ax.text(75.5, 29.6, "渲染", ha="center", fontsize=7.2, color=SUB)
flow(71.8, 16.2, 79.2, 19.2, col=AMBER)
ax.text(75.5, 15.0, "推送", ha="center", fontsize=7.2, color=AMBER)

ax.text(94, 4.2, "重生成: python tools/make_arch.py", ha="right",
        fontsize=6.2, color=MUT)

out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "assets", "architecture.png")
os.makedirs(os.path.dirname(out), exist_ok=True)
fig.savefig(out, bbox_inches="tight", pad_inches=0.10, facecolor="white")
im = Image.open(out); im.save(out, dpi=(96, 96))
print("saved", out, im.size)
