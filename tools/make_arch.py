# -*- coding: utf-8 -*-
"""生成 README 用架构图 assets/architecture.png。
重生成: python tools/make_arch.py  (依赖 matplotlib + Pillow, 仅开发机需要)"""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from PIL import Image

plt.rcParams["font.family"] = ["Microsoft YaHei", "Noto Sans CJK SC", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

fig, ax = plt.subplots(figsize=(10.4, 5.9), dpi=200)
ax.set_xlim(0, 104); ax.set_ylim(0, 60); ax.axis("off")

INK, GRAY, BLUE, GREEN, ORANGE = "#212121", "#8d8d8d", "#1565c0", "#2e7d32", "#e65100"

def box(x, ytop, w, title, lines, edge="#616161", fs=7.6, tf=8.6, lh=2.4):
    """画圆角框, 高度按行数自适应, 返回底边 y。lines 项可为 (颜色, 文本)。"""
    h = 5.0 + max(0, len(lines) - 1) * lh + 1.6
    ax.add_patch(FancyBboxPatch((x, ytop - h), w, h,
                                boxstyle="round,pad=0.5,rounding_size=1.2",
                                fc="#f7f7f7", ec=edge, lw=1.1))
    ax.text(x + w / 2, ytop - 2.0, title, ha="center", va="top",
            fontsize=tf, fontweight="bold", color=INK)
    for i, ln in enumerate(lines):
        col, txt = ln if isinstance(ln, tuple) else (INK, ln)
        ax.text(x + w / 2, ytop - 4.7 - i * lh, txt, ha="center", va="top",
                fontsize=fs, color=col)
    return ytop - h

def arrow(x1, y1, x2, y2, col=GRAY, ls="-"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>",
                                 mutation_scale=10, color=col, lw=1.1,
                                 linestyle=ls, shrinkA=2, shrinkB=2))

def zone(x, y0, w, h, title, col=BLUE):
    """虚线分区框 + 框内左上角标题。"""
    ax.add_patch(FancyBboxPatch((x, y0), w, h, boxstyle="round,pad=0.8,rounding_size=1.6",
                                fc="none", ec=col, lw=1.0, linestyle="--"))
    ax.text(x + 1.6, y0 + h - 1.0, title, ha="left", va="top",
            fontsize=8.2, fontweight="bold", color=col)

# 标题（两个 zone 顶边=55, 标题占 56..58.4, 不重叠）
ax.text(52, 58.4, "fleetdash 架构：数据不出本机，VPS 只做观察者", ha="center", va="top",
        fontsize=12, fontweight="bold", color=INK)

# ── 左区: DGX 机器群 (x2..35, y17.5..55) ──
zone(2, 17.5, 33, 37.5, "DGX 机器 ×N（每台同构 · systemd user 服务）")
b1 = box(6, 51.5, 25, "collector.py",
         ["30s 采一次（std-lib，CPU≈0.4%）", "温度/时钟/内存/磁盘/网口",
          "每服务: 显存·TPS·探活·并发"], edge=GREEN)        # 底 40.1
arrow(18.5, b1 - 0.2, 18.5, 36.8)
ax.text(19.7, 38.4, "写", fontsize=7, color=GRAY, va="center")
b2 = box(6, 36.4, 25, "本机 sqlite（fleet.db）",
         [("GREEN", "历史数据唯一存放处，留 14 天"), "model 表 17 列（分类字段规范）"])   # 底 27.4
arrow(18.5, b2 - 0.2, 18.5, 24.3)
ax.text(19.7, 25.8, "读", fontsize=7, color=GRAY, va="center")
box(6, 24.0, 25, "server.py --role slave",
    ["读本机库渲染 /api/agg（快照+曲线）"])                # 底 17.4, 区内

# ── 中间缝: 拉取箭头 + 标注（标注放上方空带 y46..51, 不压箭头）──
for ym, yp in ((20.0, 41.0), (21.4, 42.5), (22.8, 44.0)):
    arrow(31.6, ym, 43.6, yp, GRAY, ls=":")
ax.text(39.5, 49.8, "每 30s 拉 /api/agg\n（VPS→机器，不推库）", ha="center",
        va="top", fontsize=6.7, color=ORANGE)

# ── 右区: VPS edge (x44..80, y16.2..55) ──
zone(44, 16.2, 36, 38.8, "VPS / 观察者机（fleetdash-edge，root 系统服务）")
c1 = box(47, 51.5, 30, "edge 轮询 (server.py --role edge)",
         ["拉 N 台聚合；连挂 4 次=判失联", "失联先 ping 二次确认（降噪）",
          "edge-cache/<机>.json：最后好帧"], edge=BLUE)     # 底 40.1
arrow(62, c1 - 0.2, 62, 37.2)
c2 = box(47, 36.8, 30, "渲染：单页 HTML 60s 自刷新",
         ["主机切换 / 3h·1d·3d·7d / hover 读数",
          ("GRAY", "（曲线随 /api/agg 现取，VPS 不落库）")],   # 底 27.8
         )
arrow(62, c2 - 0.2, 62, 25.8)
box(47, 25.5, 30, "告警状态机 + 日报",
    ["失联/恢复/停滞/服务掉线/脑死/时钟", "硬件细分/TPS 日峰/日报仪表盘图"],
    edge=ORANGE)                                            # 底 15.7, 区内

# ── 最右: 出口 ──
box(84.5, 45.0, 18.5, "浏览器",
    ["nginx 反代 + basic auth", ("GRAY", "（页面本身无鉴权）")])
box(84.5, 31.5, 18.5, "企业微信",
    ["车队级告警 + 每日 09:00 日报", "电源操作留痕"], edge=ORANGE)
arrow(77.6, 43.5, 84.2, 42.0)
ax.text(82.4, 43.9, "渲染", ha="center", fontsize=6.8, color=GRAY)
arrow(77.6, 22.0, 84.2, 27.5, ORANGE)
ax.text(82.4, 23.0, "推送", ha="center", fontsize=6.8, color=ORANGE)

# ── 底部脚注（区底≥15, 注释放 13.4 以下）──
ax.text(52, 13.4, "电源链路：页面按钮 → edge 校 token → 目标机 peer sudo；唤醒=同网段在线 peer 代发 WoL。"
                   "dgx1 兼 master：另有内网渲染口 :9100", ha="center", fontsize=7.0, color=INK)
ax.text(52, 10.0, "为什么不在 VPS 集中存库：机器挂了数据仍在其本机库、VPS 挂了内网 :9100 照看；"
                   "跨机长序列统计请读各机 sqlite 或 /api/agg", ha="center", fontsize=7.0, color=GRAY)

out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "assets", "architecture.png")
os.makedirs(os.path.dirname(out), exist_ok=True)
fig.savefig(out, bbox_inches="tight", pad_inches=0.08, facecolor="white")
im = Image.open(out); im.save(out, dpi=(96, 96))
print("saved", out, im.size)
