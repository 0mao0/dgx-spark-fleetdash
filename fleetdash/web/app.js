/* fleetdash 前端：数据来自 /api/page?w= ，30s 刷新。
 * A 栏 = 系统参数(左统计右曲线)，B 栏 = 服务参数。
 * 曲线为手写 SVG sparkline，零依赖。 */
"use strict";

const $ = (s) => document.querySelector(s);
const REFRESH_MS = 30000;
const KIND_COLOR = {
  llm: "#e8590c", embed: "#1c7ed6", rerank: "#7048e8", asr: "#0ca678",
  tts: "#f59f00", face: "#868e96", parse: "#c2255c", proxy: "#adb5bd",
};
const KIND_ZH = {
  llm: "LLM", embed: "Embed", rerank: "Rerank", asr: "ASR",
  tts: "TTS", face: "人脸", parse: "解析", proxy: "代理",
};

const params = new URLSearchParams(location.search);
let win = params.get("w") || "3h";
if (!["3h", "1d", "3d", "7d"].includes(win)) win = "3h";
let selHost = params.get("hn") || "";   // 告警深链 ?hn=4c47
let lastData = null;
let sysExpanded = false;

/* ---------- 格式化 ---------- */
function fmtTok(n, dash = "—") {
  if (n === null || n === undefined) return dash;
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "G";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(Math.round(n));
}
function fmtNum(v, dp = 1, dash = "—") {
  if (v === null || v === undefined || isNaN(v)) return dash;
  return v.toFixed(dp);
}
function fmtClock(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString("zh-CN", { hour12: false });
}

/* ---------- sparkline ---------- */
function sparkline(pts, color, h = 46) {
  const W = 300, H = h, PAD = 4;
  if (!pts || pts.length < 2) {
    return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">` +
      `<text x="${W / 2}" y="${H / 2 + 4}" fill="#55606d" font-size="12" ` +
      `text-anchor="middle">窗口内无数据</text></svg>`;
  }
  const ts = pts.map((p) => p[0]), vs = pts.map((p) => p[1]);
  const t0 = ts[0], t1 = ts[ts.length - 1];
  let lo = Math.min(...vs), hi = Math.max(...vs);
  if (hi - lo < 1e-9) { hi += 0.5; lo -= 0.5; }
  const span = Math.max(1, t1 - t0);
  const gap = span / pts.length * 3;          // 断点阈值：3 倍采样间隔
  const X = (t) => PAD + (t - t0) / span * (W - 2 * PAD);
  const Y = (v) => H - PAD - (v - lo) / (hi - lo) * (H - 2 * PAD);
  let d = "", open = false, lastT = null;
  for (const [t, v] of pts) {
    const cmd = (open && t - lastT <= gap) ? "L" : "M";
    d += `${cmd}${X(t).toFixed(1)},${Y(v).toFixed(1)}`;
    open = true; lastT = t;
  }
  // 面积：按 M/L 分段闭合到底边
  let area = "";
  let seg = [];
  const flush = () => {
    if (seg.length > 1) {
      const first = seg[0], last = seg[seg.length - 1];
      area += `M${first[0]},${(H - PAD).toFixed(1)}`;
      for (const p of seg) area += `L${p[0]},${p[1]}`;
      area += `L${last[0]},${(H - PAD).toFixed(1)}Z`;
    }
    seg = [];
  };
  let lastT2 = null, open2 = false;
  for (const [t, v] of pts) {
    if (!(open2 && t - lastT2 <= gap)) flush();
    seg.push([X(t).toFixed(1), Y(v).toFixed(1)]);
    open2 = true; lastT2 = t;
  }
  flush();
  const last = pts[pts.length - 1];
  return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">` +
    `<path d="${area}" fill="${color}" opacity="0.13"/>` +
    `<path d="${d}" fill="none" stroke="${color}" stroke-width="1.6"` +
    ` stroke-linejoin="round" vector-effect="non-scaling-stroke"/>` +
    `<circle cx="${X(last[0]).toFixed(1)}" cy="${Y(last[1]).toFixed(1)}"` +
    ` r="2.6" fill="${color}"/></svg>`;
}

/* ---------- 堆叠柱状图（内存按服务归因） ---------- */
// pts: [[ts, v1, v2, ...seg...]], colors 与 seg 对齐(含可选残差段), 零宽段跳过
function stackBars(pts, colors, h = 46) {
  const W = 300, H = h, PAD = 4;
  if (!pts || pts.length < 2) {
    return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">` +
      `<text x="${W / 2}" y="${H / 2 + 4}" fill="#55606d" font-size="12"` +
      ` text-anchor="middle">窗口内无数据</text></svg>`;
  }
  const t0 = pts[0][0], t1 = pts[pts.length - 1][0];
  const span = Math.max(1, t1 - t0);
  const totals = pts.map((p) => p.slice(1).reduce((a, b) => a + b, 0));
  const hi = Math.max(...totals, 1e-6) * 1.04;
  const X = (t) => PAD + (t - t0) / span * (W - 2 * PAD);
  const bw = Math.max(1.2, ((W - 2 * PAD) / pts.length) * 0.9);
  let bars = "";
  for (const p of pts) {
    const x = X(p[0]) - bw / 2;
    let y = H - PAD;
    for (let s = 1; s < p.length; s++) {
      const v = p[s];
      if (v <= 0) continue;
      const bh = v / hi * (H - 2 * PAD);
      bars += `<rect x="${x.toFixed(1)}" y="${(y - bh).toFixed(1)}" ` +
        `width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="${colors[s - 1]}"/>`;
      y -= bh;
    }
  }
  return `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"` +
    ` shape-rendering="crispEdges">${bars}</svg>`;
}
const PALETTE = ["#4c8dff", "#e8590c", "#1c7ed6", "#7048e8", "#0ca678",
                 "#f59f00", "#c2255c", "#2fb3a7", "#868e96"];
function svcColorMap(snap) {
  const map = {};
  (snap && snap.models ? snap.models : []).forEach((m) => {
    map[m.svc] = KIND_COLOR[m.kind] || "#adb5bd";
  });
  return map;
}

/* ---------- 通用小工具 ---------- */
function seriesOf(ser, key) { return (ser && ser.heat && ser.heat[key]) || []; }
function modelSeriesOf(ser, svc, key) {
  const m = ser && ser.models && ser.models[svc];
  return (m && m[key]) || [];
}
function peak(pts) {
  if (!pts || !pts.length) return null;
  return Math.max(...pts.map((p) => p[1]));
}
function pad2(n) { return String(n).padStart(2, "0"); }
function fmtAxisTs(ts) {
  const d = new Date(ts * 1000);
  return `${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}
function fmtTipTs(ts, withDate) {
  const d = new Date(ts * 1000);
  const hm = `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
  return withDate ? `${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${hm}` : hm;
}
// 每行曲线的值格式化（与左列单位一致），tooltip 用
const SYS_FMT = {
  mem: (v) => fmtNum(v, 1) + " G",
  temp: (v) => fmtNum(v, 0) + " ℃",
  mhz: (v) => fmtNum(v, 0) + " MHz",
  pw: (v) => fmtNum(v, 0) + " W",
  util: (v) => fmtNum(v, 0) + " %",
  disk: (v) => fmtNum(v, 0) + " G",
  tok: (v) => fmtTok(v) + " tok",
};
const PTS_FMT = {
  "生成 tok/s": (v) => fmtNum(v, 1) + " tok/s",
  "并发": (v) => fmtNum(v, 1),
  "TTFT": (v) => fmtNum(v / 1000, 2) + " s",
  "请求/s": (v) => fmtNum(v, 2) + " /s",
};
function rowEl(nameHtml, val, sub, pts, color, fmt) {
  const el = document.createElement("div");
  el.className = "row";
  const axis = pts && pts.length >= 2
    ? `<span class="axis">${fmtAxisTs(pts[0][0])} →</span>` : "";
  el.innerHTML =
    `<div class="stats"><span class="name">${nameHtml}</span>` +
    `<span class="val">${val}</span><span class="sub">${sub || ""}</span></div>` +
    `<div class="chart">${axis}${sparkline(pts, color)}</div>`;
  if (pts && pts.length >= 2) {
    const svg = el.querySelector("svg");
    if (svg) svg._fd = { pts, fmt: fmt || ((v) => fmtNum(v, 1)) };
  }
  return el;
}

/* ---------- A 栏：系统参数 ---------- */
const SYS_ROWS = [
  { key: "mem",  label: "内存",   color: "#4c8dff" },
  { key: "temp", label: "温度",   color: "#e5484d" },
  { key: "mhz",  label: "频率",   color: "#2fb3a7" },
  { key: "pw",   label: "功耗",   color: "#e8a33d" },
  { key: "util", label: "利用率", color: "#7c5cff" },
  { key: "disk", label: "磁盘",   color: "#868e96" },
  { key: "tok",  label: "总生成 token", color: "#3db36e" },
];
function sysRow(cfgItem, snap, ser) {
  const k = cfgItem.key;
  let name = cfgItem.label, val = "—", sub = "", pts = [];
  if (k === "mem") {
    pts = seriesOf(ser, "mem_used");
    val = fmtNum(snap.mem_used, 1) + "G";
    sub = snap.mem_total ? `共 ${fmtNum(snap.mem_total, 0)}G · 峰 ${fmtNum(peak(pts), 1)}G` : "";
    // 堆叠柱状(按服务显存归因 + 未归因残差); 老数据没有堆叠列时退回单线
    const st = ser && ser.vram_stack;
    if (st && st.pts && st.pts.length >= 2) {
      const cmap = svcColorMap(snap);
      const names = st.order.slice();
      const cols = st.order.map((s, i) => cmap[s] || PALETTE[i % PALETTE.length]);
      // 尾部残差拆两段：系统/其他进程(已用-归因) + 空闲(总-已用)，柱高恒等于总内存
      if (st.other) { names.push("系统/其他"); cols.push("#3d4655"); }
      if (st.free) { names.push("空闲"); cols.push("#242b36"); }
      const el = rowEl(name, val, sub, [], cfgItem.color, SYS_FMT.mem);
      el.querySelector(".chart").innerHTML =
        `<span class="axis">${fmtAxisTs(st.pts[0][0])} →</span>` + stackBars(st.pts, cols);
      const svg = el.querySelector("svg");
      svg._fd = {
        pts: st.pts.map((p) => [p[0], p.slice(1).reduce((a, b) => a + b, 0)]),
        segs: st.pts, names, colors: cols, fmt: (v) => fmtNum(v, 1) + " G",
      };
      return el;
    }
  } else if (k === "temp") {
    pts = seriesOf(ser, "gpu_temp");
    val = fmtNum(snap.gpu_temp, 0) + "℃";
    const p = peak(pts);
    sub = p === null ? "" : `窗口峰值 ${fmtNum(p, 0)}℃`;
  } else if (k === "mhz") {
    pts = seriesOf(ser, "sm_mhz");
    val = snap.sm_mhz === null || snap.sm_mhz === undefined ? "—" : fmtNum(snap.sm_mhz, 0) + " MHz";
    const p = peak(pts);
    sub = p === null ? "" : `窗口峰值 ${fmtNum(p, 0)} MHz`;
  } else if (k === "pw") {
    pts = seriesOf(ser, "gpu_pw");
    val = fmtNum(snap.gpu_pw, 0) + " W";
    if (snap.month_wh !== null && snap.month_wh !== undefined) {
      const kwh = snap.month_wh / 1000;
      sub = `本月 ${kwh < 100 ? kwh.toFixed(1) : Math.round(kwh)} kWh ≈ ¥` +
            (kwh * 0.6 < 100 ? (kwh * 0.6).toFixed(1) : Math.round(kwh * 0.6));
    }
  } else if (k === "util") {
    pts = seriesOf(ser, "gpu_util");
    val = fmtNum(snap.gpu_util, 0) + "%";
    const p = peak(pts);
    sub = p === null ? "" : `窗口峰值 ${fmtNum(p, 0)}%`;
  } else if (k === "disk") {
    pts = seriesOf(ser, "disk_used");
    val = snap.disk_used === null || snap.disk_used === undefined ? "—" : fmtNum(snap.disk_used, 0) + "G";
    sub = snap.disk_total ? `共 ${fmtNum(snap.disk_total / 1024, 1)}T` : "";
  } else if (k === "tok") {
    pts = seriesOf(ser, "tok");
    name = "总生成 token";
    val = fmtTok(snap.day_tok);
    sub = snap.hist_tok !== null && snap.hist_tok !== undefined
      ? `今日 · 保留期合计 ${fmtTok(snap.hist_tok)}`
      : "今日（曲线 = 每采样周期新增）";
  }
  const el = rowEl(name, val, sub, pts, cfgItem.color, SYS_FMT[k]);
  if (!SYS_ROWS.slice(0, 3).some((r) => r.key === k)) {
    el.classList.add("collapsible");        // 手机默认隐藏; 桌面 CSS 强制显示
    el.classList.toggle("open", sysExpanded);
  }
  return el;
}

/* ---------- B 栏：服务参数 ---------- */
function svcRow(m, ser) {
  const svc = m.svc, kind = m.kind || "proxy";
  const color = KIND_COLOR[kind] || "#adb5bd";
  const dead = m.alive === 0;
  const nameHtml = `<span class="kind" style="background:${dead ? "#e5484d" : color}"></span>` +
    `${svc}<span class="badge">${KIND_ZH[kind] || kind}</span>`;
  let val = "—", sub = "", pts = [], ptsLabel = "";
  const rr = m.req_run, ms = ser && ser.models ? ser.models[svc] : null;
  const g = (k) => (ms && ms[k]) || [];
  if (kind === "llm") {
    val = (m.tps === null || m.tps === undefined) ? "—" : fmtNum(m.tps, 1) + " tok/s";
    sub = `并发 ${rr === null || rr === undefined ? "—" : rr}` +
          (m.dec_tps ? ` · 单请求 ${fmtNum(m.dec_tps, 1)} tok/s` : "");
    pts = g("tps").length ? g("tps") : (g("req_run").length ? g("req_run") : g("act_rate"));
    ptsLabel = g("tps").length ? "生成 tok/s" : "并发";
  } else if (kind === "tts") {
    const has = m.ttft_ms !== null && m.ttft_ms !== undefined;
    val = has ? fmtNum(m.ttft_ms / 1000, 2) + " s" : "—";
    sub = `并发 ${rr === null || rr === undefined ? "—" : rr}` +
          (has ? "" : " · 引擎未暴露 TTFT");
    pts = has && g("ttft_ms").length ? g("ttft_ms") : g("req_run");
    ptsLabel = has && g("ttft_ms").length ? "TTFT" : "并发";
  } else {
    val = `并发 ${rr === null || rr === undefined ? "—" : rr}`;
    const ar = m.act;
    sub = (ar === null || ar === undefined) ? "" : `请求 ${fmtNum(ar, 2)}/s`;
    pts = g("act_rate").length ? g("act_rate") : g("req_run");
    ptsLabel = g("act_rate").length ? "请求/s" : "并发";
  }
  // 每张卡片都带显存归因(GB10 统一内存: 这个数就是该服务实际占的内存)
  if (m.vram && m.vram > 0) sub = (sub ? sub + " · " : "") + `VRAM ${fmtNum(m.vram / 1024, 1)}G`;
  const el = rowEl(nameHtml + (ptsLabel ? `<span class="badge">${ptsLabel}</span>` : ""),
                   val, sub, pts, dead ? "#e5484d" : color, PTS_FMT[ptsLabel]);
  if (dead) el.classList.add("dead");
  return el;
}

/* ---------- 渲染 ---------- */
function pickHost(data) {
  const hs = data.hosts || [];
  if (!hs.length) return null;
  let h = hs.find((x) => x.host === selHost || x.label === selHost);
  if (!h) h = hs.find((x) => !x.down && !x.error);
  return h || hs[0];
}
function renderHostTabs(data) {
  const box = $("#hosts");
  box.innerHTML = "";
  (data.hosts || []).forEach((h) => {
    const b = document.createElement("button");
    const bad = h.down || h.error;
    b.textContent = (bad ? "✕ " : "") + (h.label || h.host);
    if (h === pickHost(data)) b.classList.add("on");  // 跟实际展示的机走，无参加载也有高亮
    b.style.color = bad && !b.classList.contains("on") ? "var(--red)" : "";
    b.onclick = () => { selHost = h.host; render(data); };
    box.appendChild(b);
  });
}
function render(data) {
  lastData = data;
  const host = pickHost(data);
  renderHostTabs(data);
  const banner = $("#banner");
  if (!host) {
    banner.hidden = false;
    banner.textContent = "没有任何主机数据（edge 尚未拉到好帧）";
    return;
  }
  const snap = host.snapshot || {}, ser = host.series || {};
  banner.hidden = !(host.down || host.error);
  if (host.down || host.error) {
    banner.textContent = `${host.label || host.host} 离线：` +
      (host.error || "edge 连续拉取失败，页面显示的是最后一次好帧之后的状态");
  }
  // A 栏
  const sysBox = $("#sysRows");
  sysBox.innerHTML = "";
  SYS_ROWS.forEach((c) => sysBox.appendChild(sysRow(c, snap, ser)));
  const more = $("#sysMore");
  more.hidden = false;
  more.textContent = sysExpanded ? "收起 ▴" : "展开其余参数 ▾";
  // B 栏
  const svcBox = $("#svcRows");
  svcBox.innerHTML = "";
  const models = snap.models || [];
  if (!models.length) {
    svcBox.innerHTML = `<div class="row"><div class="stats">` +
      `<span class="name">服务</span><span class="val">—</span>` +
      `<span class="sub">无注册服务</span></div><div class="chart"></div></div>`;
  }
  models.forEach((m) => svcBox.appendChild(svcRow(m, ser)));
  // 顶栏状态
  $("#tsInfo").textContent = `数据 ${fmtClock(host.snapshot ? host.snapshot.ts : data.ts)} · 刷新 ${fmtClock(data.ts)}`;
  $("#liveDot").classList.toggle("bad", !!host.down || !!host.error);
}

/* ---------- 数据获取 ---------- */
async function load() {
  try {
    const r = await fetch(`api/page?w=${win}`, { cache: "no-store" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    render(await r.json());
  } catch (e) {
    $("#liveDot").classList.add("bad");
    toast("拉取失败：" + e.message);
  }
}
let toastTimer = 0;
function toast(msg) {
  const t = $("#toast");
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.hidden = true; }, 6000);
}

/* ---------- 交互 ---------- */
// 下拉开合(窗口/电源共用一套: 各自按钮 toggle, 点别处全关)
function menuToggle(btnId, menuId) {
  const wasOpen = !$(menuId).hidden;
  menusClose();               // 两个下拉互斥: 同一时刻最多开一个
  if (!wasOpen) {
    $(menuId).hidden = false;
    $(btnId).classList.add("on");
  }
}
function menusClose() {
  for (const [b, m] of [["#winsBtn", "#winsMenu"], ["#pwrBtn", "#pwrMenu"]]) {
    $(m).hidden = true;
    $(b).classList.remove("on");
  }
}
$("#winsBtn").addEventListener("click", (e) => {
  e.stopPropagation();
  menuToggle("#winsBtn", "#winsMenu");
});
$("#winsMenu").addEventListener("click", (e) => {
  const b = e.target.closest("button"); if (!b) return;
  win = b.dataset.w;
  $("#winsBtn").textContent = win + " ▾";
  menusClose();
  load();
});
$("#sysMore").addEventListener("click", () => {
  sysExpanded = !sysExpanded;
  document.querySelectorAll("#sysRows .collapsible").forEach((el) => {
    el.classList.toggle("open", sysExpanded);
  });
  $("#sysMore").textContent = sysExpanded ? "收起 ▴" : "展开其余参数 ▾";
});
// 电源下拉: 先点开菜单再选项, 加上原有 confirm+token, 三道防误点
function pwrMenu(open) {
  $("#pwrMenu").hidden = !open;
  $("#pwrBtn").classList.toggle("on", open);
}
$("#pwrBtn").addEventListener("click", (e) => {
  e.stopPropagation();
  menuToggle("#pwrBtn", "#pwrMenu");
});
document.addEventListener("click", (e) => {
  if (!e.target.closest("#pwr") && !e.target.closest("#wins")) menusClose();
});
$("#pwrMenu").addEventListener("click", async (e) => {
  const b = e.target.closest("button"); if (!b) return;
  pwrMenu(false);
  if (!lastData) return;
  const host = pickHost(lastData);
  if (!host) return;
  const act = b.dataset.act;
  const verb = { wake: "唤醒", reboot: "重启", poweroff: "关机" }[act];
  const nm = host.label || host.host;
  if (!confirm(`对「${nm}」执行${verb}？`)) return;
  let tok = sessionStorage.getItem("fd_pwr") || "";
  tok = prompt("电源操作 token：", tok) || "";
  if (!tok) return;
  try {
    const r = await fetch("api/power", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: act, token: tok, host: host.host, target: host.host }),
    });
    const j = await r.json();
    if (r.ok) {
      sessionStorage.setItem("fd_pwr", tok);
      toast(j.note || (nm + verb + "指令已下达"));
    } else {
      toast(j.err || ("失败 HTTP " + r.status));
    }
  } catch (err) {
    toast("请求失败：" + err.message);
  }
});

/* 曲线悬停提示：时间 + 数值（svg 存 _fd，30s 重绘自动换新数据） */
const tip = document.createElement("div");
tip.id = "tip"; tip.hidden = true;
document.body.appendChild(tip);
document.addEventListener("mousemove", (e) => {
  const svg = e.target.closest ? e.target.closest(".chart svg") : null;
  const fd = svg && svg._fd;
  if (!fd) { tip.hidden = true; return; }
  const r = svg.getBoundingClientRect();
  if (e.clientX < r.left || e.clientX > r.right) { tip.hidden = true; return; }
  const PADR = 4 / 300;                        // 与 sparkline 的 PAD/W 对齐
  let f = (e.clientX - r.left) / r.width;
  f = Math.min(1, Math.max(0, (f - PADR) / (1 - 2 * PADR)));
  const n = fd.pts.length;
  const t0 = fd.pts[0][0], t1 = fd.pts[n - 1][0];
  const tHit = t0 + f * (t1 - t0);             // 按时间轴取最近点(采样非等距时更准)
  let lo = 0, hi = n - 1;
  while (hi - lo > 1) { const mid = (lo + hi) >> 1;
    if (fd.pts[mid][0] < tHit) lo = mid; else hi = mid; }
  const i = (fd.pts[hi][0] - tHit) < (tHit - fd.pts[lo][0]) ? hi : lo;
  const t = fd.pts[i][0], v = fd.pts[i][1];
  if (fd.segs && fd.segs[i]) {
    const row = fd.segs[i];
    const total = row.slice(1).reduce((a, b) => a + b, 0) || 1;
    let html = `<div style="margin-bottom:3px">${fmtTipTs(t, t1 - t0 > 14400)}` +
      ` · 合计 ${fmtNum(total, 1)} G</div>`;
    const BAR = 96;                               // 弹框内水平条基准宽(px)
    for (let s = 1; s < row.length; s++) {
      if (!row[s] || row[s] < 0.05) continue;
      const w = Math.max(1, Math.round(row[s] / total * BAR));
      html += `<div style="display:flex;align-items:center;gap:5px">` +
        `<span style="flex:0 0 68px;text-align:right;color:#93a0b4">${fd.names[s - 1]}</span>` +
        `<span style="flex:0 0 ${BAR}px;height:6px;background:#1b222c;border-radius:3px;overflow:hidden">` +
        `<span style="display:block;height:100%;width:${w}px;background:${fd.colors[s - 1]}"></span></span>` +
        `<span>${fmtNum(row[s], 1)} G</span></div>`;
    }
    tip.innerHTML = html;
  } else {
    tip.textContent = `${fmtTipTs(t, t1 - t0 > 14400)} · ${fd.fmt(v)}`;
  }
  tip.hidden = false;
  const flip = e.clientX + 160 > innerWidth;
  tip.style.left = (flip ? e.clientX - 150 : e.clientX + 12) + "px";
  tip.style.top = Math.min(e.clientY + 16, innerHeight - 34) + "px";
});

/* 初始窗口按钮同步 + 启动 */
$("#winsBtn").textContent = win + " ▾";
load();
setInterval(load, REFRESH_MS);
