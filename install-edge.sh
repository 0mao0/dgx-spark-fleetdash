#!/usr/bin/env bash
# fleetdash edge 部署 — 在腾讯云 VPS(124.221.238.70) 上以 root 运行
#   bash install-edge.sh [server.py 路径]      # 默认 ./server.py
# 幂等: 重拷 server.py 后重跑即完成升级(覆盖文件 + restart)。
#
# edge 是观察者角色(见 server.py 头注释): 拉三台 DGX 的 /api/agg、渲染页面、
# 失联缓存、企业微信车队级告警、日报。DGX 侧零改动。
set -euo pipefail

SRC="${1:-./server.py}"
DIR=/opt/fleetdash
WEBHOOK_FILE=/etc/fleetdash/webhook
PEER_4C47="${PEER_4C47:-http://100.125.117.85:9100}"
PEER_53FA="${PEER_53FA:-http://100.107.85.39:9100}"
PEER_DBDD="${PEER_DBDD:-http://100.86.101.0:9100}"
# ⚠ 加机器时这里和下面的 for / ExecStart 三处都要改: 本脚本会**整份重写** unit
# 文件, 少写一台 = 重跑升级时把那台从 edge 的 peer 列表里悄悄删掉。
# 解释器: VPS 系统 python3 可能只有 3.6, 用 PYTHON=python3.11 指定
PYTHON="${PYTHON:-python3}"
PYBIN="$(command -v "$PYTHON" || true)"

[ -f "$SRC" ] || { echo "[!] 找不到 $SRC" >&2; exit 1; }

# 前置: 每台 peer 的 9100 必须从 VPS 经 tailnet 可达(装之前先探, 避免瞎装)
for pair in "4c47:$PEER_4C47" "53fa:$PEER_53FA" "dbdd:$PEER_DBDD"; do
  k="${pair%%:*}"; u="${pair#*:}"
  if curl -s --max-time 6 "$u/api/health" >/dev/null; then
    echo "[ok] tailnet 可达 $k $u"
  else
    echo "[!!] tailnet 不可达 $k $u —— 先检查该机的 tailscale/tailscaled 与防火墙, 再继续" >&2
    FAIL=1
  fi
done
[ "${FAIL:-0}" = 1 ] && exit 1

PYBIN="$(command -v "$PYTHON" || true)"
[ -n "$PYBIN" ] || { echo "[!] 找不到解释器 $PYTHON" >&2; exit 1; }
"$PYBIN" -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" || {
  echo "[!] $PYBIN 需 >= 3.8。用 PYTHON=python3.11 指定(VPS 已装 dnf 的 python3.11)" >&2; exit 1; }

mkdir -p "$DIR" /etc/fleetdash
cp "$SRC" "$DIR/server.py"
chmod 644 "$DIR/server.py"

if [ ! -s "$WEBHOOK_FILE" ]; then
  echo "[!] $WEBHOOK_FILE 不存在: edge 会正常运行但告警只写日志不推送。启用推送:"
  echo "    echo '<企业微信机器人 webhook URL>' > $WEBHOOK_FILE && chmod 600 $WEBHOOK_FILE"
  echo "    systemctl restart fleetdash-edge"
fi

cat > /etc/systemd/system/fleetdash-edge.service <<EOF
[Unit]
Description=fleetdash edge observer (dgx fleet)
After=network-online.target

[Service]
Type=simple
ExecStart=$PYBIN $DIR/server.py --role edge \
  --db $DIR/fleet.db \
  --peers 4c47=$PEER_4C47 \
  --peers 53fa=$PEER_53FA \
  --peers dbdd=$PEER_DBDD \
  --poll 30 --fail-th 4 \
  --bind 127.0.0.1 --port 9101
Restart=always
RestartSec=5
Nice=5
MemoryMax=192M

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now fleetdash-edge.service
systemctl restart fleetdash-edge.service
sleep 3
echo "--- status ---"
systemctl --no-pager --lines=3 status fleetdash-edge.service | head -6 || true
echo "--- health ---"
curl -s --max-time 5 http://127.0.0.1:9101/api/health || echo "(health 无响应)"
echo
