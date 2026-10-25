#!/usr/bin/env bash
# fleetdash 部署脚本 — 在目标 DGX 上以 faye 身份运行
#   ./install.sh <host_key> <role> [peers]
# 例: ./install.sh 4c47 master "53fa=http://100.107.85.39:9100"
#     ./install.sh 53fa slave
set -euo pipefail

KEY="${1:?usage: install.sh <host_key> <master|slave> [peers]}"
ROLE="${2:?usage: install.sh <host_key> <master|slave> [peers]}"
PEERS="${3:-}"

DIR="$HOME/fleetdash"
mkdir -p "$DIR"

# 停掉手工起的测试进程(只杀本项目)
pkill -f "fleetdash/collector.py" 2>/dev/null || true
pkill -f "fleetdash/server.py" 2>/dev/null || true
sleep 1

# ---- systemd user units -------------------------------------------------
mkdir -p "$HOME/.config/systemd/user"

if [ "$ROLE" = "master" ]; then
  PEER_ARG=""
  if [ -n "$PEERS" ]; then PEER_ARG="--peers $PEERS"; fi
  SERVER_EXEC="$HOME/fleetdash/server.py --role master --label $KEY --port 9100 $PEER_ARG"
else
  SERVER_EXEC="$HOME/fleetdash/server.py --role slave --label $KEY --port 9100"
fi

cat > "$HOME/.config/systemd/user/fleetdash-collector.service" <<EOF
[Unit]
Description=fleetdash collector ($KEY)
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 $HOME/fleetdash/collector.py --host $KEY --db $HOME/fleetdash/fleet.db --interval 30 --keep-days 14
Restart=always
RestartSec=10
Nice=10
MemoryMax=64M
CPUQuota=5%

[Install]
WantedBy=default.target
EOF

cat > "$HOME/.config/systemd/user/fleetdash-server.service" <<EOF
[Unit]
Description=fleetdash server ($KEY, $ROLE)
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 $SERVER_EXEC
Restart=always
RestartSec=5
Nice=5
MemoryMax=128M

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now fleetdash-collector.service
systemctl --user enable --now fleetdash-server.service
# 允许用户服务在未登录时也运行
sudo -n loginctl enable-linger "$USER" 2>/dev/null || \
  echo "[!] 无法 enable-linger (需要 sudo)，注销后服务会停。可手动执行: sudo loginctl enable-linger $USER"

sleep 4
echo "--- status ---"
systemctl --user --no-pager --lines=0 status fleetdash-collector.service | head -5 || true
systemctl --user --no-pager --lines=0 status fleetdash-server.service  | head -5 || true
echo "--- health ---"
curl -s --max-time 5 "http://127.0.0.1:9100/api/health" || echo "(health 端点无响应)"
echo
echo "--- 本机页面字节数 (master 才有内容) ---"
curl -s --max-time 8 "http://127.0.0.1:9100/" | wc -c
