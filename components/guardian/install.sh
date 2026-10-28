#!/usr/bin/env bash
# vram-guardian 一键安装（GB10 统一内存保护）。
# 用法: sudo bash components/guardian/install.sh
#   脚本入 ~/scripts/，unit 由模板生成（@HOME@ 按当前用户替换），
#   override 不存在时才落样例（不覆盖现场调好的阈值）。
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
TARGET_USER="${SUDO_USER:-$USER}"
HOME_DIR=$(getent passwd "$TARGET_USER" | cut -d: -f6)
UNIT_DIR=/etc/systemd/system
OVR_DIR="$UNIT_DIR/vram-guardian.service.d"

mkdir -p "$HOME_DIR/scripts" "$HOME_DIR/logs"
install -m 644 "$SRC/vram_guardian.py" "$HOME_DIR/scripts/vram_guardian.py"

sed -e "s|@HOME@|$HOME_DIR|g" "$SRC/vram-guardian.service.template" \
    > "$UNIT_DIR/vram-guardian.service"

mkdir -p "$OVR_DIR"
if [ ! -f "$OVR_DIR/override.conf" ]; then
    cp "$SRC/override.conf.example" "$OVR_DIR/override.conf"
fi

systemctl daemon-reload
systemctl enable --now vram-guardian
echo "installed. check: systemctl status vram-guardian / tail -f $HOME_DIR/logs/vram_guardian.log"
