#!/usr/bin/env bash
# fleetdash 一键部署(在仓库目录内、目标机上运行)
#   ./deploy.sh dgx <主机标识> <master|slave> [peers]   # 被监控机, 普通用户即可
#   ./deploy.sh edge                                    # 观察者机(VPS), root 运行
# 例: ./deploy.sh dgx node2 slave
#     ./deploy.sh dgx node1 master "node2=http://100.x.x.x:9100"
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CMD="${1:-}"

staging_note() {
  echo "[i] 服务表放 ~/fleetdash/services.json 即可免改代码(样例 services.example.json)"
  echo "[i] 引擎 key 放 ~/fleetdash/vllm.key(600) 或 env FLEETDASH_VLLM_KEY; 没有也能跑(/metrics 无鉴权)"
}

case "$CMD" in
dgx)
  shift
  [ $# -ge 2 ] || { echo "usage: $0 dgx <host_key> <master|slave> [peers]" >&2; exit 1; }
  DIR="$HOME/fleetdash"
  mkdir -p "$DIR"
  # 覆盖前备份旧代码(不碰 fleet.db): 出问题可回滚 server.py + collector.py + 包
  if [ -f "$DIR/server.py" ]; then
    BK="$DIR/code-bak-$(date +%Y%m%d-%H%M%S).tar"
    tar -cf "$BK" -C "$DIR" --ignore-failed-read \
        server.py collector.py fleetdash 2>/dev/null \
      && echo "[ok] 旧代码已备份: $BK"
  fi
  cp "$HERE/collector.py" "$HERE/server.py" "$DIR/"
  rm -rf "$DIR/fleetdash"
  cp -r "$HERE/fleetdash" "$DIR/"
  cp "$HERE/scripts/install.sh" "$DIR/"
  cp "$HERE/scripts/fleetdash-wol.service" "$HERE/scripts/power-sudoers" "$DIR/"
  [ -f "$HERE/services.example.json" ] && cp "$HERE/services.example.json" "$DIR/"
  if [ ! -s "$DIR/power.token" ]; then
    if command -v openssl >/dev/null; then
      (umask 177; openssl rand -hex 16 > "$DIR/power.token")
      echo "[ok] 已生成电源 token: $DIR/power.token  值=$(cat "$DIR/power.token")"
      echo "     各机与 edge 需一致(重启/关机/唤醒按钮要用它)"
    else
      echo "[!] 无 openssl, 跳过 power.token 生成(电源按钮功能保持关闭)"
    fi
  fi
  staging_note
  bash "$DIR/install.sh" "$@"
  ;;
edge)
  shift || true
  PYTHON="${PYTHON:-python3}"
  bash "$HERE/scripts/install-edge.sh" "$HERE/server.py" "$HERE/fleetdash"
  ;;
*)
  sed -n '2,5p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 1
  ;;
esac
