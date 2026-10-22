#!/usr/bin/env bash
# Install the edge-node units on the target host (run with sudo on the Raspberry Pi).
# Pure ASCII on purpose. Idempotent: safe to re-run.
#
# Usage:
#   sudo bash install.sh --repo /home/ddf/cems --user ddf --platform-host 192.168.31.18
#
# What it does:
#   1. copies the three unit files into /etc/systemd/system
#   2. rewrites WorkingDirectory / User / python path / MQTT_HOST for this host
#   3. enables + restarts the services
#   4. prints status
set -euo pipefail

REPO=/home/ddf/cems
RUN_USER=ddf
PLATFORM_HOST=192.168.31.18

while [ $# -gt 0 ]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --user) RUN_USER="$2"; shift 2 ;;
    --platform-host) PLATFORM_HOST="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

SRC="$(cd "$(dirname "$0")" && pwd)"
PY="$REPO/.venv/bin/python"
if [ ! -x "$PY" ]; then
  echo "ERROR: $PY not found; create the venv first (deploy/raspi/provision_edge.sh)" >&2
  exit 1
fi

for unit in cems-device cems-gateway-plant1 cems-gateway-plant2; do
  sed -e "s#/home/ddf/cems#$REPO#g" \
      -e "s#^User=.*#User=$RUN_USER#" \
      -e "s#^Environment=MQTT_HOST=.*#Environment=MQTT_HOST=$PLATFORM_HOST#" \
      "$SRC/$unit.service" > "/etc/systemd/system/$unit.service"
  echo "installed /etc/systemd/system/$unit.service"
done

systemctl daemon-reload
systemctl enable --now cems-device cems-gateway-plant1 cems-gateway-plant2
systemctl restart cems-device
sleep 2
systemctl restart cems-gateway-plant1 cems-gateway-plant2

echo
systemctl --no-pager --lines=0 status cems-device cems-gateway-plant1 cems-gateway-plant2 || true
