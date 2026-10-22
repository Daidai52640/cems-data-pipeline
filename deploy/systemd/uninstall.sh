#!/usr/bin/env bash
# Stop and remove the edge-node units (run with sudo on the Raspberry Pi).
# Pure ASCII on purpose.
set -euo pipefail

systemctl disable --now cems-gateway-plant2 cems-gateway-plant1 cems-device || true
mkdir -p /var/backups/cems-systemd
for unit in cems-device cems-gateway-plant1 cems-gateway-plant2; do
  if [ -f "/etc/systemd/system/$unit.service" ]; then
    mv "/etc/systemd/system/$unit.service" "/var/backups/cems-systemd/"
    echo "removed $unit.service (backup in /var/backups/cems-systemd)"
  fi
done
systemctl daemon-reload
