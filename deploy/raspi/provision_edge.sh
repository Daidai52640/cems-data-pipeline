#!/usr/bin/env bash
# Edge-node provisioning for the Raspberry Pi (run ON the Pi, as the login user).
# Pure ASCII on purpose. Idempotent: safe to run twice.
#
# What it does:
#   1. prints host facts (arch / os / memory / disk)
#   2. sets timezone to Asia/Shanghai
#   3. installs python3-venv + python3-pip (no full upgrade)
#   4. creates ~/cems/.venv and installs requirements-edge.txt (if the repo was synced)
#   5. checks the RS485 USB adapter (/dev/ttyUSB0) and group membership
#   6. runs a read-only RTU smoke test when the adapter is present
#
# Usage:
#   bash _pi_provision.sh

set -euo pipefail

say() { printf '\n== %s ==\n' "$1"; }

say "host"
uname -a
grep -E '^(PRETTY_NAME|VERSION_ID)=' /etc/os-release || true
printf 'python3: '; python3 -V
printf 'memory: '; free -m | awk '/MemTotal/ {print $2 " MB"}'
df -h / | tail -1
printf 'user: '; whoami
printf 'groups: '; id -nG

say "timezone"
sudo timedatectl set-timezone Asia/Shanghai
timedatectl | sed -n '1,6p'

say "packages"
sudo apt-get update -qq
sudo apt-get install -y -qq python3-venv python3-pip
printf 'apt ok\n'

say "venv + edge requirements"
mkdir -p "$HOME/cems"
if [ ! -x "$HOME/cems/.venv/bin/python" ]; then
  python3 -m venv "$HOME/cems/.venv"
fi
"$HOME/cems/.venv/bin/pip" install -q --upgrade pip
if [ -f "$HOME/cems/requirements-edge.txt" ]; then
  "$HOME/cems/.venv/bin/pip" install -q -r "$HOME/cems/requirements-edge.txt"
  printf 'requirements-edge.txt installed\n'
else
  printf 'WARN: %s not found - sync the repo first\n' "$HOME/cems/requirements-edge.txt"
fi
"$HOME/cems/.venv/bin/python" - <<'PY'
import importlib
for name in ("pymodbus", "paho.mqtt.client", "serial"):
    try:
        mod = importlib.import_module(name)
        print("import %-18s ok %s" % (name, getattr(mod, "__version__", "")))
    except Exception as exc:
        print("import %-18s FAILED %s" % (name, exc))
PY

say "RS485 adapter"
if ls /dev/ttyUSB* >/dev/null 2>&1; then
  ls -l /dev/ttyUSB*
  if id -nG | tr ' ' '\n' | grep -qx dialout; then
    printf 'dialout: ok\n'
  else
    printf 'WARN: user is not in dialout; run: sudo usermod -aG dialout %s (then re-login)\n' "$(whoami)"
  fi
else
  printf 'no /dev/ttyUSB* - the USB-RS485 adapter is not plugged in (or not detected)\n'
fi

say "RTU smoke test (read-only)"
if [ -x "$HOME/cems/.venv/bin/python" ] && [ -f "$HOME/cems/src/device/rtu_master.py" ] && ls /dev/ttyUSB* >/dev/null 2>&1; then
  cd "$HOME/cems"
  "$HOME/cems/.venv/bin/python" -m src.device.rtu_master --port /dev/ttyUSB0 --count 5 --interval 1 || true
else
  printf 'skipped (adapter or repo not ready)\n'
fi

say "summary"
printf 'edge node base setup finished\n'
