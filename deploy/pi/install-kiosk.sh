#!/usr/bin/env bash
set -euo pipefail

MIRROR_URL="${JARVIS_MIRROR_URL:-http://127.0.0.1:8000/mirror}"
SERVICE_DIR="${HOME}/.config/systemd/user"
SERVICE_FILE="${SERVICE_DIR}/jarvis-mirror-kiosk.service"

if ! command -v chromium >/dev/null 2>&1 && ! command -v chromium-browser >/dev/null 2>&1; then
  echo "Chromium is required. Install it with: sudo apt install chromium"
  exit 1
fi

CHROMIUM="$(command -v chromium || command -v chromium-browser)"
mkdir -p "${SERVICE_DIR}"

cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Jarvis Smart Mirror kiosk
After=graphical-session.target network-online.target
Wants=network-online.target

[Service]
ExecStart=${CHROMIUM} --kiosk --noerrdialogs --disable-infobars --disable-session-crashed-bubble --autoplay-policy=no-user-gesture-required ${MIRROR_URL}
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now jarvis-mirror-kiosk.service
echo "Jarvis mirror kiosk installed: ${MIRROR_URL}"
