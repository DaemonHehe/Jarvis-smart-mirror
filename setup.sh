#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 -m venv "${ROOT}/.venv"
"${ROOT}/.venv/bin/python" -m pip install --upgrade pip
"${ROOT}/.venv/bin/python" -m pip install -r "${ROOT}/backend/requirements.txt"
[[ -f "${ROOT}/.env" ]] || cp "${ROOT}/.env.sample" "${ROOT}/.env"
echo "Setup complete. Edit .env, then run ./start_jarvis.sh"
