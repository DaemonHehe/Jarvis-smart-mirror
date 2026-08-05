#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
fi
export JARVIS_HOST="${JARVIS_HOST:-0.0.0.0}"
export JARVIS_PORT="${JARVIS_PORT:-8000}"
PYTHON="${ROOT}/.venv/bin/python"
[[ -x "${PYTHON}" ]] || PYTHON="python3"
exec "${PYTHON}" "${ROOT}/backend/bridge_api.py"
