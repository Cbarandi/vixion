#!/bin/bash
# Secretos: ~/.vixion_pipeline.env (chmod 600). No guardar tokens en el repo.

set -euo pipefail

cd /Users/cristianbarandiaran/VIXION

ENV_FILE="${HOME}/.vixion_pipeline.env"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[vixion] Falta $ENV_FILE — créalo con los export TELEGRAM_*, RESEND_*, ALERTS_* y VIXION_DASHBOARD_URL." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

exec /Users/cristianbarandiaran/VIXION/.venv/bin/python /Users/cristianbarandiaran/VIXION/scripts/run_pipeline.py
