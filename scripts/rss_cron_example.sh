#!/usr/bin/env bash
# Ejemplo operativo: alinear el cron con --slot-minutes (aquí 15).
# Cada ventana UTC encola como máximo un INGEST_SOURCE_TICK (idempotencia por hash).
#
# Uso:
#   export DATABASE_URL='postgresql://...'
#   export VIXION_FEEDS_CONFIG=/ruta/a/feeds.yaml   # opcional
#   */15 * * * * /ruta/a/VIXION/scripts/rss_cron_example.sh
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec .venv/bin/vixion-enqueue-rss-tick --sync-config --slot-minutes 15
