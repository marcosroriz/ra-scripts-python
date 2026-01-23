#!/bin/bash
set -e

cd /app

# ===== CONFIG =====
INTERVAL_SECONDS=$((15 * 60))   # 15 minutes
# ==================

trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] monitoramento_regra_os scheduler started"
echo "[start] interval = ${INTERVAL_SECONDS}s"
echo "[start] time = $(date)"

while true; do
  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') starting job"

  python -u /app/monitoramento_regra_os.py >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-monitoramento-regra-os.txt

  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"

  sleep "${INTERVAL_SECONDS}"
done
