#!/bin/bash

cd /app

INTERVAL_SECONDS=$((15 * 60))
LOG_DIR="/home/grupo_fctufg/logs"

trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] monitoramento_regra_os scheduler started"
echo "[start] interval = ${INTERVAL_SECONDS}s"
echo "[start] time = $(date)"

while true; do
  (
    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') starting job"

    DATE=$(date +%Y-%m-%d)

    python -u /app/monitoramento_regra_os.py >> "${LOG_DIR}/${DATE}-monitoramento-regra-os.txt" 2>&1 || echo "[error] monitoramento_regra_os failed"

    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"

  ) || echo "[critical] unexpected failure in cycle"

  sleep "${INTERVAL_SECONDS}"
done
