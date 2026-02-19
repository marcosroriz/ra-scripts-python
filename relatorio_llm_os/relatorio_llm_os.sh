#!/bin/bash

cd /app

INTERVAL_SECONDS=$((15 * 60))
LOG_DIR="/home/grupo_fctufg/logs"

trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] relatorio_llm_os scheduler started"
echo "[start] interval = ${INTERVAL_SECONDS}s"
echo "[start] time = $(date)"

while true; do
  (
    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') starting job"

    DATE=$(date +%Y-%m-%d)

    python -u /app/relatorio_llm_os.py >> "${LOG_DIR}/${DATE}-relatorio-os.txt" 2>&1 || echo "[error] relatorio_llm_os failed"

    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"

  ) || echo "[critical] unexpected failure in cycle"

  sleep "${INTERVAL_SECONDS}"
done
