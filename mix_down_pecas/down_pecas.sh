#!/bin/bash

cd /app

INTERVAL_SECONDS=$((3 * 60 * 60))   # 3 hours
LOG_DIR="/home/grupo_fctufg/logs"

trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] down_pecas scheduler started"
echo "[start] interval = ${INTERVAL_SECONDS}s"
echo "[start] time = $(date)"

while true; do
  (
    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') starting job"

    DATE=$(date +%Y-%m-%d)

    python -u /app/down_pecas.py >> "${LOG_DIR}/${DATE}-pecas.txt" 2>&1 || echo "[error] down_pecas failed"

    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"

  ) || echo "[critical] unexpected failure in cycle"

  sleep "${INTERVAL_SECONDS}"
done
