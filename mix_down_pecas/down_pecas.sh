#!/bin/bash
set -e

cd /app

# ===== CONFIG =====
INTERVAL_SECONDS=$((3 * 60 * 60))   # 3 hours
# ==================

trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] down_pecas scheduler started"
echo "[start] interval = ${INTERVAL_SECONDS}s"
echo "[start] time = $(date)"

while true; do
  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') starting job"

  python -u /app/down_pecas.py >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-pecas.txt

  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"

  sleep "${INTERVAL_SECONDS}"
done
