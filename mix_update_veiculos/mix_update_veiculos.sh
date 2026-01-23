#!/bin/bash
set -e

cd /app

INTERVAL_SECONDS=120

echo "[start] mix_update_veiculos started"
echo "[start] interval = ${INTERVAL_SECONDS}s"
echo "[start] time = $(date)"

while true; do
  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') starting job"

  python -u /app/mix_update_veiculos.py >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-veiculos.txt

  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"

  sleep "${INTERVAL_SECONDS}"
done
