#!/bin/bash
set -e

cd /app

# ===== CONFIG =====
TARGET_TIME="14:00"
TZ="America/Sao_Paulo"
LOG_DIR="/home/grupo_fctufg/logs"
# ==================

export TZ
trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] down_pos scheduler started"
echo "[start] target time = ${TARGET_TIME}"
echo "[start] time now = $(date)"

while true; do
  now_ts=$(date +%s)

  # Today at target time
  today_target_ts=$(date -d "today ${TARGET_TIME}" +%s)

  if [ "$now_ts" -lt "$today_target_ts" ]; then
    next_run_ts="$today_target_ts"
  else
    # Tomorrow at target time
    next_run_ts=$(date -d "tomorrow ${TARGET_TIME}" +%s)
  fi

  sleep_seconds=$((next_run_ts - now_ts))

  echo "[wait] next run at $(date -d "@$next_run_ts")"
  echo "[wait] sleeping ${sleep_seconds}s"

  sleep "$sleep_seconds"

  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') starting job"

  dia_hoje=$(date +%Y-%m-%d)
  python -u /app/down_pos.py --data_baixar=$dia_hoje >> /home/grupo_fctufg/logs/${dia_hoje}-pos.txt

  # Baixa o dado do dia anterior (-1)
  dia_anterior=$(date -d "-1 day" +%Y-%m-%d)
  python -u /app/down_pos.py --data_baixar=$dia_anterior >> /home/grupo_fctufg/logs/${dia_anterior}-pos-dia-anterior.txt

  # Baixa o dado do dia anterior (-2)
  dia_anterior=$(date -d "-2 day" +%Y-%m-%d)
  python -u /app/down_pos.py --data_baixar=$dia_anterior >> /home/grupo_fctufg/logs/${dia_anterior}-pos-dia-anterior.txt

  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"
done

