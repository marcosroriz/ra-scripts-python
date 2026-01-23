#!/bin/bash
set -e

cd /app

# ===== CONFIG =====
TARGET_TIME="18:00"
TZ="America/Sao_Paulo"
LOG_DIR="/home/grupo_fctufg/logs"
# ==================

export TZ
trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] down_rel_pos_trip scheduler started"
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

  python -u /app/down_rel_pos_trip.py >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-rel-trip-pos.txt

  echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"
done
