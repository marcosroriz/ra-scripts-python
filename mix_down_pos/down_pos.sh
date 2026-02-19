#!/bin/bash

cd /app

TARGET_TIME="14:00"
TZ="America/Sao_Paulo"
LOG_DIR="/home/grupo_fctufg/logs"

export TZ
trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] scheduler started"

while true; do
  (
    now_ts=$(date +%s)
    today_target_ts=$(date -d "today ${TARGET_TIME}" +%s)

    if [ "$now_ts" -lt "$today_target_ts" ]; then
      next_run_ts="$today_target_ts"
    else
      next_run_ts=$(date -d "tomorrow ${TARGET_TIME}" +%s)
    fi

    sleep_seconds=$((next_run_ts - now_ts))

    echo "[wait] next run at $(date -d "@$next_run_ts")"
    sleep "$sleep_seconds"

    echo "[run] starting job"

    dia_hoje=$(date +%Y-%m-%d)
    python -u /app/down_pos.py --data_baixar=$dia_hoje >> ${LOG_DIR}/${dia_hoje}-pos.txt 2>&1 || echo "[error] today failed"

    dia_anterior=$(date -d "-1 day" +%Y-%m-%d)
    python -u /app/down_pos.py --data_baixar=$dia_anterior >> ${LOG_DIR}/${dia_anterior}-pos-dia-anterior.txt 2>&1 || echo "[error] -1 failed"

    dia_anterior=$(date -d "-2 day" +%Y-%m-%d)
    python -u /app/down_pos.py --data_baixar=$dia_anterior >> ${LOG_DIR}/${dia_anterior}-pos-dia-anterior.txt 2>&1 || echo "[error] -2 failed"

    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"
  ) || echo "[critical] unexpected failure in cycle"

  echo "[loop] cycle ended, restarting..."
  sleep 5
done
