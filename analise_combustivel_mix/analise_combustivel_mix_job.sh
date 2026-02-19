#!/bin/bash

cd /app

# ===== CONFIG =====
TARGET_TIME="05:00"
TZ="America/Sao_Paulo"
LOG_DIR="/home/grupo_fctufg/logs"
# ==================

export TZ
trap "echo '[stop] SIGTERM received'; exit 0" SIGTERM SIGINT

echo "[start] analise_combustivel_mix_job scheduler started"
echo "[start] target time = ${TARGET_TIME}"
echo "[start] time now = $(date)"

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
    echo "[wait] sleeping ${sleep_seconds}s"

    sleep "$sleep_seconds"

    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') starting job"

    for i in 0 1 2
    do
      DATE=$(date -d "-$i day" +%Y-%m-%d)

      python -u /app/analise_combustivel_mix_main.py --data_baixar="$DATE" >> "${LOG_DIR}/${DATE}-analise-combustivel-mix.txt" 2>&1 || echo "[error] failed for $DATE"
    done

    echo "[run] $(date '+%Y-%m-%d %H:%M:%S') finished job"

  ) || echo "[critical] unexpected failure in cycle"

  echo "[loop] cycle ended, restarting..."
  sleep 5
done
