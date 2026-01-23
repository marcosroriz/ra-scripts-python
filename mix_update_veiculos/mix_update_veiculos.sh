#!/bin/bash
set -e
cd /app
set -o allexport
source /app/.env
set +o allexport

python -u mix_update_veiculos.py >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-veiculos.txt
