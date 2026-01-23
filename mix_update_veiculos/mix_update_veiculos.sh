#!/bin/bash
set -e
cd /app

python -u mix_update_veiculos.py >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-veiculos.txt
