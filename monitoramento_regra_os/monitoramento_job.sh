#!/bin/bash

# Cron para rodar monitoramento_regra
echo "INICIO DO SCRIPT"
echo $(date)

# Executa o script
python -u monitoramento_regra_os.py >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-monitoramento-regra-os.txt

echo "TERMINO DO SCRIPT"
echo $(date)
