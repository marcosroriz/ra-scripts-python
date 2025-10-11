#!/bin/bash

# Cron para rodar monitoramento_regra
echo "INICIO DO SCRIPT"
echo $(date)

# CD na pasta do script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Diretório atual"
echo $SCRIPT_DIR

# Sourcing do conda.sh para permitir 'conda activate'
source ~/anaconda3/etc/profile.d/conda.sh

# Ativa o ambiente 'ra'
conda activate ra

# Executa o script
python monitoramento_regra_os.py >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-monitoramento-regra-os.txt

echo "TERMINO DO SCRIPT"
echo $(date)
