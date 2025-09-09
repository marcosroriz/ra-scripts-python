#!/bin/bash

# Cron para rodar analise_combustivel_rmtc

# CD na pasta do script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "DIR"
echo $SCRIPT_DIR

# Sourcing do conda.sh para permitir 'conda activate'
source ~/anaconda3/etc/profile.d/conda.sh

# Ativa o ambiente 'ra'
conda activate ra

# Lista de datas (ordenadas e sem duplicadas)
# Gera a lista de datas de 30 dias até hoje
start_date=$(date -I -d "$end_date - 30 days")
end_date=$(date +"%Y-%m-%d")

datas=()
current_date="$start_date"
while [[ "$current_date" < "$end_date" || "$current_date" == "$end_date" ]]; do
  datas+=("$current_date")
  current_date=$(date -I -d "$current_date + 1 day")
done

# Lista de scripts a executar
scripts=(
"analise_combustivel_main.py"
)

# Arquivo de log
log_file="/home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-analise-combustivel.txt"

# Loop principal
for data in "${datas[@]}"; do
  echo "📅 Data: $data"
  for script in "${scripts[@]}"; do
    echo "🔄 Iniciando: $script --data_baixar=$data" | tee -a "$log_file"


    start_time=$(date +%s)
    python "$script" --data_baixar="$data"
    end_time=$(date +%s)
    duration=$((end_time - start_time))

    echo "✅ Finalizado: $script --data_baixar=$data"
    echo "🕒 Tempo: $duration segundos" | tee -a "$log_file"
    echo "$data,$script,$duration" >> "$log_file"
    echo " Vamos descansar 10 segundos"
    sleep 10
  done
  echo "-----------------------------------------"
  echo " Vamos para a próxima data"
  echo " Vamos descansar 10 segundos"
  sleep 10
done