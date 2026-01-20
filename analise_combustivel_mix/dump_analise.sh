#!/bin/bash

# Lista de datas (ordenadas e sem duplicadas)
# Gera a lista de datas de 2025-01-01 até hoje
start_date="2026-01-01"
end_date=$(date +"%Y-%m-%d")

datas=()
current_date="$start_date"
while [[ "$current_date" < "$end_date" || "$current_date" == "$end_date" ]]; do
  datas+=("$current_date")
  current_date=$(date -I -d "$current_date + 1 day")
done

# Lista de scripts a executar
scripts=(
"analise_combustivel_mix_main.py"
)

# Arquivo de log
log_file="debug_script_single.txt"

# Loop principal
for data in "${datas[@]}"; do
  echo "📅 Data: $data"
  for script in "${scripts[@]}"; do
    echo "🔄 Iniciando: $script --data_baixar=$data"
    start_time=$(date +%s)
    python "$script" --data_baixar="$data"
    end_time=$(date +%s)
    duration=$((end_time - start_time))

    echo "🕒 Tempo: $duration segundos" | tee -a "$log_file"
    echo "$data,$script,$duration" >> "$log_file"
    echo "✅ Finalizado: $script --data_baixar=$data"
    echo " Vamos descansar 10 segundos"
    sleep 10
  done
  echo "-----------------------------------------"
  echo " Vamos para a próxima data"
  echo " Vamos descansar 10 segundos"
  sleep 10
done