#!/bin/bash

# Lista de datas (ordenadas e sem duplicadas)
# Gera a lista de datas de 2025-01-01 até hoje
start_date="2025-12-30"
end_date=$(date +"%Y-%m-%d")

datas=()
current_date="$start_date"
while [[ "$current_date" < "$end_date" || "$current_date" == "$end_date" ]]; do
  datas+=("$current_date")
  current_date=$(date -I -d "$current_date + 1 day")
done

# Lista de scripts a executar
scripts=(
"down_evt.py"
)


# Loop principal
for data in "${datas[@]}"; do
  echo "📅 Data: $data"
  for script in "${scripts[@]}"; do
    echo "🔄 Iniciando: $script --data_baixar=$data"
    python -u "$script" --data_baixar="$data"
    echo "✅ Finalizado: $script --data_baixar=$data"
    echo " Vamos descansar 10 segundos"
    sleep 10
  done
  echo "-----------------------------------------"
  echo " Vamos para a próxima data"
  echo " Vamos descansar 10 segundos"
  sleep 10
done
