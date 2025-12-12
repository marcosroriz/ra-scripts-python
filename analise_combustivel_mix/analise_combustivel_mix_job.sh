#!/bin/bash

# Cron para rodar analise_combustivel_rmtc_mix

# Faz o loop para analisar o dado de hoje e os dos 7 dias anteriores
for i in {0..7}
do
  DATE=$(date -d "-$i day" +%Y-%m-%d)
  python -u analise_combustivel_mix_main.py --data_baixar=$DATE >> /home/grupo_fctufg/logs/${DATE}-analise-combustivel-mix.txt
done
