#!/bin/bash

# Faz o loop para baixar o dado de hoje e os dos 4 dias anteriores
for i in {0..4}
do
  DATE=$(date -d "-$i day" +%Y-%m-%d)
  /home/grupo_fctufg/anaconda3/envs/ra/bin/python -u /home/grupo_fctufg/down_trips.py --data_baixar=$DATE >> /home/grupo_fctufg/logs/${DATE}-trips.txt
done
