#!/bin/bash

# Faz o loop para baixar o dado de hoje e os dos 4 dias anteriores
for i in {0..210}
do
  DATE=$(date -d "-$i day" +%Y-%m-%d)
  python -u down_pos.py --data_baixar=$DATE
done
