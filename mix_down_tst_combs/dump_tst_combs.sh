#!/bin/bash

# Faz o loop para baixar o dado de hoje e os dos 4 dias anteriores
for i in {0..30}
do
  DATE=$(date -d "-$i day" +%Y-%m-%d)
  python -u down_tst_combs.py --data_baixar=$DATE
done
