#!/bin/bash

# Baixa o dado de hoje
dia_hoje=$(date +%Y-%m-%d)
python -u down_evt.py --data_baixar=$dia_hoje >> /home/grupo_fctufg/logs/${dia_hoje}-evt.txt

# Baixa o dado do dia anterior (-1)
dia_anterior=$(date -d "-1 day" +%Y-%m-%d)
python -u down_evt.py --data_baixar=$dia_anterior >> /home/grupo_fctufg/logs/${dia_anterior}-evt-dia-anterior.txt

# Baixa o dado do dia anterior (-2)
dia_anterior=$(date -d "-2 day" +%Y-%m-%d)
python -u down_evt.py --data_baixar=$dia_anterior >> /home/grupo_fctufg/logs/${dia_anterior}-evt-dia-anterior.txt
