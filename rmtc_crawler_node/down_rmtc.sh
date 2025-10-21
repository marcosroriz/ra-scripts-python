#!/bin/bash
echo "$(date '+%Y-%m-%d %H:%M:%S') =INICIO do script..." 
echo "$(date '+%Y-%m-%d %H:%M:%S') =INICIO do script..." >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-crawler.txt

node /app/index.js >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-crawler.txt

echo "$(date '+%Y-%m-%d %H:%M:%S') =FIM do script..." 
echo "$(date '+%Y-%m-%d %H:%M:%S') =FIM do script..." >> /home/grupo_fctufg/logs/$(date +\%Y-\%m-\%d)-crawler.txt