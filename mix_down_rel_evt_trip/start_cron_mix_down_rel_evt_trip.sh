#!/bin/bash
set -e

echo "=Iniciando container MIX DOWN REL EVT TRIP..."
echo "=Iniciando container na data $(date '+%Y-%m-%d %H:%M:%S') ===="
echo "=Horário do cron: ${CONTAINER_CRON_MIX_DOWN_REL_EVT_TRIP}"

# 1️ Executa o script no início se CONTAINER_EXEC_NO_INICIO_MIX_DOWN_PECAS for True
if [ "${CONTAINER_EXEC_NO_INICIO_MIX_DOWN_REL_EVT_TRIP}" = "True" ] || [ "${CONTAINER_EXEC_NO_INICIO_MIX_DOWN_REL_EVT_TRIP}" = "true" ]; then
    echo "CONTAINER_EXEC_NO_INICIO_MIX_DOWN_REL_EVT_TRIP está ativo. Executando script imediatamente..."
    bash /app/down_rel_evt_trip.sh
else
    echo "CONTAINER_EXEC_NO_INICIO_MIX_DOWN_REL_EVT_TRIP não está ativo. O script será executado apenas pelo cron."
fi

# 2️ Adiciona a linha do cron dinamicamente
echo "${CONTAINER_CRON_MIX_DOWN_REL_EVT_TRIP} /app/down_rel_evt_trip.sh" | crontab -

# 3️ Mostra o crontab atual (útil para debug)
echo "Crontab atual:"
crontab -l
echo 

# 4️ Inicia o cron em foreground (necessário para manter o container vivo)
exec cron -f
