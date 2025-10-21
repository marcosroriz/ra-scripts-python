#!/bin/bash
set -e

echo "=Iniciando container..."
echo "=Iniciando container na data $(date '+%Y-%m-%d %H:%M:%S') ===="
echo "=Horário do cron: ${CONTAINER_CRON_RMTC_CRAWLER_NODE}"

# 1️⃣ Executa o script no início se CONTAINER_EXEC_NO_INICIO_RMTC_CRAWLER_NODE for True
if [ "${CONTAINER_EXEC_NO_INICIO_RMTC_CRAWLER_NODE}" = "True" ] || [ "${CONTAINER_EXEC_NO_INICIO_RMTC_CRAWLER_NODE}" = "true" ]; then
    echo "CONTAINER_EXEC_NO_INICIO_RMTC_CRAWLER_NODE está ativo. Executando script imediatamente..."
    bash /app/down_rmtc.sh
else
    echo "CONTAINER_EXEC_NO_INICIO_RMTC_CRAWLER_NODE não está ativo. O script será executado apenas pelo cron."
fi

# 2️⃣ Adiciona a linha do cron dinamicamente
echo "${CONTAINER_CRON_RMTC_CRAWLER_NODE} /app/down_rmtc.sh" | crontab -

# 3️⃣ Mostra o crontab atual (útil para debug)
echo "Crontab atual:"
crontab -l
echo 

# 4️⃣ Inicia o cron em foreground (necessário para manter o container vivo)
exec cron -f
