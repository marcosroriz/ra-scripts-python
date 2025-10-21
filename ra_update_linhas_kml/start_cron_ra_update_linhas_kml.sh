#!/bin/bash
set -e

echo "=Iniciando container..."
echo "=Iniciando container na data $(date '+%Y-%m-%d %H:%M:%S') ===="
echo "=Horário do cron: ${CONTAINER_CRON_RA_UPDATE_LINHAS_KML}"

# 1️⃣ Executa o script no início se CONTAINER_EXEC_NO_INICIO_RA_UPDATE_LINHAS_KML for True
if [ "${CONTAINER_EXEC_NO_INICIO_RA_UPDATE_LINHAS_KML}" = "True" ] || [ "${CONTAINER_EXEC_NO_INICIO_RA_UPDATE_LINHAS_KML}" = "true" ]; then
    echo "CONTAINER_EXEC_NO_INICIO_RA_UPDATE_LINHAS_KML está ativo. Executando script imediatamente..."
    bash /app/ra_update_linhas_kml.sh
else
    echo "CONTAINER_EXEC_NO_INICIO_RA_UPDATE_LINHAS_KML não está ativo. O script será executado apenas pelo cron."
fi

# 2️⃣ Adiciona a linha do cron dinamicamente
echo "${CONTAINER_CRON_RA_UPDATE_LINHAS_KML} /app/ra_update_linhas_kml.sh" | crontab -

# 3️⃣ Mostra o crontab atual (útil para debug)
echo "Crontab atual:"
crontab -l
echo 

# 4️⃣ Inicia o cron em foreground (necessário para manter o container vivo)
exec cron -f
