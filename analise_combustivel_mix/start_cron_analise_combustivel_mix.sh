#!/bin/bash
set -e

echo "=Iniciando container ANALISE_COMBUSTIVEL_MIX..."
echo "=Iniciando container na data $(date '+%Y-%m-%d %H:%M:%S') ===="
echo "=Horário do cron: ${CONTAINER_CRON_ANALISE_COMBUSTIVEL_MIX}"

# 1️ Executa o script no início se CONTAINER_EXEC_NO_INICIO_ANALISE_COMBUSTIVEL_MIX for True
if [ "${CONTAINER_EXEC_NO_INICIO_ANALISE_COMBUSTIVEL_MIX}" = "True" ] || [ "${CONTAINER_EXEC_NO_INICIO_ANALISE_COMBUSTIVEL_MIX}" = "true" ]; then
    echo "CONTAINER_EXEC_NO_INICIO_ANALISE_COMBUSTIVEL_MIX está ativo. Executando script imediatamente..."
    bash /app/analise_combustivel_mix_job.sh
else
    echo "CONTAINER_EXEC_NO_INICIO_ANALISE_COMBUSTIVEL_MIX não está ativo. O script será executado apenas pelo cron."
fi

# 2️ Adiciona a linha do cron dinamicamente
echo "${CONTAINER_CRON_ANALISE_COMBUSTIVEL_MIX} /app/analise_combustivel_mix_job.sh" | crontab -

# 3️ Mostra o crontab atual (útil para debug)
echo "Crontab atual:"
crontab -l
echo 

# 4️ Inicia o cron em foreground (necessário para manter o container vivo)
exec cron -f
