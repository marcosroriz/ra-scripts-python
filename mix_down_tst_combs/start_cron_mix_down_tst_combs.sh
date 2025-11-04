#!/bin/bash
set -e

echo "=Iniciando container MIX DOWN TST COMBS..."
echo "=Iniciando container na data $(date '+%Y-%m-%d %H:%M:%S') ===="
echo "=Horário do cron: ${CONTAINER_CRON_MIX_DOWN_TST_COMBS}"

# 1️ Executa o script no início se CONTAINER_EXEC_NO_INICIO_MIX_DOWN_TST_COMBS for True
if [ "${CONTAINER_EXEC_NO_INICIO_MIX_DOWN_TST_COMBS}" = "True" ] || [ "${CONTAINER_EXEC_NO_INICIO_MIX_DOWN_TST_COMBS}" = "true" ]; then
    echo "CONTAINER_EXEC_NO_INICIO_MIX_DOWN_TST_COMBS está ativo. Executando script imediatamente..."
    bash /app/down_tst_combs.sh
else
    echo "CONTAINER_EXEC_NO_INICIO_MIX_DOWN_TST_COMBS não está ativo. O script será executado apenas pelo cron."
fi

# 2️ Adiciona a linha do cron dinamicamente
echo "${CONTAINER_CRON_MIX_DOWN_TST_COMBS} /app/down_tst_combs.sh" | crontab -

# 3️ Mostra o crontab atual (útil para debug)
echo "Crontab atual:"
crontab -l
echo 

# 4️ Inicia o cron em foreground (necessário para manter o container vivo)
exec cron -f
