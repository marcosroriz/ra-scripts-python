#!/bin/bash
set -e

echo "=Iniciando container MIX MONITORAMENTO OS..."
echo "=Iniciando container na data $(date '+%Y-%m-%d %H:%M:%S') ===="
echo "=Horário do cron: ${CONTAINER_CRON_MONITORAMENTO_REGRA_OS}"

# 1️ Executa o script no início se CONTAINER_EXEC_NO_INICIO_MONITORAMENTO_REGRA_OS for True
if [ "${CONTAINER_EXEC_NO_INICIO_MONITORAMENTO_REGRA_OS}" = "True" ] || [ "${CONTAINER_EXEC_NO_INICIO_MONITORAMENTO_REGRA_OS}" = "true" ]; then
    echo "CONTAINER_EXEC_NO_INICIO_MONITORAMENTO_REGRA_OS está ativo. Executando script imediatamente..."
    bash /app/monitoramento_job.sh
else
    echo "CONTAINER_EXEC_NO_INICIO_MONITORAMENTO_REGRA_OS não está ativo. O script será executado apenas pelo cron."
fi

# 2️ Adiciona a linha do cron dinamicamente
{
  echo "SHELL=/bin/bash"
  echo "PATH=/usr/local/bin:/usr/bin:/bin"
  echo "${CONTAINER_CRON_MONITORAMENTO_REGRA_OS} /bin/bash /app/monitoramento_job.sh >> /home/grupo_fctufg/logs/cron.log 2>&1"
} | crontab -

# 3️ Mostra o crontab atual (útil para debug)
echo "Crontab atual:"
crontab -l
echo 

# 4️ Inicia o cron em foreground (necessário para manter o container vivo)
exec cron -f
