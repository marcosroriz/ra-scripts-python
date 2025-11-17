#!/bin/bash
set -e

echo "=Iniciando container RELATORIO_LLM_OS..."
echo "=Iniciando container na data $(date '+%Y-%m-%d %H:%M:%S') ===="
echo "=Horário do cron: ${CONTAINER_CRON_RELATORIO_LLM_OS}"

# 1️ Executa o script no início se CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_OS for True
if [ "${CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_OS}" = "True" ] || [ "${CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_OS}" = "true" ]; then
    echo "CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_OS está ativo. Executando script imediatamente..."
    bash /app/relatorio_llm_os.sh
else
    echo "CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_OS não está ativo. O script será executado apenas pelo cron."
fi

# 2️ Adiciona a linha do cron dinamicamente
echo "${CONTAINER_CRON_RELATORIO_LLM_OS} /app/relatorio_llm_os.sh" | crontab -

# 3️ Mostra o crontab atual (útil para debug)
echo "Crontab atual:"
crontab -l
echo 

# 4️ Inicia o cron em foreground (necessário para manter o container vivo)
exec cron -f
