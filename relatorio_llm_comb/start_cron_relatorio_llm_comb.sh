#!/bin/bash
set -e

echo "=Iniciando container RELATORIO_LLM_COMB..."
echo "=Iniciando container na data $(date '+%Y-%m-%d %H:%M:%S') ===="
echo "=Horário do cron: ${CONTAINER_CRON_RELATORIO_LLM_COMB}"

# 1️ Executa o script no início se CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_COMB for True
if [ "${CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_COMB}" = "True" ] || [ "${CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_COMB}" = "true" ]; then
    echo "CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_COMB está ativo. Executando script imediatamente..."
    bash /app/relatorio_llm_comb.sh
else
    echo "CONTAINER_EXEC_NO_INICIO_RELATORIO_LLM_COMB não está ativo. O script será executado apenas pelo cron."
fi

# 2️ Adiciona a linha do cron dinamicamente
echo "${CONTAINER_CRON_RELATORIO_LLM_COMB} /app/relatorio_llm_comb.sh" | crontab -

# 3️ Mostra o crontab atual (útil para debug)
echo "Crontab atual:"
crontab -l
echo 

# 4️ Inicia o cron em foreground (necessário para manter o container vivo)
exec cron -f
