FROM python:3.12.8-slim

WORKDIR /app

# Instala cron, tzdata e dependências de compilação
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    gcc \
    libpq-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Define timezone do container
ENV TZ=America/Sao_Paulo

# Copia requirements.txt e instala libs
COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código para /app
COPY . .

# Dá permissão de execução para o script
RUN chmod +x /app/analise_combustivel_mix_job.sh

# Cria diretórios de logs
RUN mkdir -p /home/grupo_fctufg/logs/

# Comando final
CMD ["/app/analise_combustivel_mix_job.sh"]
