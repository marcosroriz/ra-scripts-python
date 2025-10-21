FROM node:20-slim

WORKDIR /app

# Instala cron, tzdata e dependências de compilação
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libxrender1 \
    cron \
    gcc \
    libpq-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Define timezone do container
ENV TZ=America/Sao_Paulo

# Copia package.json e instala libs
COPY package*.json ./

RUN npm install

# Copia todo o código para /app
COPY . .

# Dá permissão de execução para o script
RUN chmod +x /app/start_cron_down_rmtc.sh

RUN chmod +x /app/down_rmtc.sh

# Cria diretórios de logs
RUN mkdir -p /home/grupo_fctufg/logs/

# Comando final
CMD ["/app/start_cron_down_rmtc.sh"]
