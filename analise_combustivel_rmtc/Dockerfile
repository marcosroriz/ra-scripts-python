# Imagem leve com micromamba (conda mais rápido)
FROM mambaorg/micromamba:1.5.8

# Timezone e locale (útil para logs e datas)
ENV TZ=America/Sao_Paulo
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Diretório do app
WORKDIR /app

# Copia apenas o environment primeiro para cache eficiente
COPY environment.yml /app/environment.yml

# Cria o ambiente conda com tudo que precisamos (geopandas stack inclusa)
RUN micromamba create -y -n analise-combustivel -f /app/environment.yml && \
    micromamba clean --all --yes

# Garante que os comandos usem o env
SHELL ["bash", "-lc"]
ENV MAMBA_DOCKERFILE_ACTIVATE=1
ENV CONDA_DEFAULT_ENV=analise-combustivel
ENV PATH=/opt/conda/envs/analise-combustivel/bin:$PATH

# Copia o restante do projeto
COPY . /app

# (opcional) dá permissão de execução ao seu bash job
RUN chmod +x /app/scripts_python/analise_combustivel_rmtc/analise_job.sh

# Padrão: mostra ajuda ao subir sem args
CMD ["bash", "-lc", "echo 'Use: docker compose run analise-combustivel <comando>' && ls -la /app"]
