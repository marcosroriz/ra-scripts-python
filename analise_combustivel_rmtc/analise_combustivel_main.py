#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

# Import de sistema
import os
import subprocess

# CLI
import click

# Imports básicos
import pandas as pd

# Import de datas
import datetime as dt
from datetime import datetime

# Banco de Dados
from sqlalchemy import create_engine

# Thread Pool
from concurrent.futures import ThreadPoolExecutor, as_completed

# DotEnv
from dotenv import load_dotenv

# Carrega variáveis de ambiente
CURRENT_WORKINGD_DIR = os.getcwd()
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))

load_dotenv()
load_dotenv("../.env")
load_dotenv(os.path.join(CURRENT_WORKINGD_DIR, ".env"))
load_dotenv(os.path.join(CURRENT_PATH, "..", ".env"))


###################################################################################
# Constantes e variávesis globais
###################################################################################
# Dados da conexão
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")


# Engine para pandas (SQLAlchemy)
def get_pg_engine():
    pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    return pg_engine


###################################################################################
# Execução do subprocesso
###################################################################################


def run_script(data_str, asset_id, timeout=900):
    cmd = [
        "/home/grupo_fctufg/anaconda3/envs/ra/bin/python",
        os.path.join(CURRENT_PATH, "analise_combustivel_subprocess.py"),
        f"--data_baixar={data_str}",
        f"--vec_asset_id={asset_id}",
    ]

    try:
        result = subprocess.run(cmd, timeout=timeout)
        return (asset_id, result.returncode)
    except subprocess.TimeoutExpired:
        return (asset_id, -999)


###################################################################################
# Função principal
###################################################################################


@click.command()
@click.option("--data_baixar", type=str, help="Data que irei baixar")
def main(data_baixar):
    # Conecta ao banco
    pg_engine = get_pg_engine()
    df_veiculos = pd.read_sql("SELECT * FROM veiculos_api", pg_engine)
    pg_engine.dispose()

    # Total de veículos
    total_veiculos = len(df_veiculos)

    # Data a processar
    dia_dt = datetime.now()
    if data_baixar:
        dia_dt = pd.to_datetime(data_baixar)

    dia_str = dia_dt.strftime("%Y-%m-%d")

    print("-------------------------------------------------------------------------------------")
    print(f"🚍 Processando {total_veiculos} veículos para o dia: {dia_str}")

    # Limite de subprocessos paralelos
    max_workers = int(os.cpu_count() * 0.8)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_script, dia_str, row["AssetId"]) for _, row in df_veiculos.iterrows()]

        for future in as_completed(futures):
            asset_id, code = future.result()
            if code == 0:
                status = "✅ ok"
            else:
                status = f"❌ erro (code {code})"
            print(f"Veículo {asset_id} -> {status}")


if __name__ == "__main__":
    # Salva tempo de inicio
    start = dt.datetime.now()

    # Executa
    main()

    # Salva tempo de fim
    end = dt.datetime.now()

    # Obtem o tempo total em minutos
    tempo_minutos = (end - start).seconds // 60

    print("Tempo para executar o script (em minutos)", tempo_minutos)
