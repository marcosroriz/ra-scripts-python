#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Arquivo principal (main) que vai disparar subprocessos para o cáclulo do combustível
###################################################################################
# Cada subprocesso calcula os dados das trips de um dia de um determinado veículo
#
# Requer como parâmetro de linha de comando a data que será utilizada:
# --data_baixar="YYYY-MM-DD" (Ex: --data_baixar="2025-02-27")
# Para cada data, o sistema dispara um subprocesso que analisa as trips e posições GPS do veículo naquele dia
# e calcula o combustível utilizado, bem como a relação dele com os demais veículos na mesma linha
#
# Além deste parâmetro, o sistema requer um arquivo .env com as seguintes variáveis de ambiente:
# - DB_HOST: Endereço do banco
# - DB_PORT: Porta do banco
# - DB_USER: Usuário do banco
# - DB_PASS: Senha do usuário
# - DB_NAME: Nome do banco de dados
# - DEBUG: Se deve ou não imprimir na tela informações de DEBUG
###################################################################################

###################################################################################
# Imports
###################################################################################

# Import de sistema
import gc
import os
import subprocess

# CLI
import click

# Imports básicos
import pandas as pd

# Import de datas
import datetime as dt
from datetime import datetime

# DotEnv
from dotenv import load_dotenv

# Carrega variáveis de ambiente
CURRENT_WORKINGD_DIR = os.getcwd()
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))

load_dotenv()
load_dotenv("../.env")
load_dotenv(os.path.join(CURRENT_WORKINGD_DIR, ".env"))
load_dotenv(os.path.join(CURRENT_PATH, "..", ".env"))

# Banco de Dados
from db import PostgresSingleton

# Thread Pool
from concurrent.futures import ThreadPoolExecutor, as_completed


###################################################################################
# Execução do subprocesso
###################################################################################


def run_script(data_str, asset_id, timeout=900):
    cmd = [
        "/home/grupo_fctufg/anaconda3/envs/ra/bin/python",
        os.path.join(CURRENT_PATH, "analise_combustivel_mix_subprocess.py"),
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
    pgDB = PostgresSingleton.get_instance()
    pg_engine = pgDB.get_engine()
    df_veiculos = pgDB.read_sql_safe("SELECT * FROM veiculos_api")
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
            elif code == -999:
                status = "🕒 timeout"
            else:
                status = f"❌ erro (code {code})"
            print(f"Veículo {asset_id} -> {status}")
            gc.collect()


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
