#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

# Bibliotecas padrão
import os
import sys
import gc

# Hash
import hashlib

# HTTP
import requests

# Datas
import datetime as dt
from datetime import datetime, timedelta

# Pandas e Numpy
import pandas as pd
import numpy as np

# BD
import psycopg2 as pg
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text

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
# Configurações
###################################################################################

# Não bufferiza a saída
sys.stdout.flush()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Conexão com o banco de dados
pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# RA
RA_API_URL = os.getenv("RA_API_URL") + "/mapacustofrotaceia"  
RA_API_KEY = os.getenv("RA_API_KEY") 

###################################################################################
# Funções
###################################################################################

def download_pecas(data_inicio_str, data_fim_str):
    url = RA_API_URL
    headers = {"Authorization": RA_API_KEY}
    payload = {"DataInicial": data_inicio_str, "DataFinal": data_fim_str, "CodEmpresa": 2, "NumOS": ""}
    max_retries = 5
    tentativa = 0

    response = None
    while tentativa < max_retries:
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=300)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a requisição: {e}")
            tentativa += 1

    if tentativa == max_retries:
        print("Número máximo de tentativas excedido. Abortando.")
        raise Exception("Número máximo de tentativas excedido.")
    else:
        print("Requisição realizada com sucesso.")
        df = pd.DataFrame(response.json()["data"])
        return df

def preprocessa_os(df):
    # Limpa os dados

    # Replace empty strings with NaN for numeric columns
    numeric_columns = ["COD_EMPRESA", "COD_PRODUTO", "OS", "QUANTIDADE", "VALOR"]
    df[numeric_columns] = df[numeric_columns].replace("", np.nan)

    # Optionally fill NaN with 0.0 or another default value
    df[numeric_columns] = df[numeric_columns].fillna(0.0)

    # Replace NUL characters with an empty string
    df = df.map(lambda x: x.replace("\x00", "") if isinstance(x, str) else x).copy()

    # Replace NaN or None with an empty string
    df.fillna("", inplace=True)

    colunas_unicas = [
        "COD_EMPRESA",
        "EQUIPAMENTO",
        "MODELO",
        "GRUPO",
        "SUB_GRUPO",
        "COD_PRODUTO",
        "PRODUTO",
        "DATA",
        "OS",
        # "QUANTIDADE",
        # "VALOR",
    ]

    df["KEY"] = df[colunas_unicas].astype(str).agg("_".join, axis=1)
    df["KEY_HASH"] = df["KEY"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

    return df

def main(engine_pg):
    today = dt.date.today()
    end_date = today - pd.Timedelta(days=0)
    start_date = today - pd.Timedelta(days=60)

    # Carrega a tabela do banco de dados
    table = sqlalchemy.Table("pecas_gerais", sqlalchemy.MetaData(), autoload_with=engine_pg)

    # Recupera as colunas existentes na tabela
    tbl_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'pecas_gerais';
    """
    tbl_existing_columns = pd.read_sql(tbl_query, pg_engine)["column_name"].tolist()

    # Processa dados em lotes de 7 dias
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + dt.timedelta(days=7)
        if next_date > end_date:
            next_date = end_date

        data_inicio_str = current_date.strftime("%d/%m/%Y")
        data_fim_str = next_date.strftime("%d/%m/%Y")

        print(f"Processando dados de {data_inicio_str} a {data_fim_str}...")

        df = download_pecas(data_inicio_str, data_fim_str)
        df_clean = preprocessa_os(df)

        # Filtra colunas que existem no banco ou que são obrigatórias para lógica
        colunas_validas = [col for col in df_clean.columns if col in tbl_existing_columns or col == "KEY_HASH"]
        df_clean = df_clean[colunas_validas].copy()

        data_dict = df_clean.to_dict(orient="records")
        total_records = len(data_dict)

        with engine_pg.begin() as conn:
            for index, row in enumerate(data_dict, start=1):
                print(
                    f"{data_inicio_str} a {data_fim_str} >> Inserindo registro {index}/{total_records} >> {row['KEY_HASH']}"
                )
                # stmt = insert(table).values(row).on_conflict_do_nothing(index_elements=["KEY_HASH"])
                stmt = insert(table).values(row).on_conflict_do_update(
                    index_elements=["KEY_HASH"],
                    set_={key: row[key] for key in row if key != "KEY_HASH"}
                )
                conn.execute(stmt)

        current_date = next_date

###################################################################################
# Execução
###################################################################################

if __name__ == "__main__":
    start_time = dt.datetime.now()

    try:
        main(engine_pg=pg_engine)
    except Exception as e:
        print(f"Erro ao executar o script: {e}")

    end_time = dt.datetime.now()
    minutes = (end_time - start_time).seconds // 60
    print(f"Tempo para executar o script (em minutos): {minutes}")
