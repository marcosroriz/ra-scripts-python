#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

# Imports de sistema
import os
import gc
import sys

# Imports básicos
import pandas as pd
import time
import json

# CLI
import click

# Import de datas
import datetime as dt

# Requests
import requests

# Banco de Dados
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert

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

# Dados da conexão
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Criar a conexão com o SQLAlchemy
pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Constantes
MIX_API_IDENTITY_URL = os.getenv("MIX_API_IDENTITY_URL")
MIX_API_URL = os.getenv("MIX_API_URL")
MIX_USERNAME = os.getenv("MIX_USERNAME")
MIX_PASSWORD = os.getenv("MIX_PASSWORD")
MIX_API_USERNAME = os.getenv("MIX_API_USERNAME")
MIX_API_PASSWORD = os.getenv("MIX_API_PASSWORD")
MIX_GROUP_ID = os.getenv("MIX_GROUP_ID")

# Tabela
metadata = MetaData()
posicao_gps_table = Table("posicao_gps", metadata, autoload_with=pg_engine)

###################################################################################
# Autenticação
###################################################################################

payload = f"""
grant_type=password&
username={MIX_USERNAME}&
password={MIX_PASSWORD}&
scope=offline_access%20MiX.Integrate
"""
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
}

TOKEN = None
AUTH_HEADERS = None
AUTH_HEADERS_JSON = None
ULTIMO_LOGIN = None


def get_auth_token():
    response = requests.request(
        "POST",
        MIX_API_IDENTITY_URL + "/core/connect/token",
        headers=headers,
        data=payload,
        auth=(MIX_API_USERNAME, MIX_API_PASSWORD),
    )
    print(response.json())
    return response.json()["access_token"]


def authenticate():
    global TOKEN, AUTH_HEADERS, AUTH_HEADERS_JSON, ULTIMO_LOGIN

    if ULTIMO_LOGIN is None:
        TOKEN = get_auth_token()
        ULTIMO_LOGIN = dt.datetime.now()
    elif (dt.datetime.now() - ULTIMO_LOGIN).seconds > 3000:
        TOKEN = get_auth_token()
        ULTIMO_LOGIN = dt.datetime.now()

    AUTH_HEADERS = {"Authorization": f"Bearer {TOKEN}"}
    AUTH_HEADERS_JSON = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


#### Download de Evento
def download_posicao(asset_ids, data_inicio, data_fim):
    global AUTH_HEADERS_JSON

    # Datas
    data_inicio_str = data_inicio.strftime("%Y%m%d%H%M%S")
    data_final_str = data_fim.strftime("%Y%m%d%H%M%S")

    # URL
    url = MIX_API_URL + f"/api/positions/assets/from/{data_inicio_str}/to/{data_final_str}"

    # Resposta
    return requests.request(
        "POST", url, headers=AUTH_HEADERS_JSON, data=json.dumps(list(map(int, asset_ids))), timeout=600
    )


@click.command()
@click.option("--data_baixar", type=str, help="Data que irei baixar")
def main(data_baixar):
    global TOKEN, AUTH_HEADERS, AUTH_HEADERS_JSON
    authenticate()

    # Lista de Veículos
    url = MIX_API_URL + f"/api/assets/group/{MIX_GROUP_ID}"
    resp = requests.request("GET", url, headers=AUTH_HEADERS, data={})
    df_veiculos = pd.json_normalize(resp.json())

    batch_size = 10
    n_batches = (df_veiculos.shape[0] // batch_size) + 1
    erros = []

    # Vamos baixar cada batch
    event_name = "posicao_gps"

    # A tabela que iremos inserir
    tbl_name = event_name

    # Step 1: Fetch existing columns from the target table
    tbl_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{tbl_name}';
    """
    tbl_existing_columns = pd.read_sql(tbl_query, pg_engine)["column_name"].tolist()

    i = 0
    while i < n_batches:
        # Pega datas
        datahoje = dt.datetime.today().date()
        if data_baixar:
            datahoje = pd.to_datetime(data_baixar)
        dataontem = datahoje - dt.timedelta(days=1)

        # Printa informações do batch
        print(datahoje, f"POS Batch {i+1} / {n_batches} valores de {i*batch_size} até {(i+1)*batch_size}")

        # Pega dados do batch
        asset_ids = df_veiculos["AssetId"].values[i * batch_size : (i + 1) * batch_size]
        print(datahoje, f"POS Tamanho do batch: {len(asset_ids)}")

        try:
            # Autentica novamente para evitar timeout
            authenticate()

            response = download_posicao(asset_ids, dataontem, datahoje)

            # Resposta está OK?
            if response.status_code == 200:
                # Ajusta o dado no pandas
                df_evt = pd.json_normalize(response.json())
                df_evt = df_evt[
                    [
                        "Timestamp",
                        "Longitude",
                        "Latitude",
                        "DriverId",
                        "AssetId",
                        "PositionId",
                        "OdometerKilometres",
                        "SpeedKilometresPerHour",
                    ]
                ].copy()
                df_evt.columns = df_evt.columns.str.replace(".", "_")

                df_filtered = df_evt
                # Remove colunas que não existem na tabela se tbl_existing_columns não for vazio
                if tbl_existing_columns:
                    # Filter existing_columns to include only columns that are actually in df_evt
                    tbl_filtered_columns = [col for col in tbl_existing_columns if col in df_evt.columns]
                    df_filtered = df_evt[tbl_filtered_columns]

                # Salva no banco
                pg_engine.dispose()
                with pg_engine.begin() as conn:
                    # Faz o insert
                    stmt = insert(posicao_gps_table).values(df_filtered.to_dict(orient="records"))
                    stmt = stmt.on_conflict_do_nothing(index_elements=["AssetId", "PositionId"])
                    conn.execute(stmt)

                # Printa informações da operação
                print(datahoje, event_name, "SALVAMOS ", df_evt.shape[0], " REGISTROS")

            # Deu erro de autenticação? Vamos tentar novamente
            if response.status_code == 401:
                authenticate()
                print(datahoje, event_name, "DEU 401")
                continue
        except requests.exceptions.Timeout as te:
            print(datahoje, event_name, "DEU TIMEOUT")
            time.sleep(10)
            authenticate()
            continue
        except Exception as e:
            print("Erro no batch", e)
            erros.append((datahoje, event_name, "BATCH", i + 1, "ERRO", e))

        print(datahoje, event_name, "TERMINAMOS O BATCH", i + 1)
        # Incrementa o batch
        i += 1
        time.sleep(15)
        # Limpa a memória
        gc.collect()

    # Imprime os erros
    print("NUMERO DE ERROS", len(erros))
    print("ERROS -------------------")
    for e in erros:
        print(e)


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
