#!/usr/bin/env python
# coding: utf-8


###################################################################################
# Imports
###################################################################################

# Imports de sistema
import os
import gc
import sys
import math

# Imports básicos
import os
import json
import pandas as pd
import time

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
from execution_logger import ExecutionLogger

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

# Config
BIGINT_MIN = -9223372036854775808
BIGINT_MAX = 9223372036854775807


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
def download_evento(asset_ids, event_id, data_inicio, data_fim):
    global AUTH_HEADERS_JSON

    # Datas
    data_inicio_str = data_inicio.strftime("%Y%m%d%H%M%S")
    data_final_str = data_fim.strftime("%Y%m%d%H%M%S")

    # URL
    url = MIX_API_URL + f"/api/events/assets/from/{data_inicio_str}/to/{data_final_str}"

    # Payload
    evt_payload = {"EntityIds": list(map(int, asset_ids)), "EventTypeIds": [event_id], "MenuId": "string"}

    # Resposta
    return requests.request("POST", url, headers=AUTH_HEADERS_JSON, data=json.dumps(evt_payload), timeout=300)

# Limpa Registro
def clean_record(record, table):
    cleaned = {}
    for key, value in record.items():
        column = table.columns.get(key)

        if column is None:
            continue

        col_type = getattr(column.type, "__visit_name__", "")

        # Trata NaN
        if isinstance(value, float) and (math.isnan(value) or pd.isna(value)):
            cleaned[key] = None
            continue

        # Trata bigint
        if col_type == 'bigint':
            try:
                value = int(value)
                if BIGINT_MIN <= value <= BIGINT_MAX:
                    cleaned[key] = value
                else:
                    cleaned[key] = None
            except:
                cleaned[key] = None

        # Trata float
        elif col_type in ('float', 'float8', 'double precision'):
            try:
                cleaned[key] = float(value)
            except:
                cleaned[key] = None

        # Demais tipos
        else:
            cleaned[key] = value

    return cleaned


    
@click.command()
@click.option("--data_baixar", type=str, help="Data que irei baixar")
def main(data_baixar):
    global TOKEN, AUTH_HEADERS, AUTH_HEADERS_JSON
    authenticate()

    # Lista de Veículos
    url = MIX_API_URL + f"/api/assets/group/{MIX_GROUP_ID}"
    resp = requests.request("GET", url, headers=AUTH_HEADERS, data={})
    df_veiculos = pd.json_normalize(resp.json())

    batch_size = 150
    n_batches = (df_veiculos.shape[0] // batch_size) + 1
    erros = []

    # Eventos que tenho que baixar
    df_eventos = pd.read_sql('SELECT * FROM tipos_eventos_api tbl WHERE tbl."Baixar" = True', pg_engine)

    # Para cada evento que eu quero baixar, vou fazer um loop
    for _, row in df_eventos.iterrows():
        event_id = row["EventTypeId"]
        event_name = row["DescriptionCLEAN"]

        print("PROCESSANDO O EVENTO", event_id, event_name)

        # A tabela que iremos inserir
        tbl_name = event_name

        # Step 1: Fetch existing columns from the target table
        tbl_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{tbl_name}';
        """
        tbl_existing_columns = pd.read_sql(tbl_query, pg_engine)["column_name"].tolist()

        if len(tbl_existing_columns) == 0:
            print("Não existe a tabela", tbl_name)
            continue

        # Metadata para sql do evento
        metadata = MetaData()
        event_table = Table(tbl_name, metadata, autoload_with=pg_engine)

        # Descobre quais colunas são booleanas no banco
        boolean_columns = [
            c.name for c in event_table.columns
            if hasattr(c.type, '__visit_name__') and c.type.__visit_name__ == 'boolean'
        ]

        i = 0
        while i < n_batches:
            # Pega datas
            datahoje = dt.datetime.now()
            if data_baixar:
                datahoje = pd.to_datetime(data_baixar)

            dataontem = datahoje - dt.timedelta(days=1)

            # Printa informações do batch
            print(datahoje, event_name, f"Batch {i+1} / {n_batches} valores de {i*batch_size} até {(i+1)*batch_size}")

            # Pega dados do batch
            asset_ids = df_veiculos["AssetId"].values[i * batch_size : (i + 1) * batch_size]
            print(datahoje, event_name, f"Tamanho do batch: {len(asset_ids)}")

            try:
                # Autentica novamente para evitar timeout
                authenticate()

                response = download_evento(asset_ids, event_id, dataontem, datahoje)

                # Resposta está OK?
                if response.status_code == 200:
                    # Ajusta o dado no pandas
                    df_evt = pd.json_normalize(response.json())
                    df_evt.columns = df_evt.columns.str.replace(".", "_")

                    # Remove colunas que não existem na tabela se tbl_existing_columns não for vazio
                    if tbl_existing_columns:
                        # Filter existing_columns to include only columns that are actually in df_evt
                        tbl_filtered_columns = [col for col in tbl_existing_columns if col in df_evt.columns]

                        df_filtered = df_evt[tbl_filtered_columns]
                    else:
                        df_filtered = df_evt

                    # Converte NaN para False somente nas colunas booleanas
                    for col in boolean_columns:
                        if col in df_filtered.columns:
                            df_filtered[col] = df_filtered[col].fillna(False).astype(bool)

                    # Salva no banco
                    pg_engine.dispose()
                    with pg_engine.begin() as conn:
                        raw_records = df_filtered.to_dict(orient="records")

                        for registro in raw_records:
                            try:
                                registro_limpo = clean_record(registro, event_table)
                                stmt = insert(event_table).values(registro_limpo)
                                stmt = stmt.on_conflict_do_nothing(index_elements=["EventTypeId", "EventId", "DriverId", "AssetId"])
                                conn.execute(stmt)
                            except Exception as e:
                                print(f"[ERRO] Linha {registro} causou erro: {e}")
                                erros.append(("registro", registro, "erro", e))
                        
                    # Printa informações da operação
                    print(datahoje, event_name, "SALVAMOS ", df_filtered.shape[0], " REGISTROS")

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

        print("TERMINAMOS O EVENTO", event_name)
        print("NUMERO DE ERROS", erros)

    # Imprime os erros
    print("NUMERO DE ERROS", len(erros))
    print("ERROS -------------------")
    for e in erros:
        print(e)



if __name__ == "__main__":
    # Executa com Logger
    with ExecutionLogger(pg_engine, "mix_down_evt"):
        main()
