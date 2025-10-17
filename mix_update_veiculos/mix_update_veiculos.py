#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

# Imports básicos
import os
import pandas as pd

# Import de datas
import datetime as dt

# BD
from sqlalchemy import create_engine

# Requests
import requests


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

# Dados da conexão
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Criar o engine SQLAlchemy
pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Constantes
MIX_API_IDENTITY_URL = os.getenv("MIX_API_IDENTITY_URL")
MIX_API_URL = os.getenv("MIX_API_URL")
MIX_USERNAME = os.getenv("MIX_USERNAME")
MIX_PASSWORD = os.getenv("MIX_PASSWORD")
MIX_API_USERNAME = os.getenv("MIX_API_USERNAME")
MIX_API_PASSWORD = os.getenv("MIX_API_PASSWORD")
MIX_GROUP_ID = os.getenv("MIX_GROUP_ID")


###################################################################################
# Funções
###################################################################################

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


###################################################################################
# Main
###################################################################################


def get_veiculos_api():
    """
    Pega os veículos da API do Mix.
    """
    url = MIX_API_URL + f"/api/assets/group/{MIX_GROUP_ID}"
    resp = requests.request("GET", url, headers=AUTH_HEADERS, data={})
    df_veiculos = pd.json_normalize(resp.json())

    return df_veiculos


def get_veiculos_db():
    """
    Pega os veículos do banco de dados.
    """
    df_veiculos = pd.read_sql("SELECT * FROM veiculos_api", pg_engine)
    return df_veiculos


if __name__ == "__main__":
    authenticate()

    df_veiculos_api = get_veiculos_api()
    df_veiculos_db = get_veiculos_db()

    # Verifica se há veículos que não estão no banco de dados
    df_veiculos_api_not_db = df_veiculos_api[~df_veiculos_api["AssetId"].isin(df_veiculos_db["AssetId"])]

    if not df_veiculos_api_not_db.empty:
        # Remove colunas não utilizadas
        db_colunas = [col for col in df_veiculos_api.columns if col in df_veiculos_db.columns]
        df_veiculos_api_not_db_normalized = df_veiculos_api_not_db[db_colunas]
        
        # Insere os veículos que não estão no banco de dados
        df_veiculos_api_not_db_normalized.to_sql("veiculos_api", pg_engine, if_exists="append", index=False)

        # Imprime o número de veículos que foram inseridos
        print(f"Número de veículos inseridos: {len(df_veiculos_api_not_db)}")
    else:
        print("Não inseri nenhum veículo")
