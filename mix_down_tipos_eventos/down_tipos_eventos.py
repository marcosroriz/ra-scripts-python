#!/usr/bin/env python
# coding: utf-8


###################################################################################
# Imports
###################################################################################

# Imports básicos
import os
import json
import pandas as pd
import time
import sys

# Import de datas
import datetime as dt

# Requests
import requests

# Banco de Dados
from sqlalchemy import create_engine

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


# Função que retorna os eventos existentes no banco
def get_eventos_existentes():
    """
    Função que verifica se uma determinada viagem já foi processada
    """
    query = f"""
    SELECT 
        "EventTypeId"
    FROM 
        tipos_eventos_api
    """

    df_eventos_existentes = pd.read_sql(query, pg_engine)

    return df_eventos_existentes


def main():
    global TOKEN, AUTH_HEADERS, AUTH_HEADERS_JSON
    authenticate()

    # Lista de Motoristas
    url = MIX_API_URL + f"/api/libraryevents/organisation/{MIX_GROUP_ID}"
    resp = requests.request("GET", url, headers=AUTH_HEADERS, data={})
    df_eventos = pd.json_normalize(resp.json())

    # Filtra os eventos que ainda não existem no banco
    df_eventos_existentes = get_eventos_existentes()

    # Filtra os eventos que ainda não existem no banco
    df_eventos_a_inserir = df_eventos[~df_eventos["EventTypeId"].isin(df_eventos_existentes["EventTypeId"])].copy()

    # Por padrão coloca baixar como falso
    df_eventos_a_inserir["Baixar"] = False

    # Insere no banco
    df_eventos_a_inserir.to_sql("tipos_eventos_api", pg_engine, if_exists="append", index=False)

    # Imprime o número de eventos que foram inseridos
    print(f"Número de eventos inseridos: {df_eventos_a_inserir.shape[0]}")


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
