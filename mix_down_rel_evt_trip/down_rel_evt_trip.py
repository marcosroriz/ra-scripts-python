#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

import os
import sys
import datetime as dt
from tqdm import tqdm
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
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

# Configurações do banco de dados
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "dbname": os.getenv("DB_NAME"),
}

# Conexão com o banco de dados
pg_engine = pg.connect(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

###################################################################################
# Funções
###################################################################################


def return_all_tables(engine: any) -> list:
    """
    Retorna uma lista com os nomes das tabelas que possuem a coluna 'StartDateTime'.
    """
    query = """
    SELECT DISTINCT table_name
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND column_name = 'StartDateTime';
    """
    df = pd.read_sql(query, engine)
    return df["table_name"].to_list()


def trips_of_event(event_name: str, engine: any) -> None:
    """
    Insere dados de eventos ocorridos nos últimos 5 dias que estão associados a viagens.
    """
    query = f"""
    INSERT INTO trip_possui_evento (asset_id, trip_id, event_id, event_type_id, dia_evento)
    SELECT DISTINCT 
        ra."AssetId" AS asset_id,
        ta."TripId" AS trip_id,
        ra."EventId" AS event_id,
        ra."EventTypeId" AS event_type_id,
        ra."StartDateTime" AS dia_evento
    FROM {event_name} ra
    INNER JOIN trips_api ta 
        ON ra."AssetId" = ta."AssetId" 
        AND ra."StartDateTime" BETWEEN ta."TripStart" AND ta."TripEnd"
    WHERE ra."StartDateTime"::timestamp 
        BETWEEN (CURRENT_DATE - INTERVAL '10 days') AND CURRENT_DATE
    ON CONFLICT (event_id) DO NOTHING;
    """
    with engine.cursor() as cursor:
        cursor.execute(query)
        engine.commit()


def main(engine_pg: any) -> None:
    table_names = return_all_tables(engine=engine_pg)
    for table_name in tqdm(table_names, desc="Processando tabelas", unit="tabela"):
        print(f"Tabela: {table_name}")
        trips_of_event(event_name=table_name, engine=engine_pg)


###################################################################################
# Execução
###################################################################################

if __name__ == "__main__":
    # Cria engine para o logger
    engine_logger = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    with ExecutionLogger(engine_logger, "mix_down_rel_evt_trip"):
        try:
            main(engine_pg=pg_engine)
        finally:
            pg_engine.close()
