#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

import datetime as dt
import os
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

def trips_of_position(engine, date):
    """
    Insere dados de trips e suas posições no banco para a data especificada.
    """
    query = """
    INSERT INTO trip_possui_posicao 
    (
        asset_id, 
        driver_id, 
        position_id, 
        trip_id, 
        dia_posicao,
        longitude,
        latitude
    ) 
    SELECT DISTINCT 
        pg."AssetId",
        pg."DriverId",
        pg."PositionId", 
        ta."TripId",
        pg."Timestamp",
        pg."Longitude",
        pg."Latitude"
    FROM posicao_gps pg 
    LEFT JOIN trips_api ta 
        ON ta."AssetId" = pg."AssetId"
        AND ta."DriverId" = pg."DriverId" 
    WHERE pg."Timestamp" BETWEEN ta."TripStart" AND ta."TripEnd"
      AND DATE(pg."Timestamp") = %s
    ON CONFLICT (position_id) DO NOTHING;
    """
    print(f"Executando query para a data: {date}")
    with engine.cursor() as cursor:
        cursor.execute(query, (date,))
        engine.commit()


def main(engine_pg):
    # Loop para os últimos 5 dias (dias -5 até -1)
    for i in range(5, 0, -1):
        date_to_process = (dt.date.today() - dt.timedelta(days=i)).isoformat()
        trips_of_position(engine=engine_pg, date=date_to_process)

###################################################################################
# Execução
###################################################################################

if __name__ == "__main__":
    # Cria engine para o logger
    engine_logger = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    with ExecutionLogger(engine_logger, "mix_down_rel_pos_trip"):
        try:
            main(engine_pg=pg_engine)
        finally:
            pg_engine.close()
