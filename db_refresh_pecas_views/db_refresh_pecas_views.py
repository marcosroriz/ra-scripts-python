#!/usr/bin/env python
# coding: utf-8

# Script que atualiza as views de peças e hodômetro no banco de dados

###################################################################################
# Imports
###################################################################################

# Imports variáveis de ambiente
from dotenv import load_dotenv

# Tenta carregar o .env atual e o do diretório pai
load_dotenv("./env")
load_dotenv("../.env")

# Bibliotecas padrão
import os
import sys

# BD
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from execution_logger import ExecutionLogger


###################################################################################
# Configurações e Variáveis de ambiente
###################################################################################

# Não bufferiza a saída
sys.stdout.flush()

# Variáveis do banco
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Conexão com o banco de dados
pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

if pg_engine is None:
    print("Erro ao conectar ao banco de dados.")
    sys.exit(1)


###################################################################################
# Main
###################################################################################


def main():
    with pg_engine.begin() as conn:
        conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_os_pecas_hodometro_v3;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_odometro_diario;"))


if __name__ == "__main__":
    with ExecutionLogger(pg_engine, "db_refresh_pecas_views"):
        main()
