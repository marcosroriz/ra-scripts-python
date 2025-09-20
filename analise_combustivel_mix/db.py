#!/usr/bin/env python
# coding: utf-8

# Singleton para controlar o acesso ao banco de dados PostgreSQL
# Requer arquivo .env com os seguintes parâmetros
# - DB_HOST: Endereço do banco
# - DB_PORT: Porta do banco
# - DB_USER: Usuário do banco
# - DB_PASS: Senha do usuário
# - DB_NAME: Nome do banco de dados

###################################################################################
# Imports
###################################################################################

# Imports de sistema
import os

# Imports básicos
import pandas as pd

# PostgresSQL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from threading import Lock

# DotEnv
from dotenv import load_dotenv

# Re-carrega variáveis de ambiente
CURRENT_WORKINGD_DIR = os.getcwd()
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))

load_dotenv()
load_dotenv("../.env")
load_dotenv(os.path.join(CURRENT_WORKINGD_DIR, ".env"))
load_dotenv(os.path.join(CURRENT_PATH, "..", ".env"))


class PostgresSingleton:
    """
    Singleton para acessar o banco de dados PostgreSQL
    """

    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        """
        Cria ou retorna uma instância do singleton
        """
        with cls._lock:  # Garante thead safety
            if cls._instance is None:
                cls._instance = super(PostgresSingleton, cls).__new__(cls)
                cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self):
        """
        Inicializa a conexão
        """
        if hasattr(self, "_initialized") and self._initialized:
            # Evita reinicializar
            return

        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASS")
        db_name = os.getenv("DB_NAME")
        debug_mode = False # os.getenv("DEBUG", "True").lower() in ("true", "1", "yes")

        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        self._engine = create_engine(
            db_url,
            pool_size=10,        # Número de conexões na pool
            pool_pre_ping=True,  # Verifica se conexão tá viva antes de usar
            echo=debug_mode,     # Se true, mostra os logs das queries
        )
        self._Session = sessionmaker(bind=self._engine)
        self._initialized = True  # Marca como inicializado

    @classmethod
    def get_instance(cls):
        """
        Retorna a singleton
        """
        return cls()

    def get_engine(self):
        """
        Retorna a SQLAlchemy engine.
        """
        return self._engine

    def get_session(self):
        """
        Retorna a SQLAlchemy session
        """
        return self._Session()

    # Rotina para ler dados do banco de dados de forma segura
    def read_sql_safe(self, query):
        df = pd.DataFrame()
        with self._engine.connect() as conn:
            df = pd.read_sql(query, conn)

        return df
