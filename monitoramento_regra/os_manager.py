#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer funções para gerenciar Ordens de Serviço (OS), como preprocessamento e inserção

# Imports básicos
import pandas as pd

# Banco de Dados
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text


# Classe
class OSManager(object):
    def __init__(self, db_engine):
        self.db_engine = db_engine

    def __get_os_abertas(self, ultima_qtd_dias=30):
        query = f"""
        SELECT
            "NUMERO DA OS"
        FROM
            os_dados 
        WHERE
            "DATA DA ABERTURA DA OS"::timestamp BETWEEN CURRENT_DATE - INTERVAL '{ultima_qtd_dias} days' AND CURRENT_DATE + INTERVAL '5 days'
        """

        df_os_abertas = pd.read_sql_query(query, self.db_engine)
        return df_os_abertas

    def get_os_novas(self, df_os):
        # Pega as OS abertas nos últimos 30 dias
        df_os_abertas = self.__get_os_abertas(ultima_qtd_dias=30)

        # Filtra as OS que não estão na base de dados
        df_os_novas = df_os[~df_os["NUMERO DA OS"].isin(df_os_abertas["NUMERO DA OS"])]

        return df_os_novas

    def insert_os_novas(self, df_os_novas):
        table = sqlalchemy.Table("os_dados", sqlalchemy.MetaData(), autoload_with=self.db_engine)

        os_list = df_os_novas.to_dict(orient="records")

        with self.db_engine.begin() as conn:
            for os_dict in os_list:
                stmt = (
                    insert(table)
                    .values(os_dict)
                    .on_conflict_do_update(
                        index_elements=["KEY_HASH"], set_={col: os_dict[col] for col in os_dict if col != "KEY_HASH"}
                    )
                )
                conn.execute(stmt)

    def refresh_views(self):
        with self.db_engine.begin() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_retrabalho_10_dias;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_retrabalho_15_dias;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_retrabalho_30_dias;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_retrabalho_10_dias_distinct;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_retrabalho_15_dias_distinct;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_retrabalho_30_dias_distinct;"))
            conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_os_pecas_hodometro_v3;"))
