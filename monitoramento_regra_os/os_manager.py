#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer funções para gerenciar Ordens de Serviço (OS), como preprocessamento e inserção

# Imports básicos
import pandas as pd

# Banco de Dados
import sqlalchemy
from sqlalchemy import update, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text


# Classe
class OSManager(object):
    def __init__(self, db_engine):
        self.db_engine = db_engine

    def __get_os_abertas(self, ultima_qtd_dias=30):
        query = f"""
        SELECT
            *
        FROM
            os_dados 
        WHERE
            "DATA DA ABERTURA DA OS"::timestamp BETWEEN CURRENT_DATE - INTERVAL '{ultima_qtd_dias} days' AND CURRENT_DATE + INTERVAL '5 days'
        """

        df_os_abertas = pd.read_sql_query(query, self.db_engine)
        return df_os_abertas

    def __get_os_abertas_sem_colaborador(self, ultima_qtd_dias=30):
        query = f"""
        SELECT
            *
        FROM
            os_dados 
        WHERE
            "DATA DA ABERTURA DA OS"::timestamp BETWEEN CURRENT_DATE - INTERVAL '{ultima_qtd_dias} days' AND CURRENT_DATE + INTERVAL '5 days'
            AND "COLABORADOR QUE EXECUTOU O SERVICO" = 0
        """

        df_os_abertas_sem_colaborador = pd.read_sql_query(query, self.db_engine)
        return df_os_abertas_sem_colaborador

    def get_os_novas(self, df_os_api):
        # Pega as OS abertas nos últimos 30 dias na base de dados (que foram salvas já!)
        # Vamos comparar com as os que baixamos da API (df_os)
        df_os_banco = self.__get_os_abertas(ultima_qtd_dias=30)

        # Filtra as OS que não estão na base de dados
        df_os_novas = df_os_api[~df_os_api["NUMERO DA OS"].isin(df_os_banco["NUMERO DA OS"])]

        return df_os_novas

    def get_os_atualizadas_com_colaborador(self, df_os_api):
        # Pega as OS abertas sem colaborador na base de dados (que foram salvas já!)
        # Vamos comparar com as os que baixamos da API (df_os)
        df_os_banco_sem_colaborador = self.__get_os_abertas_sem_colaborador(ultima_qtd_dias=30)

        # Filtra as OS que estão na base de dados e que agora possuem colaborador
        df_os_novas_sem_colaborador = df_os_api[
            (df_os_api["NUMERO DA OS"].isin(df_os_banco_sem_colaborador["NUMERO DA OS"]))
            & (df_os_api["COLABORADOR QUE EXECUTOU O SERVICO"] != 0)
        ]

        return df_os_novas_sem_colaborador

    def get_os_fecharam_agora(self, df_os_api):
        # Pega as OS abertas nos últimos 30 dias na base de dados (que foram salvas já!)
        # Vamos comparar com as os que baixamos da API (df_os)
        df_os_banco = self.__get_os_abertas(ultima_qtd_dias=30)

        # Filtrar as OS que estão na base de dados previamente e possuem data de fechamento == NULL ou ''
        df_os_sem_data_fechamento = df_os_api[
            df_os_api["NUMERO DA OS"].isin(
                df_os_banco[
                    df_os_banco["DATA DO FECHAMENTO DA OS"].isnull()
                    | (df_os_banco["DATA DO FECHAMENTO DA OS"].str.strip() == "")
                ]["NUMERO DA OS"]
            )
        ]

        lista_os_sem_data_fechamento_bd = df_os_sem_data_fechamento["NUMERO DA OS"]

        # Ok, agora a gente vai filtrar df_os_api para OS que possuem data de fechamento na API e não possuem no BD
        df_os_fechou_agora = df_os_api[
            df_os_api["NUMERO DA OS"].isin(lista_os_sem_data_fechamento_bd)
            & df_os_api["DATA DO FECHAMENTO DA OS"].notnull()
            & (df_os_api["DATA DO FECHAMENTO DA OS"] != "")
        ]

        return df_os_fechou_agora

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

    def atualizar_os(self, df_os_atualizadas):
        table = sqlalchemy.Table("os_dados", sqlalchemy.MetaData(), autoload_with=self.db_engine)

        os_list = df_os_atualizadas.to_dict(orient="records")

        # Para cada os que teve atualização
        for os_dict in os_list:
            delete_stmt = delete(table).where(table.c["NUMERO DA OS"] == os_dict["NUMERO DA OS"])
            insert_nova_os_stmt = insert(table).values(os_dict).on_conflict_do_nothing(index_elements=["KEY_HASH"])

            with self.db_engine.begin() as conn:
                conn.execute(delete_stmt)
                conn.execute(insert_nova_os_stmt)

    def fecha_os_com_data_nulas(self, df_os_fecharam_agora):
        table = sqlalchemy.Table("os_dados", sqlalchemy.MetaData(), autoload_with=self.db_engine)

        # Excluí as OS com COLABORADOR = 0 que foram atribuídas para outros colaboradores
        delete_query = text(
            """
        DELETE 
        FROM 
            os_dados
        WHERE 
            "COLABORADOR QUE EXECUTOU O SERVICO" = 0
            AND "NUMERO DA OS" IN (
                SELECT "NUMERO DA OS"
                FROM os_dados
                GROUP BY "NUMERO DA OS"
                HAVING 
                    SUM(CASE WHEN "COLABORADOR QUE EXECUTOU O SERVICO" = 0 THEN 1 ELSE 0 END) > 0
                    AND 
                    SUM(CASE WHEN "COLABORADOR QUE EXECUTOU O SERVICO" <> 0 THEN 1 ELSE 0 END) > 0
            )
        """
        )
        with self.db_engine.begin() as conn:
            conn.execute(delete_query)

        # Atualiza as OS com a data de fechamento adequada
        os_list = df_os_fecharam_agora.to_dict(orient="records")
        with self.db_engine.begin() as conn:
            for os_dict in os_list:
                stmt = (
                    update(table)
                    .where(table.c["NUMERO DA OS"] == os_dict["NUMERO DA OS"])
                    .values(
                        {
                            "DATA DE FECHAMENTO DO SERVICO": os_dict.get("DATA DE FECHAMENTO DO SERVICO"),
                            "DATA DO FECHAMENTO DA OS": os_dict.get("DATA DO FECHAMENTO DA OS"),
                            "COMPLEMENTO DO SERVICO": os_dict.get("COMPLEMENTO DO SERVICO"),
                            "OBSERVACAO DA OS": os_dict.get("OBSERVACAO DA OS"),
                        }
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
            # conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_os_pecas_hodometro_v3;"))
            # conn.execute(text("REFRESH MATERIALIZED VIEW mat_view_odometro_diario;"))
