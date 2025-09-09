#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer funções para gerenciar Ordens de Serviço (OS), como preprocessamento e inserção

# Imports comuns
import pandas as pd
import datetime as dt

# BD
import psycopg2 as pg
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text


##########################################################
# Funções utilitárias
##########################################################

# Função para definir o status de uma OS
def definir_status(os_row):
    if os_row.get("correcao_primeira") == True:
        return "🟩 Correção Primeira"
    elif os_row.get("correcao") == True:
        return "🟨 Correção Tardia"
    elif os_row.get("retrabalho") == True:
        return "🟥 Retrabalho"
    elif os_row.get("nova_os_com_retrabalho_anterior") == True:
        return "🟨 Nova OS, com retrabalho prévio"
    elif os_row.get("nova_os_sem_retrabalho_anterior") == True:
        return "🟦 Nova OS, sem retrabalho prévio"
    else:
        return "❓ Não classificado"


# Subqueries para filtrar as oficinas, seções e ordens de serviço quando TODAS não for selecionado
def subquery_oficinas(lista_oficinas, prefix="", termo_all="TODAS"):
    query = ""
    if termo_all not in lista_oficinas:
        query = f"""AND {prefix}"DESCRICAO DA OFICINA" IN ({', '.join([f"'{x}'" for x in lista_oficinas])})"""

    return query


def subquery_secoes(lista_secaos, prefix="", termo_all="TODAS"):
    query = ""
    if termo_all not in lista_secaos:
        query = f"""AND {prefix}"DESCRICAO DA SECAO" IN ({', '.join([f"'{x}'" for x in lista_secaos])})"""

    return query


def subquery_os(lista_os, prefix="", termo_all="TODAS"):
    if not lista_os or termo_all in lista_os:
        return ""
    valores = ", ".join([f"'{x}'" for x in lista_os if x])
    if not valores:
        return ""

    return f'AND {prefix}"DESCRICAO DO SERVICO" IN ({valores})'


def subquery_modelos(lista_modelos, prefix="", termo_all="TODAS"):
    query = ""
    if termo_all not in lista_modelos:
        query = f"""AND {prefix}"DESCRICAO DO MODELO" IN ({', '.join([f"'{x}'" for x in lista_modelos])})"""

    return query


def subquery_checklist(checklist_alvo, prefix=""):
    query = ""
    query_parts = [f"""{prefix}"{alvo}" = TRUE""" for alvo in checklist_alvo]

    if query_parts:
        query_or = " OR ".join(query_parts)
        query = f"AND ({query_or})"

    return query


class Rule(object):
    def __init__(self, row_regra, pg_engine):
        self.regra_dict = self.__obtem_dados_regra(row_regra)
        self.pg_engine = pg_engine
        self.df_regra = pd.DataFrame()
        
    def get_dados_regra(self):
        return self.regra_dict

    def __obtem_dados_regra(self, row_regra):
        dict_regra = {}

        dict_regra["id_regra"] = row_regra["id"]
        dict_regra["nome_regra"] = row_regra["nome"]
        dict_regra["data_periodo_regra"] = row_regra["data_periodo_regra"]
        dict_regra["min_dias_retrabalho"] = row_regra["min_dias_retrabalho"]
        dict_regra["lista_modelos"] = row_regra["modelos_veiculos"]
        dict_regra["lista_oficinas"] = row_regra["oficinas"]
        dict_regra["lista_secoes"] = row_regra["secoes"]
        dict_regra["lista_os"] = row_regra["os"]
        dict_regra["alerta_alvo"] = []

        if row_regra["target_nova_os_sem_retrabalho_previo"]:
            dict_regra["alerta_alvo"].append("nova_os_sem_retrabalho_anterior")

        if row_regra["target_nova_os_com_retrabalho_previo"]:
            dict_regra["alerta_alvo"].append("nova_os_com_retrabalho_anterior")

        if row_regra["target_retrabalho"]:
            dict_regra["alerta_alvo"].append("retrabalho")

        dict_regra["email_ativo"] = row_regra["target_email"]
        dict_regra["email_dest_1"] = row_regra["target_email_dest1"]
        dict_regra["email_dest_2"] = row_regra["target_email_dest2"]
        dict_regra["email_dest_3"] = row_regra["target_email_dest3"]
        dict_regra["email_dest_4"] = row_regra["target_email_dest4"]
        dict_regra["email_dest_5"] = row_regra["target_email_dest5"]

        dict_regra["wpp_ativo"] = row_regra["target_wpp"]
        dict_regra["wpp_dest_1"] = row_regra["target_wpp_dest1"]
        dict_regra["wpp_dest_2"] = row_regra["target_wpp_dest2"]
        dict_regra["wpp_dest_3"] = row_regra["target_wpp_dest3"]
        dict_regra["wpp_dest_4"] = row_regra["target_wpp_dest4"]
        dict_regra["wpp_dest_5"] = row_regra["target_wpp_dest5"]

        dict_regra["hora_disparar"] = row_regra["hora_disparar"].strftime("%H:%M")

        return dict_regra

    def get_os_filtradas_pela_regra(self):
        """Função para obter a prévia das OS detectadas pela regra (que será usado para envio do e-mail / WhatsApp)"""

        dados_regra = self.regra_dict

        data_periodo_regra = dados_regra["data_periodo_regra"]
        min_dias_retrabalho = dados_regra["min_dias_retrabalho"]
        lista_modelos = dados_regra["lista_modelos"]
        lista_oficinas = dados_regra["lista_oficinas"]
        lista_secoes = dados_regra["lista_secoes"]
        lista_os = dados_regra["lista_os"]
        alerta_alvo = dados_regra["alerta_alvo"]

        # Obtém as OS filtradas pela regra
        df_os_filtradas = self.__get_os_da_regra(
            data_periodo_regra, min_dias_retrabalho, lista_modelos, lista_oficinas, lista_secoes, lista_os, alerta_alvo
        )

        self.df_regra = df_os_filtradas

        return df_os_filtradas

    def __get_os_da_regra(
        self, data_periodo_regra, min_dias, lista_modelos, lista_oficinas, lista_secaos, lista_os, checklist_alvo
    ):
        # Subqueries
        subquery_modelos_str = subquery_modelos(lista_modelos, termo_all="TODOS")
        subquery_oficinas_str = subquery_oficinas(lista_oficinas)
        subquery_secoes_str = subquery_secoes(lista_secaos)
        subquery_os_str = subquery_os(lista_os)
        subquery_checklist_str = subquery_checklist(checklist_alvo)

        # Query
        query = f"""
        WITH 
        pecas_agg AS (
            SELECT 
                pg."OS", 
                SUM(pg."VALOR") AS total_valor, 
                STRING_AGG(pg."VALOR"::TEXT, '__SEP__' ORDER BY pg."PRODUTO") AS pecas_valor_str,
                STRING_AGG(pg."PRODUTO"::text, '__SEP__' ORDER BY pg."PRODUTO") AS pecas_trocadas_str
            FROM 
                view_pecas_desconsiderando_combustivel pg 
            WHERE 
                to_timestamp(pg."DATA", 'DD/MM/YYYY') BETWEEN CURRENT_DATE - INTERVAL '{data_periodo_regra} days' AND CURRENT_DATE + INTERVAL '1 day'
            GROUP BY 
                pg."OS"
        ),
        os_avaliadas AS (
            SELECT
                *
            FROM
                mat_view_retrabalho_{min_dias}_dias_distinct m
            LEFT JOIN 
                os_dados_classificacao odc
            ON 
                m."KEY_HASH" = odc."KEY_HASH" 
            WHERE
                "DATA DA ABERTURA DA OS"::timestamp BETWEEN CURRENT_DATE - INTERVAL '{data_periodo_regra} days' AND CURRENT_DATE + INTERVAL '1 day'
                {subquery_modelos_str}
                {subquery_oficinas_str}
                {subquery_secoes_str}
                {subquery_os_str}
                {subquery_checklist_str}
        ),
        os_avaliadas_com_pecas AS (
            SELECT *
            FROM os_avaliadas os
            LEFT JOIN pecas_agg p
            ON os."NUMERO DA OS" = p."OS"
        )
        SELECT *
        FROM os_avaliadas_com_pecas os
        LEFT JOIN colaboradores_frotas_os cfo 
        ON os."COLABORADOR QUE EXECUTOU O SERVICO" = cfo.cod_colaborador
        """

        # Executa a query
        df = pd.read_sql(query, self.pg_engine)

        # Preenche valores nulos
        df["total_valor"] = df["total_valor"].fillna(0)
        df["pecas_valor_str"] = df["pecas_valor_str"].fillna("0")
        df["pecas_trocadas_str"] = df["pecas_trocadas_str"].fillna("Nenhuma / Não inserida ainda")

        # Campos da LLM
        df["SCORE_SYMPTOMS_TEXT_QUALITY"] = df["SCORE_SYMPTOMS_TEXT_QUALITY"].fillna("-")
        df["SCORE_SOLUTION_TEXT_QUALITY"] = df["SCORE_SOLUTION_TEXT_QUALITY"].fillna("-")
        df["WHY_SOLUTION_IS_PROBLEM"] = df["WHY_SOLUTION_IS_PROBLEM"].fillna("Não classificado")

        # Aplica a função para definir o status de cada OS
        df["status_os"] = df.apply(definir_status, axis=1)

        # Datas aberturas (converte para DT)
        df["DATA DA ABERTURA DA OS DT"] = pd.to_datetime(df["DATA DA ABERTURA DA OS"])
        df["DATA DO FECHAMENTO DA OS DT"] = pd.to_datetime(df["DATA DO FECHAMENTO DA OS"])

        # Dias OS Anterior
        df["prev_days"] = df["prev_days"].fillna("Não há OS anterior para esse problema")

        # Ordena por data de abertura
        df = df.sort_values(by="DATA DA ABERTURA DA OS DT", ascending=False)

        return df


    def salvar_dados_regra(self, df_os_filtradas): 
        # Pega os dados da regra
        id_regra = self.regra_dict["id_regra"]

        # Pega somente as colunas necessárias
        df_raw = df_os_filtradas.copy()
        df_raw = df_raw[["NUMERO DA OS", "KEY_HASH", "nova_os_sem_retrabalho_anterior", "nova_os_com_retrabalho_anterior", "retrabalho"]].copy()
        df_raw.rename(columns={"NUMERO DA OS": "os_num", "KEY_HASH": "os_key_hash"}, inplace=True)
        df_raw.columns = ['os_num', 'os_key_hash', 'os_key_hash_duplicated', 'nova_os_sem_retrabalho_anterior', 'nova_os_com_retrabalho_anterior', 'retrabalho']

        df_os = df_raw[["os_num", "os_key_hash", 'nova_os_sem_retrabalho_anterior', 'nova_os_com_retrabalho_anterior', 'retrabalho']].copy()
        
        # Anexa id_regra e dia de hoje
        df_os["id_regra"] = id_regra
        df_os["dia"] = dt.date.today().strftime("%Y-%m-%d")

        # Insere no sistema
        table = sqlalchemy.Table("relatorio_regra_monitoramento_os", sqlalchemy.MetaData(), autoload_with=self.pg_engine)

        with self.pg_engine.begin() as conn:
            try:
                stmt = insert(table).values(df_os.to_dict(orient="records")).on_conflict_do_nothing()
                conn.execute(stmt)
            except Exception as e:
                print(f"Error: {e}")
                return False
            
        return True

