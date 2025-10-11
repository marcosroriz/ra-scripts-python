#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer funções para gerenciar Ordens de Serviço (OS), como preprocessamento e inserção

# Imports comuns
import os
import pandas as pd
import datetime as dt

# BD
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert


# Constante indica o número mínimo de viagens que devem existir para poder classificar o consumo de uma viagem
# Por exemplo, NUM_MIN_VIAGENS_PARA_CLASSIFICAR = 5 indica que somente as viagens cuja configuração possuam outras 5
# viagens iguais (mesma linha, sentido, dia, etc) será incluída na análise
NUM_MIN_VIAGENS_PARA_CLASSIFICAR = os.getenv("NUM_MIN_VIAGENS_PARA_CLASSIFICAR", 5)


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
        dict_regra["nome_regra"] = row_regra["nome_regra"]
        dict_regra["periodo"] = row_regra["periodo"]
        dict_regra["modelos_veiculos"] = row_regra["modelos_veiculos"]
        dict_regra["qtd_min_motoristas"] = row_regra["qtd_min_motoristas"]
        dict_regra["dias_marcados"] = row_regra["dias_marcados"]
        dict_regra["qtd_min_viagens"] = row_regra["qtd_min_viagens"]
        dict_regra["limite_mediana"] = row_regra["limite_mediana"]
        dict_regra["usar_mediana_viagem"] = row_regra["usar_mediana_viagem"]
        dict_regra["limite_baixa_perfomance"] = row_regra["limite_baixa_perfomance"]
        dict_regra["usar_indicativo_baixa_performace"] = row_regra["usar_indicativo_baixa_performace"]
        dict_regra["limite_erro_telemetria"] = row_regra["limite_erro_telemetria"]
        dict_regra["usar_erro_telemetria"] = row_regra["usar_erro_telemetria"]
        dict_regra["criar_os_automatica"] = row_regra["criar_os_automatica"]

        dict_regra["target_email"] = row_regra["target_email"]
        dict_regra["target_email_dest1"] = row_regra["target_email_dest1"]
        dict_regra["target_email_dest2"] = row_regra["target_email_dest2"]
        dict_regra["target_email_dest3"] = row_regra["target_email_dest3"]
        dict_regra["target_email_dest4"] = row_regra["target_email_dest4"]
        dict_regra["target_email_dest5"] = row_regra["target_email_dest5"]

        dict_regra["target_wpp"] = row_regra["target_wpp"]
        dict_regra["target_wpp_dest1"] = row_regra["target_wpp_dest1"]
        dict_regra["target_wpp_dest2"] = row_regra["target_wpp_dest2"]
        dict_regra["target_wpp_dest3"] = row_regra["target_wpp_dest3"]
        dict_regra["target_wpp_dest4"] = row_regra["target_wpp_dest4"]
        dict_regra["target_wpp_dest5"] = row_regra["target_wpp_dest5"]

        dict_regra["hora_disparar"] = row_regra["hora_disparar"].strftime("%H:%M")

        return dict_regra

    def __get_subquery_dia_marcado(self, dias_marcado):
        dias_subquery = ""

        if "SEG_SEX" in dias_marcado:
            dias_subquery = "AND dia_numerico BETWEEN 2 AND 6"
        elif "SABADO" in dias_marcado:
            dias_subquery = "AND dia_numerico = 7"
        elif "DOMINGO" in dias_marcado:
            dias_subquery = "AND dia_numerico = 1"
        elif "FERIADO" in dias_marcado:
            dias_subquery = "AND dia_eh_feriado = TRUE"

        return dias_subquery

    def __get_subquery_modelos_combustivel(self, lista_modelos, prefix="", termo_all="TODOS"):
        query = ""
        # Não adiciona a cláusula IN se a lista tiver "TODOS"
        if termo_all not in lista_modelos:
            query = f"""AND {prefix}"vec_model" IN ({', '.join([f"'{x}'" for x in lista_modelos])})"""

        return query

    def get_veiculos_filtrados_regra(self):
        """Função para obter a lista dos veículos que será filtrado pela regra (que será usado para envio do e-mail / WhatsApp)"""

        # Extraí as variáveis
        periodo = self.regra_dict["periodo"]
        lista_modelos = self.regra_dict["modelos_veiculos"]
        qtd_min_motoristas = self.regra_dict["qtd_min_motoristas"]
        qtd_min_viagens = self.regra_dict["qtd_min_viagens"]
        dias_marcados = self.regra_dict["dias_marcados"]
        limite_mediana = self.regra_dict["limite_mediana"]
        limite_baixa_perfomance = self.regra_dict["limite_baixa_perfomance"]
        limite_erro_telemetria = self.regra_dict["limite_erro_telemetria"]

        subquery_dia_marcado_str = self.__get_subquery_dia_marcado(dias_marcados)
        subquery_modelos_str = self.__get_subquery_modelos_combustivel(lista_modelos, termo_all="TODOS")

        # Ajusta os limites antes de executar a query
        if limite_mediana is None:
            limite_mediana = 0

        if limite_baixa_perfomance is None:
            limite_baixa_perfomance = 0

        if limite_erro_telemetria is None:
            limite_erro_telemetria = 0

        query = f"""
        WITH viagens_agg_periodo AS (
            SELECT
                vec_num_id,
                vec_model,
                COUNT(*) AS total_viagens,
                AVG(km_por_litro) AS media_km_por_litro,

                -- Total abaixo da mediana
                SUM(
                    CASE
                        WHEN analise_diff_mediana_90_dias < 0 THEN 1
                        ELSE 0
                    END
                ) AS total_abaixo_mediana,

                -- Percentual abaixo da mediana
                100 * (
                    SUM(
                        CASE
                            WHEN analise_diff_mediana_90_dias < 0 THEN 1
                            ELSE 0
                        END
                    )::NUMERIC / COUNT(*)::NUMERIC
                ) AS perc_total_abaixo_mediana,

                -- Total baixa performance
                SUM(
                    CASE
                        WHEN analise_status_90_dias = 'BAIXA PERFOMANCE (<= 2 STD)' THEN 1
                        WHEN analise_status_90_dias = 'BAIXA PERFORMANCE (<= 1.5 STD)' THEN 1
                        ELSE 0
                    END
                ) AS total_baixa_perfomance,

                -- Percentual baixa performance
                100 * (
                    SUM(
                        CASE
                            WHEN analise_status_90_dias = 'BAIXA PERFOMANCE (<= 2 STD)' THEN 1
                            WHEN analise_status_90_dias = 'BAIXA PERFORMANCE (<= 1.5 STD)' THEN 1
                            ELSE 0
                        END
                    )::NUMERIC / COUNT(*)::NUMERIC
                ) AS perc_baixa_perfomance,

                -- Total de consumo
                SUM(total_comb_l) AS total_consumo_litros,

                -- Litros excedentes
                SUM(
                    ABS(
                        total_comb_l - (tamanho_linha_km_sobreposicao / analise_valor_mediana_90_dias)
                    )
                ) AS litros_excedentes,

                -- Total erro de telemetria
                SUM(
                    CASE
                        WHEN analise_status_90_dias = 'ERRO TELEMETRIA (>= 2.0 STD)' THEN 1
                        ELSE 0
                    END
                ) AS total_erro_telemetria,

                -- Percentual de erro de telemetria
                100 * (
                    SUM(
                        CASE
                            WHEN analise_status_90_dias = 'ERRO TELEMETRIA (>= 2.0 STD)' THEN 1
                            ELSE 0
                        END
                    )::NUMERIC / COUNT(*)::NUMERIC
                ) AS perc_erro_telemetria

            FROM
                rmtc_viagens_analise_mix
            WHERE
                encontrou_linha = true
                AND analise_num_amostras_90_dias >= {NUM_MIN_VIAGENS_PARA_CLASSIFICAR}
                AND CAST("dia" AS date) BETWEEN CURRENT_DATE - INTERVAL '{350} days' AND CURRENT_DATE + INTERVAL '2 days'
                AND km_por_litro >= 1
                AND km_por_litro <= 10
                {subquery_modelos_str}
                {subquery_dia_marcado_str}
            GROUP BY
                vec_num_id,
                vec_model
            HAVING
                COUNT(*) >= {qtd_min_viagens}
                AND COUNT(DISTINCT "DriverId") >= {qtd_min_motoristas}
        )
        SELECT
            *
        FROM
            viagens_agg_periodo
        WHERE
            perc_total_abaixo_mediana >= {limite_mediana}
            AND perc_baixa_perfomance >= {limite_baixa_perfomance}
            AND perc_erro_telemetria >= {limite_erro_telemetria}
        ORDER BY
            perc_baixa_perfomance DESC;
        """

        # Executa a query
        df = pd.read_sql(query, self.pg_engine)

        # Arrendonda as colunas necessárias
        df["media_km_por_litro"] = df["media_km_por_litro"].round(2)
        df["perc_total_abaixo_mediana"] = df["perc_total_abaixo_mediana"].round(2)
        df["perc_baixa_perfomance"] = df["perc_baixa_perfomance"].round(2)
        df["perc_erro_telemetria"] = df["perc_erro_telemetria"].round(2)
        df["litros_excedentes"] = df["litros_excedentes"].round(2)
        df["total_consumo_litros"] = df["total_consumo_litros"].round(2)

        return df

    def salvar_dados_regra(self, df_regra):
        # Insere no sistema
        table = sqlalchemy.Table(
            "relatorio_regra_monitoramento_combustivel", sqlalchemy.MetaData(), autoload_with=self.pg_engine
        )

        with self.pg_engine.begin() as conn:
            try:
                stmt = insert(table).values(df_regra.to_dict(orient="records")).on_conflict_do_nothing()
                conn.execute(stmt)
            except Exception as e:
                print(f"Error: {e}")
                return False

        return True
