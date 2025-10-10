#!/usr/bin/env python
# coding: utf-8

# Classe que gerencia a análise da linha de ônibus
# - Combustível utilizado
# - Percentil, etc

###################################################################################
# Imports
###################################################################################

# Imports básicos
import pandas as pd

# Banco de Dados
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert

# Módulos Específicos
from bus_line_trip import BusLineInMixTrip

###################################################################################
# Classe
###################################################################################

class BusLineCombAnalyzer(object):
    def __init__(self, pgDB, debug, bus_line: BusLineInMixTrip, count_num_viagem):
        # Credenciais e debug
        self.pgDB = pgDB
        self.debug = debug
        self.bus_line = bus_line

        # Dados de combustível
        self.total_comb_l = bus_line.total_comb_l
        self.km_por_litro = bus_line.km_por_litro

        # Dados da viagem, qual é a viagem do dia?
        self.count_num_viagem = count_num_viagem

        # Slot da viagem
        self.time_slot_viagem = None

        # Dados comparativos
        self.analise_dict = dict()

    def computa_combustivel(self):
        # Computa o tempo de viagem
        # Salva na classe os timestamps da viagens que serão usados a seguir e também na hora de salvar
        # Destaca-se que o dt_start_round_timestamp > timestamp_inicio uma vez que
        # - Isso porque dt_start_round_timestamp arredonda o tempo a fração de 5 minutos anterior
        # - Isso é feito porque o sistema retorna o combustível de 5 em 5 minutos, assim, precisamos disso para pegar o
        #   combustível que ocorre no intervalo. Fazemos a interpolação para considerar esse consumo.
        self.bus_line.computa_duracao_viagem()

        # Filtra os dados de combustível
        df_comb_filtered = self.bus_line.df_comb_sort[
            (self.bus_line.df_comb_sort["StartDateTime"] >= self.dt_start_round_timestamp)
            & (self.bus_line.df_comb_sort["StartDateTime"] <= self.dt_end_round_timestamp)
        ].copy()
        df_comb_filtered["StartDateTime"] = pd.to_datetime(df_comb_filtered["StartDateTime"])

        # Calcula quantidade de combustível utilizada
        # Soma o consumo de 5 em 5 minutos e faz a interpolação para a parte inicial e final
        total_comb = self.__calcula_combustivel(
            df_comb_filtered, self.bus_line.timestamp_inicio, self.bus_line.timestamp_final
        )

        # Computa km / l (convertendo para km e L)
        tam_linha_km = self.bus_line.tam_linha_metros / 1000
        self.total_comb_L = total_comb / 1000

        # Caso o modelo do veículo seja VW dividimos o total de combustível por 5614
        if self.vec_model == "VW 17230 APACHE VIP-SC " or self.vec_model == "VW 17230 APACHE VIP-SC":
            self.total_comb_L = total_comb / 5614

        # Computa km / l
        tam_linha_km_sobreposicao = tam_linha_km * (self.bus_line.overlap_final / 100)

        # Caso total_comb_L seja != 0, computa km por litro
        if self.total_comb_L != 0:
            self.km_por_litro = tam_linha_km_sobreposicao / self.total_comb_L
        else:
            self.km_por_litro = None
            self.total_comb_L = None

        return self.total_comb_L, self.km_por_litro

    # Calcula a quantidade de combustível utilizada
    def __calcula_combustivel(df_comb_filtered, timestamp_inicio, timestamp_final):
        # Verifica se o DataFrame está vazio
        # ou
        # só tem um único dado (impossibilita calcular pq fazemos o calculo pela diferença entre dados subsequenetos)
        if df_comb_filtered.empty or len(df_comb_filtered) == 1:
            return 0

        # Primeira parte (buffer até inicial)
        # 300 = 5 minutos antes até o tempo inicial, para interpolar e pegar o combustível ponderado
        first_part_diff_seconds = (
            300 - (pd.to_datetime(timestamp_inicio) - df_comb_filtered.iloc[0]["StartDateTime"]).total_seconds()
        )
        first_part_diff_comb = df_comb_filtered.iloc[1]["Value"] - df_comb_filtered.iloc[0]["Value"]
        first_part_comb = first_part_diff_comb * first_part_diff_seconds / 300

        # Parte do meio
        middle_comb = 0
        total_rows = len(df_comb_filtered)

        for i in range(1, total_rows - 2):
            df_comb_antes = df_comb_filtered.iloc[i]
            df_comb_depois = df_comb_filtered.iloc[i + 1]

            diff_comb = df_comb_depois["Value"] - df_comb_antes["Value"]

            if diff_comb > 0:
                middle_comb = middle_comb + (df_comb_depois["Value"] - df_comb_antes["Value"])
            else:
                middle_comb = middle_comb + (df_comb_depois["Value"])

        # Última parte
        last_part_diff_seconds = (
            300 - (df_comb_filtered.iloc[-1]["StartDateTime"] - pd.to_datetime(timestamp_final)).total_seconds()
        )
        last_part_diff_comb = df_comb_filtered.iloc[-1]["Value"] - df_comb_filtered.iloc[-2]["Value"]
        last_part_comb = last_part_diff_comb * last_part_diff_seconds / 300

        # Combustível total
        total_comb = first_part_comb + middle_comb + last_part_comb

        return total_comb

    def classifica_combustivel_gasto(self):
        # Função que classifica o combustível gasto nesta viagem

        # Primeiro identifica o time slot da viagem
        self.time_slot_viagem = self.__get_time_slot_viagem()

        # Segundo, constrói a query para retornar TODAS as viagens com a mesma configuração
        # dia da semana + time slot + linha + sentido + modelo do veículo
        query = self.__get_texto_query_viagens_na_mesma_configuracao_horario_linha_modelo()

        # Executa a query
        df_viagens_mesma_config = self.pgDB.read_sql_safe(query)

        # Analisa as viagens
        self.analise_dict = self.__analisa_viagens_mesma_config(df_viagens_mesma_config)

        return self.analise_dict

    def salvar(self, table_name, trip_id, driver_id):
        bus_line_dict = {
            "TripId": trip_id,
            "DriverId": driver_id,
            "dia": self.bus_line.dia,
            "dia_numerico": self.bus_line.dia_numerico,
            "dia_eh_feriado": self.bus_line.dia_eh_feriado,
            "dia_vespera_feriado": self.bus_line.dia_vespera_feriado,
            "time_slot": self.time_slot_viagem,
            "vec_num_id": self.bus_line.vec_num_id,
            "vec_asset_id": self.bus_line.vec_asset_id,
            "vec_model": self.bus_line.vec_model,
            "num_viagem": self.count_num_viagem,
            "rmtc_linha_prevista": self.bus_line.numero_linha_overlap,
            "rmtc_timestamp_inicio": self.bus_line.timestamp_inicio,
            "rmtc_timestamp_fim": self.bus_line.timestamp_final,
            "rmtc_destino_curto": self.bus_line.numero_linha_overlap,
            "encontrou_linha": self.bus_line.encontrou_linha,
            "encontrou_numero_linha": self.bus_line.numero_linha_overlap,
            "encontrou_numero_sublinha": self.bus_line.numero_sublinha,
            "encontrou_sentido_linha": self.bus_line.sentido_linha_overlap,
            "encontrou_timestamp_inicio": self.bus_line.timestamp_inicio,
            "encontrou_timestamp_fim": self.bus_line.timestamp_final,
            "encontrou_tempo_viagem_segundos": self.bus_line.tempo_viagem_segundos,
            "overlap_inicial": self.bus_line.overlap_inicial,
            "overlap_final": self.bus_line.overlap_final,
            "teve_overlap_ponto_final": self.bus_line.teve_overlap,
            "tamanho_linha_km": self.bus_line.tam_linha_metros / 1000,
            "tamanho_linha_km_sobreposicao": self.bus_line.tam_linha_km_sobreposicao,
            "total_comb_l": self.bus_line.total_comb_l,
            "km_por_litro": self.bus_line.km_por_litro,
        }

        # Combina dados da linha com os da analise
        dict_final = {**bus_line_dict, **self.analise_dict}

        # Cria o dataframe
        df_salvar = pd.DataFrame(dict_final, index=[0])

        # Pega a engine
        pg_engine = self.pgDB.get_engine()

        # Salva no banco de dado PostgreSQL
        metadata = MetaData()
        tabela_rmtc_viagens_analise = Table(table_name, metadata, autoload_with=pg_engine)
        
        with pg_engine.begin() as conn:
            # Faz o insert
            stmt = insert(tabela_rmtc_viagens_analise).values(df_salvar.to_dict(orient="records"))
            stmt = stmt.on_conflict_do_nothing(index_elements=["dia", "num_viagem", "vec_asset_id"])
            conn.execute(stmt)

    def __get_time_slot_viagem(self):
        data_str = self.bus_line.timestamp_inicio
        data_dt_utc = pd.to_datetime(data_str, utc=True)

        # Converte para fuso de Brasília
        dt_dt_local = data_dt_utc.tz_convert("America/Sao_Paulo")

        # Arredonda para o múltiplo de 30 min mais próximo
        dt_trunc = dt_dt_local.round("30min")

        # Formata apenas hora:minuto
        time_slot = dt_trunc.strftime("%H:%M")

        return time_slot

    def __get_subquery_dias(self):
        dias_subquery = ""
        data_local_str = """EXTRACT(DOW FROM (TO_TIMESTAMP(encontrou_timestamp_inicio, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') - INTERVAL '3 hours')::date)"""

        if self.bus_line.dia_numerico in [2, 3, 4, 5, 6]:  # Dia da semana
            dias_subquery = f"AND {data_local_str} BETWEEN 2 AND 6 "
        elif self.bus_line.dia_numerico == 7:  # Sábado
            dias_subquery = f"AND {data_local_str} = 7 "
        elif self.bus_line.dia_numerico == 0:  # Domingo
            dias_subquery = f"AND {data_local_str} = 1 "

        if not self.bus_line.dia_eh_feriado:
            dias_subquery += """
                AND NOT EXISTS (
                    SELECT 1
                    FROM feriados_goias fg
                    WHERE 
                        fg.data = (TO_TIMESTAMP(encontrou_timestamp_inicio, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') - INTERVAL '3 hours')::date
                        AND fg.municipio IN ('Brasil', 'Goiás', 'Goiânia')
                )
                """
        else:
            dias_subquery += """
                AND EXISTS (
                    SELECT 1
                    FROM feriados_goias fg
                    WHERE 
                        fg.data = (TO_TIMESTAMP(encontrou_timestamp_inicio, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') - INTERVAL '3 hours')::date
                        AND fg.municipio IN ('Brasil', 'Goiás', 'Goiânia')
                )
            """

        return dias_subquery

    def __get_texto_query_viagens_na_mesma_configuracao_horario_linha_modelo(self):
        subquery_dias_semana = self.__get_subquery_dias()
        query = f"""
        WITH vec_model AS (
            SELECT 
                DISTINCT CASE
                    WHEN vec_model ILIKE 'MB OF 1721%%' THEN 'MB OF 1721 MPOLO TORINO U'
                    WHEN vec_model ILIKE 'IVECO/MASCA%%' THEN 'IVECO/MASCA GRAN VIA'
                    WHEN vec_model ILIKE 'VW 17230 APACHE VIP%%' THEN 'VW 17230 APACHE VIP-SC'
                    WHEN vec_model ILIKE 'O500%%' THEN 'O500'
                    WHEN vec_model ILIKE 'ELETRA INDUSCAR MILLENNIUM%%' THEN 'ELETRA INDUSCAR MILLENNIUM'
                    WHEN vec_model ILIKE 'Induscar%%' THEN 'INDUSCAR'
                    WHEN vec_model ILIKE 'VW 22.260 CAIO INDUSCAR%%' THEN 'VW 22.260 CAIO INDUSCAR'
                    ELSE vec_model
                END AS vec_model_padronizado
            FROM rmtc_viagens_analise_mix
            WHERE vec_num_id = '{self.bus_line.vec_num_id}'
            LIMIT 1
        )

        SELECT 
            (
                DATE_TRUNC('hour', TO_TIMESTAMP(encontrou_timestamp_inicio, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') - INTERVAL '3 hours') +
                MAKE_INTERVAL(mins => (
                    ROUND(EXTRACT(minute FROM TO_TIMESTAMP(encontrou_timestamp_inicio, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') - INTERVAL '3 hours') / 30.0) * 30
                )::int)
            )::time AS slot_horario,

            CASE
                WHEN vec_model ILIKE 'MB OF 1721%%' THEN 'MB OF 1721 MPOLO TORINO U'
                WHEN vec_model ILIKE 'IVECO/MASCA%%' THEN 'IVECO/MASCA GRAN VIA'
                WHEN vec_model ILIKE 'VW 17230 APACHE VIP%%' THEN 'VW 17230 APACHE VIP-SC'
                WHEN vec_model ILIKE 'O500%%' THEN 'O500'
                WHEN vec_model ILIKE 'ELETRA INDUSCAR MILLENNIUM%%' THEN 'ELETRA INDUSCAR MILLENNIUM'
                WHEN vec_model ILIKE 'Induscar%%' THEN 'INDUSCAR'
                WHEN vec_model ILIKE 'VW 22.260 CAIO INDUSCAR%%' THEN 'VW 22.260 CAIO INDUSCAR'
                ELSE vec_model
            END AS vec_model_padronizado,
            *

        FROM rmtc_viagens_analise_mix

        WHERE
            (
                DATE_TRUNC('hour', TO_TIMESTAMP(encontrou_timestamp_inicio, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') - INTERVAL '3 hours') +
                MAKE_INTERVAL(mins => (
                    ROUND(EXTRACT(minute FROM TO_TIMESTAMP(encontrou_timestamp_inicio, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') - INTERVAL '3 hours') / 30.0) * 30
                )::int)
            )::time = TIME '{self.time_slot_viagem}'

            AND encontrou_linha = TRUE
            AND encontrou_numero_sublinha = '{self.bus_line.numero_sublinha}'
            AND encontrou_sentido_linha = '{self.bus_line.sentido_linha_overlap}'
            AND km_por_litro > 0.5
            AND km_por_litro < 10
            {subquery_dias_semana}
            AND (
                CASE
                    WHEN vec_model ILIKE 'MB OF 1721%%' THEN 'MB OF 1721 MPOLO TORINO U'
                    WHEN vec_model ILIKE 'IVECO/MASCA%%' THEN 'IVECO/MASCA GRAN VIA'
                    WHEN vec_model ILIKE 'VW 17230 APACHE VIP%%' THEN 'VW 17230 APACHE VIP-SC'
                    WHEN vec_model ILIKE 'O500%%' THEN 'O500'
                    WHEN vec_model ILIKE 'ELETRA INDUSCAR MILLENNIUM%%' THEN 'ELETRA INDUSCAR MILLENNIUM'
                    WHEN vec_model ILIKE 'Induscar%%' THEN 'INDUSCAR'
                    WHEN vec_model ILIKE 'VW 22.260 CAIO INDUSCAR%%' THEN 'VW 22.260 CAIO INDUSCAR'
                    ELSE vec_model
                END
            ) = (SELECT vec_model_padronizado FROM vec_model)
        """
        return query

    def __analisa_viagens_mesma_config(self, df):
        ref_date = pd.to_datetime(self.bus_line.dia)
        current_kml = self.bus_line.km_por_litro

        # Converte dia para datetime
        df["dia"] = pd.to_datetime(df["dia"])

        periodos = {
            "30_dias": (df["dia"] >= ref_date - pd.Timedelta(days=30)) & (df["dia"] <= ref_date),
            "60_dias": (df["dia"] >= ref_date - pd.Timedelta(days=60)) & (df["dia"] <= ref_date),
            "90_dias": (df["dia"] >= ref_date - pd.Timedelta(days=90)) & (df["dia"] <= ref_date),
            "full_dias": df["dia"] <= ref_date,
        }

        results = {}

        for label, mascara in periodos.items():
            df_subset = df.loc[mascara, ["km_por_litro"]]

            if df_subset.empty:
                # Caso não haja resultado, utiliza o valor atual como inicial, com desvpadrao = 0 e diff = 0
                results[f"analise_valor_mediana_{label}"] = current_kml
                results[f"analise_std_mediana_{label}"] = 0
                results[f"analise_diff_mediana_{label}"] = 0
                results[f"analise_status_{label}"] = "NORMAL"
                results[f"analise_num_amostras_{label}"] = 1
            else:
                median_val = df_subset["km_por_litro"].median()
                std_val = df_subset["km_por_litro"].std(ddof=0)
                diff_val = current_kml - median_val

                # Classifica
                if current_kml <= median_val - 2 * std_val:
                    status = "BAIXA PERFOMANCE (<= 2 STD)"
                elif current_kml <= median_val - 1.5 * std_val:
                    status = "BAIXA PERFORMANCE (<= 1.5 STD)"
                elif current_kml <= median_val - 1.0 * std_val:
                    status = "SUSPEITA BAIXA PERFORMANCE (<= 1.0 STD)"
                elif current_kml >= median_val + 2.0 * std_val:
                    status = "ERRO TELEMETRIA (>= 2.0 STD)"
                else:
                    status = "NORMAL"

                results[f"analise_valor_mediana_{label}"] = median_val
                results[f"analise_std_mediana_{label}"] = std_val
                results[f"analise_diff_mediana_{label}"] = diff_val
                results[f"analise_status_{label}"] = status
                results[f"analise_num_amostras_{label}"] = len(df_subset)

        return results
