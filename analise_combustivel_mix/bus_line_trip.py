#!/usr/bin/env python
# coding: utf-8

# Classe que representa uma viagem do ônibus da RA dentro de uma trip da Mix

# Imports básicos
import pandas as pd

# Import de datas
from datetime import datetime
import holidays

###################################################################################
# Função utilitária e constantes
###################################################################################

# Criar calendário de feriados
feriados_goias = holidays.Brazil(prov="GO")  # Goiás

# Retorna os timestamps arredondados para x=5 minutos antes e depois
def get_round_timestamps(timestamp_inicio, timestamp_final, minutes_to_round=5):
    dt_start_round_raw = datetime.strptime(timestamp_inicio, "%Y-%m-%dT%H:%M:%SZ")
    df_end_round_raw = datetime.strptime(timestamp_final, "%Y-%m-%dT%H:%M:%SZ")

    new_minute = (dt_start_round_raw.minute // minutes_to_round) * minutes_to_round
    dt_start_round = dt_start_round_raw.replace(minute=new_minute, second=0, microsecond=0)
    dt_start_round_timestamp = dt_start_round.strftime("%Y-%m-%dT%H:%M:%SZ")
    dt_start_round_timestamp

    new_minute = ((df_end_round_raw.minute // minutes_to_round) + 1) * minutes_to_round
    if new_minute == 60:  # Handle hour overflow
        dt_end_round = df_end_round_raw.replace(
            hour=(df_end_round_raw.hour + 1) % 24, minute=0, second=0, microsecond=0
        )
    else:
        dt_end_round = df_end_round_raw.replace(minute=new_minute, second=0, microsecond=0)

    dt_end_round_timestamp = dt_end_round.strftime("%Y-%m-%dT%H:%M:%SZ")

    return dt_start_round_timestamp, dt_end_round_timestamp


###################################################################################
# Classe
###################################################################################


class BusLineInMixTrip:
    # Construtor, por padrão
    # Desliga formatter para não desalinhar comentários
    # fmt: off
    def __init__(self, pgDB, debug, vec_num_id, vec_asset_id, vec_model, dia, df_gps_sort, df_comb_sort):
        # Credenciais e debug
        self.pgDB = pgDB
        self.debug = debug

        # Dados do veículo
        self.vec_num_id = vec_num_id        # ID do veículo na RA
        self.vec_asset_id = vec_asset_id    # ID do veículo na Mix
        self.vec_model = vec_model          # Modelo do veículo
        self.df_gps_sort = df_gps_sort      # Dataframe com as posições GPS do veículo ao longo da Trip
        self.df_comb_sort = df_comb_sort    # Dataframe com os eventos de combustível do veículo ao longo da Trip

        # Dados do dia
        self.dia = dia                                                  # Dia em que a viagem ocorreu
        self.dia_numerico = self.__get_dia_semana(dia)                  # Dia da semana (1 = Domingo, 2 = Segunda ... SQL DOW)
        self.dia_eh_feriado = self.__get_dia_eh_feriado(dia)            # Dia é feriado (True/False)?
        self.dia_vespera_feriado = self.__get_dia_vespera_feriado(dia)  # Dia antes de feriado (True/False)

        # Parâmetros padrão
        self.encontrou_linha = False        # encontrou a linha?
        self.numero_linha_overlap = None    # Numero da linha
        self.numero_sublinha = None         # Numero da sublinha
        self.sentido_linha_overlap = None   # Sentido da linha
        self.start_idx = None               # ponteiro para a posição inicial da linha
        self.end_idx = None                 # ponteiro para a posição final da linha
        self.overlap_inicial = None         # overlap com KML antes de ir até o ponte extremo da linha
        self.overlap_final = None           # overlap com KML antes de ir até o ponte extremo da linha
        self.teve_overlap = False           # teve overlap com ponto final
        self.overlap_df = None              # dataframe filtrado com as posições de start idx até end idx

        # Duração da viagem
        self.timestamp_inicio = None
        self.timestamp_final = None
        self.tempo_viagem_segundos = 0
        self.dt_start_round_timestamp = None
        self.dt_end_round_timestamp = None

        # Dados da linha
        self.tam_linha_metros = 0
        self.tam_linha_km_sobreposicao = 0

        # Dados da viagem, qual é a viagem do dia?
        self.num_viagem = 0

        # Dados de combustível (serão analisados posteriormentes)
        self.total_comb_l = 0
        self.km_por_litro = 0
    # fmt: on

    # Rotina para extrair o dia da semana (day of week - dow) de forma numérica
    def __get_dia_semana(self, dia_str):
        if dia_str == "" or dia_str is None:
            return -1

        data_dt = pd.to_datetime(dia_str)
        # Pandas começa a semana com segunda = 0, terça = 1, etc
        dow_dt = data_dt.weekday()
        # Como vamos pro banco, e o banco usa domingo = 1, segunda = 2, etc, temos que converter
        # Basta adicionar 2 no pandas, porém se for domingo no dt (=6), deve-se converter para dom = 1
        dow_sql = dow_dt + 2 if dow_dt <= 5 else 1

        return dow_sql

    # Rotina que verifica se o dia é feriado
    def __get_dia_eh_feriado(self, dia_str):
        if dia_str == "" or dia_str is None:
            return -1

        data_dt = pd.to_datetime(dia_str)

        return data_dt in feriados_goias

    # Rotina que verifica se o dia é vespera de feriado
    def __get_dia_vespera_feriado(self, dia_str):
        if dia_str == "" or dia_str is None:
            return -1

        data_dt = pd.to_datetime(dia_str)
        data_dt_mais_um = data_dt + pd.Timedelta(days=1)

        return data_dt_mais_um in feriados_goias

    # Seta o número dessa viagem
    def set_num_viagem(self, num_viagem):
        self.num_viagem = num_viagem

    # Função que verifica se a linha já foi processada
    def viagem_ja_foi_processada(self):
        query = f"""
        SELECT 
            *
        FROM 
            rmtc_viagens_analise_mix
        WHERE 
            dia = '{self.dia}'
            AND vec_asset_id = '{self.vec_asset_id}'
            AND num_viagem = {self.num_viagem}
        """
        df_viagem = self.pgDB.read_sql_safe(query)

        return not df_viagem.empty

    # Retorna tempo da viagem em segundos
    def get_tempo_viagem_segundos(self):
        timestamp_inicio = pd.to_datetime(self.df_gps_sort.iloc[self.start_idx]["Timestamp"])
        timestamp_final = pd.to_datetime(self.df_gps_sort.iloc[self.end_idx]["Timestamp"])
        return (timestamp_final - timestamp_inicio).total_seconds()

    # Computa a duração da viagem
    def computa_duracao_viagem(self):
        # Obtém índices de início e fim do GPS
        self.timestamp_inicio = self.df_gps_sort.iloc[self.start_idx]["Timestamp"]
        self.timestamp_final = self.df_gps_sort.iloc[self.end_idx]["Timestamp"]

        # Calcula a duração da viagem em segundos
        self.tempo_viagem_segundos = (
            pd.to_datetime(self.timestamp_final) - pd.to_datetime(self.timestamp_inicio)
        ).total_seconds()

        # Arredonda os timestamps para 5 minutos antes e depois
        self.dt_start_round_timestamp, self.dt_end_round_timestamp = get_round_timestamps(
            self.timestamp_inicio, self.timestamp_final
        )

        return (
            self.timestamp_inicio,
            self.timestamp_final,
            self.tempo_viagem_segundos,
            self.dt_start_round_timestamp,
            self.dt_end_round_timestamp,
        )

    # Computa o tamanho da linha
    # Recebe como parâmetro o banco de linha (geopandas)
    # Usamos o banco para filtar a linha identificada e projetá-la em outro sistema de coordenada para calcular o tamanho
    def computa_tamanho_linha(self, gdf_linha_raw):
        # Copia e joga para UTM
        gdf_linhas_metros = gdf_linha_raw.copy().to_crs(
            epsg=31982
        )  # Substitua 31982 pela zona UTM correta, se necessário

        # Calcular o comprimento de cada linha em metros
        gdf_linhas_metros["length_m"] = gdf_linhas_metros["geometry"].length

        tam_linha_metros = gdf_linhas_metros[
            (gdf_linhas_metros["sentido"] == self.sentido_linha_overlap)
            & (gdf_linhas_metros["numero_sublinha"] == self.numero_sublinha)
        ]["length_m"].sum()

        self.tam_linha_metros = tam_linha_metros

        # Calcula o tamanho da sobreposição
        tam_linha_km = tam_linha_metros / 1000

        # Ajuda o tamanho da linha para a % de sobreposicao
        tam_linha_km_sobreposicao = tam_linha_km * (self.overlap_final / 100)
        self.tam_linha_km_sobreposicao = tam_linha_km_sobreposicao

        return tam_linha_km, tam_linha_km_sobreposicao

    def computa_combustivel(self):
        # Computa o tempo de viagem
        # Salva na classe os timestamps da viagens que serão usados a seguir e também na hora de salvar
        # Destaca-se que o dt_start_round_timestamp > timestamp_inicio uma vez que
        # - Isso porque dt_start_round_timestamp arredonda o tempo a fração de 5 minutos anterior
        # - Isso é feito porque o sistema retorna o combustível de 5 em 5 minutos, assim, precisamos disso para pegar o
        #   combustível que ocorre no intervalo. Fazemos a interpolação para considerar esse consumo.
        self.computa_duracao_viagem()

        # Filtra os dados de combustível
        df_comb_filtered = self.df_comb_sort[
            (self.df_comb_sort["StartDateTime"] >= self.dt_start_round_timestamp)
            & (self.df_comb_sort["StartDateTime"] <= self.dt_end_round_timestamp)
        ].copy()
        df_comb_filtered["StartDateTime"] = pd.to_datetime(df_comb_filtered["StartDateTime"])

        # Calcula quantidade de combustível utilizada
        # Soma o consumo de 5 em 5 minutos e faz a interpolação para a parte inicial e final
        total_comb = self.__calcula_combustivel(df_comb_filtered, self.timestamp_inicio, self.timestamp_final)

        # Computa km / l (convertendo para km e L)
        tam_linha_km = self.tam_linha_metros / 1000
        self.total_comb_l = total_comb / 1000

        # Caso o modelo do veículo seja VW dividimos o total de combustível por 5614
        if self.vec_model == "VW 17230 APACHE VIP-SC " or self.vec_model == "VW 17230 APACHE VIP-SC":
            self.total_comb_l = total_comb / 5614

        # Computa km / l
        tam_linha_km_sobreposicao = tam_linha_km * (self.overlap_final / 100)

        # Caso total_comb_l seja != 0, computa km por litro
        if self.total_comb_l != 0:
            self.km_por_litro = tam_linha_km_sobreposicao / self.total_comb_l
        else:
            self.km_por_litro = None
            self.total_comb_l = None

        return self.total_comb_l, self.km_por_litro

    # Calcula a quantidade de combustível utilizada
    def __calcula_combustivel(self, df_comb_filtered, timestamp_inicio, timestamp_final):
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

    def __str__(self):
        return f"""
---------
{self.dia} - {self.vec_num_id} - LINHA: {self.numero_sublinha} ({self.overlap_final}%)
INÍCIO: {self.timestamp_inicio} / FIM: {self.timestamp_final} / {self.tempo_viagem_segundos} segundos
CONSUMO: {self.km_por_litro}
---------
"""
