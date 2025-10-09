#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Subprocesso disparado pelo main que analisa os dados de um único veículo em um único dia
###################################################################################
# Funcionamento:
# 1 - O subprocesso obtêm os dados das trips e posições GPS do veículo da Mix.
#
# 2 - A partir desses dados, o algoritmo tenta reconstruir as linhas da Trip.
# Note, uma Trip da Mix pode conter várias linhas diferentes, bem como sentidos diferentes, como ida e volta.
#
# 3 - Após a identificação da linha, o sistema calcula o combustível através do evento tst_combs,
# que acumua o combustível gasto a cada 5 minutos.
#
# 4 - Classifica se o combustível gasto encontra-se abaixo da mediana utilizando o método do interquartil (IQR).
# O método IQR aproxima a análise de fatores de desvio padrão.
# Mais informações: https://online.stat.psu.edu/stat200/lesson/3/3.2
#
# 5 - O dado classificado é salvo no banco de dados.
#
# Requer como parâmetro de linha de comando a data que será utilizada:
# --data_baixar="YYYY-MM-DD" (Ex: --data_baixar="2025-02-27")
# --vec_asset_id=ID_VEICULO_MIX (Ex: 1581106874799685632)
#
# Além deste parâmetro, o sistema requer um arquivo .env com as seguintes variáveis de ambiente:
# - DB_HOST: Endereço do banco
# - DB_PORT: Porta do banco
# - DB_USER: Usuário do banco
# - DB_PASS: Senha do usuário
# - DB_NAME: Nome do banco de dados
# - DEBUG: Se deve ou não imprimir na tela informações de DEBUG
###################################################################################

###################################################################################
# Imports
###################################################################################

# Import de sistema
import gc
import os
import sys

# DotEnv
from dotenv import load_dotenv

# Carrega variáveis de ambiente
CURRENT_WORKINGD_DIR = os.getcwd()
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))

load_dotenv()
load_dotenv("../.env")
load_dotenv(os.path.join(CURRENT_WORKINGD_DIR, ".env"))
load_dotenv(os.path.join(CURRENT_PATH, "..", ".env"))

# CLI
import click

# Imports básicos
import json
import pandas as pd

# Import de datas
import datetime as dt
from datetime import datetime, timedelta

# Biblioteca espaciais
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import unary_union
from shapely.geometry import shape, MultiLineString, MultiPolygon

# Banco de Dados
from sqlalchemy import create_engine, text
from db import PostgresSingleton

# Módulos locais
from discover_bus_line import DiscoverBusLinesAlgorithm
from bus_line_trip import BusLineInMixTrip
from bus_line_comb_analyzer import BusLineCombAnalyzer


###################################################################################
# Constantes e variáveis globais
###################################################################################
# Debug
DEBUG = os.getenv("DEBUG", "True").lower() in ("true", "1", "yes")

# Banco de Dados
pgDB = PostgresSingleton.get_instance()
pg_engine = pgDB.get_engine()

# Buffer da linha em metros
TAMANHO_BUFFER_LINHA = 15

# Buffer dos pontos extremos
# Para linhas menores que 10km, o buffer é de 150m
# Para linhas maiores que 10km, o buffer é de 300m
TAMANHO_BUFFER_PONTOS_EXTREMOS_LINHA_PEQUENA = 100
TAMANHO_BUFFER_PONTOS_EXTREMOS_LINHA_GRANDE = 250

# Buffer do veículo
TAMANHO_BUFFER_POSICAO_VEICULO = 125

# Buffer de tempo em minutos
TEMPO_BUFFER = 10

# % de sobreposição para considerar como rota
THRESHOLD_OVERLAP = 90

# Tempo mínimo da viagem em segundos
TEMPO_MINIMO_VIAGEM_SEGUNDOS = 60 * 10


###################################################################################
# Etapa de Leitura de Dados
# --> Funções auxiliares
###################################################################################


# Função que verifica se uma determinada viagem já foi processada
def verifica_viagem_processada(dia, vec_asset_id, num_viagem):
    """
    Função que verifica se uma determinada viagem já foi processada
    """
    query = f"""
    SELECT 
        *
    FROM 
        rmtc_viagens_analise_mix
    WHERE 
        dia = '{dia}'
        AND vec_asset_id = '{vec_asset_id}'
        AND num_viagem = {num_viagem}
    """
    df_viagem = pgDB.read_sql_safe(query)

    return not df_viagem.empty


# Função que retorna as trips MIX de um veículo em um determinado dia
def get_trips_mix_dia(vec_asset_id, dia):
    """
    Função que retorna as trips MIX de um veículo em um determinado dia
    """
    # Query
    query = f"""
        SELECT * 
        FROM trips_api ta 
        WHERE ta."AssetId" = '{vec_asset_id}'
        AND CAST(ta."TripStart" as timestamp)::date = DATE '{dia}'
    """
    df_trips = pgDB.read_sql_safe(query)
    return df_trips


###################################################################################
# Etapa de Leitura de Dados
# --> Funções auxiliares espaciais
###################################################################################


# Função para ler o GEOJSON das linhas
def parse_geojson(geojson_str):
    """
    Função que converte um GeoJSON em uma geometria Shapely
    """
    geojson = json.loads(geojson_str)  # Converte string para dic
    geometries = [shape(feature["geometry"]) for feature in geojson["features"]]  # Extraí as geometrias

    if len(geometries) == 1:
        return geometries[0]  # Retorna a geometria única
    elif all(geom.geom_type == "LineString" for geom in geometries):
        return MultiLineString(geometries)  # Combina em MultiLineString
    elif all(geom.geom_type == "Polygon" for geom in geometries):
        return MultiPolygon(geometries)  # Combina em MultiPolygon
    else:
        return geometries  # Retorna uma lista com os tipos mistos


# Função para obter o KML da linha
def get_linhas_kml(mix_timestamp_inicio):
    """
    Função que retorna o KML da linha (geopanadas)
    """
    # Query
    query_linha = f"""    
    SELECT
        id,
        diahorario,
        numero,
        numero_sublinha,
        desc_linha,
        sentido,
        tamanhokm,
        geojsondata,
        kmldata
    FROM (
        SELECT DISTINCT ON (numero_sublinha, sentido) *
        FROM rmtc_kml_via_ra kml
        ORDER BY numero_sublinha, sentido, ABS(EXTRACT(EPOCH FROM (kml.diahorario::timestamptz - '{mix_timestamp_inicio}'::timestamptz)))
    ) AS via_ra

    UNION ALL

    -- Parte 2: dados do kml (com valores fixos para colunas ausentes)
    SELECT
        id,
        diahorario,
        numero,
        'NAO_DEFINIDO' AS numero_sublinha,
        NULL AS desc_linha,
        sentido,
        tamanhokm,
        geojsondata,
        kmldata
    FROM (
        SELECT DISTINCT ON (numero, sentido) *
        FROM rmtc_kml rk
        WHERE rk.numero NOT IN (
            SELECT DISTINCT numero FROM rmtc_kml_via_ra
        )
        ORDER BY numero, sentido, ABS(EXTRACT(EPOCH FROM (rk.diahorario::timestamptz - '{mix_timestamp_inicio}'::timestamptz)))
    ) AS kml;
    """
    df_linha_raw = pgDB.read_sql_safe(query_linha)

    if df_linha_raw.empty:
        return None

    # Cria geometria
    df_linha_raw["geometry"] = df_linha_raw["geojsondata"].apply(parse_geojson)

    # Cria o GeoDF
    gdf_linha_raw = gpd.GeoDataFrame(df_linha_raw, geometry="geometry", crs="EPSG:4326")  # WGS84 CRS

    return gdf_linha_raw


# Função principal para obter linha com buffer
def get_linha_buffer_gdf(linhagdf, tamanho_buffer_linha=TAMANHO_BUFFER_LINHA):
    gdf_linha_buffer = linhagdf.copy()

    # Converte para 5641
    gdf_linha_buffer = gdf_linha_buffer.to_crs("EPSG:5641")

    # Aplica buffer
    gdf_linha_buffer["geometry"] = gdf_linha_buffer["geometry"].buffer(tamanho_buffer_linha)

    return gdf_linha_buffer


# Função para buffer condicional
def buffer_start_point(row):
    # Se a linha for menor que 10km, o buffer é de 150m
    # Senão, o buffer é de 300m
    if row["tamanhokm"] < 10:
        return row["start_point"].buffer(TAMANHO_BUFFER_PONTOS_EXTREMOS_LINHA_PEQUENA)
    else:
        return row["start_point"].buffer(TAMANHO_BUFFER_PONTOS_EXTREMOS_LINHA_GRANDE)


def buffer_end_point(row):
    # Se a linha for menor que 10km, o buffer é de 150m
    # Senão, o buffer é de 300m
    if row["tamanhokm"] < 10:
        return row["end_point"].buffer(TAMANHO_BUFFER_PONTOS_EXTREMOS_LINHA_PEQUENA)
    else:
        return row["end_point"].buffer(TAMANHO_BUFFER_PONTOS_EXTREMOS_LINHA_GRANDE)


# Função para obter os pontos extremos da linha
def get_pontos_extremos_buffers_gdf(linhagdf):
    # Extraí o primeiro e último ponto de cada linha
    extremes = []
    for _, row in linhagdf.iterrows():
        line = row["geometry"]
        if not line.is_empty:
            start_point = line.coords[0]  # Primeiro Ponto
            end_point = line.coords[-1]  # Último Ponto
            extremes.append(
                {
                    "numero": row["numero"],
                    "numero_sublinha": row["numero_sublinha"],
                    "sentido": row["sentido"],
                    "tamanhokm": row["tamanhokm"],
                    "start_point": Point(start_point),
                    "START_LATITUDE": start_point[1],
                    "START_LONGITUDE": start_point[0],
                    "end_point": Point(end_point),
                    "END_LATITUDE": end_point[1],
                    "END_LONGITUDE": end_point[0],
                }
            )

    gdf_linhas_extremo_start = gpd.GeoDataFrame(extremes, geometry="start_point")
    gdf_linhas_extremo_start.set_crs(epsg=4326, inplace=True)

    gdf_linhas_extremo_end = gpd.GeoDataFrame(extremes, geometry="end_point")
    gdf_linhas_extremo_end.set_crs(epsg=4326, inplace=True)

    # Cria os buffers
    gdf_linhas_extremo_start_buffer = gdf_linhas_extremo_start.copy()
    gdf_linhas_extremo_start_buffer = gdf_linhas_extremo_start_buffer.to_crs("EPSG:5641")
    gdf_linhas_extremo_start_buffer["start_point"] = gdf_linhas_extremo_start_buffer.apply(buffer_start_point, axis=1)

    gdf_linhas_extremo_end_buffer = gdf_linhas_extremo_end.copy()
    gdf_linhas_extremo_end_buffer = gdf_linhas_extremo_end_buffer.to_crs("EPSG:5641")
    gdf_linhas_extremo_end_buffer["end_point"] = gdf_linhas_extremo_end_buffer.apply(buffer_end_point, axis=1)

    return gdf_linhas_extremo_start_buffer, gdf_linhas_extremo_end_buffer


# Função auxiliar para obter o combustível
def get_combustivel(asset_id, data_trip_inicio, data_trip_final, tempo_buffer=TEMPO_BUFFER):
    # Converte datas para datetime
    data_trip_inicio_dt = pd.to_datetime(data_trip_inicio)
    data_trip_final_dt = pd.to_datetime(data_trip_final)

    # Adiciona buffer para o período, como o combustível é lido de 5 em 5 minutos adicionamos 10 minutos para cada lado
    # para uma boa margem de segurança
    data_trip_inicio_buffer_dt = data_trip_inicio_dt - pd.Timedelta(minutes=tempo_buffer)
    data_trip_final_buffer_dt = data_trip_final_dt + pd.Timedelta(minutes=tempo_buffer)

    # Converte para formato ISO
    inicio_str = data_trip_inicio_buffer_dt.isoformat()
    fim_str = data_trip_final_buffer_dt.isoformat()

    query_combustivel = f"""
    SELECT 
        *
    FROM 
        tst_combs tc 
    WHERE 
        tc."AssetId" = {asset_id}
        AND
        CAST(tc."StartDateTime" AS TIMESTAMPTZ) BETWEEN '{inicio_str}'::TIMESTAMPTZ AND '{fim_str}'::TIMESTAMPTZ
    """

    df_comb = pgDB.read_sql_safe(query_combustivel)
    df_comb_sort = df_comb.sort_values(by="StartDateTime")

    return df_comb_sort


# Função auxiliar para obtenção da posição do veículo
def get_posicoes_gps(asset_id, data_trip_inicio, data_trip_final):
    # Converte datas para datetime
    data_trip_inicio_dt = pd.to_datetime(data_trip_inicio)
    data_trip_final_dt = pd.to_datetime(data_trip_final)

    # Adiciona buffer para o período, adicionamos 10 minutos para cada lado para uma boa margem de segurança
    data_trip_inicio_buffer_dt = data_trip_inicio_dt - pd.Timedelta(minutes=10)
    data_trip_final_buffer_dt = data_trip_final_dt + pd.Timedelta(minutes=10)

    # Converte para formato ISO
    inicio_str = data_trip_inicio_buffer_dt.isoformat()
    fim_str = data_trip_final_buffer_dt.isoformat()

    query_gps = f"""
    SELECT 
        *
    FROM 
        posicao_gps pg 
    WHERE 
        pg."AssetId" = '{asset_id}'
        AND
        CAST(pg."Timestamp" AS TIMESTAMPTZ) BETWEEN '{inicio_str}'::TIMESTAMPTZ AND '{fim_str}'::TIMESTAMPTZ
    """

    df_gps = pgDB.read_sql_safe(query_gps)
    df_gps_sort = df_gps.sort_values("Timestamp")

    return df_gps_sort


###################################################################################
# Pós-Processamento
###################################################################################


def get_tamanho_linha(gdf_linha_raw, sentido_linha_overlap, sublinha_overlap):
# Obtém o tamanho da linha
    # Copia e joga para UTM
    gdf_linhas_metros = gdf_linha_raw.copy().to_crs(epsg=31982)  # Substitua 31982 pela zona UTM correta, se necessário

    # Calcular o comprimento de cada linha em metros
    gdf_linhas_metros["length_m"] = gdf_linhas_metros["geometry"].length
    gdf_linhas_metros[gdf_linhas_metros["sentido"] == sentido_linha_overlap]

    tam_linha_metros = gdf_linhas_metros[
        (gdf_linhas_metros["sentido"] == sentido_linha_overlap)
        & (gdf_linhas_metros["numero_sublinha"] == sublinha_overlap)
    ]["length_m"].sum()

    return tam_linha_metros


# Retorna os timestamps arredondados para 5 minutos antes e depois
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


# Retorna tempo da viagem em segundos
def get_tempo_viagem_segundos(df_gps_sort, linha_analise):
    start_idx = linha_analise["start_idx"]
    end_idx = linha_analise["end_idx"]

    return (
        pd.to_datetime(df_gps_sort.iloc[end_idx]["Timestamp"])
        - pd.to_datetime(df_gps_sort.iloc[start_idx]["Timestamp"])
    ).total_seconds()


# Retorna as informações relacionadas a duração da viagem
def get_duracao_viagem(df_gps_sort, linha_analise):
    # Obtém índices de início e fim do GPS
    start_idx = linha_analise["start_idx"]
    end_idx = linha_analise["end_idx"]

    # Timestamps
    timestamp_inicio = df_gps_sort.iloc[start_idx]["Timestamp"]
    timestamp_final = df_gps_sort.iloc[end_idx]["Timestamp"]

    # Calcula a duração da viagem em segundos
    tempo_viagem_segundos = (pd.to_datetime(timestamp_final) - pd.to_datetime(timestamp_inicio)).total_seconds()

    # Arredonda os timestamps para 5 minutos antes e depois
    dt_start_round_timestamp, dt_end_round_timestamp = get_round_timestamps(timestamp_inicio, timestamp_final)

    return timestamp_inicio, timestamp_final, tempo_viagem_segundos, dt_start_round_timestamp, dt_end_round_timestamp


# Calcula a quantidade de combustível utilizada
def calcula_combustivel(df_comb_filtered, timestamp_inicio, timestamp_final):
    # Verifica se o DataFrame está vazio
    # ou
    # só tem um único dado (impossibilita calcular pq fazemos o calculo pela diferença entre dados subsequenetos)
    if df_comb_filtered.empty or len(df_comb_filtered) == 1:
        return 0

    # Primeira parte (buffer até inicial)
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


###################################################################################
# MAIN
####################################################################################


# Pós-processa a linha (chamando as rotinas acimas)
def pos_processa_linha(
    linha, gdf_linha_raw, df_comb_sort, df_gps_sort, dia, vec_num_id, vec_asset_id, vec_model, count_viagem
):
    # Vamos processar
    # Obtém o tamanho da linha
    tam_linha_metros = get_tamanho_linha(gdf_linha_raw, linha["sentido_linha_overlap"], linha["numero_sublinha"])

    # Obtém informações relacionadas a duração da viagem
    (
        timestamp_inicio,
        timestamp_final,
        tempo_viagem_segundos,
        dt_start_round_timestamp,
        dt_end_round_timestamp,
    ) = get_duracao_viagem(df_gps_sort, linha)

    # Filtra os dados de combustível
    df_comb_filtered = df_comb_sort[
        (df_comb_sort["StartDateTime"] >= dt_start_round_timestamp)
        & (df_comb_sort["StartDateTime"] <= dt_end_round_timestamp)
    ].copy()
    df_comb_filtered["StartDateTime"] = pd.to_datetime(df_comb_filtered["StartDateTime"])

    # Calcula quantidade de combustível utilizada
    total_comb = calcula_combustivel(df_comb_filtered, timestamp_inicio, timestamp_final)

    # Computa km / l (convertendo para km e L só por desencargo)
    tam_linha_km = tam_linha_metros / 1000
    total_comb_L = total_comb / 1000

    # Caso o modelo do veículo seja VW dividimos o total de combustível por 5614
    if vec_model == "VW 17230 APACHE VIP-SC " or vec_model == "VW 17230 APACHE VIP-SC":
        total_comb_L = total_comb / 5614

    # Computa km / l
    km_por_litro = None

    # Ajuda o tamanho da linha para a % de sobreposicao
    tam_linha_km_sobreposicao = tam_linha_km * (linha["overlap_final"] / 100)

    # Caso total_comb_L seja != 0, computa km por litro
    if total_comb_L != 0:
        km_por_litro = tam_linha_km_sobreposicao / total_comb_L
    else:
        km_por_litro = None
        total_comb_L = None

    df_linha_processada = pd.DataFrame(
        {
            "dia": dia,
            "vec_num_id": vec_num_id,
            "vec_asset_id": vec_asset_id,
            "vec_model": vec_model,
            "num_viagem": count_viagem,
            "rmtc_linha_prevista": None,
            "rmtc_timestamp_inicio": None,
            "rmtc_timestamp_fim": None,
            "rmtc_destino_curto": None,
            "encontrou_linha": linha["encontrou_linha"],
            "encontrou_numero_linha": linha["numero_linha_overlap"],
            "encontrou_numero_sublinha": linha["numero_sublinha"],
            "encontrou_sentido_linha": linha["sentido_linha_overlap"],
            "encontrou_timestamp_inicio": timestamp_inicio,
            "encontrou_timestamp_fim": timestamp_final,
            "encontrou_tempo_viagem_segundos": tempo_viagem_segundos,
            "overlap_inicial": linha["overlap_inicial"],
            "overlap_final": linha["overlap_final"],
            "teve_overlap_ponto_final": linha["teve_overlap"],
            "tamanho_linha_km": tam_linha_km,
            "tamanho_linha_km_sobreposicao": tam_linha_km_sobreposicao,
            "total_comb_l": total_comb_L,
            "km_por_litro": km_por_litro,
        },
        index=[0],
    )
    return df_linha_processada


# Processa as viagens de um veículo
def processa_viagem(vec_num_id, vec_asset_id, vec_model, df_trips_dia_mix, dia):
    # Prepara os dados espaciais
    # Linhas KML
    gdf_linha_raw = get_linhas_kml(df_trips_dia_mix["TripStart"].min())

    # Buffer das linhas
    gdf_linha_buffer = get_linha_buffer_gdf(gdf_linha_raw, TAMANHO_BUFFER_LINHA)

    # Pontos extremos
    gdf_linhas_extremo_start_buffer, gdf_linhas_extremo_end_buffer = get_pontos_extremos_buffers_gdf(gdf_linha_raw)

    # Conta o número de viagens
    count_viagem = 1

    # Algoritmo de descoberta, pode ser reusado
    # O que muda são os dados relacinados a trips, porém os demais dados (vec_num_id, vec_model, dia, são os mesmos
    discover_algorithm = DiscoverBusLinesAlgorithm(pgDB, DEBUG, vec_num_id, vec_asset_id, vec_model, dia)

    for _, row_trip in df_trips_dia_mix.iterrows():
        try:
            mix_trip_inicio = row_trip["TripStart"]
            mix_trip_fim = row_trip["TripEnd"]

            # Combustível
            df_comb_sort = get_combustivel(vec_asset_id, mix_trip_inicio, mix_trip_fim, TEMPO_BUFFER)

            # Posições do GPS
            df_gps_sort = get_posicoes_gps(vec_asset_id, mix_trip_inicio, mix_trip_fim)

            # Descobre as linhas
            linhas_onibus = discover_algorithm.discover_bus_lines(
                df_gps_sort,
                df_comb_sort,
                gdf_linha_buffer.copy(),
                gdf_linhas_extremo_start_buffer.copy(),
                gdf_linhas_extremo_end_buffer.copy(),
                TAMANHO_BUFFER_POSICAO_VEICULO,
                THRESHOLD_OVERLAP,
                TEMPO_MINIMO_VIAGEM_SEGUNDOS
            )

            # Verifica se encontrou alguma linha nas trips
            if len(linhas_onibus) == 0:
                continue
            else:
                # Encontrou alguma linha nas trips
                for linha_analise in linhas_onibus:
                    # Seta contador da viagem no dia de hoje
                    linha_analise.set_num_viagem(count_viagem)

                    # Verifica se a linha já foi processada
                    viagem_ja_foi_processada = linha_analise.viagem_ja_foi_processada()

                    # Verifica a duração da viagem
                    tempo_viagem_segundos = linha_analise.get_tempo_viagem_segundos()

                    if viagem_ja_foi_processada or (tempo_viagem_segundos <= TEMPO_MINIMO_VIAGEM_SEGUNDOS):
                        count_viagem += 1
                        continue
                    else:
                        # Caso contrário, vamos processar
                        # Calcula tamanho da linha
                        linha_analise.computa_tamanho_linha(gdf_linha_raw)

                        # Calcula o combustível gasto nesta linha
                        linha_analise.computa_combustivel()

                        # Agora, compara os valores de combustível
                        # Separamos em outra classe para facilitar a compreensão do código
                        analise_combustivel = BusLineCombAnalyzer(pgDB, DEBUG, linha_analise, count_viagem)

                        # Classifica o combustível calculado
                        analise_combustivel.classifica_combustivel_gasto()

                        # Salva
                        # Antes adiciona o trip id e driver id para facilitar análises
                        trip_id = int(row_trip["TripId"])
                        driver_id = int(row_trip["DriverId"])
                        analise_combustivel.salvar("rmtc_viagens_analise_mix", trip_id, driver_id)

                        count_viagem += 1

        except Exception as e:
            print(f"Erro: {e}")

        # Libera memória
        gc.collect()


def processa_veiculo(row, dia):
    vec_num_id = row["Description"]
    vec_asset_id = row["AssetId"]
    vec_model = row["Model"]

    try:
        df_trips_dia_mix = get_trips_mix_dia(vec_asset_id, dia)

        # Se possuí viagens nesse dia
        if len(df_trips_dia_mix) > 0:
            processa_viagem(vec_num_id, vec_asset_id, vec_model, df_trips_dia_mix, dia)

    except Exception as e:
        print(f"Erro ao processar veículo: {vec_num_id} - {vec_asset_id} - {vec_model}")
        print(f"Erro: {e}")

    # Libera memória
    gc.collect()


@click.command()
@click.option("--data_baixar", type=str, help="Data para baixar")
@click.option("--vec_asset_id", type=int, help="ID do veículo")
def main(data_baixar, vec_asset_id):
    df_veiculos = pd.DataFrame()

    # Debug variables
    # data_baixar = "2025-09-12"
    #vec_asset_id = 1344837246205296640 # 5017
    #vec_asset_id = 1344825662024634368 #50480
    #vec_asset_id = 1344824845351702528 #50485
    # vec_asset_id = 1589151677707223040 #50057
    # Obtém o veículo a ser processado
    df_veiculos = pgDB.read_sql_safe(
        f"""
        SELECT * FROM veiculos_api WHERE "AssetId" = {vec_asset_id}
        """,
    )

    # Obtem a data a ser processada, assume hoje caso não seja informado
    dia_dt = dt.datetime.now()

    if data_baixar:
        dia_dt = pd.to_datetime(data_baixar)

    dia_str = dia_dt.strftime("%Y-%m-%d")

    # Dado do veículo
    row_veiculo = df_veiculos.iloc[0]
    processa_veiculo(row_veiculo, dia_str)


if __name__ == "__main__":
    main()
