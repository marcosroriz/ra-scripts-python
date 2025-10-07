#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

# Import de sistema
import gc
import os
import sys

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

# Multiprocessamento
from multiprocessing import Pool, cpu_count
from multiprocessing import get_context

# DotEnv
from dotenv import load_dotenv

load_dotenv()

###################################################################################
# Constantes e variáveis globais
###################################################################################
# Dados da conexão
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")


# Engine para pandas (SQLAlchemy)
def get_pg_engine():
    pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    return pg_engine

pg_engine = get_pg_engine()

# Rotina para ler dados do banco de dados de forma segura
def read_sql_safe(query):
    global pg_engine
    df = pd.DataFrame()
    with pg_engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return df


# Buffer da linha em metros
TAMANHO_BUFFER_LINHA = 15

# Buffer dos pontos extremos
# Para linhas menores que 10km, o buffer é de 150m
# Para linhas maiores que 10km, o buffer é de 300m
TAMANHO_BUFFER_PONTOS_EXTREMOS_LINHA_PEQUENA = 150
TAMANHO_BUFFER_PONTOS_EXTREMOS_LINHA_GRANDE = 300

# Buffer do veículo
TAMANHO_BUFFER_POSICAO_VEICULO = 200

# Buffer de tempo em minutos
TEMPO_BUFFER = 10

# % de sobreposição para considerar como rota
THRESHOLD_OVERLAP = 90

# Tempo mínimo da viagem em segundos
TEMPO_MINIMO_VIAGEM_SEGUNDOS = 60 * 5

# Variáveis globais espaciais (kml, buffer e pontos extremos das linhas)
# GDF_LINHA_RAW = None
# GDF_LINHA_BUFFER = None
# GDF_LINHAS_EXTREMO_START_BUFFER = None
# GDF_LINHAS_EXTREMO_END_BUFFER = None

###################################################################################
# Etapa de Leitura de Dados
# --> Funções auxiliares
###################################################################################


# Função que retorna a última viagem contabilizada para um veículo em um determinado dia
def get_ultima_viagem_contabilizada(vec_asset_id, dia):
    """
    Função que retorna a última viagem contabilizada para um veículo em um determinado dia
    """
    query = f"""
    SELECT 
        *
    FROM
        rmtc_viagens_analise
    WHERE
        dia = '{dia}'
        AND vec_asset_id = '{vec_asset_id}'
    ORDER BY
        num_viagem DESC
    LIMIT 1
    """
    df_viagem = read_sql_safe(query)
    return df_viagem["num_viagem"].values[0] if not df_viagem.empty else 0


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
    df_viagem = read_sql_safe(query)

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
    df_trips = read_sql_safe(query)
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
    df_linha_raw = read_sql_safe(query_linha)

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

    df_comb = read_sql_safe(query_combustivel)
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

    df_gps = read_sql_safe(query_gps)
    df_gps_sort = df_gps.sort_values("Timestamp")

    return df_gps_sort


# Função auxiliar para obter o sentido da linha suspeita, bem como os buffer das posições extrema dela
# Filtra as linhas suspeitas e retorna aquelas cuja posição inicial corresponde a posição inicial do GPS do veículo
def filtra_sentido(
    df_gps_sort,
    gdf_linha_buffer,
    gdf_linhas_extremo_start_buffer,
    gdf_linhas_extremo_end_buffer,
    tamanho_buffer_posicao_veiculo=TAMANHO_BUFFER_POSICAO_VEICULO,
):
    gdf_linha_buffer_filtrado = gdf_linha_buffer.copy()
    gdf_linha_extremo_start_buffer_filtrado = gdf_linhas_extremo_start_buffer.copy()
    gdf_linha_extremo_end_buffer_filtrado = gdf_linhas_extremo_end_buffer.copy()

    total_posicoes = len(df_gps_sort)

    for i in range(0, total_posicoes):
        df_gps_filtro_ponto = df_gps_sort.iloc[[i]]
        buf_ponto = gera_shape_posicoes(df_gps_filtro_ponto, tam_buffer_positions=tamanho_buffer_posicao_veiculo)

        # Vamos fazer a intersecção na origem da linha
        intersection_start_lines = gdf_linhas_extremo_start_buffer["start_point"].intersection(buf_ponto)

        # Verifica se as interseções são vazias e inverte o sinal para obter quando pelo menso uma teve intersecção
        teve_overlap = not (intersection_start_lines.is_empty.all())

        if teve_overlap:
            gdf_linha_buffer_filtrado = gdf_linha_buffer[~intersection_start_lines.is_empty].copy()
            gdf_linha_extremo_start_buffer_filtrado = gdf_linhas_extremo_start_buffer[
                ~intersection_start_lines.is_empty
            ].copy()
            gdf_linha_extremo_end_buffer_filtrado = gdf_linhas_extremo_end_buffer[
                ~intersection_start_lines.is_empty
            ].copy()
            break

    sentido = gdf_linha_buffer_filtrado["sentido"].values[0]
    return [
        sentido,
        gdf_linha_buffer_filtrado,
        gdf_linha_extremo_start_buffer_filtrado,
        gdf_linha_extremo_end_buffer_filtrado,
    ]


###################################################################################
# Etapa de Processamento
# --> Funções auxiliares
###################################################################################


# Gera o shape a partir de um conjunto de posicoes
def gera_shape_posicoes(df_posicoes, tam_buffer_positions=TAMANHO_BUFFER_POSICAO_VEICULO):
    geodf_positions = gpd.GeoDataFrame(
        df_posicoes, geometry=gpd.points_from_xy(df_posicoes["Longitude"], df_posicoes["Latitude"])
    )

    # Reseta as coordenadas para facilitar o processamento
    geodf_positions.set_crs(epsg=4326, inplace=True)
    geodf_positions_buffer = geodf_positions.to_crs("EPSG:5641").copy()

    # Gera o buffer
    geodf_positions_buffer["geometry"] = geodf_positions_buffer.geometry.buffer(tam_buffer_positions)
    geodf_positions_buffer.head()

    # Gera a geometria da união do buffer
    geometria_trajetoria_veiculo_buffer = unary_union(geodf_positions_buffer.geometry)

    return geometria_trajetoria_veiculo_buffer


# Calcula a sobreposição de uma trajetória com as linhas em análise
def calcula_overlap(geometria_trajetoria_veiculo_buffer, gdf_linhas_buffer):
    # Lista para armazenar as porcentagens de sobreposição
    overlap_percentages = []

    gdf_linhas_buffer_veiculo = gdf_linhas_buffer.copy()

    # Loop explícito sobre as geometrias
    for geom in gdf_linhas_buffer_veiculo["geometry"]:
        try:
            # Calcula a interseção entre a geometria da linha e o buffer
            intersection = geom.intersection(geometria_trajetoria_veiculo_buffer)

            # Verifica se a interseção é válida e calcula a porcentagem
            if not intersection.is_empty:
                percentage = (intersection.area / geom.area) * 100
            else:
                percentage = 0  # Sem interseção

        except Exception as e:
            # Lidar com erros possíveis
            percentage = 0

        # Armazena a porcentagem calculada
        overlap_percentages.append(percentage)

    # Adicionar a nova coluna com as porcentagens ao GeoDataFrame
    gdf_linhas_buffer_veiculo["overlap_percentage"] = overlap_percentages
    return gdf_linhas_buffer_veiculo


###################################################################################
# Etapa de Processamento
# --> Descobre linha da viagem
###################################################################################
def get_esqueleto_resposta():
    return {
        "encontrou_linha": False,
        "numero_sublinha": None,
        "numero_linha_overlap": None,
        "sentido_linha_overlap": None,
        "search_idx": None,
        "start_idx": None,
        "curr_idx": None,
        "end_idx": None,
        "overlap_inicial": None,
        "overlap_final": None,
        "teve_overlap": False,
        "overlap_df": None,
    }


def discover_bus_line(
    vec_num_id,
    vec_asset_id,
    dia,
    df_gps_sort,
    gdf_linha_buffer,
    gdf_linhas_extremo_start_buffer,
    gdf_linhas_extremo_end_buffer,
    tamanho_buffer_posicao_veiculo=TAMANHO_BUFFER_POSICAO_VEICULO,
    threshold_overlap=THRESHOLD_OVERLAP,
):
    # Linhas encontradas
    linhas_encontradas = []

    # Usamos alguns ponteiros diferentes
    # Note que as posições vão desde a posição inicial (search_idx) até a posição atual (curr_idx)
    # search_idx: ponteiro para a posição inicial da busca
    # curr_idx: ponteiro para a posição atual da busca
    init_search_idx = 0
    curr_idx = 0

    # Total de posições
    total_posicoes = len(df_gps_sort)

    while curr_idx < total_posicoes:
        # Filtra as posições para a busca, vamos gerar o shape e ver o ovelap
        df_gps_filtro_raw = df_gps_sort.iloc[init_search_idx : curr_idx + 1]

        buf_raw = gera_shape_posicoes(df_gps_filtro_raw, tam_buffer_positions=tamanho_buffer_posicao_veiculo * 2)
        df_overlap_raw = calcula_overlap(buf_raw, gdf_linha_buffer)

        # Computa o maior ovelap
        df_maior_overlap_linha = df_overlap_raw.sort_values("overlap_percentage", ascending=False).head(1)

        maior_overlap = df_overlap_raw["overlap_percentage"].max()
        maior_linha = df_maior_overlap_linha["numero"].values[0]
        maior_sublinha = df_maior_overlap_linha["numero_sublinha"].values[0]

        print(
            "VEICULO",
            vec_num_id,
            vec_asset_id,
            dia,
            "CURR IDX",
            curr_idx,
            "DE",
            total_posicoes,
            "MAIOR OVERLAP",
            maior_overlap,
            "SUBLINHA",
            maior_sublinha,
        )

        # Corrida anomala
        # if curr_idx > 1000 and maior_overlap <= 25 and total_posicoes >= 5000:
        #    break

        if maior_overlap < threshold_overlap:
            curr_idx += 1
            continue
        else:
            # Encontrou a linha
            resposta = get_esqueleto_resposta()

            # Preenche dados inicias da linha na resposta
            resposta["encontrou_linha"] = True
            resposta["numero_linha_overlap"] = maior_linha
            resposta["numero_sublinha"] = maior_sublinha
            resposta["overlap_inicial"] = maior_overlap
            resposta["curr_idx"] = curr_idx

            # Limpa a linha e acha o ponto inicial (sem repetição)
            # Isso é necessário pois o veículo pode ter ficado parado gerando posições repetidas (ex: no terminal)
            # Usamos o ponto inicial para descobrir a hora que o veículo saiu e para cálculos de combustível
            df_overlap_opt = None
            j = curr_idx
            for j in range(curr_idx, init_search_idx, -1):
                # Regera as posições, porém de forma contrária (do último ponto até o ponto inicial da busca)
                # Quando acharmos uma sobreposição, achamos o ponto inicial da linha
                df_gps_filtro_opt = df_gps_sort.iloc[j : curr_idx + 1]

                buf_opt = gera_shape_posicoes(df_gps_filtro_opt, tam_buffer_positions=tamanho_buffer_posicao_veiculo)
                df_overlap_opt = calcula_overlap(buf_opt, gdf_linha_buffer)

                if df_overlap_opt["overlap_percentage"].max() >= threshold_overlap:
                    break

            # Atualiza o ponteiro inicial da busca (caso tenha encontrado), senão utiliza o ponteiro inicial da busca
            idx_start_linha = j
            start_idx = idx_start_linha
            resposta["start_idx"] = start_idx

            # Agora que temos o começo, podemos delimitar o sentido da linha
            gdf_linhas_candidatas = gdf_linha_buffer[gdf_linha_buffer["numero_sublinha"] == maior_sublinha]
            gdf_linhas_candidatas_extremo_start_buffer = gdf_linhas_extremo_start_buffer[
                gdf_linhas_extremo_start_buffer["numero_sublinha"] == maior_sublinha
            ]
            gdf_linhas_candidatas_extremo_end_buffer = gdf_linhas_extremo_end_buffer[
                gdf_linhas_extremo_end_buffer["numero_sublinha"] == maior_sublinha
            ]
            df_posicoes_veiculo = df_gps_sort.iloc[start_idx : curr_idx + 1]

            (
                sentido,
                gdf_linha_detectada_buffer,
                gdf_linha_detectada_extremo_start_buffer,
                gdf_linha_detectada_extremo_end_buffer,
            ) = filtra_sentido(
                df_posicoes_veiculo,
                gdf_linhas_candidatas,
                gdf_linhas_candidatas_extremo_start_buffer,
                gdf_linhas_candidatas_extremo_end_buffer,
            )

            resposta["sentido_linha_overlap"] = sentido

            # Procura o ponto final, começa pelo ponto atual
            k = curr_idx
            end_idx = curr_idx

            teve_overlap_ponto_final = False
            for k in range(curr_idx, total_posicoes):
                df_gps_filtro_ponto = df_gps_sort.iloc[[k]]
                buf_ponto = gera_shape_posicoes(
                    df_gps_filtro_ponto, tam_buffer_positions=tamanho_buffer_posicao_veiculo
                )

                intersection_start = gdf_linha_detectada_extremo_start_buffer["start_point"].intersection(buf_ponto)
                intersection_end = gdf_linha_detectada_extremo_end_buffer["end_point"].intersection(buf_ponto)

                # Verifica se as interseções são vazias e inverte o sinal
                teve_overlap_ponto_final = not (intersection_start.is_empty.all() and intersection_end.is_empty.all())

                # Se acharmos uma sobreposição, achamos o ponto final da linha
                if teve_overlap_ponto_final:
                    break

            # Atualiza o ponteiro final da busca (caso tenha encontrado), senão utiliza o ponteiro final da busca
            if teve_overlap_ponto_final:
                end_idx = k
                resposta["teve_overlap"] = True
            else:
                end_idx = curr_idx
                resposta["teve_overlap"] = False

            resposta["end_idx"] = end_idx

            # % de sobreposição final
            buf_final = gera_shape_posicoes(
                df_gps_sort.iloc[start_idx : end_idx + 1], tam_buffer_positions=tamanho_buffer_posicao_veiculo * 2
            )
            df_overlap_final = calcula_overlap(buf_final, gdf_linha_detectada_buffer)["overlap_percentage"].max()

            resposta["overlap_final"] = df_overlap_final
            resposta["overlap_df"] = df_gps_sort.iloc[start_idx : end_idx + 1]

            # Adiciona a linha encontrada
            linhas_encontradas.append(resposta)

            # Atualiza os ponteiros de busca para a próxima linha
            init_search_idx = end_idx + 1
            curr_idx = end_idx + 1

    # Retorna as linhas encontradas
    return linhas_encontradas


###################################################################################
# Pós-Processamento
###################################################################################


# Verifica se a linha já foi processada
def verifica_linha_processada(vec_asset_id, linha_analise, df_gps_sort):
    # Vamos verificar se já há na tabela alguma linha com o mesmo vec_asset_id cujo
    # início está entre 10 minutos antes e depois do início da linha analisada

    # Obtém índices de início do GPS
    start_idx = linha_analise["start_idx"]
    timestamp_inicio = df_gps_sort.iloc[start_idx]["Timestamp"]

    # Converte para datetime
    timestamp_inicio = pd.to_datetime(timestamp_inicio)
    timestamp_inicio_buffer_antes = timestamp_inicio - timedelta(minutes=10)
    timestamp_inicio_buffer_depois = timestamp_inicio + timedelta(minutes=10)

    # Gera o timestamp em str
    timestamp_inicio_buffer_antes_str = timestamp_inicio_buffer_antes.strftime("%Y-%m-%dT%H:%M:%SZ")
    timestamp_inicio_buffer_depois_str = timestamp_inicio_buffer_depois.strftime("%Y-%m-%dT%H:%M:%SZ")

    query = f"""
    SELECT *
    FROM rmtc_viagens_analise
    WHERE
        vec_asset_id = {vec_asset_id}
        AND
        (
            (
                encontrou_timestamp_inicio IS NOT NULL
                AND encontrou_timestamp_inicio <> ''
                AND (
                    encontrou_timestamp_inicio::timestamptz
                    BETWEEN '{timestamp_inicio_buffer_antes_str}'::timestamptz AND '{timestamp_inicio_buffer_depois_str}'::timestamptz
                )
            )
            OR 
            (
                rmtc_timestamp_inicio IS NOT NULL
                AND rmtc_timestamp_inicio <> ''
                AND (
                    rmtc_timestamp_inicio::timestamptz
                    BETWEEN '{timestamp_inicio_buffer_antes_str}'::timestamptz AND '{timestamp_inicio_buffer_depois_str}'::timestamptz
                )
            )
	 )
    """
    # Executa a query
    df_linha_ja_processada = read_sql_safe(query)

    return not df_linha_ja_processada.empty


# Obtém o tamanho da linha
def get_tamanho_linha(gdf_linha_raw, sentido_linha_overlap, sublinha_overlap):
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

    # Ultima viagem contabilizada pra este veículo neste dia
    # Obtem a última viagem contabilizada para este veiculo neste dia
    count_ult_viagem = get_ultima_viagem_contabilizada(vec_asset_id, dia)
    count_viagem = count_ult_viagem + 1

    for _, row_trip in df_trips_dia_mix.iterrows():
        try:
            mix_trip_inicio = row_trip["TripStart"]
            mix_trip_fim = row_trip["TripEnd"]

            # Combustível
            df_comb_sort = get_combustivel(vec_asset_id, mix_trip_inicio, mix_trip_fim, TEMPO_BUFFER)

            # Posições do GPS
            df_gps_sort = get_posicoes_gps(vec_asset_id, mix_trip_inicio, mix_trip_fim)

            # Descobre as linhas
            linhas_onibus = discover_bus_line(
                vec_num_id,
                vec_asset_id,
                dia,
                df_gps_sort,
                gdf_linha_buffer.copy(),
                gdf_linhas_extremo_start_buffer.copy(),
                gdf_linhas_extremo_end_buffer.copy(),
                TAMANHO_BUFFER_POSICAO_VEICULO,
                THRESHOLD_OVERLAP,
            )

            # Verifica se encontrou alguma linha nas trips
            if len(linhas_onibus) == 0:
                continue
            else:
                # Encontrou alguma linha nas trips
                for linha_analise in linhas_onibus:
                    # Verifica se a linha já foi processada
                    linha_ja_processada = verifica_linha_processada(vec_asset_id, linha_analise, df_gps_sort)

                    # Verifica a duração da viagem
                    tempo_viagem_segundos = get_tempo_viagem_segundos(df_gps_sort, linha_analise)

                    if linha_ja_processada or tempo_viagem_segundos <= TEMPO_MINIMO_VIAGEM_SEGUNDOS:
                        continue
                    else:
                        # Caso contrário, vamos processar
                        df_linha_processada = pos_processa_linha(
                            linha_analise,
                            gdf_linha_raw,
                            df_comb_sort,
                            df_gps_sort,
                            dia,
                            vec_num_id,
                            vec_asset_id,
                            vec_model,
                            count_viagem,
                        )

                        # Obtem a engine
                        pg_engine = get_pg_engine()
                        with pg_engine.begin() as conn:
                            df_linha_processada.to_sql(
                                "rmtc_viagens_analise_mix", conn, if_exists="append", index=False
                            )

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
    global pg_engine
    df_veiculos = pd.DataFrame()

    # Obtém o veículo a ser processado
    df_veiculos = pd.read_sql(
        f"""
        SELECT * FROM veiculos_api WHERE "AssetId" = {vec_asset_id}
        """,
        pg_engine,
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
