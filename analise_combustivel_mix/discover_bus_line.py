#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer funções para descobrir uma linha de Ônibus com base nos dados da Mix

###################################################################################
# Imports
###################################################################################

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

# Classe com a estrutura da linha
from bus_line_trip import BusLineInMixTrip

###################################################################################
# Constantes e variáveis globais
###################################################################################
# Algumas constantes que auxiliam no algoritmo

# Buffer na posição GPS do veículo
TAMANHO_BUFFER_POSICAO_VEICULO = 300

# % de sobreposição para considerar como rota
THRESHOLD_OVERLAP = 90

# Tempo mínimo da viagem em segundos
TEMPO_MINIMO_VIAGEM_SEGUNDOS = 60 * 10


###################################################################################
# Rotinas globais
###################################################################################


class DiscoverBusLinesAlgorithm(object):
    def __init__(self, pgDB, debug, vec_num_id, vec_asset_id, vec_model, dia):
        self.pgDB = pgDB
        self.debug = debug
        self.vec_num_id = vec_num_id
        self.vec_asset_id = vec_asset_id
        self.vec_model = vec_model
        self.dia = dia

    ###################################################################################
    # Etapa de Processamento
    # --> Funções auxiliares
    ###################################################################################

    # Gera o shape a partir de um conjunto de posicoes
    def gera_shape_posicoes(self, df_posicoes, tam_buffer_positions=TAMANHO_BUFFER_POSICAO_VEICULO):
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
    def calcula_overlap(self, geometria_trajetoria_veiculo_buffer, gdf_linhas_buffer):
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

    # Função auxiliar para obter o sentido da linha suspeita, bem como os buffer das posições extrema dela
    # Filtra as linhas suspeitas e retorna aquelas cuja posição inicial corresponde a posição inicial do GPS do veículo
    def filtra_sentido(
        self,
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
            buf_ponto = self.gera_shape_posicoes(
                df_gps_filtro_ponto, tam_buffer_positions=tamanho_buffer_posicao_veiculo
            )

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
    
    # Retorna tempo da viagem em segundos
    def get_tempo_viagem_segundos(self, df_gps_sort, start_idx, end_idx):
        timestamp_inicio = pd.to_datetime(df_gps_sort.iloc[start_idx]["Timestamp"])
        timestamp_final = pd.to_datetime(df_gps_sort.iloc[end_idx]["Timestamp"])
        return (timestamp_final - timestamp_inicio).total_seconds()


    ###################################################################################
    # Etapa de Processamento
    # --> Descobre linha da viagem
    ###################################################################################
    
    # Encontra uma ou mais linha do veículo nos dados da trip Mix
    def discover_bus_lines(
        self,
        df_gps_sort,
        df_comb_sort,
        gdf_linha_buffer,
        gdf_linhas_extremo_start_buffer,
        gdf_linhas_extremo_end_buffer,
        tamanho_buffer_posicao_veiculo=TAMANHO_BUFFER_POSICAO_VEICULO,
        threshold_overlap=THRESHOLD_OVERLAP,
        tempo_min_viagem=TEMPO_MINIMO_VIAGEM_SEGUNDOS
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

            buf_raw = self.gera_shape_posicoes(
                df_gps_filtro_raw, tam_buffer_positions=tamanho_buffer_posicao_veiculo * 2
            )
            df_overlap_raw = self.calcula_overlap(buf_raw, gdf_linha_buffer)

            # Computa o maior ovelap
            df_maior_overlap_linha = df_overlap_raw.sort_values("overlap_percentage", ascending=False).head(1)

            maior_overlap = df_overlap_raw["overlap_percentage"].max()
            maior_linha = df_maior_overlap_linha["numero"].values[0]
            maior_sublinha = df_maior_overlap_linha["numero_sublinha"].values[0]

            # Tempo em segundos da viagem
            tempo_viagem_seg = self.get_tempo_viagem_segundos(df_gps_sort, init_search_idx, curr_idx)
            tempo_viagem_h = (tempo_viagem_seg / 60) / 60

            # Threshold overlap muda conforme o tamanho da linha
            tam_linha_encontrada = df_maior_overlap_linha["tamanhokm"].values[0]
            # if tam_linha_encontrada <= 5:
            #     threshold_overlap = 97
            # elif tam_linha_encontrada <= 10:
            #     threshold_overlap = 95
            # else:
            #     threshold_overlap = 90
            
            velocidade_veiculo = tam_linha_encontrada / tempo_viagem_h if tempo_viagem_h >  0 else 0

            # TODO: mudar para logging
            if self.debug:
                print(
                    f"VEICULO {self.vec_num_id:<6} {self.dia}",
                    f"CURR IDX {curr_idx:>4} DE {total_posicoes:>4}",
                    f"MAIOR OVERLAP {maior_overlap:6.2f}%",
                    f"SUBLINHA {maior_sublinha:<6}",
                    f"TAM LINHA {tam_linha_encontrada:5.2f}",
                    f"TEMPO (MIN) {(tempo_viagem_seg/60):5.2f}",
                    f"VEL: {velocidade_veiculo:7.2f} km/h"
                )


            # Corrida anomala
            # if curr_idx > 1000 and maior_overlap <= 25 and total_posicoes >= 5000:
            #    break

            if maior_overlap < threshold_overlap or tempo_viagem_seg <= tempo_min_viagem:
                curr_idx += 1
                continue
            else:
                # Encontrou a linha, cria com valores padrão + dados inciais
                resposta = BusLineInMixTrip(
                    self.pgDB,
                    self.debug,
                    self.vec_num_id,
                    self.vec_asset_id,
                    self.vec_model,
                    self.dia,
                    df_gps_sort,
                    df_comb_sort,
                )

                # Preenche demais dados inicias da linha na resposta
                resposta.encontrou_linha = True
                resposta.numero_linha_overlap = maior_linha
                resposta.numero_sublinha = maior_sublinha
                resposta.overlap_inicial = maior_overlap

                # Limpa a linha e acha o ponto inicial (sem repetição)
                # Isso é necessário pois o veículo pode ter ficado parado gerando posições repetidas (ex: no terminal)
                # Usamos o ponto inicial para descobrir a hora que o veículo saiu e para cálculos de combustível
                df_overlap_opt = None
                j = curr_idx
                for j in range(curr_idx, init_search_idx, -1):
                    # Regera as posições, porém de forma contrária (do último ponto até o ponto inicial da busca)
                    # Quando acharmos uma sobreposição, achamos o ponto inicial da linha
                    df_gps_filtro_opt = df_gps_sort.iloc[j : curr_idx + 1]

                    buf_opt = self.gera_shape_posicoes(
                        df_gps_filtro_opt, tam_buffer_positions=tamanho_buffer_posicao_veiculo * 2
                    )
                    df_overlap_opt = self.calcula_overlap(buf_opt, gdf_linha_buffer)

                    if df_overlap_opt["overlap_percentage"].max() >= threshold_overlap:
                        break

                # Atualiza o ponteiro inicial da busca (caso tenha encontrado), senão utiliza o ponteiro inicial da busca
                idx_start_linha = j
                start_idx = idx_start_linha
                resposta.start_idx = start_idx

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
                ) = self.filtra_sentido(
                    df_posicoes_veiculo,
                    gdf_linhas_candidatas,
                    gdf_linhas_candidatas_extremo_start_buffer,
                    gdf_linhas_candidatas_extremo_end_buffer,
                )

                resposta.sentido_linha_overlap = sentido

                # Procura o ponto final, começa pelo ponto atual
                k = curr_idx
                end_idx = curr_idx

                teve_overlap_ponto_final = False
                for k in range(curr_idx, total_posicoes):
                    df_gps_filtro_ponto = df_gps_sort.iloc[[k]]
                    buf_ponto = self.gera_shape_posicoes(
                        df_gps_filtro_ponto, tam_buffer_positions=tamanho_buffer_posicao_veiculo
                    )

                    intersection_start = gdf_linha_detectada_extremo_start_buffer["start_point"].intersection(buf_ponto)
                    intersection_end = gdf_linha_detectada_extremo_end_buffer["end_point"].intersection(buf_ponto)

                    # Verifica se as interseções são vazias e inverte o sinal
                    teve_overlap_ponto_final = not (
                        intersection_start.is_empty.all() and intersection_end.is_empty.all()
                    )

                    # Se acharmos uma sobreposição, achamos o ponto final da linha
                    if teve_overlap_ponto_final:
                        break

                # Atualiza o ponteiro final da busca (caso tenha encontrado), senão utiliza o ponteiro final da busca
                if teve_overlap_ponto_final:
                    end_idx = k
                    resposta.teve_overlap = True
                else:
                    end_idx = curr_idx
                    resposta.teve_overlap = False

                resposta.end_idx = end_idx

                # % de sobreposição final
                buf_final = self.gera_shape_posicoes(
                    df_gps_sort.iloc[start_idx : end_idx + 1], tam_buffer_positions=tamanho_buffer_posicao_veiculo * 2
                )
                df_overlap_final = self.calcula_overlap(buf_final, gdf_linha_detectada_buffer)[
                    "overlap_percentage"
                ].max()

                resposta.overlap_final = df_overlap_final
                resposta.overlap_df = df_gps_sort.iloc[start_idx : end_idx + 1]

                # Computa duração da viagem
                resposta.computa_duracao_viagem()

                # Adiciona a linha encontrada
                linhas_encontradas.append(resposta)

                # Atualiza os ponteiros de busca para a próxima linha
                init_search_idx = end_idx + 1
                curr_idx = end_idx + 1

        # Retorna as linhas encontradas
        return linhas_encontradas
