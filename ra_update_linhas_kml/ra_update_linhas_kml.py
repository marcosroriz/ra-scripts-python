#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

# Bibliotecas padrão
import os
import sys

# HTTP
import requests

# Datas
import datetime as dt

# Pandas
import pandas as pd

# BD
from sqlalchemy import create_engine
from sqlalchemy.sql import text

# Base64 para GeoJSON
import base64
import zipfile
import io

# Geo
import geojson
from geojson_length import calculate_distance, Unit
from fastkml import kml
from shapely.geometry import mapping

# DotEnv
from dotenv import load_dotenv

load_dotenv()


###################################################################################
# Configurações
###################################################################################

# Não bufferiza a saída
sys.stdout.flush()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Conexão com o banco de dados
pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# RA
RA_API_URL = os.getenv("RA_API_URL") + "/linhatrajeto"
RA_API_KEY = os.getenv("RA_API_KEY")
RA_API_KML_URL = os.getenv("RA_API_KML_URL")
RA_API_KML_KEY = os.getenv("RA_API_KML_KEY")

###################################################################################
# Funções
###################################################################################


def obter_listas_linhas():
    url = RA_API_URL
    headers = {"Authorization": RA_API_KEY}

    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json()["data"])


def preprocessa_linhas(df_linhas_raw):
    # Obter o número da linha original
    df_linhas_raw["LINHA_ORIGINAL"] = df_linhas_raw["LINHA"].str.split("-").str[0]

    return df_linhas_raw


def obter_kml_linha(linha):
    url = RA_API_KML_URL
    headers = {"Authorization": RA_API_KML_KEY}
    payload = {"linha": [linha]}

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        print(f"Erro na API para linha {linha}. Status code: {response.status_code}")
        print(f"Resposta: {response.text}")
        return None
    
    data = response.json()
    if not data.get("data"):
        print(f"Resposta vazia da API para linha {linha}")
        print(f"Resposta completa: {data}")
        return None
        
    return data["data"]


def decodifica_kml(kml_linha_raw):
    if kml_linha_raw is None:
        raise ValueError("kml_linha_raw é None - verifique a resposta da API")
        
    try:
        # 1. Decodificar o base64
        kmz_bytes = base64.b64decode(kml_linha_raw)
    except Exception as e:
        print(f"Erro ao decodificar base64: {e}")
        print(f"Valor recebido: {kml_linha_raw[:100] if kml_linha_raw else None}")  # Print first 100 chars
        raise

    kml_content = None

    # 2. Ler como arquivo zip (KMZ é ZIP)
    try:
        with zipfile.ZipFile(io.BytesIO(kmz_bytes)) as zf:
            # Encontrar o primeiro arquivo .kml
            kml_filename = next(name for name in zf.namelist() if name.endswith(".kml"))
            with zf.open(kml_filename) as kml_file:
                kml_content = kml_file.read()
    except Exception as e:
        print(f"Erro ao processar arquivo KMZ: {e}")
        raise

    return kml_content


def kml_para_geojson(kml_content):
    # 1. Converter o KML para GeoJSON
    kml_dom = kml.KML()
    kml_linha = kml_dom.from_string(kml_content)

    # 2. Extrair os objetos e converter para GeoJSON
    features = []

    for doc in kml_linha.features:
        for placemark in doc.features:
            if hasattr(placemark, "geometry"):
                geofeature = geojson.Feature(
                    geometry=mapping(placemark.geometry),
                    properties={"name": placemark.name},
                )
                features.append(geofeature)

    # 3. Converter para GeoJSON
    geojson_output = geojson.FeatureCollection(features)

    return geojson_output


def computa_tamanho_linha_km(kml_linha_geojson):
    # Calcula o comprimento total da linha em km
    km_total = 0
    for feature in kml_linha_geojson["features"]:
        if "geometry" in feature:
            km_total += calculate_distance(feature, Unit.kilometers)

    return km_total


def main(engine_pg):
    # Define intervalo de processamento
    # Todos os valores no formato datetime.datetime
    today = dt.date.today()

    # Baixa as listas de linhas
    df_linhas_raw = obter_listas_linhas()
    if df_linhas_raw.empty:
        print("Nenhuma linha encontrada na API")
        return

    # Preprocessa as linhas
    df_linhas = preprocessa_linhas(df_linhas_raw)

    # Para cada linha
    for linha_onibus in df_linhas["LINHA"].unique():
        try:
            # Obtem os dados da linha (só o primeiro)
            df_linha_especifica = df_linhas[df_linhas["LINHA"] == linha_onibus].iloc[0]

            # Dados básicos
            num_linha_original = df_linha_especifica["LINHA_ORIGINAL"]

            # Obtem o KML da linha
            kml_linha_raw = obter_kml_linha(linha_onibus)
            if kml_linha_raw is None:
                print(f"Pulando linha {linha_onibus} - sem dados KML")
                continue

            # Para cada sentido retornado da linha
            for kml_linha_unica in kml_linha_raw:
                try:
                    # Dados básicos complementares
                    num_linha_unica = kml_linha_unica["linha"]
                    sentido_linha_unica = kml_linha_unica["sentido"]
                    desc_linha_unica = kml_linha_unica["descricao"]
                    vigencia_inicial_linha_unica = kml_linha_unica["vigenciaInicial"]
                    vigencia_final_linha_unica = kml_linha_unica["vigenciaFinal"]

                    print("Processando linha", num_linha_original, "/", num_linha_unica, "/", sentido_linha_unica)

                    # Decodifica o KML
                    kml_linha_unica_arquivo_bytes = kml_linha_unica.get("arquivo")
                    if kml_linha_unica_arquivo_bytes is None:
                        print(f"Arquivo KML não encontrado para linha {num_linha_unica} sentido {sentido_linha_unica}")
                        continue

                    # Decodifica o KML
                    kml_linha_unica_str = decodifica_kml(kml_linha_unica_arquivo_bytes)

                    # Converte o KML para GeoJSON
                    kml_linha_unica_geojson = kml_para_geojson(kml_linha_unica_str)

                    # Calcula o comprimento total da linha em km
                    tamanho_linha_km = computa_tamanho_linha_km(kml_linha_unica_geojson)

                    # Converte o GeoJSON para string
                    kml_linha_unica_geojson_str = geojson.dumps(kml_linha_unica_geojson)

                    # Insere o dado no banco de dados
                    with engine_pg.begin() as conn:
                        data_dict = {
                            "diahorario": today,
                            "dia_vigencia_inicial": vigencia_inicial_linha_unica,
                            "dia_vigencia_final": vigencia_final_linha_unica,
                            "numero": num_linha_original,
                            "numero_sublinha": num_linha_unica,
                            "desc_linha": desc_linha_unica,
                            "sentido": sentido_linha_unica,
                            "tamanhokm": tamanho_linha_km,
                            "geojsondata": kml_linha_unica_geojson_str,
                            "kmldata": kml_linha_unica_str,
                        }

                        # Insere os dados na tabela
                        stmt = text(
                            """
                            INSERT INTO rmtc_kml_via_ra 
                            (diahorario, dia_vigencia_inicial, dia_vigencia_final, numero, numero_sublinha, 
                             desc_linha, sentido, tamanhokm, geojsondata, kmldata)
                            VALUES 
                            (:diahorario, :dia_vigencia_inicial, :dia_vigencia_final, :numero, :numero_sublinha,
                             :desc_linha, :sentido, :tamanhokm, :geojsondata, :kmldata)
                            ON CONFLICT DO NOTHING
                        """
                        )
                        conn.execute(stmt, data_dict)
                except Exception as e:
                    print(f"Erro ao processar sub_linha {num_linha_unica}: {e}")
        except Exception as e:
            print(f"Erro ao processar linha {linha_onibus}: {e}")


###################################################################################
# Execução
###################################################################################

if __name__ == "__main__":
    # Marca o início da execução
    start_time = dt.datetime.now()

    try:
        # Executa o script principal
        main(engine_pg=pg_engine)
    except Exception as e:
        print(f"Erro ao executar o script: {e}")

    # Calcula o tempo total de execução
    end_time = dt.datetime.now()
    minutes = (end_time - start_time).seconds // 60

    print(f"Tempo para executar o script (em minutos): {minutes}")
