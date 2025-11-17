#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

# Básicos
import json
import os
import sys
import time

# DotEnv
from dotenv import load_dotenv

# Carrega variáveis de ambiente
CURRENT_WORKINGD_DIR = os.getcwd()
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))

load_dotenv()
load_dotenv("../.env")
load_dotenv(os.path.join(CURRENT_WORKINGD_DIR, ".env"))
load_dotenv(os.path.join(CURRENT_PATH, "..", ".env"))


# IO
from io import StringIO

# Requests
import requests

# Texto
import re
import unidecode

# Datas
import datetime as dt

# Pandas e Numpy
import pandas as pd
import numpy as np

pd.set_option("future.no_silent_downcasting", True)

# BD
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text

# Acesso ao BD
from db_data_fetcher import DBDataFetcher

# OpenAI
from openai import OpenAIChatGPTClient

# Prompts
from prompts import get_system_instructions, get_user_prompt


# Import Wpp e Email
from crud_wpp import CRUDWppService
from crud_email import CRUDEmailService

###################################################################################
# Configurações
###################################################################################

# Não bufferiza a saída
sys.stdout.flush()

# Configurações do banco de dados
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

OPEN_AI_KEY = os.getenv("OPENAI_API_KEY")
OPEN_AI_MODEL = os.getenv("OPENAI_API_MODEL")
OPEN_AI_URL = os.getenv("OPENAI_API_URL")

# Conexão com o banco de dados
pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True)
db_fetcher = DBDataFetcher(pg_engine)

# Cliente OpenAI
openai_client = OpenAIChatGPTClient(key=OPEN_AI_KEY, model=OPEN_AI_MODEL, url=OPEN_AI_URL)

# Variáveis do Dashboard
DASHBOARD_URL = os.getenv("DASHBOARD_URL")

# Variáveis da API da Rápido Araguaia
RA_API_URL = os.getenv("RA_API_URL") + "/dadosfrotaosceia"
RA_API_KEY = os.getenv("RA_API_KEY")

# Variáveis do WhatsApp
WP_ZAPI_URL = os.getenv("WP_ZAPI_URL")
WP_ZAPI_SEND_TEXT_URL = f"{WP_ZAPI_URL}/send-text"
WP_ZAPI_SEND_LINK_URL = f"{WP_ZAPI_URL}/send-link"
WP_ZAPI_TOKEN = os.getenv("WP_ZAPI_TOKEN")
WP_ZAPI_LINK_IMAGE_URL = os.getenv("WP_ZAPI_LINK_IMAGE_URL")

# Variáveis do E-mail
SMTP_KEY = os.getenv("SMTP")

# Cria os serviços
wpp_service = CRUDWppService(
    wp_zapi_url=WP_ZAPI_URL,
    wp_zapi_send_text_url=WP_ZAPI_SEND_TEXT_URL,
    wp_zapi_send_link_url=WP_ZAPI_SEND_LINK_URL,
    wp_zapi_token=WP_ZAPI_TOKEN,
    wp_zapi_link_image_url=WP_ZAPI_LINK_IMAGE_URL,
    dashboard_url=DASHBOARD_URL,
)

email_service = CRUDEmailService(smtp_key=SMTP_KEY, dashboard_url=DASHBOARD_URL)

###################################################################################
# Funções de Apoio
###################################################################################


def convert_df_para_csv(df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    return csv_buffer.getvalue()


def buscar_dados_banco(periodo, min_dias_retrabalho, lista_modelos, lista_oficinas, lista_secoes, lista_os):
    df_colaboradores = db_fetcher.buscar_dados_colaboradores(
        periodo,
        min_dias_retrabalho,
        lista_modelos,
        lista_oficinas,
        lista_secoes,
        lista_os,
    )
    str_csv_colaboradores = convert_df_para_csv(df_colaboradores)

    df_oficinas = db_fetcher.buscar_dados_oficinas(
        periodo,
        min_dias_retrabalho,
        lista_modelos,
        lista_oficinas,
        lista_secoes,
        lista_os,
    )
    str_csv_oficinas = convert_df_para_csv(df_oficinas)

    df_veiculos = db_fetcher.buscar_dados_veiculos(
        periodo,
        min_dias_retrabalho,
        lista_modelos,
        lista_oficinas,
        lista_secoes,
        lista_os,
    )
    str_csv_veiculos = convert_df_para_csv(df_veiculos)

    return str_csv_colaboradores, str_csv_oficinas, str_csv_veiculos


###################################################################################
# Função para salvar o relatório no banco de dados
###################################################################################


def salvar_relatorio(id_regra, relatorio_md, pg_engine):
    table = sqlalchemy.Table("relatorio_regra_relatorio_llm_os", sqlalchemy.MetaData(), autoload_with=pg_engine)
    dia = dt.date.today().strftime("%Y-%m-%d")

    with pg_engine.begin() as conn:
        try:
            stmt = (
                insert(table)
                .values([{"id_regra": id_regra, "dia": dia, "relatorio_md": relatorio_md}])
                .on_conflict_do_nothing()
            )
            conn.execute(stmt)
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False


###################################################################################
## Função para enviar E-mail
###################################################################################


# Função para validar o input de email de destino
def verifica_erro_email(email_destino):
    if not email_destino:
        return False

    email_limpo = email_destino.strip()

    if not re.match(r"^[\w\.-]+@[\w\.-]+\.\w{2,}$", email_limpo):
        return True

    return False


def envia_email(email_service, regra_dict, data_hoje_str):
    # Primeiro obtém os emails válidos
    email_destinos = [
        regra_dict["target_email_dest1"],
        regra_dict["target_email_dest2"],
        regra_dict["target_email_dest3"],
        regra_dict["target_email_dest4"],
        regra_dict["target_email_dest5"],
    ]
    email_destinos_validos = []
    email_destinos_validos = [
        email for email in email_destinos if email != "" and email != None and not verifica_erro_email(email)
    ]

    # Envia o e-mail para cada email válido
    for email_valido in email_destinos_validos:
        email_service.send_msg(regra_dict, data_hoje_str, email_valido)


###################################################################################
## Função para enviar WhatsApp
###################################################################################


# Função para validar o input de telefone
def verifica_erro_wpp(wpp_telefone):
    # Se estive vazio, não considere erro
    if not wpp_telefone:
        return False

    wpp_limpo = wpp_telefone.replace(" ", "")

    padroes_validos = [
        r"^\(\d{2}\)\d{5}-\d{4}$",  # (62)99999-9999
        r"^\(\d{2}\)\d{4}-\d{4}$",  # (62)9999-9999
        r"^\d{2}\d{5}-\d{4}$",  # 6299999-9999
        r"^\d{2}\d{4}-\d{4}$",  # 629999-9999
        r"^\d{10}$",  # 6299999999 (fixo)
        r"^\d{11}$",  # 62999999999 (celular)
    ]

    if not any(re.match(padrao, wpp_limpo) for padrao in padroes_validos):
        return True

    return False


def envia_wpp(wpp_service, regra_dict, data_hoje_str):
    # Primeiro obtém os telefones válidos
    wpp_destinos = [
        regra_dict["target_wpp_dest1"],
        regra_dict["target_wpp_dest2"],
        regra_dict["target_wpp_dest3"],
        regra_dict["target_wpp_dest4"],
        regra_dict["target_wpp_dest5"],
    ]
    wpp_destinos_validos = []
    wpp_destinos_validos = [wpp for wpp in wpp_destinos if wpp != "" and wpp != None and not verifica_erro_wpp(wpp)]

    # Envia o WhatsApp para cada telefone válido
    for wpp_tel in wpp_destinos_validos:
        wpp_service.send_msg(regra_dict, data_hoje_str, wpp_tel)


###################################################################################
# Main
####################################################################################

if __name__ == "__main__":
    print("Início do processamento")

    # Data de hoje (threshold)
    data_hoje = pd.to_datetime("now")
    data_hoje_str = data_hoje.strftime("%Y-%m-%d")
    hoje = dt.date.today().weekday()  # retorna 0=segunda, 6=domingo
    hora_atual = data_hoje.strftime("%H:%M")

    # Pega todas as regras que ainda não possuem relatório para hoje
    query = f"""
        SELECT * 
        FROM regra_relatorio_llm_os
        WHERE 
            id NOT IN (SELECT id_regra FROM relatorio_regra_relatorio_llm_os WHERE dia='{data_hoje_str}')
            AND dia_semana = {hoje}
            AND hora_disparar <= '{hora_atual}'
    """
    df_regras = pd.read_sql(query, pg_engine)
    # df_regras = pd.read_sql("SELECT * FROM regra_relatorio_llm_os", pg_engine)

    # Para cada regra
    for _, regra_dict in df_regras.iterrows():
        print(f"Processando a regra: {regra_dict['id']} - {regra_dict['nome']}")

        nome = regra_dict["nome"]
        id_regra = regra_dict["id"]
        periodo = regra_dict["periodo"]
        min_dias_retrabalho = regra_dict["min_dias_retrabalho"]
        lista_modelos = regra_dict["modelos_veiculos"]
        lista_oficinas = regra_dict["oficinas"]
        lista_secoes = regra_dict["secoes"]
        lista_os = regra_dict["os"]
        relatorio_previo_md = None

        # Buscar os dados do banco
        str_csv_colaboradores, str_csv_oficinas, str_csv_veiculos = buscar_dados_banco(
            periodo,
            min_dias_retrabalho,
            lista_modelos,
            lista_oficinas,
            lista_secoes,
            lista_os,
        )

        # Gera os Inputs
        system_input = get_system_instructions(nome, periodo, lista_secoes, relatorio_previo_md)
        user_input = get_user_prompt(
            relatorio_previo_md,
            str_csv_colaboradores,
            str_csv_oficinas,
            str_csv_veiculos,
        )

        # Chama o OpenAI
        try:
            response = openai_client.gerar_relatorio_os(system_input, user_input)
            relatorio_md = response["report_md"]

            print(f"Relatório gerado para a regra {regra_dict['id']} com sucesso.")

            # Salva o relatório no banco de dados
            sucesso_salvamento = salvar_relatorio(id_regra, relatorio_md, pg_engine)
            if sucesso_salvamento:
                print(f"Relatório salvo com sucesso para a regra {regra_dict['id']}.")
            else:
                print(f"Falha ao salvar o relatório para a regra {regra_dict['id']}.")
                
            # Dispara WhatsApp se aplicável
            if regra_dict["target_wpp"]:
                envia_wpp(wpp_service, regra_dict, data_hoje_str)

            # # Dispara E-mail se aplicável
            if regra_dict["target_email"]:
                envia_email(email_service, regra_dict, data_hoje_str)

        except Exception as e:
            print(f"Erro ao gerar o relatório para a regra {regra_dict['id']}: {e}")
