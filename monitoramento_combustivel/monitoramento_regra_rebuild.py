#!/usr/bin/env python
# coding: utf-8

# Script que analisa se alguma nova OS se encaixa em alguma regra de monitoramento
# Para realizar o monitoramento contínuo, o script é executado periodicamente (por ex: a cada 5 minutos)
# O script faz o seguinte:
# - Download das OS da API da Rápido Araguaia
# - Preprocessamento dos dados
# - Verificação de novas OS (que não estão na base de dados)
# - Verificação se alguma nova OS se encaixa em alguma regra de monitoramento
# - Envio de notificação via WhatsApp e E-mail para os responsáveis pela regra

###################################################################################
# Imports
###################################################################################

# Imports variáveis de ambiente
from dotenv import load_dotenv

# Tenta carregar o .env atual e o do diretório pai
load_dotenv("./env")
load_dotenv("../.env")

# Bibliotecas padrão
import os
import sys
import re
import pandas as pd

# Datas
import datetime as dt

# BD
from sqlalchemy import create_engine

# Imports OS
from os_download import OSDownload
from os_manager import OSManager

# Imports de Regras
from rule_manager import RuleManager

# Import Wpp e Email
from crud_wpp import CRUDWppService
from crud_email import CRUDEmailService


###################################################################################
# Configurações e Variáveis de ambiente
###################################################################################

# Não bufferiza a saída
sys.stdout.flush()

# Variáveis do banco
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Conexão com o banco de dados
pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

if pg_engine is None:
    print("Erro ao conectar ao banco de dados.")
    sys.exit(1)

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


def envia_email(email_service, regra_dados_dict, os_dados_dict):
    # Primeiro obtém os emails válidos
    email_destinos = [
        regra_dados_dict["email_dest_1"],
        regra_dados_dict["email_dest_2"],
        regra_dados_dict["email_dest_3"],
        regra_dados_dict["email_dest_4"],
        regra_dados_dict["email_dest_5"],
    ]
    email_destinos_validos = []
    email_destinos_validos = [
        email for email in email_destinos if email != "" and email != None and not verifica_erro_email(email)
    ]

    # Envia o e-mail para cada email válido
    for email_valido in email_destinos_validos:
        email_service.send_msg(regra_dados_dict, os_dados_dict, email_valido)


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
        r"-group$",  # grupo do whatsapp
        r"\d{12}-\d{10}",  # grupo do whatsapp (outro formato)
    ]

    if not any(re.match(padrao, wpp_limpo) for padrao in padroes_validos):
        return True

    return False


def envia_wpp(wpp_service, regra_dados_dict, os_dados_dict):
    # Primeiro obtém os telefones válidos
    wpp_destinos = [
        regra_dados_dict["wpp_dest_1"],
        regra_dados_dict["wpp_dest_2"],
        regra_dados_dict["wpp_dest_3"],
        regra_dados_dict["wpp_dest_4"],
        regra_dados_dict["wpp_dest_5"],
    ]
    wpp_destinos_validos = []
    wpp_destinos_validos = [wpp for wpp in wpp_destinos if wpp != "" and wpp != None and not verifica_erro_wpp(wpp)]

    # Envia o WhatsApp para cada telefone válido
    for wpp_tel in wpp_destinos_validos:
        wpp_service.send_msg(regra_dados_dict, os_dados_dict, wpp_tel)


###################################################################################
# Main
###################################################################################


def main():
    # Define datas do script
    data_atual = pd.to_datetime("2025-10-01")
    data_inicio = pd.to_datetime("2025-10-01")
    data_fim = dt.datetime.now() + dt.timedelta(days=1)

    # Percorre o intervalo
    while data_atual <= data_fim:
        inicio = data_atual
        fim = data_atual + dt.timedelta(days=7)

        # Gera as strings
        data_inicio_str = inicio.strftime("%d/%m/%Y")
        data_fim_str = fim.strftime("%d/%m/%Y")

        print("Processando de", data_inicio_str, " até ", data_fim_str)
        # Faz o download das OSs
        os_download_service = OSDownload(RA_API_URL, RA_API_KEY)
        df_os_api = os_download_service.download_os(data_inicio_str, data_fim_str)

        # Obtem as novas OSs
        os_manager_service = OSManager(pg_engine)

        # DF novas
        df_os_novas = os_manager_service.get_os_novas(
            df_os_api, data_inicio=inicio.strftime("%Y-%m-%d"), data_fim=(fim + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        )

        # Obtém as OS que atualizaram colaborador (antes não tinham, i.e., COLABORADOR QUE EXECUTOU O SERVICO = 0)
        df_os_atualizadas = os_manager_service.get_os_atualizadas_com_colaborador(
            df_os_api, data_inicio=inicio.strftime("%Y-%m-%d"), data_fim=(fim + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        )

        # Vamos processar as novas OS
        if not df_os_novas.empty:
            os_manager_service.insert_os_novas(df_os_novas)
            print(len(df_os_novas), "NOVAS OS INSERIDAS")
        else:
            print("SEM NOVAS OS")

        # Vamos processar as OS atualizadas com colaborador
        if not df_os_atualizadas.empty:
            # Faz o update do colaborador nas OS que atualizaram colaborador
            os_manager_service.atualizar_os(df_os_atualizadas)
            print(len(df_os_atualizadas), "OS ATUALIZADAS")
        else:
            print("SEM OS PARA ATUALIZAR")

        # Refresh das views
        os_manager_service.refresh_views()

        # Incrementa data_atual
        data_atual = fim


if __name__ == "__main__":
    # Mede o tempo de execução
    data_inicio_script = dt.datetime.now()
    print("Script iniciado em: ", data_inicio_script)

    try:
        main()
    except Exception as e:
        print(f"Erro ao executar o script: {e}")

    data_fim_script = dt.datetime.now()
    tempo_script_minutos = (data_fim_script - data_inicio_script).seconds // 60
    print(f"Tempo para executar o script (em minutos): {tempo_script_minutos}")
