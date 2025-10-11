#!/usr/bin/env python
# coding: utf-8

# Script que analisa se algum veículo se encaixa em alguma regra de monitoramento
# Para realizar o monitoramento contínuo, o script é executado diariamente
# O script faz o seguinte:
# - Recebe as regras do banco de dados (tabela regra_monitoramento_combustivel)
# - Computa os veículos que são afetados
# - Faz inserção no banco de dados (tabela relatorio_regra_monitoramento_combustivel)
# - Envio de notificação via WhatsApp e E-mail para os responsáveis pela regra

###################################################################################
# Imports
###################################################################################

# Imports variáveis de ambiente
from dotenv import load_dotenv

# Tenta carregar o do diretório pai e o atual
load_dotenv("../.env")
load_dotenv("./env", override=True)


# Bibliotecas padrão
import os
import sys
import re

# Datas
import datetime as dt

# BD
from sqlalchemy import create_engine

# Imports de Regras
from rule_manager import RuleManager

# Import Wpp e Email
from crud_wpp import CRUDWppService
from crud_email import CRUDEmailService

# Preço do diesel
from preco_combustivel_api import get_preco_diesel


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
WP_ZAPI_STATUS_URL = f"{WP_ZAPI_URL}/status"
WP_ZAPI_RESTART_URL = f"{WP_ZAPI_URL}/restart"
WP_ZAPI_TOKEN = os.getenv("WP_ZAPI_TOKEN")
WP_ZAPI_LINK_IMAGE_URL = os.getenv("WP_ZAPI_LINK_IMAGE_URL")

# Variáveis do E-mail
SMTP_KEY = os.getenv("SMTP")

# Cria os serviços
wpp_service = CRUDWppService(
    wp_zapi_url=WP_ZAPI_URL,
    wp_zapi_send_text_url=WP_ZAPI_SEND_TEXT_URL,
    wp_zapi_send_link_url=WP_ZAPI_SEND_LINK_URL,
    wp_zapi_status_url=WP_ZAPI_STATUS_URL,
    wp_zapi_restart_url=WP_ZAPI_RESTART_URL,
    wp_zapi_token=WP_ZAPI_TOKEN,
    wp_zapi_link_image_url=WP_ZAPI_LINK_IMAGE_URL,
    dashboard_url=DASHBOARD_URL,
)

email_service = CRUDEmailService(smtp_key=SMTP_KEY, dashboard_url=DASHBOARD_URL)


# Pega o preço do diesel via API
preco_diesel = get_preco_diesel()


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


def envia_email(email_service, regra_dados_dict, veiculo_dados_dict):
    # Primeiro obtém os emails válidos
    email_destinos = [
        regra_dados_dict["target_email_dest1"],
        regra_dados_dict["target_email_dest2"],
        regra_dados_dict["target_email_dest3"],
        regra_dados_dict["target_email_dest4"],
        regra_dados_dict["target_email_dest5"],
    ]
    email_destinos_validos = []
    email_destinos_validos = [
        email for email in email_destinos if email != "" and email != None and not verifica_erro_email(email)
    ]

    # Envia o e-mail para cada email válido
    for email_valido in email_destinos_validos:
        email_service.send_msg(regra_dados_dict, veiculo_dados_dict, email_valido)


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


def envia_wpp(wpp_service, regra_dados_dict, veiculo_dados_dict):
    # Primeiro obtém os telefones válidos
    wpp_destinos = [
        regra_dados_dict["target_wpp_dest1"],
        regra_dados_dict["target_wpp_dest2"],
        regra_dados_dict["target_wpp_dest3"],
        regra_dados_dict["target_wpp_dest4"],
        regra_dados_dict["target_wpp_dest5"],
    ]
    wpp_destinos_validos = []
    wpp_destinos_validos = [wpp for wpp in wpp_destinos if wpp != "" and wpp != None and not verifica_erro_wpp(wpp)]

    # Envia o WhatsApp para cada telefone válido
    for wpp_tel in wpp_destinos_validos:
        wpp_service.send_msg(regra_dados_dict, veiculo_dados_dict, wpp_tel)



###################################################################################
# Main
###################################################################################


def main():
    # Obtem todas as regras de monitoramento
    rule_manager_service = RuleManager(pg_engine)
    regras = rule_manager_service.get_all_rules()

    # Para cada regra, verifica se existe algum veículo que se encontra nesse estado
    for r in regras:
        regra_dados_dict = r.get_dados_regra()
        nome_regra = regra_dados_dict["nome_regra"]
        df = r.get_veiculos_filtrados_regra()

        if df.empty:
            print(f"{nome_regra} não detectou veículo")
            continue
        
        # Injeta ID e DIA
        df["id_regra"] = regra_dados_dict["id_regra"]
        df["dia"] = dt.date.today().strftime("%Y-%m-%d")
        
        # Computa custo
        df["custo_excedente"] = df["litros_excedentes"] * preco_diesel  
        
        # Salva os dados da regra
        r.salvar_dados_regra(df)
        print(f"SALVOU {len(df)} veículos na regra {nome_regra}")

        # Dispara WhatsApp se aplicável
        if regra_dados_dict["target_wpp"]:
            if not wpp_service.is_alive():
                print("WPP Desconectado")
                print("Vamos reiniciar a instância do WPP")
                wpp_service.restart_instance()
            
            for _, veiculo_dados_dict in df.iterrows():
                envia_wpp(wpp_service, regra_dados_dict, veiculo_dados_dict)

        # Dispara E-mail se aplicável
        if regra_dados_dict["target_email"]:
            for _, veiculo_dados_dict in df.iterrows():
                envia_email(email_service, regra_dados_dict, veiculo_dados_dict)

        # Cria OS se aplicável
        # TODO

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
