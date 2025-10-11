#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer o serviço de teste de WhatsApp para novas regras de monitoramento

# Imports básicos
import json
import os
import re

# Imports de data
import datetime as dt

# Imports HTTP
import requests

##############################################################################
# CONFIGURAÇÕES BÁSICAS ######################################################
##############################################################################
WP_ZAPI_URL = os.getenv("WP_ZAPI_URL")
WP_ZAPI_SEND_TEXT_URL = f"{WP_ZAPI_URL}/send-text"
WP_ZAPI_SEND_LINK_URL = f"{WP_ZAPI_URL}/send-link"
WP_ZAPI_STATUS_URL = f"{WP_ZAPI_URL}/status"
WP_ZAPI_RESTART_URL = f"{WP_ZAPI_URL}/restart"
WP_ZAPI_TOKEN = os.getenv("WP_ZAPI_TOKEN")
WP_ZAPI_LINK_IMAGE_URL = os.getenv("WP_ZAPI_LINK_IMAGE_URL")

DASHBOARD_URL = os.getenv("DASHBOARD_URL")


##############################################################################
# ROTINAS DE APOIO ###########################################################
##############################################################################
def formatar_telefone(numero_br):
    # Remove tudo que não for número
    somente_digitos = re.sub(r"\D", "", numero_br)

    # Adiciona o DDI do Brasil (55) no início, se não tiver
    if not somente_digitos.startswith("55"):
        somente_digitos = "55" + somente_digitos
    return somente_digitos


##############################################################################
# TEMPLATES ##################################################################
##############################################################################

# TODO: Usar uma linguagem de template mais adequada, como Jinja2


# Classe do serviço
class CRUDWppService(object):

    def __init__(
        self,
        wp_zapi_url=WP_ZAPI_URL,
        wp_zapi_send_text_url=WP_ZAPI_SEND_TEXT_URL,
        wp_zapi_send_link_url=WP_ZAPI_SEND_LINK_URL,
        wp_zapi_status_url=WP_ZAPI_STATUS_URL,
        wp_zapi_restart_url=WP_ZAPI_RESTART_URL,
        wp_zapi_token=WP_ZAPI_TOKEN,
        wp_zapi_link_image_url=WP_ZAPI_LINK_IMAGE_URL,
        dashboard_url=DASHBOARD_URL,
    ):
        self.wp_zapi_url = wp_zapi_url
        self.wp_zapi_send_text_url = wp_zapi_send_text_url
        self.wp_zapi_send_link_url = wp_zapi_send_link_url
        self.wp_zapi_status_url=wp_zapi_status_url
        self.wp_zapi_restart_url=wp_zapi_restart_url
        self.wp_zapi_token = wp_zapi_token
        self.wp_zapi_link_image_url = wp_zapi_link_image_url
        self.dashboard_url = dashboard_url

        self.headers = {
            "Client-Token": self.wp_zapi_token,
            "Content-Type": "application/json",
        }

    def is_alive(self):
        try:
            response = requests.get(self.wp_zapi_status_url, headers=self.headers)
            return response.json()["connected"]
        except:
            return False

    def restart_instance(self):
        try:
            response = requests.get(self.wp_zapi_restart_url, headers=self.headers)
            return response.json()["value"]
        except:
            return False

    def __wpp_send_link(
        self,
        msg_str,
        title_str,
        link_url,
        link_description_str,
        telefone_destino,
    ):
        payload = {
            "phone": formatar_telefone(telefone_destino),
            "message": msg_str,
            "image": self.wp_zapi_link_image_url,
            "title": title_str,
            "linkUrl": link_url,
            "linkDescription": link_description_str,
        }

        payload_json = json.dumps(payload)
        print(payload_json)
        response = requests.post(self.wp_zapi_send_link_url, headers=self.headers, data=payload_json)

        return response.status_code

    def __get_link(self, regra_dict, veiculo_dados_dict):
        # Vec num id
        vec_num_id = veiculo_dados_dict["vec_num_id"]

        # Datas
        data_atual = dt.datetime.now()
        data_inicio = data_atual - dt.timedelta(days=int(regra_dict["periodo"]))
        data_fim = data_atual
        data_inicio_str = data_inicio.strftime("%Y-%m-%d")
        data_fim_str = data_fim.strftime("%Y-%m-%d")

        # Lista de linhas
        linhas = ["TODAS"]

        # Velocidade
        km_l_min = 1
        km_l_max = 10

        link_url = f"{DASHBOARD_URL}/combustivel-por-veiculo?vec_num_id={vec_num_id}&data_inicio={data_inicio_str}&data_fim={data_fim_str}&lista_linhas={linhas}&km_l_min={km_l_min}&km_l_max{km_l_max}"

        return link_url

    def send_msg(self, regra_dict, veiculo_dados_dict, telefone_destino):
        # Extraí os dados do dict
        nome_regra = regra_dict["nome_regra"]
        vec_num_id = veiculo_dados_dict["vec_num_id"]
        vec_model = veiculo_dados_dict["vec_model"]

        # Mensagens
        title_str = f"Veículo: {vec_num_id} / {vec_model}"
        link_description_str = f"Veículo com baixa perfomance"
        link_url = self.__get_link(regra_dict, veiculo_dados_dict)

        msg_str = f"""⚡ *REGRA*: {nome_regra}
🚍 Veículo: {vec_num_id}
⚙️ Modelo: {vec_model}
🕓 Consumo: {veiculo_dados_dict["media_km_por_litro"]:.2f} km/L
⛽ Litros excedentes: {veiculo_dados_dict["litros_excedentes"]:.2f} L
💸 Custo combustível: R$ {veiculo_dados_dict["custo_excedente"]:.2f}
📉 % Viagens abaixo da mediana: {veiculo_dados_dict["perc_total_abaixo_mediana"]:.2f} %
⚠️ % Viagens baixa perfomance: {veiculo_dados_dict["perc_baixa_perfomance"]:.2f} %
❌ % Viagens erro telemetria: {veiculo_dados_dict["perc_erro_telemetria"]:.2f} %

🔗 Relatório: {link_url}
"""

        print(f"Mandando msg do veículo {vec_num_id} para o telefone: {telefone_destino}")
        self.__wpp_send_link(msg_str, title_str, link_url, link_description_str, telefone_destino)
