#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer o serviço de teste de WhatsApp para novas regras de monitoramento

# Imports básicos
import json
import os
import re

# Imports HTTP
import requests

##############################################################################
# CONFIGURAÇÕES BÁSICAS ######################################################
##############################################################################
WP_ZAPI_URL = os.getenv("WP_ZAPI_URL")
WP_ZAPI_SEND_TEXT_URL = f"{WP_ZAPI_URL}/send-text"
WP_ZAPI_SEND_LINK_URL = f"{WP_ZAPI_URL}/send-link"
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
        wp_zapi_token=WP_ZAPI_TOKEN,
        wp_zapi_link_image_url=WP_ZAPI_LINK_IMAGE_URL,
        dashboard_url=DASHBOARD_URL,
    ):
        self.wp_zapi_url = wp_zapi_url
        self.wp_zapi_send_text_url = wp_zapi_send_text_url
        self.wp_zapi_send_link_url = wp_zapi_send_link_url
        self.wp_zapi_token = wp_zapi_token
        self.wp_zapi_link_image_url = wp_zapi_link_image_url
        self.dashboard_url = dashboard_url

        self.headers = {
            "Client-Token": self.wp_zapi_token,
            "Content-Type": "application/json",
        }

    def __wpp_send_text(self, msg_str, telefone_destino):
        payload = {
            "phone": formatar_telefone(telefone_destino),
            "message": msg_str,
        }
        payload_json = json.dumps(payload)
        print("MANDANDO MENSAGEM:")
        print(payload_json)
        response = requests.post(self.wp_zapi_send_text_url, headers=self.headers, data=payload_json)

        return response.status_code

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

    def send_msg(self, regra_dict, os_dict, telefone_destino):
        nome_regra = regra_dict["nome_regra"]
        min_dias = regra_dict["min_dias_retrabalho"]
        numero_os = os_dict["NUMERO DA OS"]
        problema_os = os_dict["DESCRICAO DO SERVICO"]
        codigo_veiculo = os_dict["CODIGO DO VEICULO"]
        modelo_veiculo = os_dict["DESCRICAO DO MODELO"]
        status_os = os_dict["status_os"]

        # Mensagens
        title_str = f"OS: {numero_os} / {codigo_veiculo}"
        link_description_str = f"Status: {status_os}"
        link_url = f"{DASHBOARD_URL}/retrabalho-por-os?os={numero_os}&mindiasretrabalho={min_dias}"

        msg_str = f"""⚡ *REGRA*: {nome_regra}
💣 Problema: {problema_os}
⚠️ Status: {status_os}
🚍 Veículo: {codigo_veiculo}
🚏 Modelo: {modelo_veiculo}
⚙️ OS: {numero_os}
🔗 Relatório: {link_url}
"""

        print(f"Mandando msg da OS {numero_os} para o telefone: {telefone_destino}")
        self.__wpp_send_link(msg_str, title_str, link_url, link_description_str, telefone_destino)
