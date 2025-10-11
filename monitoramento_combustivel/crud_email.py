#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer o serviço de teste de email para novas regras de monitoramento

# Imports básicos
import os

# Imports de data
import datetime as dt

# Import SMTP
import smtplib
from email.message import EmailMessage

##############################################################################
# CONFIGURAÇÕES BÁSICAS ######################################################
##############################################################################
DASHBOARD_URL = os.getenv("DASHBOARD_URL")
SMTP_KEY = os.getenv("SMTP")



# Classe do serviço
class CRUDEmailService:

    def __init__(self, smtp_key=SMTP_KEY, dashboard_url=DASHBOARD_URL):
        self.smtp_key = smtp_key
        self.dashboard_url = dashboard_url

    def build_email_text(self, regra_dict, veiculo_dados_dict, link_url):
        msg_str = f"""
========================================================
🚨 REGRA: {regra_dict["nome_regra"]}    
========================================================
🚍 Veículo: {veiculo_dados_dict["vec_num_id"]}
⚙️ Modelo: {veiculo_dados_dict["vec_model"]}
🕓 Consumo: {veiculo_dados_dict["media_km_por_litro"]:.2f} km/L
⛽ Litros excedentes: {veiculo_dados_dict["litros_excedentes"]:.2f} L
💸 Custo combustível: R$ {veiculo_dados_dict["custo_excedente"]:.2f}
📉 % Viagens abaixo da mediana: {veiculo_dados_dict["perc_total_abaixo_mediana"]:.2f} %
⚠️ % Viagens baixa perfomance: {veiculo_dados_dict["perc_baixa_perfomance"]:.2f} %
❌ % Viagens erro telemetria: {veiculo_dados_dict["perc_erro_telemetria"]:.2f} %

🔗 Relatório: {link_url}
"""

        return msg_str

    def build_email_html(self, regra_dict, veiculo_dados_dict, link_url):
        msg_str = f"""
<h2>🚨 REGRA: {regra_dict["nome_regra"]}</h2>
<ul>
    <li>🚍 Veículo: {veiculo_dados_dict["vec_num_id"]}</li>
    <li>⚙️ Modelo: {veiculo_dados_dict["vec_model"]}</li>
    <li>🕓 Consumo: {veiculo_dados_dict["media_km_por_litro"]:.2f} km/L</li>
    <li>⛽ Litros excedentes: {veiculo_dados_dict["litros_excedentes"]:.2f} L</li>
    <li>💸 Custo combustível: R$ {veiculo_dados_dict["custo_excedente"]:.2f}</li>
    <li>📉 % Viagens abaixo da mediana: {veiculo_dados_dict["perc_total_abaixo_mediana"]:.2f} %</li>
    <li>⚠️ % Viagens baixa perfomance: {veiculo_dados_dict["perc_baixa_perfomance"]:.2f} %</li>
    <li>❌ % Viagens erro telemetria: {veiculo_dados_dict["perc_erro_telemetria"]:.2f} %</li>
    <li>🔗 Relatório: <a href="{link_url}">{link_url}</a></li>
</ul>
"""
        return msg_str


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


    def send_msg(self, regra_dict, veiculo_dados_dict, email_destino):
        nome_regra = regra_dict["nome_regra"]
        link_url = self.__get_link(regra_dict, veiculo_dados_dict)

        # Constrói o email
        email_text = self.build_email_text(regra_dict, veiculo_dados_dict, link_url)
        email_html = self.build_email_html(regra_dict, veiculo_dados_dict, link_url)

        msg = EmailMessage()
        msg["Subject"] = f"🚨 REGRA: {nome_regra}"
        msg["From"] = "ceia.ra.ufg@gmail.com"
        msg["To"] = email_destino
        msg.set_content(email_text)
        msg.add_alternative(email_html, subtype="html")

        print("Enviando para:", email_destino)
        print("Email Texto:")
        print(email_text)
        print("Email HTML:")
        print(email_html)

        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login("ceia.ra.ufg@gmail.com", self.smtp_key)
            smtp.send_message(msg)

        print("Email enviado com sucesso")
