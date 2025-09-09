#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer o serviço de teste de email para novas regras de monitoramento

# Imports básicos
import os

# Import SMTP
import smtplib
from email.message import EmailMessage

##############################################################################
# CONFIGURAÇÕES BÁSICAS ######################################################
##############################################################################
DASHBOARD_URL = os.getenv("DASHBOARD_URL")
SMTP_KEY = os.getenv("SMTP")

##############################################################################
# TEMPLATES ##################################################################
##############################################################################


# Classe do serviço
class CRUDEmailService:

    def __init__(self, smtp_key=SMTP_KEY, dashboard_url=DASHBOARD_URL):
        self.smtp_key = smtp_key
        self.dashboard_url = dashboard_url

    def build_email_text(self, regra_dict, os_dict, link_url):
        nome_regra = regra_dict["nome_regra"]
        min_dias = regra_dict["min_dias_retrabalho"]
        numero_os = os_dict["NUMERO DA OS"]
        problema_os = os_dict["DESCRICAO DO SERVICO"]
        codigo_veiculo = os_dict["CODIGO DO VEICULO"]
        modelo_veiculo = os_dict["DESCRICAO DO MODELO"]
        status_os = os_dict["status_os"]

        msg_str = f"""
========================================================
🚨 REGRA: {nome_regra}    
========================================================
💣 Problema: {problema_os}
⚠️ Status: {status_os}
🚍 Veículo: {codigo_veiculo}
🚏 Modelo: {modelo_veiculo}
⚙️ OS: {numero_os}
🔗 Relatório: {link_url}
"""

        return msg_str

    def build_email_html(self, regra_dict, os_dict, link_url):
        nome_regra = regra_dict["nome_regra"]
        min_dias = regra_dict["min_dias_retrabalho"]
        numero_os = os_dict["NUMERO DA OS"]
        problema_os = os_dict["DESCRICAO DO SERVICO"]
        codigo_veiculo = os_dict["CODIGO DO VEICULO"]
        modelo_veiculo = os_dict["DESCRICAO DO MODELO"]
        status_os = os_dict["status_os"]

        msg_str = f"""
<h2>🚨 REGRA: {nome_regra}</h2>
<ul>
<li>💣 Problema: {problema_os}</li>
<li>⚠️ Status: {status_os}</li>
<li>🚍 Veículo: {codigo_veiculo}</li>
<li>🚏 Modelo: {modelo_veiculo}</li>
<li>⚙️ OS: {numero_os}</li>
<li>🔗 Relatório: <a href="{link_url}">{link_url}</a></li>
</ul>
"""
        return msg_str

    def send_msg(self, regra_dict, os_dict, email_destino):
        nome_regra = regra_dict["nome_regra"]
        min_dias = regra_dict["min_dias_retrabalho"]
        numero_os = os_dict["NUMERO DA OS"]
        link_url = f"{self.dashboard_url}/retrabalho-por-os?os={numero_os}&mindiasretrabalho={min_dias}"

        # Constrói o email
        email_text = self.build_email_text(regra_dict, os_dict, link_url)
        email_html = self.build_email_html(regra_dict, os_dict, link_url)

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
