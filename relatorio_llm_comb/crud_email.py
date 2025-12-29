#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer o serviço de teste de email para regras de monitoramento

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

    def build_email_text(self, regra_dict, data_hoje_str, link_url):
        nome_regra = regra_dict["nome"]
        msg_str = f"""
========================================================
🚨 RELATÒRIO: {nome_regra}    
========================================================
📅 Data do relatório: {data_hoje_str}
🔗 Relatório: {link_url}
"""
        return msg_str

    def build_email_html(self, regra_dict, data_hoje_str, link_url):
        nome_regra = regra_dict["nome"]

        msg_str = f"""
<h2>🚨 REGRA: {nome_regra}</h2>
<ul>
<li>📅 Data do relatório: {data_hoje_str}</li>
<li>🔗 Relatório: <a href="{link_url}">{link_url}</a></li>
</ul>
"""
        return msg_str

    def send_msg(self, regra_dict, data_hoje_str, email_destino):
        nome = regra_dict["nome"]
        id_regra = regra_dict["id"]
        link_url = f"{self.dashboard_url}/relatorio-ler?id_regra={id_regra}&data_relatorio={data_hoje_str}"

        # Constrói o email
        email_text = self.build_email_text(regra_dict, data_hoje_str, link_url)
        email_html = self.build_email_html(regra_dict, data_hoje_str, link_url)

        msg = EmailMessage()
        msg["Subject"] = f"📒 RELATÓRIO: {nome}"
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
