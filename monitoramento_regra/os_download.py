#!/usr/bin/env python
# coding: utf-8

# Classe que fornecer o serviço para download das OS da API da Rápido Araguaia

# Imports básicos
import pandas as pd
import numpy as np

# Hash
import hashlib

# Imports HTTP
import requests


# Classe
class OSDownload(object):

    def __init__(self, RA_API_URL, RA_API_KEY):
        self.RA_API_URL = RA_API_URL
        self.RA_API_KEY = RA_API_KEY

    def __preprocessar_os(self, df):
        # Limpa os dados

        # Replace empty strings with NaN for numeric columns
        numeric_columns = ["TEMPO PADRAO", "TEMPO TOTAL", "COLABORADOR QUE EXECUTOU O SERVICO"]
        df[numeric_columns] = df[numeric_columns].replace("", np.nan)

        # Optionally fill NaN with 0.0 or another default value
        df[numeric_columns] = df[numeric_columns].fillna(0.0)

        # Replace NUL characters with an empty string
        df = df.map(lambda x: x.replace("\x00", "") if isinstance(x, str) else x).copy()

        # Replace NaN or None with an empty string
        df.fillna("", inplace=True)

        colunas_unicas = [
            "DATA DA ABERTURA DA OS",
            # "DATA DO FECHAMENTO DA OS",
            "NUMERO DA OS",
            "SECAO",
            "DESCRICAO DO SERVICO",
            "COLABORADOR QUE EXECUTOU O SERVICO",
            "SERVICO DA OS",
        ]

        df["KEY"] = df[colunas_unicas].astype(str).agg("_".join, axis=1)
        df["KEY_HASH"] = df["KEY"].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

        return df

    def download_os(self, data_inicio, data_fim, max_retries=5):
        url = self.RA_API_URL
        headers = {"Authorization": self.RA_API_KEY}
        payload = {
            "DataInicial": data_inicio,
            "DataFinal": data_fim,
            "CodEmpresa": 2,
            "CodServico": "0",
            "NumOS": "0",
            "ServicoExecutado": "T",
        }

        response = None
        tentativa = 0
        while tentativa < max_retries:
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=300)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                tentativa += 1

        if tentativa == max_retries:
            raise Exception("Número máximo de tentativas excedido.")
        else:
            # Transforma em DataFrame
            df_raw = pd.DataFrame(response.json()["data"])

            # Preprocessa os dados
            df = self.__preprocessar_os(df_raw)

            return df
