#!/bin/python
# coding: utf-8

# Função utilitária para pegar o preço do combustível via API
# https://combustivelapi.com.br/

# Imports básicos
import json

# Imports para requisições HTTP
import requests

# Constantes
BASE_URL = "https://combustivelapi.com.br"
ENDPOINT = "/api/precos"


def get_preco_diesel():
    url = BASE_URL + ENDPOINT
    preco_padrao_diesel = 6
    preco_diesel = preco_padrao_diesel

    try:
        headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.request("GET", url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            preco_diesel = float(str(data["precos"]["diesel"]["go"]).replace(",", "."))
        else:
            print(f"Erro ao acessar a API: {response.status_code}")
    except Exception as e:
        print(f"Exceção ao acessar a API: {e}")

    return preco_diesel
