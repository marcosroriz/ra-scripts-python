#!/usr/bin/env python
# coding: utf-8

# Arquivo com uma pequena classe para interagir com a API do OpenAI

# Imports
import os
import json
import requests

###################################################################################
# OpenAI
###################################################################################
# KEY
OPEN_AI_KEY = os.getenv("OPENAI_API_KEY")
OPEN_AI_MODEL = os.getenv("OPENAI_API_MODEL")
OPEN_AI_URL = os.getenv("OPENAI_API_URL")


class OpenAIChatGPTClient:
    def __init__(self, key=OPEN_AI_KEY, model=OPEN_AI_MODEL, url=OPEN_AI_URL):
        self.api_key = key
        self.model = model
        self.base_url = url
        self.headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    def send_message(self, system_instructions, user_input):
        payload = {
            "model": self.model,
            "instructions": system_instructions,
            "input": user_input,
        }
        response = requests.post(self.base_url, headers=self.headers, json=payload)

        if response.status_code == 200:
            return response.json()["output"][1]["content"][0]["text"]
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    def gerar_relatorio_os(self, system_instructions, user_input):
        response = self.send_message(system_instructions, user_input)

        # Parse the response as JSON
        try:
            json_output = json.loads(response)
        except json.JSONDecodeError:
            print("Failed to parse the response as JSON.")
            print(response)
            raise ValueError("Failed to parse the response as JSON.")

        return json_output
