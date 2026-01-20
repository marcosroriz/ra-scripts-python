#!/usr/bin/env python
# coding: utf-8

###################################################################################
# Imports
###################################################################################

# Básicos
import json
import os
import sys
import time

# DotEnv
from dotenv import load_dotenv

# Carrega variáveis de ambiente
CURRENT_WORKINGD_DIR = os.getcwd()
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))

load_dotenv()
load_dotenv("../.env")
load_dotenv(os.path.join(CURRENT_WORKINGD_DIR, ".env"))
load_dotenv(os.path.join(CURRENT_PATH, "..", ".env"))

# Requests
import requests

# Texto
import re
import unidecode

# Pandas e Numpy
import pandas as pd
import numpy as np

pd.set_option("future.no_silent_downcasting", True)

# BD
import psycopg2 as pg
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text
from sqlalchemy.exc import OperationalError
from execution_logger import ExecutionLogger

# OpenAI
from openai import OpenAIChatGPTClient

###################################################################################
# Configurações
###################################################################################

# Não bufferiza a saída
sys.stdout.flush()

# Configurações do banco de dados
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Conexão com o banco de dados
pg_engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True)

def recriar_pg_engine():
    print("Recriando conexão com o banco de dados...")
    return create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True)

###################################################################################
# OpenAI
###################################################################################
# KEY
OPEN_AI_KEY = os.getenv("OPENAI_API_KEY")
OPEN_AI_MODEL = os.getenv("OPENAI_API_MODEL")
OPEN_AI_URL = os.getenv("OPENAI_API_URL")

# Cliente OpenAI
openai_client = OpenAIChatGPTClient(key=OPEN_AI_KEY, model=OPEN_AI_MODEL, url=OPEN_AI_URL)

# Mensagem de instruções para a LLM
system_instructions = """
You have to read a written text in Brazilian Portuguese from a mechanic service order of a bus and classify what happened. 
I want you to interpretate the text that I will provide to you using MECHANIC knowledge. I want you to interpretate and see if the text and solution is coherent to the problem reported.

The text has three parts:
* PROBLEM: The problem the driver is reporting
* TEXT_SYMPTONS: Symptoms written by the bus driver
* TEXT_MECHANIC: What the mechanic did 

The problem is that symptoms may be missing and mechanics do not usually write clearly what they do. Your task is to classify what the mechanic did, producing the following JSON OUTPUT (key fields):
* "SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "YES",
* "SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
* "SOLUTION_HAS_COHERENCE_TO_PROBLEM": "YES",
* "SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
* "DID_FIX_EXISTING_PART": YES/NO (STRING)
* "DID_FIX_EXISTING_PART_CONFIDENCE": 1-5 (NUMBER)
* "NAME_FIX_EXISTING_PART": PIECE’s name (STRING)
* "DID_CHANGE_OR_REPLACE_PART": YES/NO (STRING)
* "DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 1-5 (NUMBER)
* "NAME_CHANGE_OR_REPLACE_PART": PIECE’s name (STRING)
* "LIST_OF_ACTIVITIES_CHECKS": ARRAY of strings
* "SCORE_SYMPTOMS_TEXT_QUALITY": 1-5 (NUMBER)
* "SCORE_SOLUTION_TEXT_QUALITY": 1-5 (NUMBER)
* "WHY_SOLUTION_IS_PROBLEM": (STRING)

First, you need to specify if the symptoms are coherent to the problem reported. For example, if the text includes the vehicle temperature or that it is heating, the symptom is coherent to the (motor is heating) ‘motor esquentando’ problem. On the other hand, if the symptom reports that there is cutting off power, it is not related to a heating problem, but to a battery or lack of force problem. Output a YES if positive, otherwise, output a NO.
Second, you need to output how confident you are of your classification in a Likert scale. Value is 1 is for totally not confident, while 5 is for totally confident. Each output includes a confident scale.
Third, similar to what you did for the symptoms, you now need to classify if the text reported in the solution field by the mechanic is coherent to the problem reported.  Output a YES if positive, otherwise, output a NO.
Fourth, output how confident you are about the coherence of the mechanic solution to the problem.
Fifth, now you will need to specify if the mechanic did or not fix an existing part of the vehicle, such as water cooler. Notice that a fix is different than replacing or changing a part. Fix includes cleaning, tightening, etc. If you are more positive that he did, output a YES. If not, output a NO.
Six, classify how strong you are that he/she did a fix in this service order.
Seven, output the part that he fixed in UPPER TEXT. If he did not fix a part, output NONE.
Eight, the same logic appears for change or replaced part, but here we are interested in the scenarios where the mechanic has changed/replaced a part. Output a YES if you understood that he/she changed or replaced a part of the vehicle, otherwise output a NO.
Nine, output how strong you are that he/she changed a vehicle part in a Likert-scale.
Ten, output the part that he changed in UPPER TEXT. If he did not change or replace part, output NONE.
Eleven, output the list of actions the mechanic did in the service order as UPPER TEXT. For example, he/she might have passed a car scanner to verify for possible errors, tested the vehicle in a field, etc.
Twelve, output a score according to a Likert-scale (1 horrible to 5 excellent) for the quality of the text provided in the symptoms field. Specifically, I want you to classify how clear and complete is the provided text.
Thirteen, similarly to the last field, output a score according to a Likert-scale for the quality of the text provided by the mechanic. 
Finally, output a text in Brazilian Portuguese to why you think that the solution is coherent or not to the problem. 

Please output the JSON as instructed. Output the raw json, do not include ```json``` in the message.
DO NOT ADD ADDITIONAL FIELDS to the JSON output. Strictly follow the format I specified.
DO NOT CHANGE OR ADD TYPOS TO THE FIELDS. FOR INSTANCE DO NOT CHANGE SYMPTONS_HAS_COHERENCE_TO_PROBLEM TO SUMPTONS_HAS_COHERENCE (Y-TO-U).
FOLLOW THE FORMAT!

See some examples:
Example 0:
Input 0:
PROBLEM: Pneu TS LD com baixa pressão
TEXT_SYMPTONS: PNEU EXTERNO DO TERCEIRO EIXO COM BAIXA PRESSAO 
TEXT_MECHANIC:  TUDO OK AGORA
Output 0:
{ 
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "YES",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "YES",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 3,
"DID_FIX_EXISTING_PART": "YES",
"DID_FIX_EXISTING_PART_CONFIDENCE": 5,
"NAME_FIX_EXISTING_PART": "PNEU EXTERNO DO TERCEIRO EIXO",
"DID_CHANGE_OR_REPLACE_PART": "NO",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 5,
"NAME_CHANGE_OR_REPLACE_PART": "NONE",
"LIST_OF_ACTIVITIES_CHECKS": ["NONE"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 5
"SCORE_SOLUTION_TEXT_QUALITY": 3,
"WHY_SOLUTION_IS_PROBLEM": "Embora o sintoma de 'pneu externo do terceiro eixo com baixa pressão' esteja claro e coerente, a solução informada pelo mecânico, 'tudo ok agora', não fornece informações sobre o que exatamente foi feito para resolver o problema. Não está claro se a pressão do pneu foi ajustada ou se houve outra intervenção."
}
----
Example 1:
Input 1:
PROBLEM: MOTOR ESQUENTANDO
TEXT_SYMPTONS: LUZ DO RADIADOR ACESA E AGUA SUJA
TEXT_MECHANIC: TROCOU A TAMPA DA REVATORIO DE AGUA E LAVOU O RADIADOR
Output 1:
{ 
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "YES",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 4,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "YES",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 4,
"DID_FIX_EXISTING_PART": "YES",
"DID_FIX_EXISTING_PART_CONFIDENCE": 4,
"NAME_FIX_EXISTING_PART": "RADIADOR",
"DID_CHANGE_OR_REPLACE_PART": "YES",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 5,
"NAME_CHANGE_OR_REPLACE_PART": "TAMPA DO RESERVATORIO DE AGUA",
"LIST_OF_ACTIVITIES_CHECKS": ["TAMPA DO RESERVATÓRIO DE AGUA", "RADIADOR"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 5,
"SCORE_SOLUTION_TEXT_QUALITY": 4,
"WHY_SOLUTION_IS_PROBLEM": "Os sintomas relatados (luz do radiador acesa e água suja) estão alinhados com o problema de superaquecimento do motor. O sistema de arrefecimento é diretamente responsável por manter a temperatura do motor, e qualquer falha nesse sistema pode levar ao superaquecimento."
}
----
Example 2:
Input 2:
PROBLEM: MOTOR ESQUENTANDO
TEXT_SYMPTONS: NAO FORNECIDO
TEXT_MECHANIC: CIRCULACAO DAGUA O RADIADOR OK
Output 2:
{
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "NO",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 3,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "NO",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 3,
"DID_FIX_EXISTING_PART": "NO",
"DID_FIX_EXISTING_PART_CONFIDENCE": 3,
"NAME_FIX_EXISTING_PART": "NONE",
"DID_CHANGE_OR_REPLACE_PART": "NO",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 4,
"NAME_CHANGE_OR_REPLACE_PART": "NONE",
"LIST_OF_ACTIVITIES_CHECKS": ["CIRCULAÇÃO D'ÁGUA", "RADIADOR"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 1,
"SCORE_SOLUTION_TEXT_QUALITY": 2,
"WHY_SOLUTION_IS_PROBLEM": "A ação do mecânico é parcialmente coerente, pois verificar a circulação de água no radiador é um passo importante, mas isoladamente não resolve o problema relatado de 'motor esquentando'. Outras possíveis causas, como nível de água, tampa do reservatório, limpeza do radiador, ou funcionamento da ventoinha, deveriam ter sido investigadas. A solução apresentada parece incompleta."
}
----
Example 3:
Input 3:
PROBLEM: MOTOR ESQUENTANDO
TEXT_SYMPTONS: NAO FORNECIDO
TEXT_MECHANIC: CONFERIDO O RADIADOR NIVEL DE AGUA CONFERIR AS MANGUEIRAS E LAVOU O RADIADOR CONSERTOU TAMPA DO FILTRO DE AR
Output 3:
{
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "NO",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 3,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "YES",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 3,
"DID_FIX_EXISTING_PART": "YES",
"DID_FIX_EXISTING_PART_CONFIDENCE": 5,
"NAME_FIX_EXISTING_PART": "TAMPA DO FILTRO DE AR",
"DID_CHANGE_OR_REPLACE_PART": "NO",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 4,
"NAME_CHANGE_OR_REPLACE_PART": "NONE",
"LIST_OF_ACTIVITIES_CHECKS": ["NIVEL DE ÁGUA", "MANGUEIRAS", "RADIADOR", "TAMPA DO FILTRO DE AR"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 1,
"SCORE_SOLUTION_TEXT_QUALITY": 5,
"WHY_SOLUTION_IS_PROBLEM": "Como os sintomas não foram fornecidos, o mecânico precisou agir com base no problema genérico de "motor esquentando". Nesse contexto, verificar os principais componentes do sistema de arrefecimento (radiador, nível de água, mangueiras) é uma abordagem correta e lógica."
}
----
Example 4:
Input 4:
PROBLEM: MOTOR ESQUENTANDO
TEXT_SYMPTONS: LUZ DO RADIADOR ACESA E AGUA SUJA  
TEXT_MECHANIC:  TROCOU A TAMPA DA REVATORIO DE AGUA E LAVOU O RADIADOR
Output 4:
{
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "YES",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "YES",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
"SYMPTOMS_HAS_COHERENCE": "YES",
"DID_FIX_EXISTING_PART": "YES",
"DID_FIX_EXISTING_PART_CONFIDENCE": 5,
"NAME_FIX_EXISTING_PART": "LAVOU O RADIADOR",
"DID_CHANGE_OR_REPLACE_PART": "YES",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 5,
"NAME_CHANGE_OR_REPLACE_PART": "TAMPA DO RESERVATORIO DE AGUA",
"LIST_OF_ACTIVITIES_CHECKS": ["TAMPA DO RESERVATÓRIO DE ÁGUA", "RADIADOR"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 5,
"SCORE_SOLUTION_TEXT_QUALITY": 5,
"WHY_SOLUTION_IS_PROBLEM": "Os sintomas relatados (luz do radiador acesa e água suja) são compatíveis com um problema no sistema de arrefecimento. A luz acesa indica um alerta relacionado à temperatura ou ao nível do líquido de arrefecimento, enquanto a água suja pode comprometer a eficiência do radiador."
}
----
Example 5:
Input 5:
PROBLEM: MOTOR ESQUENTANDO
TEXT_SYMPTONS: CORTANDO ALIMENTACAO
TEXT_MECHANIC: PASSADO APARELHO DE DIAGNOSTICO NO VEICULO O MESMO SO FOI DETECTADO PEDAL DO ACELERADOR FOI TROCADO O PEDAL DO ACELERADOR E CONFERIDO CHICOTE DO PEDAL
Output 5:
{
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "NO",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "NO",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
"DID_FIX_EXISTING_PART": "NO",
"DID_FIX_EXISTING_PART_CONFIDENCE": 5,
"NAME_FIX_EXISTING_PART": "NONE",
"DID_CHANGE_OR_REPLACE_PART": "YES",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 5,
"NAME_CHANGE_OR_REPLACE_PART": "PEDAL DO ACELERADOR",
"LIST_OF_ACTIVITIES_CHECKS": ["APARELHO DE DIAGNÓSTICO", "CHICOTE DO PEDAL"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 4,
"SCORE_SOLUTION_TEXT_QUALITY": 4,
"WHY_SOLUTION_IS_PROBLEM": "O problema de 'motor esquentando' parece não ter uma relação direta com o sintoma 'cortando alimentação'. A expressão 'cortando alimentação' geralmente refere-se a uma interrupção na alimentação de combustível ou energia ao motor, o que pode causar perda de potência ou funcionamento irregular, mas não está necessariamente ligado ao superaquecimento."

}
----
Example 6:
Input 6:
PROBLEM: MOTOR ESQUENTANDO
TEXT_SYMPTONS: NAO FORNECIDO
TEXT_MECHANIC:  MANGUEIRA COTOVELO CAIXA DE CAMBIO SOLTA COLOCOU ABRACADEIRA NA MANGUEIRA DA CAIXA
Output 6:
{
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "NO",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 3,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "YES",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 4,
"DID_FIX_EXISTING_PART": "YES",
"DID_FIX_EXISTING_PART_CONFIDENCE": 5,
"NAME_FIX_EXISTING_PART": "ABRACADEIRA ",
"DID_CHANGE_OR_REPLACE_PART": "YES",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 5,
"NAME_CHANGE_OR_REPLACE_PART": "PEDAL DO ACELERADOR",
"LIST_OF_ACTIVITIES_CHECKS": ["APARELHO DE DIAGNÓSTICO", "CHICOTE DO PEDAL"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 4,
"SCORE_SOLUTION_TEXT_QUALITY": 4,
"WHY_SOLUTION_IS_PROBLEM": "A ação do mecânico é parcialmente coerente. A fixação da mangueira pode ser relevante se o vazamento ou desconexão estivesse afetando o sistema de refrigeração. No entanto, a abordagem parece incompleta, pois o problema principal de "motor esquentando" não foi investigado de maneira abrangente."
}
----
Example 7:
Input 7:
PROBLEM: MOTOR ESQUENTANDO
TEXT_SYMPTONS: PASSANDO AGUA PARA OLEO
TEXT_MECHANIC: FOI VERIFICADOR CORRIGIDO TIRADO TODO OLEO DO SISTEMA
Output 7:
{
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "YES",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "YES",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
"DID_FIX_EXISTING_PART": "YES",
"DID_FIX_EXISTING_PART_CONFIDENCE": 5,
"NAME_FIX_EXISTING_PART": "TIROU OLEO DO SISTEMA",
"DID_CHANGE_OR_REPLACE_PART": "NO",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 5,
"NAME_CHANGE_OR_REPLACE_PART": "NONE",
"LIST_OF_ACTIVITIES_CHECKS": ["VERIFICOU OLEO", "TROCOU OLEO"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 4,
"SCORE_SOLUTION_TEXT_QUALITY": 4,
"WHY_SOLUTION_IS_PROBLEM": "O sintoma de 'passando água para o óleo' é uma indicação séria e comumente associada a um problema na junta do cabeçote ou no sistema de arrefecimento. Quando há mistura de água e óleo, o motor pode superaquecer, pois o líquido de arrefecimento perde sua eficácia e o óleo não pode lubrificar adequadamente."
}
----
Example 8:
Input 8:
PROBLEM: Elevador não funciona ou inoperante
TEXT_SYMPTONS: FIOS SOLTO NO MEIO DO CARRO POR BAIXO ARRASTANDO NO CHAO POSSIVEL SEREM DO ELEVADOR 
TEXT_MECHANIC:   AMARROU A MANGUEIRA DO ELEVADOR
Output 8:
{
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM": "YES",
"SYMPTOMS_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 5,
"SOLUTION_HAS_COHERENCE_TO_PROBLEM": "NO",
"SOLUTION_HAS_COHERENCE_TO_PROBLEM_CONFIDENCE": 4,
"DID_FIX_EXISTING_PART": "YES",
"DID_FIX_EXISTING_PART_CONFIDENCE": 5,
"NAME_FIX_EXISTING_PART": "MANGUEIRA DO ELEVADOR",
"DID_CHANGE_OR_REPLACE_PART": "NO",
"DID_CHANGE_OR_REPLACE_PART_CONFIDENCE": 5,
"NAME_CHANGE_OR_REPLACE_PART": "NONE",
"LIST_OF_ACTIVITIES_CHECKS": ["AMARRAÇÃO DA MANGUEIRA DO ELEVADOR"],
"SCORE_SYMPTOMS_TEXT_QUALITY": 5,
"SCORE_SOLUTION_TEXT_QUALITY": 4,
"WHY_SOLUTION_IS_PROBLEM": "Os sintomas relatados indicam que os fios soltos no carro podem estar relacionados ao mecanismo do elevador. No entanto, a solução do mecânico, que consistiu em amarrar a mangueira do elevador, não parece abordar diretamente a causa raíz do problema, que pode envolver uma falha elétrica ou mecânica mais profunda."
}

"""


###################################################################################
# Converter texto para maiúsculas, remover acentuação e caracteres especiais
# Também pega sintomas e correcoes
###################################################################################
def formatar_texto(texto):
    texto_maiusculo = str(texto).upper()
    texto_sem_acentos = unidecode.unidecode(texto_maiusculo)
    texto_limpo = re.sub(r"[^\w\s]", "", texto_sem_acentos)  # Remover pontuação e caracteres especiais
    texto_unico_espaco = re.sub(r"\s+", " ", texto_limpo)  # Remover espaços múltiplos
    return texto_unico_espaco.strip()


def processar_sintoma(complemento):
    if not isinstance(complemento, str):
        return "NAO FORNECIDO"

    termos = complemento.split(" OBSERVACAO TOTEM")
    termos = complemento.split("OBSERVACAO TOTEM")
    if len(termos) > 1 and len(termos[0]) > 2:
        return termos[0]
    else:
        return "NAO FORNECIDO"


def processar_correcao(complemento):
    if not isinstance(complemento, str):
        return "NAO FORNECIDO"

    # termos = complemento.split(' OBSERVACAO TOTEM');
    termos = complemento.split("OBSERVACAO TOTEM")
    if len(termos) > 1 and len(termos[1]) > 2:
        return termos[1]
    else:
        return "NAO FORNECIDO"
    

def prepara_user_input_llm(problem, text_symptoms, text_mechanic):
    user_input = f"""
        PROBLEM: {problem}
        TEXT_SYMPTOMS: {text_symptoms}
        TEXT_MECHANIC: {text_mechanic}
        Please output the JSON as instructed. Output the raw json, do not include ```json``` in the message.
    """
    return user_input


###################################################################################
# Vamos processar dado de mês em mês
###################################################################################

def main():
    # Data de hoje (threshold)
    data_hoje = pd.to_datetime("now")

    # Data de início e fim do intervalo
    data_intervalo_inicio = pd.to_datetime("2025-01-01")
    data_intervalo_fim = data_intervalo_inicio + pd.DateOffset(months=1)

    # Use global pg_engine or pass it. It is global.
    global pg_engine

    while data_intervalo_inicio < data_hoje:
        data_inicio_str = data_intervalo_inicio.strftime("%Y-%m-%d")
        data_fim_str = data_intervalo_fim.strftime("%Y-%m-%d")

        # Ler os dados
        query = r"""
            SELECT 
                od."KEY_HASH",
                od."NUMERO DA OS",
                od."FILIAL",
                od."DESCRICAO DO SERVICO",
                od."COMPLEMENTO DO SERVICO"
            FROM 
                os_dados od 
            WHERE 
                od."DATA INICIO SERVIÇO" IS NOT NULL 
                AND od."DATA INICIO SERVIÇO" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'::text 
                AND od."DATA DE FECHAMENTO DO SERVICO" IS NOT NULL 
                AND od."DATA DE FECHAMENTO DO SERVICO" ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'::text 
                -- AND od."DESCRICAO DO TIPO DA OS" = 'OFICINA'::text
                AND (od."PRIORIDADE SERVICO" = ANY (ARRAY['Vermelho'::text, 'Amarelo'::text, 'Verde'::text]))
                AND od."DESCRICAO DA SECAO" in ('MANUTENCAO ELETRICA', 'MANUTENCAO MECANICA') 
                AND od."COMPLEMENTO DO SERVICO" LIKE '%%Totem%%'
        """

        query += f"""
            AND od."DATA INICIO SERVIÇO" >= '{data_inicio_str}' AND od."DATA INICIO SERVIÇO" < '{data_fim_str}'
        """

        df = pd.read_sql_query(query, pg_engine)

        # Processar os dados
        df["COMPLEMENTO DO SERVICO"] = df["COMPLEMENTO DO SERVICO"].apply(formatar_texto)
        df["UFG_SINTOMA"] = df["COMPLEMENTO DO SERVICO"].apply(processar_sintoma)
        df["UFG_CORRECAO"] = df["COMPLEMENTO DO SERVICO"].apply(processar_correcao)

        # Processando os dados
        tam_df = len(df)
        os_atual = 1
        num_errors = 0

        # Iterar sobre as linhas do dataframe
        for index, row in df.iterrows():
            print(
                f"Processando OS {os_atual} de {tam_df} do intervalo {data_inicio_str} a {data_fim_str}, erros: {num_errors}"
            )
            os_atual += 1

            key = row["KEY_HASH"]
            num_os = row["NUMERO DA OS"]
            problem = row["DESCRICAO DO SERVICO"]
            text_symptoms = row["UFG_SINTOMA"]
            text_mechanic = row["UFG_CORRECAO"]

            # Vamos verifica se a OS já não foi processada
            query = f"""
                SELECT "KEY_HASH"
                FROM 
                    os_dados_classificacao 
                WHERE 
                    "KEY_HASH" = '{key}'
            """

            df_os_classificada = None
            try:
                with pg_engine.connect() as conn:
                    df_os_classificada = pd.read_sql_query(query, conn)
            except OperationalError as e:
                print(f"Erro de conexão detectado: {e}")
                num_errors += 1

            # Houve erro de conexão, vamos recriar a conexão
            if df_os_classificada is None:
                pg_engine = recriar_pg_engine()
                num_errors += 1
                continue

            # Vamos ver se a OS ainda não foi processada
            if df_os_classificada.empty:
                print(f"OS: {num_os}", problem, text_symptoms, text_mechanic, key)
                try:
                    # Prepara user_input
                    user_input = prepara_user_input_llm(problem, text_symptoms, text_mechanic)
                    # Classifica a OS
                    result = openai_client.classificar_resposta(system_instructions, user_input)
                    result["KEY_HASH"] = key
                    result["SINTOMA"] = text_symptoms
                    result["CORRECAO"] = text_mechanic

                    df_os_dado = pd.DataFrame([result])
                    df_os_dado = df_os_dado.replace({"YES": True, "NO": False})
                    df_os_dado["DATA_ANALISE"] = pd.to_datetime("now").strftime("%Y-%m-%d %H:%M:%S")

                    # Vamos inserir os dados no banco de dados
                    df_os_dado.to_sql("os_dados_classificacao", pg_engine, if_exists="append", index=False)
                    print("--> Dado inserido com sucesso 'os_dados_classificacao'.")
                except Exception as ex:
                    print(f"Houve um erro: {ex}")
                    num_errors += 1
            else:
                print("--> OS já foi processada", key)
                continue

            # Aguardar um tempo para não sobrecarregar a API
            time.sleep(5)

        # Vamos para o próximo mês
        data_intervalo_inicio = data_intervalo_fim
        data_intervalo_fim = data_intervalo_inicio + pd.DateOffset(months=1)


    print("Finalização do processamento")


if __name__ == "__main__":
    with ExecutionLogger(pg_engine, "mix_down_llm"):
        main()
