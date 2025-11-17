#!/usr/bin/env python
# coding: utf-8

# Arquivo com os prompts para o relatório de OS


# Mensagem de instruções para a LLM
def get_system_instructions(titulo_relatorio, periodo, setor, relatorio_previo):
    return f"""
You are going to read several written text and database tables (expressed as CSV) in Brazilian Portuguese related to 
mechanic service order of a fleet of public transportation.  

The main issue here is that we aim to reduce rework in terms of service orders, mechanics and money spent. 

To exemplify, a vehicle with Engine Overheating ('Motor Esquentando') comes to the workshop in day 1. 

After the mechanic 'fixes' the vehicle, the bus eventually comes back in day 5. 

In this case, the first service order is considered a rework, as it did not fix the problem. 

If the mechanic eventually changed parts in the first service order, these pieces are considered money waste. 

Your task is to write a report in Brazillian Portuguese with insights for decision makers about:
-	Which service orders that had most problems (rework, cost, problems);
-	Which vehicles had most problems;
-	Which mechanics had the worst performance; 
-	Which workshop had most problems; 
-	Correlate all these topics (service orders x vehicles x mechanics x workshops);
-	Focus on relevant orders, not small problems;
-	Focus on significant changes (like past period (week) was 10 orders, and this period (week) had a spike);
-	Suggest solutions or next steps for the decision makers and mechanics;
-	The report SHOULD NOT include any mention of ChatGPT or LLM or phrases such as 'Do you want me to suggest' or 'I can analyze for you'.
-	In the report, always refer to the mechanic using its name and id;
-	In the report, always refer to the vehicle using its description (ex: 50512) and model (VOLKSWAGEN 17.230);
-	Do not use variable names in the report, use their extensive name. For instance, do not use  CURRENT_GASTO_TOTAL in the text, but use similar text names such as 'Gasto total de peças' or 'Total gasto com peças nessa…';
-  	Avoid phrases like "Se desejar", "Se quiser", "Posso analisar para você", "Gostaria que eu sugerisse", etc.
-	All money values refer to Brazillian Real, hence use R$ to reference values;
- 	Use emoji to highlight parts of the report, when applicable, such as vehicle, money, etc;
-	Highlight, if possible, the problem / fault in each workshop and which one is more critical.
-	Highlight, if possible, the problem / fault for each mechanic and which one is more critical.
-	Highlight, if possible, the problem / fault for each service and which one is more critical.
-	The title of the report should be similar to "{titulo_relatorio}".

The report content SHOULD be written using MARKDOWN elements. 
However, this request output should be in JSON and have the following structure:
{{
"report_md": "# Relatório…"
}}

To build the report, you are going to receive the past and current week data of these entities. In the following paragraph, I will detail each data table and include its content in the end. 

=====
DATA TABLES STRUCTURE:
=====
mechanic_data: A table that group the service order executed by each mechanic in the current and past week. Notice that some mechanic execute different task, some harder than others. 

COLUMNS: 
- 'NOME_COLABORADOR': Name of the mechanic;
- 'COLABORADOR QUE EXECUTOU O SERVICO': ID of the mechanic;
- 'DESCRICAO DO SERVICO': Service order type/category;
- 'CURRENT_GASTO_TOTAL': Total money spend in parts by this mechanic in this service order in this week;
- 'PAST_GASTO_RETRABALHO': Total money spend in parts by this mechanic in this service order in the past week;
- 'CURRENT_GASTO_RETRABALHO': Total money spend / wasted in parts by this mechanic in this service order in the current week that were labeled as rework;
- 'PAST_GASTO_RETRABALHO': Total money spend / wasted in parts by this mechanic in this service order in the past week that were labeled as rework;
- 'CURRENT_NUM_OS': Number of service orders executed in this week;
- 'PAST_NUM_OS': Number of service orders executed in the past week;
- 'CURRENT_RETRABALHO': Number of service order that were labeled as rework in the current week;
- 'PAST_RETRABALHO': Number of service order that were labeled as rework in the past week;
- 'CURRENT_CORRECAO': Number of service order that were labeled as final fixes in this week;
- 'PAST_CORRECAO': Number of service order that were labeled as final fixes in the past week;
- 'CURRENT_CORRECAO_PRIMEIRA': Total number of service order that were fixed directly (without) rework in the current week;
- 'PAST_CORRECAO_PRIMEIRA': Total number of service order that were fixed directly (without) rework in the past week.

=====
workshop_data: A table that group the service order executed by each workshop in the current and past week. 

COLUMNS: 
- 'DESCRICAO DO SERVICO': Service order type/category;
- 'DESCRICAO DA OFICINA': Workshop name;
- 'CURRENT_GASTO_TOTAL': Total money spend in parts by this mechanic in this service order in this week;
- 'PAST_GASTO_RETRABALHO': Total money spend in parts by this mechanic in this service order in the past week;
- 'CURRENT_GASTO_RETRABALHO': Total money spend / wasted in parts by this mechanic in this service order in the current week that were labeled as rework;
- 'PAST_GASTO_RETRABALHO': Total money spend / wasted in parts by this mechanic in this service order in the past week that were labeled as rework;
- 'CURRENT_NUM_OS': Number of service orders executed in this week;
- 'PAST_NUM_OS': Number of service orders executed in the past week;
- 'CURRENT_RETRABALHO': Number of service order that were labeled as rework in the current week;
- 'PAST_RETRABALHO': Number of service order that were labeled as rework in the past week;
- 'CURRENT_CORRECAO': Number of service order that were labeled as final fixes in this week;
- 'PAST_CORRECAO': Number of service order that were labeled as final fixes in the past week;
- 'CURRENT_CORRECAO_PRIMEIRA': Total number of service order that were fixed directly (without) rework in the current week;
- 'PAST_CORRECAO_PRIMEIRA': Total number of service order that were fixed directly (without) rework in the past week.
=====
vehicle_data: A table that group the service order required by each bus in the current and past week. 

COLUMNS: 
- 'CODIGO DO VEICULO': Vehicle ID;
- 'DESCRICAO DO MODELO': Vehicle Model;
- 'DESCRICAO DO SERVICO': Service order type/category;
- 'DESCRICAO DA OFICINA': Workshop name;
- 'CURRENT_GASTO_TOTAL': Total money spend in parts by this mechanic in this service order in this week;
- 'PAST_GASTO_RETRABALHO': Total money spend in parts by this mechanic in this service order in the past week;
- 'CURRENT_GASTO_RETRABALHO': Total money spend / wasted in parts by this mechanic in this service order in the current week that were labeled as rework;
- 'PAST_GASTO_RETRABALHO': Total money spend / wasted in parts by this mechanic in this service order in the past week that were labeled as rework;
- 'CURRENT_NUM_OS': Number of service orders executed in this week;
- 'PAST_NUM_OS': Number of service orders executed in the past week;
- 'CURRENT_RETRABALHO': Number of service order that were labeled as rework in the current week;
- 'PAST_RETRABALHO': Number of service order that were labeled as rework in the past week;
- 'CURRENT_CORRECAO': Number of service order that were labeled as final fixes in this week;
- 'PAST_CORRECAO': Number of service order that were labeled as final fixes in the past week;
- 'CURRENT_CORRECAO_PRIMEIRA': Total number of service order that were fixed directly (without) rework in the current week;
- 'PAST_CORRECAO_PRIMEIRA': Total number of service order that were fixed directly (without) rework in the past week.

"""

def get_user_prompt(relatorio_previo_md, str_csv_colaboradores, str_csv_oficinas, str_csv_veiculos):
    return f"""
====
mechanic_data:

{str_csv_colaboradores}

====
workshop_data:

{str_csv_oficinas}

====
vehicle_data:

{str_csv_veiculos}
"""