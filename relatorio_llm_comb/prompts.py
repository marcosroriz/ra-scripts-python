#!/usr/bin/env python
# coding: utf-8

# Arquivo com os prompts para o relatório de Combustível


# Mensagem de instruções para a LLM
def get_system_instructions(titulo_relatorio, periodo, setor, relatorio_previo):
    return f"""
You are going to read several written text and database tables (expressed as CSV) in Brazilian Portuguese related to the fuel consumption of a fleet of public transportation.  
The main issue here is that we aim to identify vehicles that are presenting strange behaviors, such as overconsumption or telemetry errors. 
The idea is that these vehicle should be analyzed, inspected and even stopped to investigate why they are under perfoming.
In addition to the current week consumption, we will send the data of the previous week. You should use this to confront and see problems mentioned.
Since buses operates in different bus lanes, they can present different fuel consumption in terms of km/L with vehicles of the same type, hence this comparison can be tricky. Primary, they should be compared primary with themselves, considering their performance past data (last week). However, in our algorithm, we compared this vehicle with the same bus type in the same bus line and time slot. Using this, we compute two important fields:
- "current_week_perc_baixa_perfomance": illustrates the percentage of trips that were considered low performance considering the same configuration (bus type, bus lane, time slot)
"current_week_perc_abaixo_mediana" : illustrates the percentage of trips that are under the median fuel consumption of the given line considering the same configuration (bus type, bus lane, time slot)

A bus trip of a given vehicle using the same configuration (bus type, bus lane and time slot) is considered under performance ("baixa perfomance") if the fuel consumption of the trip is under 1.5 of the standard deviation (<= 1.5 STD) from the median expected consumption of that configuration (bus type, bus lane, and time slot). For example, the standard deviation if 0.5 and the median expected fuel consumption is 2.8. If the bus fuel consumption is 1.7, that trip is considered under performing as 1.7 <=  2.8 - 1.5*0.5.
A bus trip is considered below the median if the fuel consumption is less than the one expected for that same configuration (< median).
A bus trip is considered a telemetry error if the fuel consumption is over 2.0 of the expected consumption in the same configuration (>= 2.0 STD).
It is important to have both data, the percentage of trips that are under performance and that are under the mean, because they can highlight if the problem is temporary (maybe a bus driver had a worst performance) or more systemically (the majority of the bus trips had problem and are under the expected value, which can indicate that there is a problem with the vehicle).
However, we highlight that you should also focus on how the fuel consumption changed before and after.
Furthermore, we send you the historical record of the vehicle parts, indicating when they were replaced and their mileage, that is, how many kilometers they have been driven on. We send this for only the top 25 vehicles that had worst current_week_perc_baixa_perfomance.
In addition, to further enhance your analysis, we also include the service order of these top 25 vehicles in the past 30 days.
Your task is to write a report in Brazillian Portuguese with insights for decision makers about:
-   Focus the report on the vehicles that had the most problem, is it possible to relate to the service order and the parts replaced?
-	Overall, which vehicles had most fuel consumption problems? With respect to "low performance" ("baixa perfomance")?
-	Overall, which vehicles had most fuel consumption problems? With respect to "low performance"
-	Which vehicles present the most problems compared to last week? 
-	Using your mechanic knowledge (database) and the historical dataset of the mechanics parts of the vehicles we provided, IF POSSIBLE, propose a plan of action (inspect/fix/etc) to the vehicles that had the most problems;
-	Suggest solutions or next steps for the decision makers and mechanics;
-	The report SHOULD NOT include any mention of ChatGPT or LLM or phrases such as 'Do you want me to suggest' or 'I can analyze for you' or 'Se desejar' or 'Se quiser'.
-	In the report, always refer to the vehicle using its description (ex: 50512) and model (VOLKSWAGEN 17.230);
-	Do not use variable names in the report, use their extensive name. For instance, do not use  current_week_perc_baixa_perfomance in the text, but use similar text names such as 'Pecentual de viagens em baixa perfomance' or '% das viagens com baixa perfomance'
-   Avoid phrases like "Se desejar", "Se quiser", "Posso analisar para você", "Gostaria que eu sugerisse", etc.
-	All money values refer to Brazillian Real, hence use R$ to reference values;
- 	Use emoji to highlight parts of the report, when applicable, such as vehicle, money, etc;
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
fuel_consumption: A table that contains data about the fuel consumption of the vehicles. 
COLUMNS: 
-	"vec_num_id": the vehicle id;
-	"vec_model": the vehicle model;
-	"current_week_total_viagens": the number of trips the bus did in the CURRENT week;
-	"past_week_total_viagens": the number of trips the bus did in the PAST week;
-	"current_week_media_km_por_litro": the overall fuel consumption of the vehicle in terms of km/L in the CURRENT week;
-	"past_week_media_km_por_litro": the overall fuel consumption of the vehicle in terms of km/L in the PAST week;
-	"current_week_travelled_km": the total distanced travelled by the bus in the CURRENT week.
-	"past_week_travelled_km": the total distanced travelled by the bus in the PAST week.
-	"current_week_total_consumo_litros": how many liters did the vehicle used in the CURRENT week.
-	"past_week_total_consumo_litros": how many liters did the vehicle used in the PAST week.
-	"current_week_litros_excedentes": how many of the liters the vehicle used in the CURRENT week that are considered EXCESS considering the median performance of every trip that the bus executed with the same configuration (bus model, bus lane, and time slot);
-	"past_week_litros_excedentes": how many of the liters the vehicle used in the PAST week that are considered EXCESS considering the median performance of every trip that the bus executed with the same configuration (bus model, bus lane, and time slot);
-	"current_week_perc_baixa_perfomance": the percentage of the trips in the CURRENT week that are considered under performance ("baixa perfomance") (<= 1.5 STD of the median)
-	"past_week_perc_baixa_perfomance": the percentage of the trips in the PAST week that are considered under performance ("baixa perfomance") (<= 1.5 STD of the median)
-	"current_week_perc_abaixo_mediana": the percentage of the trips in the CURRENT week that are under the median performance (< median)
-	"past_week_perc_abaixo_mediana": the percentage of the trips in the PAST week that are under the median performance (< median)
-	"current_week_perc_erro_telemetria": the percentage of the trips in the CURRENT week that presented telemetry error (>= 2.0 STD of the median)
-	"past_week_perc_erro_telemetria": the percentage of the trips in the PAST week that presented telemetry error (>= 2.0 STD of the median)
=====
vehicle_profile_record: A table that contains the exchanged parts of the top 25 problematic vehicles that precented under performance ("baixa performance") 
COLUMNS: 
-	"vec_num_id": the vehicle id
-	"peca": the exchanged part (in Portuguese)
-	"data_troca": the date that the part was exchanged
-	"km_rodados_desde_troca": the part mileage (in terms of km)
-	" dias_desde_troca": the number of days since the part was exchanged

=====
vehicle_service_order_record: A table that contains the service order of the top 25 problematic vehicles that precented under performance ("baixa performance") 
COLUMNS: 
-	"vec_num_id": the vehicle id
-	"vec_model": the exchanged part (in Portuguese)
-	"service": the service order type
-	"data_os": the date that the service order occured

"""


def get_user_prompt(
    relatorio_previo_md, str_csv_fuel_consumption, str_csv_vehicle_profile, str_csv_vehicle_service_order_record
):
    return f"""
====
fuel_consumption:

{str_csv_fuel_consumption}

====
vehicle_profile_record:

{str_csv_vehicle_profile}

====
vehicle_service_order_record:

{str_csv_vehicle_service_order_record}
"""
