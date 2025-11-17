#!/usr/bin/env python
# coding: utf-8

# Arquivo com rotinas para buscar os dados do banco para construção do relatório

import pandas as pd
from sql_utils import subquery_modelos, subquery_oficinas, subquery_secoes, subquery_os

class DBDataFetcher:
    def __init__(self, engine):
        self.engine = engine

    def buscar_dados_colaboradores(self, periodo, dias_retrabalho, lista_modelos, lista_oficinas, lista_setor, lista_os):
        str_subquery_modelo = subquery_modelos(lista_modelos, prefix='main.', termo_all="TODOS")
        str_subquery_oficina = subquery_oficinas(lista_oficinas, prefix='main.')
        str_subquery_secao = subquery_secoes(lista_setor, prefix='main.')
        str_subquery_os = subquery_os(lista_os, prefix='main.')

        query = f"""
WITH current_week AS (
   SELECT 
        "COLABORADOR QUE EXECUTOU O SERVICO",
        "DESCRICAO DO SERVICO", 
        COUNT(*) AS current_num_os,
        SUM(pg."VALOR") as current_gasto_total,
        SUM(CASE WHEN retrabalho THEN pg."VALOR" ELSE 0 END) current_gasto_retrabalho,
        SUM(CASE WHEN retrabalho THEN 1 ELSE 0 END) AS current_retrabalho,
        SUM(CASE WHEN correcao THEN 1 ELSE 0 END) AS current_correcao,
        SUM(CASE WHEN correcao_primeira THEN 1 ELSE 0 END) AS current_correcao_primeira
    FROM mat_view_retrabalho_{dias_retrabalho}_dias main
	LEFT JOIN
        view_pecas_desconsiderando_combustivel pg 
    ON
    	main."NUMERO DA OS" = pg."OS"
    WHERE 
    	"DATA DA ABERTURA DA OS"::timestamp  BETWEEN CURRENT_DATE - INTERVAL '{int(periodo)} days' AND CURRENT_DATE + INTERVAL '2 days'
		{str_subquery_modelo}
        {str_subquery_oficina}
        {str_subquery_secao}
        {str_subquery_os}
    GROUP BY "COLABORADOR QUE EXECUTOU O SERVICO", "DESCRICAO DO SERVICO"
),
past_week AS (
    SELECT 
        "COLABORADOR QUE EXECUTOU O SERVICO",
        "DESCRICAO DO SERVICO", 
        COUNT(*) AS past_num_os,
        SUM(pg."VALOR") as past_gasto_total,
        SUM(CASE WHEN retrabalho THEN pg."VALOR" ELSE 0 END) past_gasto_retrabalho,
        SUM(CASE WHEN retrabalho THEN 1 ELSE 0 END) AS past_retrabalho,
        SUM(CASE WHEN correcao THEN 1 ELSE 0 END) AS past_correcao,
        SUM(CASE WHEN correcao_primeira THEN 1 ELSE 0 END) AS past_correcao_primeira
    FROM mat_view_retrabalho_{dias_retrabalho}_dias main
	LEFT JOIN
        view_pecas_desconsiderando_combustivel pg 
    ON
    	main."NUMERO DA OS" = pg."OS"
    WHERE 
    	"DATA DA ABERTURA DA OS"::timestamp BETWEEN CURRENT_DATE - INTERVAL '{int(periodo * 2)} days' AND CURRENT_DATE - INTERVAL '{int(periodo)} days'
		{str_subquery_modelo}
        {str_subquery_oficina}
        {str_subquery_secao}
        {str_subquery_os}
    GROUP BY "COLABORADOR QUE EXECUTOU O SERVICO", "DESCRICAO DO SERVICO"
),
JUNCAO_DADOS_SEMANAIS_COLABORADOR as (
SELECT 
    COALESCE(c."COLABORADOR QUE EXECUTOU O SERVICO", p."COLABORADOR QUE EXECUTOU O SERVICO") AS "COLABORADOR QUE EXECUTOU O SERVICO",
    COALESCE(c."DESCRICAO DO SERVICO", p."DESCRICAO DO SERVICO") AS "DESCRICAO DO SERVICO",

    -- Total Gasto
    COALESCE(c.current_gasto_total, 0) AS "CURRENT_GASTO_TOTAL",
    COALESCE(p.past_gasto_total, 0) AS "PAST_GASTO_TOTAL",
    
    -- Gasto com Retrabalho
    COALESCE(c.current_gasto_retrabalho, 0) AS "CURRENT_GASTO_RETRABALHO",
    COALESCE(p.past_gasto_retrabalho, 0) AS "PAST_GASTO_RETRABALHO",

    -- Número de OS
    COALESCE(c.current_num_os, 0) AS "CURRENT_NUM_OS",
    COALESCE(p.past_num_os, 0) AS "PAST_NUM_OS",
    
    -- Retrabalho
    COALESCE(c.current_retrabalho, 0) AS "CURRENT_RETRABALHO",
    COALESCE(p.past_retrabalho, 0) AS "PAST_RETRABALHO",

    -- Correção
    COALESCE(c.current_correcao, 0) AS "CURRENT_CORRECAO",
    COALESCE(p.past_correcao, 0) AS "PAST_CORRECAO",

    -- Correção Primeira
    COALESCE(c.current_correcao_primeira, 0) AS "CURRENT_CORRECAO_PRIMEIRA",
    COALESCE(p.past_correcao_primeira, 0) AS "PAST_CORRECAO_PRIMEIRA"
FROM current_week c
FULL OUTER JOIN past_week p
  ON c."COLABORADOR QUE EXECUTOU O SERVICO" = p."COLABORADOR QUE EXECUTOU O SERVICO"
 AND c."DESCRICAO DO SERVICO" = p."DESCRICAO DO SERVICO"
ORDER BY "DESCRICAO DO SERVICO"
)
select 
  COALESCE(
    {r"ltrim(regexp_replace(cfo.nome_colaborador, '([A-Z])', ' \1', 'g'))"},
    'NAO INFORMADO'
  ) AS "NOME_COLABORADOR",
main.*
from JUNCAO_DADOS_SEMANAIS_COLABORADOR main
left join 
colaboradores_frotas_os cfo 
on main."COLABORADOR QUE EXECUTOU O SERVICO" = cfo.cod_colaborador
"""
        return pd.read_sql(query, self.engine)
    

    def buscar_dados_oficinas(self, periodo, dias_retrabalho, lista_modelos, lista_oficinas, lista_setor, lista_os):
        str_subquery_modelo = subquery_modelos(lista_modelos, prefix='main.', termo_all="TODOS")
        str_subquery_oficina = subquery_oficinas(lista_oficinas, prefix='main.')
        str_subquery_secao = subquery_secoes(lista_setor, prefix='main.')
        str_subquery_os = subquery_os(lista_os, prefix='main.')

        query = f"""
WITH current_week AS (
   SELECT 
   		"DESCRICAO DA SECAO",
        "DESCRICAO DO SERVICO", 
        "DESCRICAO DA OFICINA",
        COUNT(*) AS current_num_os,
        SUM(pg."VALOR") as current_gasto_total,
        SUM(CASE WHEN retrabalho THEN pg."VALOR" ELSE 0 END) current_gasto_retrabalho,
        SUM(CASE WHEN retrabalho THEN 1 ELSE 0 END) AS current_retrabalho,
        SUM(CASE WHEN correcao THEN 1 ELSE 0 END) AS current_correcao,
        SUM(CASE WHEN correcao_primeira THEN 1 ELSE 0 END) AS current_correcao_primeira
    FROM mat_view_retrabalho_{dias_retrabalho}_dias_distinct main
	LEFT JOIN
        view_pecas_desconsiderando_combustivel pg 
    ON
    	main."NUMERO DA OS" = pg."OS"
    WHERE 
    	"DATA DA ABERTURA DA OS"::timestamp  BETWEEN CURRENT_DATE - INTERVAL '{int(periodo)} days' AND CURRENT_DATE + INTERVAL '2 days'
		{str_subquery_modelo}
        {str_subquery_oficina}
        {str_subquery_secao}
        {str_subquery_os}
    GROUP BY "DESCRICAO DA SECAO", "DESCRICAO DO SERVICO", "DESCRICAO DA OFICINA"
),
past_week AS (
    SELECT 
   		"DESCRICAO DA SECAO",
        "DESCRICAO DO SERVICO", 
        "DESCRICAO DA OFICINA",
        COUNT(*) AS past_num_os,
        SUM(pg."VALOR") as past_gasto_total,
        SUM(CASE WHEN retrabalho THEN pg."VALOR" ELSE 0 END) past_gasto_retrabalho,
        SUM(CASE WHEN retrabalho THEN 1 ELSE 0 END) AS past_retrabalho,
        SUM(CASE WHEN correcao THEN 1 ELSE 0 END) AS past_correcao,
        SUM(CASE WHEN correcao_primeira THEN 1 ELSE 0 END) AS past_correcao_primeira
    FROM mat_view_retrabalho_{dias_retrabalho}_dias_distinct main
	LEFT JOIN
        view_pecas_desconsiderando_combustivel pg 
    ON
    	main."NUMERO DA OS" = pg."OS"
    WHERE 
    	"DATA DA ABERTURA DA OS"::timestamp BETWEEN CURRENT_DATE - INTERVAL '{int(periodo * 2)} days' AND CURRENT_DATE - INTERVAL '{int(periodo)} days'
		{str_subquery_modelo}
        {str_subquery_oficina}
        {str_subquery_secao}
        {str_subquery_os}
    GROUP BY "DESCRICAO DA SECAO", "DESCRICAO DO SERVICO", "DESCRICAO DA OFICINA"
),
JUNCAO_DADOS_SEMANAIS_SERVICO_OFICINA as (
SELECT 
    COALESCE(c."DESCRICAO DO SERVICO", p."DESCRICAO DO SERVICO") AS "DESCRICAO DO SERVICO",
    COALESCE(c."DESCRICAO DA OFICINA", p."DESCRICAO DA OFICINA") AS "DESCRICAO DA OFICINA",

    -- Total Gasto
    COALESCE(c.current_gasto_total, 0) AS "CURRENT_GASTO_TOTAL",
    COALESCE(p.past_gasto_total, 0) AS "PAST_GASTO_TOTAL",
    
    -- Gasto com Retrabalho
    COALESCE(c.current_gasto_retrabalho, 0) AS "CURRENT_GASTO_RETRABALHO",
    COALESCE(p.past_gasto_retrabalho, 0) AS "PAST_GASTO_RETRABALHO",

    -- Número de OS
    COALESCE(c.current_num_os, 0) AS "CURRENT_NUM_OS",
    COALESCE(p.past_num_os, 0) AS "PAST_NUM_OS",
    
    -- Retrabalho
    COALESCE(c.current_retrabalho, 0) AS "CURRENT_RETRABALHO",
    COALESCE(p.past_retrabalho, 0) AS "PAST_RETRABALHO",

    -- Correção
    COALESCE(c.current_correcao, 0) AS "CURRENT_CORRECAO",
    COALESCE(p.past_correcao, 0) AS "PAST_CORRECAO",

    -- Correção Primeira
    COALESCE(c.current_correcao_primeira, 0) AS "CURRENT_CORRECAO_PRIMEIRA",
    COALESCE(p.past_correcao_primeira, 0) AS "PAST_CORRECAO_PRIMEIRA"
FROM current_week c
FULL OUTER JOIN past_week p
  ON c."DESCRICAO DA SECAO" = p."DESCRICAO DA SECAO"
 AND c."DESCRICAO DO SERVICO" = p."DESCRICAO DO SERVICO"
 and c."DESCRICAO DA OFICINA" = p."DESCRICAO DA OFICINA"
ORDER BY "DESCRICAO DO SERVICO", "DESCRICAO DA OFICINA"
)
select 
  * 
from JUNCAO_DADOS_SEMANAIS_SERVICO_OFICINA main
"""
        print(query)
        return pd.read_sql(query, self.engine)
    

    def buscar_dados_veiculos(self, periodo, dias_retrabalho, lista_modelos, lista_oficinas, lista_setor, lista_os):
        str_subquery_modelo = subquery_modelos(lista_modelos, prefix='main.', termo_all="TODOS")
        str_subquery_oficina = subquery_oficinas(lista_oficinas, prefix='main.')
        str_subquery_secao = subquery_secoes(lista_setor, prefix='main.')
        str_subquery_os = subquery_os(lista_os, prefix='main.')

        query = f"""
WITH current_week AS (
   SELECT 
        "CODIGO DO VEICULO",
        "DESCRICAO DO MODELO",
        "DESCRICAO DA OFICINA",
        "DESCRICAO DO SERVICO", 
        COUNT(*) AS current_num_os,
        SUM(pg."VALOR") as current_gasto_total,
        SUM(CASE WHEN retrabalho THEN pg."VALOR" ELSE 0 END) current_gasto_retrabalho,
        SUM(CASE WHEN retrabalho THEN 1 ELSE 0 END) AS current_retrabalho,
        SUM(CASE WHEN correcao THEN 1 ELSE 0 END) AS current_correcao,
        SUM(CASE WHEN correcao_primeira THEN 1 ELSE 0 END) AS current_correcao_primeira
    FROM mat_view_retrabalho_{dias_retrabalho}_dias_distinct main
	LEFT JOIN
        view_pecas_desconsiderando_combustivel pg 
    ON
    	main."NUMERO DA OS" = pg."OS"
    WHERE 
    	"DATA DA ABERTURA DA OS"::timestamp  BETWEEN CURRENT_DATE - INTERVAL '{int(periodo)} days' AND CURRENT_DATE + INTERVAL '2 days'
		{str_subquery_modelo}
        {str_subquery_oficina}
        {str_subquery_secao}
        {str_subquery_os}
    GROUP BY "CODIGO DO VEICULO",
        "DESCRICAO DO MODELO",
        "DESCRICAO DA OFICINA",
        "DESCRICAO DO SERVICO"
),
past_week AS (
       SELECT 
        "CODIGO DO VEICULO",
        "DESCRICAO DO MODELO",
        "DESCRICAO DA OFICINA",
        "DESCRICAO DO SERVICO", 
        COUNT(*) AS past_num_os,
        SUM(pg."VALOR") as past_gasto_total,
        SUM(CASE WHEN retrabalho THEN pg."VALOR" ELSE 0 END) past_gasto_retrabalho,
        SUM(CASE WHEN retrabalho THEN 1 ELSE 0 END) AS past_retrabalho,
        SUM(CASE WHEN correcao THEN 1 ELSE 0 END) AS past_correcao,
        SUM(CASE WHEN correcao_primeira THEN 1 ELSE 0 END) AS past_correcao_primeira
    FROM mat_view_retrabalho_{dias_retrabalho}_dias_distinct main
	LEFT JOIN
        view_pecas_desconsiderando_combustivel pg 
    ON
    	main."NUMERO DA OS" = pg."OS"
    WHERE 
    	"DATA DA ABERTURA DA OS"::timestamp BETWEEN CURRENT_DATE - INTERVAL '{int(periodo * 2)} days' AND CURRENT_DATE - INTERVAL '{int(periodo)} days'
		{str_subquery_modelo}
        {str_subquery_oficina}
        {str_subquery_secao}
        {str_subquery_os}
    GROUP BY "CODIGO DO VEICULO",
        "DESCRICAO DO MODELO",
        "DESCRICAO DA OFICINA",
        "DESCRICAO DO SERVICO"
),
JUNCAO_DADOS_SEMANAIS_VEICULOS as (
SELECT 
    COALESCE(c."CODIGO DO VEICULO", p."CODIGO DO VEICULO") AS "CODIGO DO VEICULO",
    COALESCE(c."DESCRICAO DO MODELO", p."DESCRICAO DO MODELO") AS "DESCRICAO DO MODELO",
    COALESCE(c."DESCRICAO DO SERVICO", p."DESCRICAO DO SERVICO") AS "DESCRICAO DO SERVICO",
    COALESCE(c."DESCRICAO DA OFICINA", p."DESCRICAO DA OFICINA") AS "DESCRICAO DA OFICINA",

    -- Total Gasto
    COALESCE(c.current_gasto_total, 0) AS "CURRENT_GASTO_TOTAL",
    COALESCE(p.past_gasto_total, 0) AS "PAST_GASTO_TOTAL",
    
    -- Gasto com Retrabalho
    COALESCE(c.current_gasto_retrabalho, 0) AS "CURRENT_GASTO_RETRABALHO",
    COALESCE(p.past_gasto_retrabalho, 0) AS "PAST_GASTO_RETRABALHO",

    -- Número de OS
    COALESCE(c.current_num_os, 0) AS "CURRENT_NUM_OS",
    COALESCE(p.past_num_os, 0) AS "PAST_NUM_OS",
    
    -- Retrabalho
    COALESCE(c.current_retrabalho, 0) AS "CURRENT_RETRABALHO",
    COALESCE(p.past_retrabalho, 0) AS "PAST_RETRABALHO",

    -- Correção
    COALESCE(c.current_correcao, 0) AS "CURRENT_CORRECAO",
    COALESCE(p.past_correcao, 0) AS "PAST_CORRECAO",

    -- Correção Primeira
    COALESCE(c.current_correcao_primeira, 0) AS "CURRENT_CORRECAO_PRIMEIRA",
    COALESCE(p.past_correcao_primeira, 0) AS "PAST_CORRECAO_PRIMEIRA"
FROM current_week c
FULL OUTER JOIN past_week p
  ON c."CODIGO DO VEICULO" = p."CODIGO DO VEICULO"
  and c."DESCRICAO DO MODELO" = p."DESCRICAO DO MODELO"
 AND c."DESCRICAO DO SERVICO" = p."DESCRICAO DO SERVICO"
 and c."DESCRICAO DA OFICINA" = p."DESCRICAO DA OFICINA"
ORDER BY "CODIGO DO VEICULO", "DESCRICAO DO SERVICO", "DESCRICAO DA OFICINA"
)
SELECT 
  * 
FROM JUNCAO_DADOS_SEMANAIS_VEICULOS main
"""
        return pd.read_sql(query, self.engine)