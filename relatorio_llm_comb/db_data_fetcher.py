#!/usr/bin/env python
# coding: utf-8

# Arquivo com rotinas para buscar os dados do banco para construção do relatório

import pandas as pd
from sql_utils import subquery_modelos, subquery_oficinas, subquery_secoes, subquery_os


class DBDataFetcher:
    def __init__(self, engine):
        self.engine = engine

    def buscar_dados_combustivel(self, periodo):
        query = f"""
WITH current_week AS (
    SELECT
        vec_num_id,
        vec_model,
        COUNT(*) AS current_week_total_viagens,
        AVG(km_por_litro) AS current_week_media_km_por_litro,
        SUM(tamanho_linha_km_sobreposicao) as current_week_travelled_km,

        -- Total abaixo da mediana
        100 * SUM(CASE WHEN analise_diff_mediana_90_dias < 0 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*)::NUMERIC AS current_week_perc_abaixo_mediana,

        -- Baixa performance
        100 * SUM(CASE 
            WHEN analise_status_90_dias IN ('BAIXA PERFOMANCE (<= 2 STD)', 'BAIXA PERFORMANCE (<= 1.5 STD)') THEN 1 
            ELSE 0 
        END)::NUMERIC / COUNT(*)::NUMERIC AS current_week_perc_baixa_perfomance,

        -- Consumo
        SUM(total_comb_l) AS current_week_total_consumo_litros,
        SUM(ABS(total_comb_l - (tamanho_linha_km_sobreposicao / analise_valor_mediana_90_dias))) AS current_week_litros_excedentes,

        -- Erro telemetria
       	100 * SUM(CASE WHEN analise_status_90_dias = 'ERRO TELEMETRIA (>= 2.0 STD)' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*)::NUMERIC AS current_week_perc_erro_telemetria

    FROM rmtc_viagens_analise_mix
    WHERE 
        encontrou_linha = true
        AND analise_num_amostras_90_dias >= 5
        AND km_por_litro BETWEEN 1 AND 5
        AND CAST("dia" AS date) BETWEEN CURRENT_DATE - INTERVAL '{int(periodo)} days' AND CURRENT_DATE + INTERVAL '2 days'
    GROUP BY vec_num_id, vec_model
    HAVING COUNT(*) >= 10
),

past_week AS (
    SELECT
        vec_num_id,
        vec_model,
        COUNT(*) AS past_week_total_viagens,
        AVG(km_por_litro) AS past_week_media_km_por_litro,
		SUM(tamanho_linha_km_sobreposicao) as past_week_travelled_km,
		
        100 * SUM(CASE WHEN analise_diff_mediana_90_dias < 0 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*)::NUMERIC AS past_week_perc_abaixo_mediana,
        
        100 * SUM(CASE 
            WHEN analise_status_90_dias IN ('BAIXA PERFOMANCE (<= 2 STD)', 'BAIXA PERFORMANCE (<= 1.5 STD)') THEN 1 
            ELSE 0 
        END)::NUMERIC / COUNT(*)::NUMERIC AS past_week_perc_baixa_perfomance,

        SUM(total_comb_l) AS past_week_total_consumo_litros,
        SUM(ABS(total_comb_l - (tamanho_linha_km_sobreposicao / analise_valor_mediana_90_dias))) AS past_week_litros_excedentes,

        100 * SUM(CASE WHEN analise_status_90_dias = 'ERRO TELEMETRIA (>= 2.0 STD)' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*)::NUMERIC AS past_week_perc_erro_telemetria

    FROM rmtc_viagens_analise_mix
    WHERE 
        encontrou_linha = true
        AND analise_num_amostras_90_dias >= 5
        AND km_por_litro BETWEEN 1 AND 5
        AND CAST("dia" AS date) BETWEEN CURRENT_DATE - INTERVAL '{int(periodo * 2)} days' AND CURRENT_DATE - INTERVAL '7 days'
    GROUP BY vec_num_id, vec_model
    HAVING COUNT(*) >= 10
)

SELECT 
    COALESCE(c.vec_num_id, p.vec_num_id) AS vec_num_id,
    COALESCE(c.vec_model, p.vec_model) AS vec_model,

    -- Totals and averages
    COALESCE(c.current_week_total_viagens, 0) AS current_week_total_viagens,
    COALESCE(p.past_week_total_viagens, 0) AS past_week_total_viagens,

    COALESCE(c.current_week_media_km_por_litro, 0) AS current_week_media_km_por_litro,
    COALESCE(p.past_week_media_km_por_litro, 0) AS past_week_media_km_por_litro,
    
    COALESCE(c.current_week_travelled_km, 0) AS current_week_travelled_km,
    COALESCE(p.past_week_travelled_km, 0) AS past_week_travelled_km,

    -- Consumption and performance
    COALESCE(c.current_week_total_consumo_litros, 0) AS current_week_total_consumo_litros,
    COALESCE(p.past_week_total_consumo_litros, 0) AS past_week_total_consumo_litros,

    COALESCE(c.current_week_litros_excedentes, 0) AS current_week_litros_excedentes,
    COALESCE(p.past_week_litros_excedentes, 0) AS past_week_litros_excedentes,

    -- Performance indicators
    COALESCE(c.current_week_perc_baixa_perfomance, 0) AS current_week_perc_baixa_perfomance,
    COALESCE(p.past_week_perc_baixa_perfomance, 0) AS past_week_perc_baixa_perfomance,

    COALESCE(c.current_week_perc_abaixo_mediana, 0) AS current_week_perc_abaixo_mediana,
    COALESCE(p.past_week_perc_abaixo_mediana, 0) AS past_week_perc_abaixo_mediana,

    COALESCE(c.current_week_perc_erro_telemetria, 0) AS current_week_perc_erro_telemetria,
    COALESCE(p.past_week_perc_erro_telemetria, 0) AS past_week_perc_erro_telemetria

FROM current_week c
FULL OUTER JOIN past_week p
  ON c.vec_num_id = p.vec_num_id
 AND c.vec_model = p.vec_model
ORDER BY current_week_perc_baixa_perfomance DESC NULLS LAST;
        """
        return pd.read_sql(query, self.engine)

    def buscar_pecas_trocadas_top_veiculos(self, periodo, top_n=25):
        query = f"""
-- 1. Get the top 25 vehicles by baixa performance
WITH top_25_veiculos AS (
    SELECT vec_num_id
    FROM (
        SELECT
            vec_num_id,
            100 * SUM(
                CASE 
                    WHEN analise_status_90_dias IN ('BAIXA PERFOMANCE (<= 2 STD)', 'BAIXA PERFORMANCE (<= 1.5 STD)') 
                    THEN 1 ELSE 0 
                END
            )::NUMERIC / COUNT(*)::NUMERIC AS current_week_perc_baixa_perfomance
        FROM rmtc_viagens_analise_mix
        WHERE 
            encontrou_linha = true
            AND analise_num_amostras_90_dias >= 5
            AND km_por_litro BETWEEN 1 AND 5
            AND CAST("dia" AS date) 
                BETWEEN CURRENT_DATE - INTERVAL '7 days' 
                AND CURRENT_DATE + INTERVAL '2 days'
        GROUP BY vec_num_id
        HAVING COUNT(*) >= 10
        ORDER BY current_week_perc_baixa_perfomance DESC NULLS LAST
        LIMIT {top_n}
    ) ranked
),

-- 2. Get the most recent part per product/vehicle
ultima_peca_por_veiculo_produto AS (
    SELECT 
        pg."EQUIPAMENTO",
        pg."PRODUTO",
        TO_TIMESTAMP(pg."DATA", 'DD/MM/YYYY') AS data_peca,
        pg."COD_PRODUTO",
        pg."VALOR",
        ROW_NUMBER() OVER (
            PARTITION BY pg."EQUIPAMENTO", pg."PRODUTO"
            ORDER BY TO_TIMESTAMP(pg."DATA", 'DD/MM/YYYY') DESC
        ) AS rn
    FROM view_pecas_desconsiderando_combustivel pg
),

-- 3. Filter only top 25 vehicles
ultima_peca_top25 AS (
    SELECT up.*
    FROM ultima_peca_por_veiculo_produto up
    JOIN top_25_veiculos t25
      ON up."EQUIPAMENTO" = t25.vec_num_id::text
    WHERE up.rn = 1
      AND up.data_peca >= '2025-01-01'
),

-- 4. Precompute odometer_atual per vehicle (only once)
ultimo_odometro_por_veiculo AS (
    SELECT DISTINCT ON ("Description")
        "Description",
        "maior_km_dia" AS odometro_atual
    FROM mat_view_odometro_diario_completo
    ORDER BY "Description", "year_month_day" DESC
),

-- 5. Get odometer at the date of the part change
odometro_na_troca AS (
    SELECT 
        m."Description",
        m."year_month_day"::date AS data_odometro,
        m."maior_km_dia"
    FROM mat_view_odometro_diario_completo m
)

-- 6. Final computation
SELECT 
    up."EQUIPAMENTO" as vec_num_id,
    up."PRODUTO" as peca,
    TO_CHAR(up.data_peca, 'YYYY-MM-DD') AS data_troca,
    ROUND((uo.odometro_atual - ot."maior_km_dia")) AS km_rodados_desde_troca,
	(CURRENT_DATE - up.data_peca::date) AS dias_desde_troca
FROM ultima_peca_top25 up
LEFT JOIN ultimo_odometro_por_veiculo uo
  ON uo."Description" = up."EQUIPAMENTO"
LEFT JOIN LATERAL (
    SELECT ot2."maior_km_dia"
    FROM odometro_na_troca ot2
    WHERE ot2."Description" = up."EQUIPAMENTO"
      AND ot2.data_odometro <= up.data_peca::date
    ORDER BY ot2.data_odometro DESC
    LIMIT 1
) ot ON TRUE
WHERE uo.odometro_atual IS NOT NULL
  AND ot."maior_km_dia" IS NOT NULL
ORDER BY up."EQUIPAMENTO", up."PRODUTO" DESC NULLS LAST;
"""
        return pd.read_sql(query, self.engine)

    def buscar_os_top_veiculos(self, periodo, top_n=25):
        query = f"""
WITH top_25_veiculos AS (
    SELECT vec_num_id
    FROM (
        SELECT
            vec_num_id,
            100 * SUM(
                CASE 
                    WHEN analise_status_90_dias IN ('BAIXA PERFOMANCE (<= 2 STD)', 'BAIXA PERFORMANCE (<= 1.5 STD)') 
                    THEN 1 ELSE 0 
                END
            )::NUMERIC / COUNT(*)::NUMERIC AS current_week_perc_baixa_perfomance
        FROM rmtc_viagens_analise_mix
        WHERE 
            encontrou_linha = true
            AND analise_num_amostras_90_dias >= 5
            AND km_por_litro BETWEEN 1 AND 5
            AND CAST("dia" AS date) 
                BETWEEN CURRENT_DATE - INTERVAL '{int(periodo)} days' 
                AND CURRENT_DATE + INTERVAL '2 days'
        GROUP BY vec_num_id
        HAVING COUNT(*) >= 10
        ORDER BY current_week_perc_baixa_perfomance DESC NULLS LAST
        LIMIT {top_n}
    ) ranked
)
SELECT 
        "CODIGO DO VEICULO" as vec_num_id,
        "DESCRICAO DO MODELO" as vec_model,
        REPLACE("DESCRICAO DO SERVICO", ',', '_') AS "servico",
		TO_CHAR("DATA DA ABERTURA DA OS"::timestamp, 'YYYY-MM-DD') AS "data_os",
		CASE 
			WHEN "COMPLEMENTO DO SERVICO" <> ''
			THEN REPLACE("COMPLEMENTO DO SERVICO", ',', '_')
			ELSE 'NÃO FORNECIDO'
		END AS "COMENTARIO_OS"
   	FROM mat_view_retrabalho_30_dias_distinct main
	WHERE 
    	"DATA DA ABERTURA DA OS"::timestamp  BETWEEN CURRENT_DATE - INTERVAL '30 days' 
        AND CURRENT_DATE + INTERVAL '2 days'

    	and "CODIGO DO VEICULO" in (select * from top_25_veiculos)
        """
        return pd.read_sql(query, self.engine)
