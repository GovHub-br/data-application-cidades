{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: financiamentos SBPE por modalidade
-- (Construção / Aquisição), mensal, da ABECIP.
--
-- Substitui o preenchimento manual do indicador "Financiamentos
-- Habitacionais (UH) — SBPE Const." (Página 2 do boletim).
--
-- Validado em 2026-08-29: a soma trimestral de `unidades_construcao` bate
-- EXATO com o que os boletins publicam — 1T2025 = 19.130, 3T2025 = 43.782,
-- 4T2025 = 47.766, 1T2026 = 47.609 — e o acumulado de 12 meses até mar/2026
-- (161.338) também. O `unidades_total` confere com a extração independente
-- do colega em `staging/abecip/financiamentos_sbpe_mensal.parquet`.

select
    data_referencia::date                       as data_referencia,
    extract(year from data_referencia::date)::int  as ano,
    extract(month from data_referencia::date)::int as mes,
    unidades_construcao::numeric                as unidades_construcao,
    unidades_aquisicao::numeric                 as unidades_aquisicao,
    unidades_total::numeric                     as unidades_total,
    valor_construcao_milhoes::numeric           as valor_construcao_milhoes,
    valor_aquisicao_milhoes::numeric            as valor_aquisicao_milhoes,
    valor_total_milhoes::numeric                as valor_total_milhoes,
    dt_ingest
from {{ ref('bronze_continuo_abecip_financiamentos') }}
