{{ config(materialized='table') }}

-- Prata do conjuntura: financiamentos SBPE por modalidade
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
--
-- Ingestão nova (plugins/ingestion, 10/2026): a staging guarda a aba
-- `BD_Unidades` como veio, com o cabeçalho da linha 5. O período não tem nome
-- (`column_1`) e as modalidades se repetem para unidades e valores
-- (`Construção`, `Aquisição `, `Total`, depois `Construção_2`…; o espaço em
-- `Aquisição ` é da planilha). Meses futuros vêm vazios e ficam de fora. O teste
-- `conjuntura_abecip_financiamentos_totais` confere Total = Construção + Aquisição.

select
    cast(column_1 as timestamp)::date                       as data_referencia,
    extract(year from cast(column_1 as timestamp))::int     as ano,
    extract(month from cast(column_1 as timestamp))::int    as mes,
    "Construção"::numeric                                   as unidades_construcao,
    "Aquisição "::numeric                                   as unidades_aquisicao,
    "Total"::numeric                                        as unidades_total,
    "Construção_2"::numeric                                 as valor_construcao_milhoes,
    "Aquisição _2"::numeric                                 as valor_aquisicao_milhoes,
    "Total_2"::numeric                                      as valor_total_milhoes,
    {{ lake_dt_ingest() }}                                  as dt_ingest
from {{ ref('bronze_abecip_financiamentos') }}
where column_1 like '____-__-__%'
  and "Total" is not null
