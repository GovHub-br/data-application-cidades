{{ config(materialized="table") }}

-- Bronze: Evolução física da obra do Rural — TODAS as competências empilhadas, cópia fiel.
-- Cada arquivo é um retrato do mês (1 linha por APF), não um acumulado: a série de
-- execução física só existe empilhando. A competência sai do nome (`_MENSAL_<aaaamm>_`)
-- na prata; `_LAYOUT_`, `_SEMANAL_` e `_DIARIO_` não casam com o padrão. Remessas de
-- 202602 também foram arquivadas na pasta do FAR; o glob recursivo pega as duas cópias e a
-- prata fica com uma por APF e mês.
-- Fonte: SHPT — sharepoint/Novo MCMV - Rural/ (e a cópia em Novo MCMV - FAR/)
{% set padrao = "s3://data-lake-mcid/snh/staging/**/*MONIT_MOV_OBRA_RURAL_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
