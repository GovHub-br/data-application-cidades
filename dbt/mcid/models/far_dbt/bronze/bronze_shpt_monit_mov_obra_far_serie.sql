{{ config(materialized="table") }}

-- Bronze: Movimento mensal de obra do FAR — TODAS as competências empilhadas.
-- Cada arquivo é um retrato do mês (1 linha por APF), não um acumulado: a série de
-- execução física só existe empilhando. A competência sai do nome (`_MENSAL_<aaaamm>_`)
-- na prata; `_LAYOUT_`, `_SEMANAL_` e `_DIARIO_` não casam com o padrão.
-- Fonte: SHPT — sharepoint/Novo MCMV - FAR/
{% set padrao = "s3://data-lake-mcid/staging/sharepoint/**/*MONIT_MOV_OBRA_FAR_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
