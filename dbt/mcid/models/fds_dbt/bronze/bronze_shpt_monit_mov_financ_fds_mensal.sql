{{ config(materialized="table") }}

-- Bronze: Movimento financeiro mensal do FDS — série de liberações e desembolsos.
-- Fonte: SHPT — sharepoint/Novo MCMV - FDS/
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_MOV_FINANC_FDS_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
