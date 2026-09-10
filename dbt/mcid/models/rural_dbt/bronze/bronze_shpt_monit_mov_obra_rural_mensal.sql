{{ config(materialized="table") }}

-- Bronze: Evolucao fisica da obra do Rural, retrato mensal.
-- Fonte: SHPT — sharepoint/Novo MCMV - Rural/
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_MOV_OBRA_RURAL_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
