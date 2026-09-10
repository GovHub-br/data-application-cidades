{{ config(materialized="table") }}

-- Bronze: Movimento mensal de obra do FAR — medições físicas e entregas.
-- Fonte: SHPT — sharepoint/Novo MCMV - FAR/
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_MOV_OBRA_FAR_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
