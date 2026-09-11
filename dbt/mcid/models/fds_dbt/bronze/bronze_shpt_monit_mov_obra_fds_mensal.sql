{{ config(materialized="table") }}

-- Bronze: Movimento mensal de obra do FDS — medições físicas e entregas.
-- Fonte: SHPT — sharepoint/Novo MCMV - FDS/ (a competência atual chega na pasta do FAR)
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_MOV_OBRA_FDS_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
