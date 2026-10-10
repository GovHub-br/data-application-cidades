{{ config(materialized="table") }}

-- Bronze: Liberacoes financeiras do Rural, decompostas por componente (obra, TS, ATEC, cisternas/efluentes, custos indiretos).
-- Fonte: SHPT — sharepoint/Novo MCMV - Rural/
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_MOV_FINANC_RURAL_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
