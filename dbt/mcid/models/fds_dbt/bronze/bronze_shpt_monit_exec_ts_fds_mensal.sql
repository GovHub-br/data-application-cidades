{{ config(materialized="table") }}

-- Bronze: Execução do trabalho social do FDS por empreendimento.
-- Fonte: SHPT — sharepoint/Novo MCMV - FDS/
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_EXEC_TS_FDS_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
