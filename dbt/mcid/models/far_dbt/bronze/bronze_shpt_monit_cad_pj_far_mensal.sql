{{ config(materialized="table") }}

-- Bronze: Cadastro PJ mensal do FAR — dados cadastrais do empreendimento contratado.
-- Fonte: SHPT — sharepoint/Novo MCMV - FAR/
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_CAD_PJ_FAR_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
