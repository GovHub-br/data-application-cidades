{{ config(materialized="table") }}

-- Bronze: Cadastro PJ mensal do Rural — entidade organizadora, composicao do investimento, cisternas e efluentes, prazo de construcao.
-- Fonte: SHPT — sharepoint/Novo MCMV - Rural/
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_CAD_PJ_RURAL_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
