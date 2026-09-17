{{ config(materialized="table") }}

-- Bronze: Cadastro PF do Rural — beneficiarios com perfil socioeconomico (renda, pessoas por familia, Bolsa Familia, BPC).
-- Fonte: SHPT — sharepoint/Novo MCMV - Rural/
{% set padrao = "s3://data-lake-mcid/staging/**/*MONIT_CADASTRO_PF_RURAL_MENSAL_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
