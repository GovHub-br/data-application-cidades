{{ config(materialized="table") }}

-- Bronze: Acompanhamento do Trabalho Social (PTS) do PNHR pela CAIXA. Export avulso: o feed datado vira um parquet por aba e o sufixo da aba muda de nome todo mes, entao nao ha padrao estavel para pescar a aba certa.
-- Fonte: SHPT — sharepoint/ (export canonico)
{% set padrao = "s3://data-lake-mcid/staging/**/base_trabalho_social_pnhr_rural_caixa.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
