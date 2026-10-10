{{ config(materialized="table") }}

-- Bronze: Acompanhamento do Trabalho Social (PTS) do PNHR pelo Banco do Brasil. Layout proprio, diferente do da CAIXA. Mesma ressalva de export avulso.
-- Fonte: SHPT — sharepoint/ (export canonico)
{% set padrao = "s3://data-lake-mcid/staging/**/base_trabalho_social_pnhr_bb.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
