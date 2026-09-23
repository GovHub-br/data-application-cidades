{{ config(materialized="table") }}

-- Bronze: Tabela de domínio dos municípios do FGTS: traduz o código de município da CAIXA (4 dígitos) para o código IBGE (7 dígitos).
-- Fonte: SHPT — sharepoint/ (remessa mensal do Canal FGTS, MCaaaammdd.zip, expandida um CSV por tabela)
{% set padrao = "s3://data-lake-mcid/staging/**/fgts_canal_tdom_ao_1_municipios.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
