{{ config(materialized="table") }}

-- Bronze: Tabela de domínio das linhas de financiamento do FGTS. É ela que identifica o Pró-Moradia: código `26` = `HAB / PRO-MORADIA`.
-- Fonte: SHPT — sharepoint/ (remessa mensal do Canal FGTS, MCaaaammdd.zip, expandida um CSV por tabela)
{% set padrao = "s3://data-lake-mcid/staging/**/fgts_canal_tdom_ao_1_linha.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
