{{ config(materialized="table") }}

-- Bronze: Acompanhamento mensal de obra por contrato do FGTS (canal AO_2): percentual previsto e realizado acumulado, situação da obra e providências. Todos os programas.
-- Fonte: SHPT — sharepoint/ (remessa mensal do Canal FGTS, MCaaaammdd.zip, expandida um CSV por tabela)
{% set padrao = "s3://data-lake-mcid/staging/**/fgts_canal_tab_ao_2_tab_execucoes_obras.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
