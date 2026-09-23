{{ config(materialized="table") }}

-- Bronze: Liberações mensais de recurso do FGTS por contrato (canal AO_2), desde 2000. Todos os programas; valores negativos são estornos.
-- Fonte: SHPT — sharepoint/ (remessa mensal do Canal FGTS, MCaaaammdd.zip, expandida um CSV por tabela)
{% set padrao = "s3://data-lake-mcid/staging/**/fgts_canal_tab_ao_2_tab_desembolsos_fgts.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
