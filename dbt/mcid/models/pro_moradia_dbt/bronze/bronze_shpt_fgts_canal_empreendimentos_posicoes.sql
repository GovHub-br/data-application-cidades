{{ config(materialized="table") }}

-- Bronze: Última posição de obra por empreendimento do FGTS (canal AO_1): percentual executado e datas de início, término e inauguração. Todos os programas.
-- Fonte: SHPT — sharepoint/ (remessa mensal do Canal FGTS, MCaaaammdd.zip, expandida um CSV por tabela)
{% set padrao = "s3://data-lake-mcid/staging/**/fgts_canal_tab_ao_1_tab_empreendimentos_posicoes.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
