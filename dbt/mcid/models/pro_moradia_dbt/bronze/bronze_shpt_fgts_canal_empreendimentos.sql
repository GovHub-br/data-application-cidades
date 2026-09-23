{{ config(materialized="table") }}

-- Bronze: Cadastro de empreendimentos do FGTS (canal AO_1): nome, projeto/objeto, município (código CAIXA) e metas de UH, população beneficiada e empregos. Todos os programas.
-- Fonte: SHPT — sharepoint/ (remessa mensal do Canal FGTS, MCaaaammdd.zip, expandida um CSV por tabela)
{% set padrao = "s3://data-lake-mcid/staging/**/fgts_canal_tab_ao_1_tab_empreendimentos.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
