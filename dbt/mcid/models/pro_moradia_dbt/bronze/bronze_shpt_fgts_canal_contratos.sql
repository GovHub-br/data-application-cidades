{{ config(materialized="table") }}

-- Bronze: Contratos do FGTS (canal AO_1), de TODOS os programas do fundo — a prata filtra o Pró-Moradia pela linha `26`. Um contrato por linha, com linha, modalidade, tomador, agente financeiro, valores e ano do orçamento.
-- Fonte: SHPT — sharepoint/ (remessa mensal do Canal FGTS, MCaaaammdd.zip, expandida um CSV por tabela)
{% set padrao = "s3://data-lake-mcid/staging/**/fgts_canal_tab_ao_1_contratos_fgts.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
