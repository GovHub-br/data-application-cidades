{{ config(materialized="table") }}

-- Bronze: Operações do FGTS com o setor público cuja obra está paralisada (canal AO_2): dias sem evolução, faixa e motivo da paralisação, plano de ação.
-- Fonte: SHPT — sharepoint/ (remessa mensal do Canal FGTS, MCaaaammdd.zip, expandida um CSV por tabela)
{% set padrao = "s3://data-lake-mcid/staging/**/fgts_canal_tab_ao_2_operacoes_paralisadas_fgts_setorpublico.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
