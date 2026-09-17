{{ config(materialized="table") }}

-- Bronze: Dados prioritarios disponibilizados pela SNH, todas as modalidades — a prata filtra RURAL. ATENCAO: e um export avulso, sobrescrito in place na origem, e NAO acompanha o feed mensal.
-- Fonte: SHPT — sharepoint/ (export canonico)
{% set padrao = "s3://data-lake-mcid/staging/**/dados_prioritarios_disponibilizados_snh_empreendimentos.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
