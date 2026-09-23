{{ config(materialized="table") }}

-- Bronze: Todas as propostas apresentadas ao Novo MCMV FNHIS Sub-50, com o resultado da seleção (selecionada, não enquadrada, cota insuficiente da UF...), a justificativa de não enquadramento e as UH propostas.
-- Fonte: SHPT — sharepoint/Novo MCMV - FNHIS Sub 50/ (resultado da seleção, portaria MCID nº 673/2024)
{% set padrao = "s3://data-lake-mcid/staging/**/todas_propostas_FNHIS_v0_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
