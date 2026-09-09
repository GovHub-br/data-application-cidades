{{ config(materialized="table") }}

-- Bronze: Consolidado GFAR — propostas, seleção e contratação do Novo MCMV FAR.
-- Fonte: SHPT — sharepoint/Novo MCMV - FAR/
{% set padrao = "s3://data-lake-mcid/staging/**/*HIS_MCIDADES_CONSOLIDADO_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
