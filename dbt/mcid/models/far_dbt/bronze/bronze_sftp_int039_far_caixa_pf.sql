{{ config(materialized="table") }}

-- Bronze: carteira de contratos PF do FAR na CAIXA (INT039).
-- Retrato completo por remessa; fica só a mais recente.
{% set padrao = "s3://data-lake-mcid/staging/sftp/fabrica/GEFUS/INT039_MinisterioCidades_FAR_CAIXA_PF_*.parquet" %}

select *
from {{ fonte_lake(
    'far_contratos_pf_caixa',
    'lake_staging_far',
    filename=true,
    union_by_name=true
) }} as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
