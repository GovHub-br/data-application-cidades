{{ config(materialized="table") }}

-- Bronze: carteira de contratos PF do FAR no Banco do Brasil (INT021).
-- Retrato completo por remessa; fica só a mais recente.
{% set padrao = "s3://data-lake-mcid/staging/sftp/fabrica/GEFUS/INT021_MinisterioCidades_FAR_BB_PF_*.parquet" %}

select *
from {{ fonte_lake(
    'far_contratos_pf_bb',
    'lake_staging_far',
    filename=true,
    union_by_name=true
) }} as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
