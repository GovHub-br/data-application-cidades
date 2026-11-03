{{ config(materialized="table") }}

-- Bronze: contratos das famílias do Novo MCMV FAR (MONT_CONT_PF_FAR, layout 015).
-- Retrato completo por remessa; fica só a mais recente.
{% set padrao = "s3://data-lake-mcid/staging/sftp/fabrica/GEFUS/MONT_CONT_PF_FAR_2*.parquet" %}

select *
from {{ fonte_lake(
    'far_contratos_pf_novo',
    'lake_staging_far',
    filename=true,
    union_by_name=true
) }} as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
