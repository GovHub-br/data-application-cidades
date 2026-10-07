{{ config(materialized="table") }}

-- Snapshot mais recente de contratação do Reforma Casa Brasil recebido pelo
-- SFTP/GEFUS. Cada arquivo é fotografia completa; empilhar competências duplicaria
-- contratos. A Bronze preserva texto, layout e colunas de linhagem da staging.
{% set padrao = "s3://data-lake-mcid/staging/sftp/fabrica/GEFUS/PMCMV_REFORMAS_MCID_*.parquet" %}

select *
from {{ fonte_lake(
    'reforma_contratos',
    'lake_staging_reforma_casa_brasil',
    filename=true,
    union_by_name=true
) }} as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
