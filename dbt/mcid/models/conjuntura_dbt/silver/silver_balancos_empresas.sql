{{ config(materialized="table") }}

select
    nome_empresa,
    ano_balanco,
    trimestre_balanco,

    lancamento,
    vendas,
    {{ add_metadata_timestamps("silver", has_ingest_date=false) }}
from {{ source("conjuntura_bronze", "bronze_balancos_empresas") }}
