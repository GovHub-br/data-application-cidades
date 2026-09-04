{{ config(materialized="table") }}

select
    variavel_id,
    periodo,
    cast(valor as numeric) / 10.0 as valor_percentual,
    {{ add_metadata_timestamps("silver") }}
from {{ ref("bronze_ibge_pib_construcao_civil") }}
where variavel_id in (6562, 6563, 6564)
