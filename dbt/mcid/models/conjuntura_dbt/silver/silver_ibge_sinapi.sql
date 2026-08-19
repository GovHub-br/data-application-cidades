{{ config(materialized="table") }}

with
    sinapi as (
        select
            data_referencia,
            max(case when variavel_id = 48 then valor / 100.0 end) as custo_m2,
            max(case when variavel_id = 1196 then valor / 100.0 end) as var_mes,
            max(case when variavel_id = 1197 then valor / 100.0 end) as var_ano,
            max(case when variavel_id = 1198 then valor / 100.0 end) as var_12m,
            max(dt_ingest) as dt_ingest
        from {{ source("conjuntura_bronze", "bronze_ibge_sinapi") }}
        group by data_referencia
    )

select
    data_referencia,
    custo_m2,
    var_mes,
    var_ano,
    var_12m,
    {{ add_metadata_timestamps("silver") }}
from sinapi
