{{ config(materialized="table") }}

select
    cast(data_referencia as date) as data_referencia,
    cast(
        imoveis_residenciais_locacao_var_mensal_total as numeric
    ) as imoveis_residenciais_locacao_var_mensal_total,
    cast(
        imoveis_residenciais_locacao_var_ano_total as numeric
    ) as imoveis_residenciais_locacao_var_ano_total,
    cast(fonte as varchar) as fonte,
    cast(dt_ingest as timestamp) as dt_ingest
from {{ source("fipe", "indice_locacao") }}
