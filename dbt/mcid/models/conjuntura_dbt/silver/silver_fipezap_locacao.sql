{{ config(materialized="table") }}

select
    data_referencia::date as data_referencia,
    imoveis_residenciais_locacao_var_mensal_total::numeric as var_mensal,
    imoveis_residenciais_locacao_var_ano_total::numeric as var_ano,
    round(
        (
            (1 + imoveis_residenciais_locacao_var_mensal_total::numeric) / nullif(
                1 + lag(imoveis_residenciais_locacao_var_mensal_total::numeric, 12) over (
                    order by data_referencia::date
                ),
                0
            )
            - 1
        )
        * 100,
        2
    ) as var_12_meses_calc,
    {{ add_metadata_timestamps("silver") }}
from {{ ref("bronze_fipezap_locacao") }}
