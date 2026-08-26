{{ config(materialized="table") }}

with
    construcao as (
        select
            periodo,
            data_referencia,
            -- Ano e mês do período móvel
            left(periodo, 4)::int as ano,
            right(periodo, 2)::int as mes,
            -- Trimestre móvel baseado no mês central
            case
                when right(periodo, 2)::int in (1, 2, 3)
                then 1
                when right(periodo, 2)::int in (4, 5, 6)
                then 2
                when right(periodo, 2)::int in (7, 8, 9)
                then 3
                when right(periodo, 2)::int in (10, 11, 12)
                then 4
            end as trimestre,
            max(case when categoria_id = '47949' then valor end) as ocupados_construcao,
            max(case when categoria_id = '47946' then valor end) as ocupados_total,
            max(dt_ingest) as dt_ingest
        from {{ ref("bronze_ibge_pnadc_ocupados_construcao") }}
        group by periodo, data_referencia
    )

select
    periodo,
    data_referencia,
    ano,
    mes,
    trimestre,
    ocupados_construcao,
    ocupados_total,
    -- Variação vs período anterior
    round(
        (
            (
                ocupados_construcao
                / nullif(lag(ocupados_construcao) over (order by periodo), 0)
            )
            - 1
        )
        * 100,
        1
    ) as var_mes,
    -- Variação vs mesmo período ano anterior (12 meses atrás)
    round(
        (
            (
                ocupados_construcao
                / nullif(lag(ocupados_construcao, 12) over (order by periodo), 0)
            )
            - 1
        )
        * 100,
        1
    ) as var_ano,
    {{ add_metadata_timestamps("silver") }}
from construcao
