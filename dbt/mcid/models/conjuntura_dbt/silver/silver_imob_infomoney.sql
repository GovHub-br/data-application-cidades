{{ config(materialized="table") }}

with
    mensal as (
        select
            date_trunc('month', data_pregao)::date as data_referencia,
            last_value(close) over (
                partition by date_trunc('month', data_pregao)
                order by data_pregao
                rows between unbounded preceding and unbounded following
            ) as close_fim_mes,
            dt_ingest
        from {{ source("conjuntura_bronze", "bronze_imob_infomoney") }}
        where symbol = 'IMOB.SA'
    ),

    distinct_mensal as (
        select data_referencia, close_fim_mes, max(dt_ingest) as dt_ingest
        from mensal
        group by data_referencia, close_fim_mes
    ),

    com_variacoes as (
        select
            data_referencia,
            close_fim_mes,
            round(
                (
                    (
                        close_fim_mes
                        / nullif(lag(close_fim_mes, 1) over (order by data_referencia), 0)
                    )
                    - 1
                )
                * 100,
                1
            ) as var_mes,
            round(
                (
                    (
                        close_fim_mes / nullif(
                            lag(close_fim_mes, 12) over (order by data_referencia), 0
                        )
                    )
                    - 1
                )
                * 100,
                1
            ) as var_12_meses,
            round(
                (
                    (
                        close_fim_mes / nullif(
                            first_value(close_fim_mes) over (
                                order by data_referencia
                                rows between unbounded preceding and unbounded following
                            ),
                            0
                        )
                    )
                    - 1
                )
                * 100,
                1
            ) as var_acum_serie,
            dt_ingest
        from distinct_mensal
    )

select
    data_referencia,
    close_fim_mes,
    var_mes,
    var_12_meses,
    var_acum_serie,
    {{ add_metadata_timestamps("silver") }}
from com_variacoes
