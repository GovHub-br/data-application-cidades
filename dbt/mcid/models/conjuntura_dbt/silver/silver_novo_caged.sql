{{ config(materialized="table") }}

with
    base as (
        select
            ano,
            mes,
            admitidos,
            desligados,
            saldo,
            estoque,
            variacao,
            -- Trimestre
            case
                when mes in (1, 2, 3)
                then 1
                when mes in (4, 5, 6)
                then 2
                when mes in (7, 8, 9)
                then 3
                when mes in (10, 11, 12)
                then 4
            end as trimestre,
            -- Variação saldo vs mês anterior
            round(
                ((saldo::numeric / nullif(lag(saldo) over (order by ano, mes), 0)) - 1)
                * 100,
                1
            ) as saldo_var_mes,
            -- Variação estoque vs mesmo mês ano anterior
            round(
                (
                    (
                        estoque::numeric
                        / nullif(lag(estoque, 12) over (order by ano, mes), 0)
                    )
                    - 1
                )
                * 100,
                1
            ) as estoque_var_ano,
            {{ add_metadata_timestamps("silver") }}
        from {{ ref("bronze_novo_caged") }}
    )

select *
from base
