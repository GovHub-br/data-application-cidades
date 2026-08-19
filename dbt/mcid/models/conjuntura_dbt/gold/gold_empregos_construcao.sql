{{ config(materialized="table") }}

with
    base as (
        select
            ano,
            mes,
            trimestre,
            saldo,
            estoque,
            saldo_var_mes,
            estoque_var_ano,
            dt_ingest,
            dt_silver
        from {{ ref("silver_novo_caged") }}
    ),

    trimestral as (
        select
            ano,
            trimestre,
            min(mes) as mes_ini,
            max(mes) as mes_fim,
            sum(saldo) as saldo_tri,
            max(estoque) as estoque_fim_tri,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from base
        group by ano, trimestre
    ),

    com_var as (
        select
            t.ano,
            t.trimestre,
            t.mes_ini,
            t.mes_fim,
            t.saldo_tri,
            t.estoque_fim_tri,
            t.dt_ingest,
            t.dt_silver,
            round(
                (
                    (
                        t.saldo_tri::numeric
                        / nullif(lag(t.saldo_tri) over (order by t.ano, t.trimestre), 0)
                    )
                    - 1
                )
                * 100,
                0
            ) as saldo_var_tri,
            round(
                (
                    (
                        t.estoque_fim_tri::numeric / nullif(
                            lag(t.estoque_fim_tri, 4) over (order by t.ano, t.trimestre),
                            0
                        )
                    )
                    - 1
                )
                * 100,
                0
            ) as estoque_var_ano
        from trimestral t
    ),

    acumulado as (
        select
            ano,
            trimestre,
            sum(saldo_tri) over (partition by ano order by trimestre) as saldo_acum_ano,
            max(estoque_fim_tri) over (
                partition by ano order by trimestre
            ) as estoque_acum_ano
        from com_var
    )

select
    v.ano,
    v.trimestre,
    v.mes_ini,
    v.mes_fim,
    v.saldo_tri as criacao_liquida_saldo,
    v.saldo_var_tri as saldo_var_tri_pct,
    v.estoque_fim_tri as total_postos_estoque,
    v.estoque_var_ano as estoque_var_ano_pct,
    a.saldo_acum_ano,
    a.estoque_acum_ano,
    {{ add_metadata_timestamps("gold") }}
from com_var v
join acumulado a on v.ano = a.ano and v.trimestre = a.trimestre
order by v.ano, v.trimestre
