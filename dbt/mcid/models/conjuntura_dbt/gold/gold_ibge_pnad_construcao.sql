{{ config(materialized="table") }}

with
    ocupados as (
        select
            ano,
            mes,
            trimestre,
            periodo,
            ocupados_construcao,
            ocupados_total,
            var_mes,
            var_ano,
            dt_ingest,
            dt_silver
        from {{ ref("silver_ibge_pnadc_ocupados_construcao") }}
        where right(periodo, 2)::int in (3, 6, 9, 12)
    ),

    rendimento as (
        select
            periodo,
            rendimento_construcao,
            rendimento_total,
            var_mes as rend_var_mes,
            var_ano as rend_var_ano,
            dt_ingest as dt_ingest_rend,
            dt_silver as dt_silver_rend
        from {{ ref("silver_ibge_pnadc_rendimento_construcao") }}
        where right(periodo, 2)::int in (3, 6, 9, 12)
    ),

    resultado as (
        select
            o.ano,
            o.trimestre,
            o.periodo,
            o.ocupados_construcao as pnad_const_milhares,
            o.ocupados_total as pnad_total_milhares,
            o.var_mes as pnad_const_var_mes,
            o.var_ano as pnad_const_var_ano,
            r.rendimento_construcao as rend_const_rs,
            r.rendimento_total as rend_total_rs,
            r.rend_var_mes as rend_const_var_mes,
            r.rend_var_ano as rend_const_var_ano,
            greatest(o.dt_ingest, r.dt_ingest_rend) as dt_ingest,
            greatest(o.dt_silver, r.dt_silver_rend) as dt_silver
        from ocupados o
        left join rendimento r on o.periodo = r.periodo
    )

select
    ano,
    trimestre,
    periodo,
    pnad_const_milhares,
    pnad_total_milhares,
    pnad_const_var_mes,
    pnad_const_var_ano,
    rend_const_rs,
    rend_total_rs,
    rend_const_var_mes,
    rend_const_var_ano,
    {{ add_metadata_timestamps("gold") }}
from resultado
order by ano, trimestre
