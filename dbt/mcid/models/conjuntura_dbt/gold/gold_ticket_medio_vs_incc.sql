{{ config(materialized="table") }}

with
    incc_tri as (
        select
            ano,
            trimestre,
            max(
                case
                    when extract(month from data_referencia) in (3, 6, 9, 12) then var_tri
                end
            ) as incc_var_tri,
            max(
                case
                    when extract(month from data_referencia) in (3, 6, 9, 12) then indice
                end
            ) as indice,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_fgv_incc_m") }}
        group by ano, trimestre
    ),

    base_4t20 as (
        select indice as indice_base
        from {{ ref("silver_fgv_incc_m") }}
        where data_referencia = '2020-12-01'
    ),

    incc_com_acum as (
        select
            t.ano,
            t.trimestre,
            t.incc_var_tri,
            t.dt_ingest,
            t.dt_silver,
            round(((t.indice / b.indice_base) - 1) * 100, 1) as incc_acum_4t20
        from incc_tri t, base_4t20 b
    ),

    resultado as (
        select
            e.ano,
            e.trimestre,
            round(i.incc_var_tri::numeric, 1) as incc_var_tri,
            round(e.mrv_var_tri::numeric, 1) as mrv_var_tri,
            round(e.dir_var_tri::numeric, 1) as dir_var_tri,
            round(e.ten_var_tri::numeric, 1) as ten_var_tri,
            round(i.incc_acum_4t20::numeric, 1) as incc_acum_4t20,
            round(e.mrv_acum::numeric, 1) as mrv_acum_4t20,
            round(e.dir_acum::numeric, 1) as dir_acum_4t20,
            round(e.ten_acum::numeric, 1) as ten_acum_4t20,
            greatest(e.dt_ingest, i.dt_ingest) as dt_ingest,
            greatest(e.dt_silver, i.dt_silver) as dt_silver
        from {{ ref("silver_ticket_medio_empresas") }} e
        left join incc_com_acum i on e.ano = i.ano and e.trimestre = i.trimestre
        where (e.ano > 2023) or (e.ano = 2023 and e.trimestre = 4)
    )

select
    ano,
    trimestre,
    incc_var_tri,
    mrv_var_tri,
    dir_var_tri,
    ten_var_tri,
    incc_acum_4t20,
    mrv_acum_4t20,
    dir_acum_4t20,
    ten_acum_4t20,
    {{ add_metadata_timestamps("gold") }}
from resultado
order by ano, trimestre
