{{ config(materialized="table") }}

with
    periodos as (
        select
            'DEZ 25 vs. NOV 25' as periodo,
            1 as ordem,
            '2025-12-01'::date as data_atual,
            '2025-11-01'::date as data_anterior,
            '2024-12-01'::date as data_ano_anterior
        union all
        select 'DEZ 25 vs. DEZ 24', 2, '2025-12-01', '2024-12-01', '2023-12-01'
        union all
        select 'JAN-DEZ/25', 3, '2025-12-01', '2024-12-01', '2023-12-01'
        union all
        select 'JAN-DEZ/24', 4, '2024-12-01', '2023-12-01', '2022-12-01'
    ),

    imob as (
        select data_referencia, close_fim_mes, var_mes, var_12_meses, dt_ingest, dt_silver
        from {{ ref("silver_imob_infomoney") }}
    ),

    fipezap as (
        select data_referencia, var_mensal, var_ano, dt_ingest, dt_silver
        from {{ ref("silver_fipezap_locacao") }}
    ),

    icst as (
        select
            data_referencia,
            icst_sem_ajuste_sazonal,
            var_mes,
            var_12_meses,
            dt_ingest,
            dt_silver
        from {{ ref("silver_fgv_icst") }}
    ),

    abramat as (
        select data_referencia, var_mes, var_12_meses, dt_ingest, dt_silver
        from {{ ref("silver_abramat_indice") }}
    ),

    resultado as (
        select
            p.periodo,
            round(
                ((ia.close_fim_mes / nullif(ib.close_fim_mes, 0)) - 1) * 100, 1
            ) as imob_var,
            case
                when p.ordem = 1
                then round(fa.var_mensal * 100, 2)
                when p.ordem = 2
                then
                    round(
                        ((1 + fa.var_mensal) / nullif(1 + fb.var_mensal, 0) - 1) * 100, 2
                    )
                when p.ordem = 3
                then round(fa.var_ano * 100, 2)
                when p.ordem = 4
                then round(fb.var_ano * 100, 2)
            end as fipezap_var,
            case
                when p.ordem = 1
                then
                    round(
                        (
                            (
                                ca.icst_sem_ajuste_sazonal
                                / nullif(cb.icst_sem_ajuste_sazonal, 0)
                            )
                            - 1
                        )
                        * 100,
                        2
                    )
                when p.ordem = 2
                then
                    round(
                        (
                            (
                                ca.icst_sem_ajuste_sazonal
                                / nullif(cc.icst_sem_ajuste_sazonal, 0)
                            )
                            - 1
                        )
                        * 100,
                        2
                    )
                when p.ordem = 3
                then
                    round(
                        (
                            (
                                ca.icst_sem_ajuste_sazonal
                                / nullif(cc.icst_sem_ajuste_sazonal, 0)
                            )
                            - 1
                        )
                        * 100,
                        2
                    )
                when p.ordem = 4
                then
                    round(
                        (
                            (
                                cc.icst_sem_ajuste_sazonal
                                / nullif(cd.icst_sem_ajuste_sazonal, 0)
                            )
                            - 1
                        )
                        * 100,
                        2
                    )
            end as icst_var,
            case
                when p.ordem = 1
                then aa.var_mes
                when p.ordem = 2
                then aa.var_12_meses
                when p.ordem = 3
                then aa.var_12_meses
                when p.ordem = 4
                then ab.var_12_meses
            end as abramat_var,
            greatest(ia.dt_ingest, fa.dt_ingest, ca.dt_ingest, aa.dt_ingest) as dt_ingest,
            greatest(ia.dt_silver, fa.dt_silver, ca.dt_silver, aa.dt_silver) as dt_silver
        from periodos p
        left join imob ia on ia.data_referencia = p.data_atual
        left join imob ib on ib.data_referencia = p.data_anterior
        left join fipezap fa on fa.data_referencia = p.data_atual
        left join fipezap fb on fb.data_referencia = p.data_anterior
        left join icst ca on ca.data_referencia = p.data_atual
        left join icst cb on cb.data_referencia = p.data_anterior
        left join icst cc on cc.data_referencia = p.data_ano_anterior
        left join icst cd on cd.data_referencia = '2023-12-01'
        left join abramat aa on aa.data_referencia = p.data_atual
        left join abramat ab on ab.data_referencia = p.data_anterior
    )

select
    periodo,
    imob_var,
    fipezap_var,
    icst_var,
    abramat_var,
    {{ add_metadata_timestamps("gold") }}
from resultado
order by (select ordem from periodos where periodo = resultado.periodo)
