{{ config(materialized="table") }}

with
    fgts as (
        select
            ano,
            sum(financiamento_pf_uh_pro_cotista_geral) as uh_usadas,
            sum(
                financiamento_pf_uh_total_geral
                - coalesce(financiamento_pf_uh_pro_cotista_geral, 0)
            ) as uh_novas,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_fgts_financiamentos_habitacionais") }}
        where ano in (2024, 2025)
        group by ano
    ),

    abecip as (
        select
            ano,
            sum(sbpe_aq_usados_uh) as uh_usadas,
            sum(sbpe_aq_novos_uh) as uh_novas,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_abecip_sbpe_financiamentos_habitacionais") }}
        where ano in (2024, 2025)
        group by ano
    ),

    f24 as (select * from fgts where ano = 2024),
    f25 as (select * from fgts where ano = 2025),
    a24 as (select * from abecip where ano = 2024),
    a25 as (select * from abecip where ano = 2025),

    resultado as (
        select
            1 as ordem,
            'FGTS - PF' as categoria,
            f24.uh_usadas as uh_usadas_2024,
            f24.uh_novas as uh_novas_2024,
            f25.uh_usadas as uh_usadas_2025,
            round(
                ((f25.uh_usadas::numeric / nullif(f24.uh_usadas, 0)) - 1) * 100, 0
            ) as var_usadas,
            f25.uh_novas as uh_novas_2025,
            round(
                ((f25.uh_novas::numeric / nullif(f24.uh_novas, 0)) - 1) * 100, 0
            ) as var_novas,
            greatest(f25.dt_ingest, f24.dt_ingest) as dt_ingest,
            greatest(f25.dt_silver, f24.dt_silver) as dt_silver
        from f24, f25
        union all
        select
            2,
            'SBPE (Aquisição)',
            a24.uh_usadas,
            a24.uh_novas,
            a25.uh_usadas,
            round(((a25.uh_usadas::numeric / nullif(a24.uh_usadas, 0)) - 1) * 100, 0),
            a25.uh_novas,
            round(((a25.uh_novas::numeric / nullif(a24.uh_novas, 0)) - 1) * 100, 0),
            greatest(a25.dt_ingest, a24.dt_ingest),
            greatest(a25.dt_silver, a24.dt_silver)
        from a24, a25
        union all
        select
            3,
            'Total',
            (f24.uh_usadas + a24.uh_usadas),
            (f24.uh_novas + a24.uh_novas),
            (f25.uh_usadas + a25.uh_usadas),
            round(
                (
                    (
                        (f25.uh_usadas + a25.uh_usadas)::numeric
                        / nullif(f24.uh_usadas + a24.uh_usadas, 0)
                    )
                    - 1
                )
                * 100,
                0
            ),
            (f25.uh_novas + a25.uh_novas),
            round(
                (
                    (
                        (f25.uh_novas + a25.uh_novas)::numeric
                        / nullif(f24.uh_novas + a24.uh_novas, 0)
                    )
                    - 1
                )
                * 100,
                0
            ),
            greatest(f25.dt_ingest, a25.dt_ingest),
            greatest(f25.dt_silver, a25.dt_silver)
        from f24, f25, a24, a25
    )

select
    ordem,
    categoria,
    uh_usadas_2024,
    uh_novas_2024,
    uh_usadas_2025,
    var_usadas,
    uh_novas_2025,
    var_novas,
    {{ add_metadata_timestamps("gold") }}
from resultado
order by ordem
