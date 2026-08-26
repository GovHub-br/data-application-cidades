{{ config(materialized="table") }}

with
    t_4t25 as (
        select fgts_uh, sbpe_uh, dt_ingest, dt_silver
        from {{ ref("silver_financiamentos_habitacionais") }}
        where ano = 2025 and trimestre = 4
    ),

    t_3t25 as (
        select fgts_uh, sbpe_uh, dt_ingest, dt_silver
        from {{ ref("silver_financiamentos_habitacionais") }}
        where ano = 2025 and trimestre = 3
    ),

    t_4t24 as (
        select fgts_uh, sbpe_uh, dt_ingest, dt_silver
        from {{ ref("silver_financiamentos_habitacionais") }}
        where ano = 2024 and trimestre = 4
    ),

    acum_25 as (
        select
            sum(fgts_uh) as fgts_uh,
            sum(sbpe_uh) as sbpe_uh,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_financiamentos_habitacionais") }}
        where ano = 2025
    ),

    acum_24 as (
        select
            sum(fgts_uh) as fgts_uh,
            sum(sbpe_uh) as sbpe_uh,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_financiamentos_habitacionais") }}
        where ano = 2024
    ),

    resultado as (
        select '4º TRI 2025' as periodo, fgts_uh, sbpe_uh, dt_ingest, dt_silver
        from t_4t25
        union all
        select '3º TRI 2025', fgts_uh, sbpe_uh, dt_ingest, dt_silver
        from t_3t25
        union all
        select '4º TRI 2024', fgts_uh, sbpe_uh, dt_ingest, dt_silver
        from t_4t24
        union all
        select '12 MESES - DEZ/2025', fgts_uh, sbpe_uh, dt_ingest, dt_silver
        from acum_25
        union all
        select '12 MESES - DEZ/2024', fgts_uh, sbpe_uh, dt_ingest, dt_silver
        from acum_24
    )

select periodo, fgts_uh, sbpe_uh, {{ add_metadata_timestamps("gold") }}
from resultado
