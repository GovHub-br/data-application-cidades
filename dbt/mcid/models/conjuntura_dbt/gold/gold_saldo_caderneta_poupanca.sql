{{ config(materialized="table") }}

with
    base as (
        select
            data_referencia,
            captacao_liquida_valor,
            round(captacao_liquida_valor::numeric / 1e3, 1) as cap_liq_bi,
            dt_ingest,
            dt_silver
        from {{ ref("silver_abecip_poupanca_sbpe") }}
    ),

    dez25 as (select * from base where data_referencia = '2025-12-01'),
    nov25 as (select * from base where data_referencia = '2025-11-01'),
    dez24 as (select * from base where data_referencia = '2024-12-01'),

    acum_dez25 as (
        select
            round(sum(captacao_liquida_valor::numeric / 1e3), 1) as total,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from base
        where data_referencia between '2025-01-01' and '2025-12-01'
    ),

    acum_dez24 as (
        select
            round(sum(captacao_liquida_valor::numeric / 1e3), 1) as total,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from base
        where data_referencia between '2024-01-01' and '2024-12-01'
    ),

    resultado as (
        select
            1 as ordem,
            'DEZ 2025' as periodo,
            d25.cap_liq_bi as cap_liq_bi,
            d25.dt_ingest,
            d25.dt_silver
        from dez25 d25
        union all
        select 2, 'NOV 2025', n25.cap_liq_bi, n25.dt_ingest, n25.dt_silver
        from nov25 n25
        union all
        select 3, 'DEZ 2024', d24.cap_liq_bi, d24.dt_ingest, d24.dt_silver
        from dez24 d24
        union all
        select 4, '12 MESES – DEZ/25', a25.total, a25.dt_ingest, a25.dt_silver
        from acum_dez25 a25
        union all
        select 5, '12 MESES – DEZ/24', a24.total, a24.dt_ingest, a24.dt_silver
        from acum_dez24 a24
    )

select ordem, periodo, cap_liq_bi, {{ add_metadata_timestamps("gold") }}
from resultado
order by ordem
