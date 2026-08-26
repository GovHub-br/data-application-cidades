{{ config(materialized="table") }}

with
    base as (
        select
            ano,
            trimestre,
            cbic_vendas_total as total,
            cbic_vendas_mcmv as mcmv,
            cbic_vendas_demais as demais,
            dt_ingest,
            dt_silver
        from {{ ref("silver_cbic_lancamentos_vendas") }}
    ),

    periodos as (
        select
            1 as ordem,
            '4º TRI 2025' as periodo,
            total,
            mcmv,
            demais,
            dt_ingest,
            dt_silver
        from base
        where ano = 2025 and trimestre = 4
        union all
        select 2, '3º TRI 2025', total, mcmv, demais, dt_ingest, dt_silver
        from base
        where ano = 2025 and trimestre = 3
        union all
        select 3, '4º TRI 2024', total, mcmv, demais, dt_ingest, dt_silver
        from base
        where ano = 2024 and trimestre = 4
        union all
        select
            4,
            '12 MESES - DEZ/2025',
            sum(total),
            sum(mcmv),
            sum(demais),
            max(dt_ingest),
            max(dt_silver)
        from base
        where ano = 2025
        union all
        select
            5,
            '12 MESES - DEZ/2024',
            sum(total),
            sum(mcmv),
            sum(demais),
            max(dt_ingest),
            max(dt_silver)
        from base
        where ano = 2024
    )

select periodo, total, mcmv, demais, {{ add_metadata_timestamps("gold") }}
from periodos
order by ordem
