{{ config(materialized="table") }}

with
    fgts as (

        select ano, trimestre, sum(financiamento_pf_uh_total_geral) as fgts_uh

        from {{ source("conjuntura_bronze", "bronze_fgts_financiamentos_habitacionais") }}

        group by ano, trimestre

    ),

    abecip as (

        select ano, trimestre, sum(sbpe_const) as sbpe_uh

        from
            {{
                source(
                    "conjuntura_bronze",
                    "bronze_abecip_sbpe_financiamentos_habitacionais",
                )
            }}

        group by ano, trimestre

    )

select
    fgts.ano,
    fgts.trimestre,

    fgts.fgts_uh,
    abecip.sbpe_uh,
    {{ add_metadata_timestamps("silver", has_ingest_date=false) }}
from fgts

left join abecip on fgts.ano = abecip.ano and fgts.trimestre = abecip.trimestre
