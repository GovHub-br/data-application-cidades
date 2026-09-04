{{ config(materialized="table") }}

with
    sinapi_dez25 as (
        select custo_m2, var_mes, var_12m, dt_ingest, dt_silver
        from {{ ref("silver_ibge_sinapi") }}
        where data_referencia = '2025-12-01'
    ),

    sinapi_dez24 as (
        select var_12m
        from {{ ref("silver_ibge_sinapi") }}
        where data_referencia = '2024-12-01'
    ),

    incc_dez25 as (
        select indice, var_mes, var_12_meses, dt_ingest, dt_silver
        from {{ ref("silver_fgv_incc_m") }}
        where data_referencia = '2025-12-01'
    ),

    incc_dez24 as (
        select var_12_meses
        from {{ ref("silver_fgv_incc_m") }}
        where data_referencia = '2024-12-01'
    ),

    resultado as (
        select
            round(s25.custo_m2::numeric, 1) as sinapi_custo_m2_dez25,
            round(s25.var_mes::numeric, 2) as sinapi_var_mes_dez25,
            round(s25.var_12m::numeric, 2) as sinapi_var_12m_dez25,
            round(s24.var_12m::numeric, 2) as sinapi_var_12m_dez24,
            round(i25.indice::numeric, 1) as incc_indice_dez25,
            round(i25.var_mes::numeric, 2) as incc_var_mes_dez25,
            round(i25.var_12_meses::numeric, 2) as incc_var_12m_dez25,
            round(i24.var_12_meses::numeric, 2) as incc_var_12m_dez24,
            greatest(s25.dt_ingest, i25.dt_ingest) as dt_ingest,
            greatest(s25.dt_silver, i25.dt_silver) as dt_silver
        from sinapi_dez25 s25, sinapi_dez24 s24, incc_dez25 i25, incc_dez24 i24
    )

select
    sinapi_custo_m2_dez25,
    sinapi_var_mes_dez25,
    sinapi_var_12m_dez25,
    sinapi_var_12m_dez24,
    incc_indice_dez25,
    incc_var_mes_dez25,
    incc_var_12m_dez25,
    incc_var_12m_dez24,
    {{ add_metadata_timestamps("gold") }}
from resultado
