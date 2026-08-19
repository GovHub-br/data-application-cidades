{{ config(materialized="table") }}

select
    trimestre,
    ano,
    mes,
    -- UH
    sbpe_const,
    sbpe_aquisicao,
    sbpe_total,
    -- Valores em R$ milhões
    sbpe_const_milhoes,
    sbpe_aquisicao_milhoes,
    sbpe_total_milhoes,
    -- Aquisição por condição de uso
    sbpe_aq_novos_uh,
    sbpe_aq_usados_uh,
    sbpe_aq_novos_milhoes,
    sbpe_aq_usados_milhoes,
    -- Calculados
    round(
        ((sbpe_const::numeric / nullif(lag(sbpe_const) over (order by ano, mes), 0)) - 1)
        * 100,
        1
    ) as sbpe_const_var_mes,
    round(
        (
            (
                sbpe_aquisicao::numeric
                / nullif(lag(sbpe_aquisicao) over (order by ano, mes), 0)
            )
            - 1
        )
        * 100,
        1
    ) as sbpe_aq_var_mes,
    round(
        ((sbpe_total::numeric / nullif(lag(sbpe_total) over (order by ano, mes), 0)) - 1)
        * 100,
        1
    ) as sbpe_total_var_mes,
    round(
        (
            (
                sbpe_const::numeric
                / nullif(lag(sbpe_const, 12) over (order by ano, mes), 0)
            )
            - 1
        )
        * 100,
        1
    ) as sbpe_const_var_12m,
    round(
        (
            (
                sbpe_aquisicao::numeric
                / nullif(lag(sbpe_aquisicao, 12) over (order by ano, mes), 0)
            )
            - 1
        )
        * 100,
        1
    ) as sbpe_aq_var_12m,
    round(
        (
            (
                sbpe_total::numeric
                / nullif(lag(sbpe_total, 12) over (order by ano, mes), 0)
            )
            - 1
        )
        * 100,
        1
    ) as sbpe_total_var_12m,
    {{ add_metadata_timestamps("silver", has_ingest_date=false) }}
from {{ source("conjuntura_bronze", "bronze_abecip_sbpe_financiamentos_habitacionais") }}
