{{ config(materialized="table") }}

with
    fgts as (
        select
            ano,
            sum(financiamento_pf_uh_total_geral) as uh_total,
            sum(
                financiamento_pf_valor_total_faixa_1
                + financiamento_pf_valor_total_faixa_2
                + coalesce(financiamento_pf_valor_faixa_3_sem_fundo_social, 0)
                + coalesce(financiamento_pf_valor_faixa_3_fundo_social, 0)
                + coalesce(financiamento_pf_valor_total_classe_media, 0)
                + coalesce(financiamento_pf_valor_fora_mcmv, 0)
            ) as val_total,
            sum(financiamento_pf_uh_pro_cotista_geral) as uh_pro_cotista,
            sum(financiamento_pf_valor_pro_cotista_geral) as val_pro_cotista,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_fgts_financiamentos_habitacionais") }}
        where ano in (2024, 2025)
        group by ano
    ),

    abecip as (
        select
            ano,
            sum(sbpe_aquisicao) as sbpe_aq_uh,
            sum(sbpe_const) as sbpe_const_uh,
            sum(sbpe_const_milhoes) / 1000.0 as sbpe_const_bi,
            sum(sbpe_aquisicao_milhoes) / 1000.0 as sbpe_aq_bi,
            sum(sbpe_total_milhoes) / 1000.0 as sbpe_total_bi,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_abecip_sbpe_financiamentos_habitacionais") }}
        where ano in (2024, 2025)
        group by ano
    ),

    f25 as (
        select
            ano,
            uh_total,
            val_total,
            uh_pro_cotista,
            val_pro_cotista,
            dt_ingest,
            dt_silver
        from fgts
        where ano = 2025
    ),
    f24 as (
        select ano, uh_total, val_total, uh_pro_cotista, val_pro_cotista
        from fgts
        where ano = 2024
    ),
    a25 as (
        select
            ano,
            sbpe_aq_uh,
            sbpe_const_uh,
            sbpe_const_bi,
            sbpe_aq_bi,
            sbpe_total_bi,
            dt_ingest,
            dt_silver
        from abecip
        where ano = 2025
    ),
    a24 as (
        select ano, sbpe_aq_uh, sbpe_const_uh, sbpe_const_bi, sbpe_aq_bi, sbpe_total_bi
        from abecip
        where ano = 2024
    ),

    resultado as (
        select
            (f25.uh_total + coalesce(a25.sbpe_aq_uh, 0)) as mcmv_sbpe_uh_25,
            round(
                (f25.val_total::numeric / 1e9) + coalesce(a25.sbpe_aq_bi, 0), 2
            ) as mcmv_sbpe_val_bi_25,
            round(
                (
                    (
                        (f25.uh_total + coalesce(a25.sbpe_aq_uh, 0))::numeric
                        / nullif(f24.uh_total + coalesce(a24.sbpe_aq_uh, 0), 0)
                    )
                    - 1
                )
                * 100,
                0
            ) as mcmv_sbpe_var_uh,
            round(
                (
                    (
                        (
                            f25.val_total::numeric / 1e9 + coalesce(a25.sbpe_aq_bi, 0)
                        ) / nullif(
                            f24.val_total::numeric / 1e9 + coalesce(a24.sbpe_aq_bi, 0), 0
                        )
                    )
                    - 1
                )
                * 100,
                0
            ) as mcmv_sbpe_var_val,
            a25.sbpe_const_uh as sbpe_const_uh_25,
            round(a25.sbpe_const_bi::numeric, 2) as sbpe_const_bi_25,
            round(
                ((a25.sbpe_const_uh::numeric / nullif(a24.sbpe_const_uh, 0)) - 1) * 100, 0
            ) as sbpe_const_var_uh,
            round(
                ((a25.sbpe_const_bi / nullif(a24.sbpe_const_bi, 0)) - 1) * 100, 0
            ) as sbpe_const_var_bi,
            f25.uh_pro_cotista as pro_cotista_uh_25,
            round(f25.val_pro_cotista::numeric / 1e9, 2) as pro_cotista_val_bi_25,
            round(
                ((f25.uh_pro_cotista::numeric / nullif(f24.uh_pro_cotista, 0)) - 1) * 100,
                0
            ) as pro_cotista_var_uh,
            round(
                ((f25.val_pro_cotista::numeric / nullif(f24.val_pro_cotista, 0)) - 1)
                * 100,
                0
            ) as pro_cotista_var_val,
            f25.uh_total as fin_pf_uh_25,
            round(f25.val_total::numeric / 1e9, 2) as fin_pf_val_bi_25,
            round(
                ((f25.uh_total::numeric / nullif(f24.uh_total, 0)) - 1) * 100, 0
            ) as fin_pf_var_uh,
            round(
                ((f25.val_total::numeric / nullif(f24.val_total, 0)) - 1) * 100, 0
            ) as fin_pf_var_val,
            greatest(f25.dt_ingest, a25.dt_ingest) as dt_ingest,
            greatest(f25.dt_silver, a25.dt_silver) as dt_silver
        from f25, f24, a25, a24
    )

select
    mcmv_sbpe_uh_25,
    mcmv_sbpe_val_bi_25,
    mcmv_sbpe_var_uh,
    mcmv_sbpe_var_val,
    sbpe_const_uh_25,
    sbpe_const_bi_25,
    sbpe_const_var_uh,
    sbpe_const_var_bi,
    pro_cotista_uh_25,
    pro_cotista_val_bi_25,
    pro_cotista_var_uh,
    pro_cotista_var_val,
    fin_pf_uh_25,
    fin_pf_val_bi_25,
    fin_pf_var_uh,
    fin_pf_var_val,
    {{ add_metadata_timestamps("gold") }}
from resultado
