{{ config(materialized="table") }}

with
    base as (
        select
            ano,
            sum(financiamento_pf_uh_total_faixa_1) as f1_uh,
            sum(financiamento_pf_valor_total_faixa_1) as f1_val,
            sum(financiamento_pf_uh_total_faixa_2) as f2_uh,
            sum(financiamento_pf_valor_total_faixa_2) as f2_val,
            sum(financiamento_pf_uh_faixa_3_sem_fundo_social) as f3_uh,
            sum(financiamento_pf_valor_faixa_3_sem_fundo_social) as f3_val,
            sum(financiamento_pf_uh_pro_cotista_geral) as pc_uh,
            sum(financiamento_pf_valor_pro_cotista_geral) as pc_val,
            sum(financiamento_pf_uh_pro_cotista_faixa_1) as pc_f1_uh,
            sum(financiamento_pf_valor_pro_cotista_faixa_1) as pc_f1_val,
            sum(financiamento_pf_uh_pro_cotista_faixa_2) as pc_f2_uh,
            sum(financiamento_pf_valor_pro_cotista_faixa_2) as pc_f2_val,
            sum(financiamento_pf_uh_pro_cotista_faixa_3) as pc_f3_uh,
            sum(financiamento_pf_valor_pro_cotista_faixa_3) as pc_f3_val,
            sum(financiamento_pf_uh_pro_cotista_classe_media) as pc_cm_uh,
            sum(financiamento_pf_valor_pro_cotista_classe_media) as pc_cm_val,
            sum(financiamento_pf_uh_fora_mcmv) as fora_uh,
            sum(financiamento_pf_valor_fora_mcmv) as fora_val,
            sum(financiamento_pf_uh_faixa_3_fundo_social) as f3fs_uh,
            sum(financiamento_pf_valor_faixa_3_fundo_social) as f3fs_val,
            sum(financiamento_pf_uh_total_classe_media) as cm_uh,
            sum(financiamento_pf_valor_total_classe_media) as cm_val,
            sum(financiamento_pf_uh_total_geral) as total_uh,
            sum(valor_total_calculado) as total_val,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_fgts_financiamentos_habitacionais") }}
        where ano in (2024, 2025)
        group by ano
    ),

    y24 as (select * from base where ano = 2024),
    y25 as (select * from base where ano = 2025),

    resultado as (
        select
            1 as ordem,
            'Faixa 1' as categoria,
            a.f1_uh as uh_2024,
            round(a.f1_val::numeric / 1e9, 2) as val_bi_2024,
            b.f1_uh as uh_2025,
            round(b.f1_val::numeric / 1e9, 2) as val_bi_2025,
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            2,
            'Faixa 2',
            a.f2_uh,
            round(a.f2_val::numeric / 1e9, 2),
            b.f2_uh,
            round(b.f2_val::numeric / 1e9, 2),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            3,
            'Faixa 3',
            a.f3_uh,
            round(a.f3_val::numeric / 1e9, 2),
            b.f3_uh,
            round(b.f3_val::numeric / 1e9, 2),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            4,
            'Pró-Cotista',
            a.pc_uh,
            round(a.pc_val::numeric / 1e9, 2),
            b.pc_uh,
            round(b.pc_val::numeric / 1e9, 2),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            5,
            '  Faixa 1',
            a.pc_f1_uh,
            round(a.pc_f1_val::numeric / 1e9, 3),
            b.pc_f1_uh,
            round(b.pc_f1_val::numeric / 1e9, 3),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            6,
            '  Faixa 2',
            a.pc_f2_uh,
            round(a.pc_f2_val::numeric / 1e9, 3),
            b.pc_f2_uh,
            round(b.pc_f2_val::numeric / 1e9, 3),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            7,
            '  Faixa 3',
            a.pc_f3_uh,
            round(a.pc_f3_val::numeric / 1e9, 3),
            b.pc_f3_uh,
            round(b.pc_f3_val::numeric / 1e9, 3),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            8,
            '  Faixa Classe Média',
            a.pc_cm_uh,
            round(a.pc_cm_val::numeric / 1e9, 3),
            b.pc_cm_uh,
            round(b.pc_cm_val::numeric / 1e9, 3),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            9,
            '  Fora MCMV',
            a.fora_uh,
            round(a.fora_val::numeric / 1e9, 2),
            b.fora_uh,
            round(b.fora_val::numeric / 1e9, 2),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            10,
            'Faixa 3 Fundo Social',
            a.f3fs_uh,
            round(a.f3fs_val::numeric / 1e9, 2),
            b.f3fs_uh,
            round(b.f3fs_val::numeric / 1e9, 2),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            11,
            'Faixa Classe Média',
            a.cm_uh,
            round(a.cm_val::numeric / 1e9, 2),
            b.cm_uh,
            round(b.cm_val::numeric / 1e9, 2),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
        union all
        select
            12,
            'TOTAL',
            a.total_uh,
            round(a.total_val::numeric / 1e9, 2),
            b.total_uh,
            round(b.total_val::numeric / 1e9, 2),
            b.dt_ingest,
            b.dt_silver
        from y24 a, y25 b
    )

select
    ordem,
    categoria,
    uh_2024,
    val_bi_2024,
    uh_2025,
    val_bi_2025,
    {{ add_metadata_timestamps("gold") }}
from resultado
order by ordem
