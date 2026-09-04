{{ config(materialized="table") }}

with
    base as (
        select
            ano,
            sum(financiamento_pf_uh_pro_cotista_faixa_1) as f1_usadas,
            sum(
                financiamento_pf_uh_total_faixa_1
                - coalesce(financiamento_pf_uh_pro_cotista_faixa_1, 0)
            ) as f1_novas,
            sum(financiamento_pf_uh_pro_cotista_faixa_2) as f2_usadas,
            sum(
                financiamento_pf_uh_total_faixa_2
                - coalesce(financiamento_pf_uh_pro_cotista_faixa_2, 0)
            ) as f2_novas,
            sum(financiamento_pf_uh_pro_cotista_faixa_3) as f3_usadas,
            sum(
                financiamento_pf_uh_faixa_3_sem_fundo_social
                - coalesce(financiamento_pf_uh_pro_cotista_faixa_3, 0)
            ) as f3_novas,
            sum(financiamento_pf_uh_faixa_3_fundo_social) as f3fs_novas,
            sum(financiamento_pf_uh_pro_cotista_classe_media) as cm_usadas,
            sum(
                financiamento_pf_uh_total_classe_media
                - coalesce(financiamento_pf_uh_pro_cotista_classe_media, 0)
            ) as cm_novas,
            sum(financiamento_pf_uh_fora_mcmv) as fora_usadas,
            sum(financiamento_pf_uh_pro_cotista_geral) as total_usadas,
            sum(
                financiamento_pf_uh_total_geral
                - coalesce(financiamento_pf_uh_pro_cotista_geral, 0)
            ) as total_novas,
            max(dt_ingest) as dt_ingest,
            max(dt_silver) as dt_silver
        from {{ ref("silver_fgts_financiamentos_habitacionais") }}
        where ano in (2024, 2025)
        group by ano
    ),

    y24 as (select * from base where ano = 2024),
    y25 as (select * from base where ano = 2025)

select
    1 as ordem,
    'FAIXA 1' as categoria,
    a.f1_usadas as uh_usadas_2024,
    a.f1_novas as uh_novas_2024,
    b.f1_usadas as uh_usadas_2025,
    round(((b.f1_usadas::numeric / nullif(a.f1_usadas, 0)) - 1) * 100, 0) as var_usadas,
    b.f1_novas as uh_novas_2025,
    round(((b.f1_novas::numeric / nullif(a.f1_novas, 0)) - 1) * 100, 0) as var_novas,
    b.dt_ingest,
    b.dt_silver
from y24 a, y25 b
union all
select
    2,
    'FAIXA 2',
    a.f2_usadas,
    a.f2_novas,
    b.f2_usadas,
    round(((b.f2_usadas::numeric / nullif(a.f2_usadas, 0)) - 1) * 100, 0),
    b.f2_novas,
    round(((b.f2_novas::numeric / nullif(a.f2_novas, 0)) - 1) * 100, 0),
    b.dt_ingest,
    b.dt_silver
from y24 a, y25 b
union all
select
    3,
    'FAIXA 3',
    a.f3_usadas,
    a.f3_novas,
    b.f3_usadas,
    round(((b.f3_usadas::numeric / nullif(a.f3_usadas, 0)) - 1) * 100, 0),
    b.f3_novas,
    round(((b.f3_novas::numeric / nullif(a.f3_novas, 0)) - 1) * 100, 0),
    b.dt_ingest,
    b.dt_silver
from y24 a, y25 b
union all
select
    4,
    'FAIXA 3 FS',
    null,
    null,
    null,
    null,
    b.f3fs_novas,
    round(((b.f3fs_novas::numeric / nullif(a.f3fs_novas, 0)) - 1) * 100, 0),
    b.dt_ingest,
    b.dt_silver
from y24 a, y25 b
union all
select
    5,
    'FAIXA CLASSE MÉDIA',
    null,
    null,
    b.cm_usadas,
    round(((b.cm_usadas::numeric / nullif(a.cm_usadas, 0)) - 1) * 100, 0),
    b.cm_novas,
    round(((b.cm_novas::numeric / nullif(a.cm_novas, 0)) - 1) * 100, 0),
    b.dt_ingest,
    b.dt_silver
from y24 a, y25 b
union all
select
    6,
    'FORA MCMV',
    a.fora_usadas,
    null,
    b.fora_usadas,
    round(((b.fora_usadas::numeric / nullif(a.fora_usadas, 0)) - 1) * 100, 0),
    null,
    null,
    b.dt_ingest,
    b.dt_silver
from y24 a, y25 b
union all
select
    7,
    'TOTAL',
    a.total_usadas,
    a.total_novas,
    b.total_usadas,
    round(((b.total_usadas::numeric / nullif(a.total_usadas, 0)) - 1) * 100, 0),
    b.total_novas,
    round(((b.total_novas::numeric / nullif(a.total_novas, 0)) - 1) * 100, 0),
    b.dt_ingest,
    b.dt_silver
from y24 a, y25 b
order by ordem
