{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: Canal FGTS — Pró-Cotista, por faixa de renda
-- (UH e R$ milhões). Página 5. Dado MANUAL (boletim.xlsx /
-- manual_conjuntura.dados_mensais).
select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    financiamento_pf_uh_pro_cotista_geral,
    financiamento_pf_valor_pro_cotista_geral,
    financiamento_pf_uh_pro_cotista_faixa_1,
    financiamento_pf_valor_pro_cotista_faixa_1,
    financiamento_pf_uh_pro_cotista_faixa_2,
    financiamento_pf_valor_pro_cotista_faixa_2,
    financiamento_pf_uh_pro_cotista_faixa_3,
    financiamento_pf_valor_pro_cotista_faixa_3,
    financiamento_pf_uh_pro_cotista_classe_media,
    financiamento_pf_valor_pro_cotista_classe_media
from {{ ref("silver_continuo_manual_mensais") }}
where
    coalesce(
        financiamento_pf_uh_pro_cotista_faixa_1,
        financiamento_pf_uh_pro_cotista_faixa_2,
        financiamento_pf_uh_pro_cotista_faixa_3,
        financiamento_pf_uh_pro_cotista_classe_media
    )
    is not null
order by ano desc, mes desc
