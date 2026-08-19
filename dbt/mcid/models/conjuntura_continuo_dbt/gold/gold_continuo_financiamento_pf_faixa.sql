{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: Financiamento PF por Faixa de Renda (UH e R$
-- milhões) — Faixa 1, 2, 3 (com e sem Fundo Social), Classe Média e Fora do
-- MCMV (Apoio/CCI). Páginas 4 e 5. Dado MANUAL (boletim.xlsx /
-- manual_conjuntura.dados_mensais).
select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    financiamento_pf_uh_total_geral,
    financiamento_pf_valor_total_geral,
    financiamento_pf_uh_total_faixa_1,
    financiamento_pf_valor_total_faixa_1,
    financiamento_pf_uh_total_faixa_2,
    financiamento_pf_valor_total_faixa_2,
    financiamento_pf_uh_total_faixa_3,
    financiamento_pf_valor_total_faixa_3,
    financiamento_pf_uh_faixa_3_sem_fundo_social,
    financiamento_pf_valor_faixa_3_sem_fundo_social,
    financiamento_pf_uh_faixa_3_fundo_social,
    financiamento_pf_valor_faixa_3_fundo_social,
    financiamento_pf_uh_total_classe_media,
    financiamento_pf_valor_total_classe_media,
    financiamento_pf_uh_fora_mcmv_apoio_e_cci,
    financiamento_pf_valor_fora_mcmv_apoio_e_cci
from {{ ref("silver_continuo_manual_mensais") }}
where
    coalesce(
        financiamento_pf_uh_total_faixa_1,
        financiamento_pf_uh_total_faixa_2,
        financiamento_pf_uh_total_faixa_3,
        financiamento_pf_uh_total_classe_media
    )
    is not null
order by ano desc, mes desc
