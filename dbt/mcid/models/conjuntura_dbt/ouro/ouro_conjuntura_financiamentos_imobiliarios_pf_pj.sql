{{ config(materialized='table') }}

-- Ouro do conjuntura: Financiamentos Imobiliários PF/PJ — concessões,
-- taxas de juros e inadimplência. Página 2/3, seção 3. Fonte: BACEN SGS
-- (prata_conjuntura_bacen_financiamentos_imobiliarios), automatizado.
-- só as séries de crédito: a prata do BACEN também traz outras do SGS (ex.: o INCC-M,
-- série 7456), que aqui virariam uma linha de data sem nenhum valor
with
    base as (
        select *
        from {{ ref('prata_conjuntura_bacen_financiamentos_imobiliarios') }}
        where
            tipo in (
                'pf_concessoes_rs_mi',
                'pj_concessoes_rs_mi',
                'pf_taxa_juros_aa',
                'pj_taxa_juros_aa',
                'pf_inadimplencia_pct',
                'pj_inadimplencia_pct'
            )
    )

select
    data,
    max(case when tipo = 'pf_concessoes_rs_mi' then valor end) as concessoes_pf_rs_mi,
    max(case when tipo = 'pj_concessoes_rs_mi' then valor end) as concessoes_pj_rs_mi,
    max(case when tipo = 'pf_taxa_juros_aa' then valor end) as taxa_juros_pf_aa,
    max(case when tipo = 'pj_taxa_juros_aa' then valor end) as taxa_juros_pj_aa,
    max(case when tipo = 'pf_inadimplencia_pct' then valor end) as inadimplencia_pf_pct,
    max(case when tipo = 'pj_inadimplencia_pct' then valor end) as inadimplencia_pj_pct
from base
group by data
order by data desc
