{{ config(materialized="table") }}

-- Gold do conjuntura contínuo: Financiamentos Imobiliários PF/PJ — concessões,
-- taxas de juros e inadimplência. Página 2/3, seção 3. Fonte: BACEN SGS
-- (silver_continuo_bacen_financiamentos_imobiliarios), automatizado.
with
    base as (select * from {{ ref("silver_continuo_bacen_financiamentos_imobiliarios") }})

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
