{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Novos Financiamentos Imobiliários por banco
-- (SBPE — Caixa, Bradesco, Itaú, Santander, BB), acumulado no ano.
-- Página 3, seção 6. Dado MANUAL (boletim.xlsx / conjuntura.bnz_manual_dados_mensais).

select
    periodo,
    ano,
    mes,
    make_date(ano::int, mes::int, 1) as data_referencia,
    abecip_sbpe_fin_uh_acum_caixa,
    abecip_sbpe_fin_uh_acum_bradesco,
    abecip_sbpe_fin_uh_acum_itau,
    abecip_sbpe_fin_uh_acum_santander,
    abecip_sbpe_fin_uh_acum_bb,
    abecip_sbpe_fin_uh_acum_demais,
    abecip_sbpe_fin_uh_acum_total,
    abecip_sbpe_fin_milhoes_acum_caixa,
    abecip_sbpe_fin_milhoes_acum_bradesco,
    abecip_sbpe_fin_milhoes_acum_itau,
    abecip_sbpe_fin_milhoes_acum_santander,
    abecip_sbpe_fin_milhoes_acum_bb,
    abecip_sbpe_fin_milhoes_acum_demais,
    abecip_sbpe_fin_milhoes_acum_total
from {{ ref('slv_manual_mensais') }}
where coalesce(abecip_sbpe_fin_uh_acum_caixa, abecip_sbpe_fin_uh_acum_bradesco,
               abecip_sbpe_fin_uh_acum_itau, abecip_sbpe_fin_uh_acum_santander,
               abecip_sbpe_fin_uh_acum_bb) is not null
order by ano desc, mes desc
