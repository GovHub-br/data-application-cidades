{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: N° de UH por condição de uso — FGTS-PF e
-- SBPE Aquisição (novos x usados). Página 4.
--
-- Lado FGTS-PF agora é AUTOMATIZADO (2026-08-25): sistema GEAVO da Caixa,
-- via silver_continuo_geavo_fgts_pf. Lado SBPE Aquisição continua MANUAL
-- (boletim.xlsx / manual_conjuntura.dados_mensais) — pendente da mesma
-- base automatizada do ABECIP do SBPE Const (aguardando o colega rodar a
-- fonte de novo com dado mais recente).

with fgts_pf as (
    select
        ano, mes,
        sum(qtd_unidades) filter (where tipo_imovel = 'Novo')  as fgts_pf_uh_novos,
        sum(qtd_unidades) filter (where tipo_imovel = 'Usado') as fgts_pf_uh_usados,
        sum(valor_emprestimo) filter (where tipo_imovel = 'Novo')  as fgts_pf_milhoes_novos,
        sum(valor_emprestimo) filter (where tipo_imovel = 'Usado') as fgts_pf_milhoes_usados
    from {{ ref('silver_continuo_geavo_fgts_pf') }}
    group by ano, mes
),

sbpe as (
    select
        periodo, ano, mes,
        abecip_sbpe_fin_uh_aq_novos,
        abecip_sbpe_fin_uh_aq_usados,
        abecip_sbpe_fin_milhoes_aq_novos,
        abecip_sbpe_fin_milhoes_aq_usados
    from {{ ref('silver_continuo_manual_mensais') }}
    where coalesce(abecip_sbpe_fin_uh_aq_novos, abecip_sbpe_fin_uh_aq_usados) is not null
)

select
    coalesce(sbpe.periodo, fgts_pf.ano || '-' || lpad(fgts_pf.mes::text, 2, '0')) as periodo,
    coalesce(sbpe.ano, fgts_pf.ano)  as ano,
    coalesce(sbpe.mes, fgts_pf.mes)  as mes,
    make_date(coalesce(sbpe.ano, fgts_pf.ano)::int, coalesce(sbpe.mes, fgts_pf.mes)::int, 1) as data_referencia,
    fgts_pf.fgts_pf_uh_novos,
    fgts_pf.fgts_pf_uh_usados,
    fgts_pf.fgts_pf_milhoes_novos,
    fgts_pf.fgts_pf_milhoes_usados,
    sbpe.abecip_sbpe_fin_uh_aq_novos,
    sbpe.abecip_sbpe_fin_uh_aq_usados,
    sbpe.abecip_sbpe_fin_milhoes_aq_novos,
    sbpe.abecip_sbpe_fin_milhoes_aq_usados
from fgts_pf
full outer join sbpe on sbpe.ano = fgts_pf.ano and sbpe.mes = fgts_pf.mes
order by ano desc, mes desc
