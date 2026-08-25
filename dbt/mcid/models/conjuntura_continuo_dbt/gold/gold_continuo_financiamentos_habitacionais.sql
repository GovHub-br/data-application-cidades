{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Financiamentos Habitacionais (UH) — FGTS-PJ e
-- SBPE Construção, trimestral + acumulado 12 meses. Página 2, seção 3, e
-- Página 5.
--
-- Lado FGTS-PJ agora é AUTOMATIZADO (2026-08-25): sistema GEAVO da Caixa
-- (desembolso, não valor contratado), via silver_continuo_geavo_fgts_pj —
-- substitui a planilha manual (CEAG). Lado SBPE Construção continua
-- MANUAL (boletim.xlsx / manual_conjuntura.dados_trimestrais) — pendente
-- da mesma base automatizada do ABECIP do SBPE Const (aguardando o colega
-- rodar a fonte de novo com dado mais recente).

with fgts_pj as (
    select
        ano,
        trimestre,
        ano * 4 + trimestre as periodo_idx,
        fgts_pj_desembolsado
    from {{ ref('silver_continuo_geavo_fgts_pj') }}
),

fgts_pj_12m as (
    select
        ano,
        trimestre,
        sum(fgts_pj_desembolsado) over (
            order by periodo_idx
            rows between 3 preceding and current row
        ) as fgts_pj_desembolsado_12m
    from fgts_pj
),

sbpe as (
    select
        periodo, ano, trimestre,
        financ_hab_sbpe_constr,
        financ_hab_sbpe_constr_acumulado_12_meses
    from {{ ref('silver_continuo_manual_trimestrais') }}
    where financ_hab_sbpe_constr is not null
)

select
    coalesce(sbpe.periodo, fgts_pj.trimestre::text || 'T' || fgts_pj.ano) as periodo,
    coalesce(sbpe.ano::int, fgts_pj.ano)                                   as ano,
    coalesce(left(sbpe.trimestre, 1)::int, fgts_pj.trimestre)                       as trimestre,
    fgts_pj.fgts_pj_desembolsado         as financ_hab_fgts_pj,
    fgts_pj_12m.fgts_pj_desembolsado_12m as financ_hab_fgts_pj_acumulado_12_meses,
    sbpe.financ_hab_sbpe_constr,
    sbpe.financ_hab_sbpe_constr_acumulado_12_meses
from fgts_pj
left join fgts_pj_12m on fgts_pj_12m.ano = fgts_pj.ano and fgts_pj_12m.trimestre = fgts_pj.trimestre
full outer join sbpe on sbpe.ano::int = fgts_pj.ano and left(sbpe.trimestre, 1)::int = fgts_pj.trimestre
order by ano desc, trimestre desc
