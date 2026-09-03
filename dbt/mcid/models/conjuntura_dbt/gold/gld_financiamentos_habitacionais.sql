{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: Financiamentos Habitacionais (UH) — FGTS-PJ e
-- SBPE Construção, trimestral + acumulado 12 meses. Página 2, seção 3, e
-- Página 5.
--
-- Lado FGTS-PJ agora é AUTOMATIZADO (2026-08-25): sistema GEAVO da Caixa
-- (Base_PJ_FGTS, contagem de UH — ver comentário em
-- slv_geavo_fgts_pj.sql, validado exato contra os 3 boletins
-- publicados que o Lucas tem) — substitui a planilha manual (CEAG). Lado
-- SBPE Construção continua MANUAL (boletim.xlsx /
-- conjuntura.bnz_manual_dados_trimestrais) — pendente da mesma base
-- automatizada do ABECIP do SBPE Const (aguardando o colega rodar a
-- fonte de novo com dado mais recente).

with fgts_pj as (
    select
        ano,
        trimestre,
        ano * 4 + trimestre as periodo_idx,
        fgts_pj_uh
    from {{ ref('slv_geavo_fgts_pj') }}
),

fgts_pj_12m as (
    select
        ano,
        trimestre,
        sum(fgts_pj_uh) over (
            order by periodo_idx
            rows between 3 preceding and current row
        ) as fgts_pj_uh_12m
    from fgts_pj
),

-- SBPE Construção: agrega a série mensal da ABECIP por trimestre.
-- Substitui o preenchimento manual (2026-08-29). O manual estava **errado**
-- nos dois trimestres conferíveis: tinha 13.115 no 1T2025 e 18.950 no
-- 2T2025, mas o boletim publica 19.130 no 1T2025, e o acumulado de 12 meses
-- que ele próprio publica implica 22.181 no 2T2025 — os dois valores que a
-- fonte automatizada devolve.
sbpe_mensal as (
    select
        ano,
        trimestre,
        ano * 4 + trimestre as periodo_idx,
        sum(unidades_construcao) as sbpe_constr_uh
    from (
        select
            ano,
            (floor((mes - 1) / 3))::int + 1 as trimestre,
            unidades_construcao
        from {{ ref('slv_abecip_financiamentos') }}
    ) m
    group by ano, trimestre
),

sbpe as (
    select
        trimestre::text || 'T' || ano as periodo,
        ano,
        trimestre,
        sbpe_constr_uh as financ_hab_sbpe_constr,
        sum(sbpe_constr_uh) over (
            order by periodo_idx
            rows between 3 preceding and current row
        ) as financ_hab_sbpe_constr_acumulado_12_meses
    from sbpe_mensal
)

select
    coalesce(sbpe.periodo, fgts_pj.trimestre::text || 'T' || fgts_pj.ano) as periodo,
    coalesce(sbpe.ano, fgts_pj.ano)                                       as ano,
    coalesce(sbpe.trimestre, fgts_pj.trimestre)                           as trimestre,
    make_date(coalesce(sbpe.ano, fgts_pj.ano)::int,
              (coalesce(sbpe.trimestre, fgts_pj.trimestre)::int - 1) * 3 + 1, 1) as data_referencia,
    (coalesce(sbpe.trimestre, fgts_pj.trimestre)::int::text || 'T'
     || coalesce(sbpe.ano, fgts_pj.ano)::int::text)                       as edicao,
    fgts_pj.fgts_pj_uh             as financ_hab_fgts_pj,
    fgts_pj_12m.fgts_pj_uh_12m     as financ_hab_fgts_pj_acumulado_12_meses,
    sbpe.financ_hab_sbpe_constr,
    sbpe.financ_hab_sbpe_constr_acumulado_12_meses
from fgts_pj
left join fgts_pj_12m on fgts_pj_12m.ano = fgts_pj.ano and fgts_pj_12m.trimestre = fgts_pj.trimestre
full outer join sbpe on sbpe.ano = fgts_pj.ano and sbpe.trimestre = fgts_pj.trimestre
order by ano desc, trimestre desc
