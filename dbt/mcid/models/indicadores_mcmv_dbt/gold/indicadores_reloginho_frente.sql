{{ config(materialized="table") }}

-- GOLD do reloginho MCMV (grupo A): série mensal SNH quebrada por FRENTE.
--
-- Mesma regra da indicadores_reloginho, com frente_mcmv (FAR / Entidades /
-- Rural, derivada de `modalidade` na silver) no grão. Serve para:
-- * o reloginho por frente no dashboard;
-- * a verificação de cobertura histórica por frente
-- (test assert_reloginho_frente_cobertura_mensal + doc
-- issue-130-refatoracao-medalhao-reloginho.md).
--
-- Grão: uma linha por (agente_financeiro, frente_mcmv, dt_referencia).
-- Somar todas as frentes reproduz a indicadores_reloginho.
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
with

    base as (select * from {{ ref("silver_reloginho_snh_apf_mes") }}),

    mensal as (
        select
            agente_financeiro,
            frente_mcmv,
            dt_referencia,
            coalesce(sum(uh_contratadas), 0) as uh_contratadas,
            coalesce(sum(uh_entregues), 0) as uh_entregues,
            coalesce(sum(uh_vigentes), 0) as uh_vigentes,
            count(distinct apf) as n_apf
        from base
        where frente_mcmv is not null
        group by agente_financeiro, frente_mcmv, dt_referencia
    )

select
    dt_referencia,
    agente_financeiro,
    frente_mcmv,
    uh_contratadas,
    uh_entregues,
    uh_vigentes,
    n_apf,
    count(*) over (
        partition by agente_financeiro, frente_mcmv order by dt_referencia
    ) as n_meses_observados
from mensal
order by agente_financeiro, frente_mcmv, dt_referencia
