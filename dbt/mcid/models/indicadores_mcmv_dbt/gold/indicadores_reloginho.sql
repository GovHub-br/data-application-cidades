{{ config(materialized="table") }}

-- GOLD do reloginho MCMV (grupo A): série mensal SNH consolidada por agente.
--
-- Apenas a regra de negócio — a ingestão, a tipagem e a deduplicação por APF
-- ficam nas camadas bronze/silver (ver models/docs/arquitetura-medalhao-mcid.md
-- e docs/entregas/issue-130-refatoracao-medalhao-reloginho.md):
--
-- bronze_reloginho_snh_serie_mensal  -> cópia fiel dos snapshots mensais SNH
-- silver_reloginho_snh_apf_mes       -> tipado + domínio + dedup por APF
-- indicadores_reloginho (este)       -> soma mensal por agente
-- indicadores_reloginho_frente       -> soma mensal por agente x frente
--
-- Grão: uma linha por (agente_financeiro, dt_referencia) com os acumulados
-- uh_contratadas / uh_entregues / uh_vigentes, n_apf (APFs distintos) e a
-- contagem corrida de meses observados por agente (n_meses_observados).
--
-- A saída é idêntica à versão anterior do modelo (que lia o parquet direto):
-- a reconciliação contra a referência #66 (CAIXA 2026-03) continua válida.
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
with

    base as (select * from {{ ref("silver_reloginho_snh_apf_mes") }}),

    mensal as (
        select
            agente_financeiro,
            dt_referencia,
            coalesce(sum(uh_contratadas), 0) as uh_contratadas,
            coalesce(sum(uh_entregues), 0) as uh_entregues,
            coalesce(sum(uh_vigentes), 0) as uh_vigentes,
            count(distinct apf) as n_apf
        from base
        group by agente_financeiro, dt_referencia
    )

select
    dt_referencia,
    agente_financeiro,
    uh_contratadas,
    uh_entregues,
    uh_vigentes,
    n_apf,
    count(*) over (
        partition by agente_financeiro order by dt_referencia
    ) as n_meses_observados
from mensal
order by agente_financeiro, dt_referencia
