{{ config(materialized="table") }}

-- SILVER — série histórica mensal de empreendimentos MCMV, consolidada.
--
-- UNION das silvers por frente (FAR, FDS/Entidades, Rural), mesmo contrato
-- semântico. Substitui o snapshot único multi-frente anterior.
--
-- Grão: empreendimento × mês de referência × frente. Filtrável por frente_mcmv.
-- Alimenta gold_mcmv_snapshot_empreendimento_atual (estado corrente).
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
select *
from {{ ref('silver_mcmv_historico_empreendimento_far') }}
union all
select *
from {{ ref('silver_mcmv_historico_empreendimento_fds') }}
union all
select *
from {{ ref('silver_mcmv_historico_empreendimento_rural') }}
