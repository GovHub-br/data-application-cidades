{{ config(materialized='table') }}

-- Prata do conjuntura: Saldo/poupança SBPE (ABECIP).
-- A staging é texto (ADR-0002): a tipagem é aqui.
-- A bronze guarda todas as cargas; cada carga traz a série inteira, então fica a última.
select
    data_referencia::date as data_referencia,
    deposito::numeric as deposito,
    retirada::numeric as retirada,
    captacao_liquida_valor::numeric as captacao_liquida_valor,
    captacao_liquida_pct::numeric as captacao_liquida_pct,
    saldo::numeric as saldo
from {{ ref('bronze_abecip_poupanca_sbpe') }}
where {{ ultima_carga(ref('bronze_abecip_poupanca_sbpe')) }}
