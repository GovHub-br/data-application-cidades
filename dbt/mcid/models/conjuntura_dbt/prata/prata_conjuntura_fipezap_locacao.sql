{{ config(materialized='table') }}

-- Prata do conjuntura: Índice FipeZap de locação (FIPE).
-- A staging é texto (ADR-0002): a tipagem é aqui.
-- A bronze guarda todas as cargas; cada carga traz a série inteira, então fica a última.
select
    data_referencia::date as data_referencia,
    imoveis_residenciais_locacao_numero_indice_total::numeric
    as imoveis_residenciais_locacao_numero_indice_total,
    imoveis_residenciais_locacao_var_mensal_total::numeric
    as imoveis_residenciais_locacao_var_mensal_total,
    imoveis_residenciais_locacao_var_ano_total::numeric
    as imoveis_residenciais_locacao_var_ano_total,
    left(_ingested_at, 19)::timestamp as dt_ingest
from {{ ref('bronze_fipezap_locacao') }}
where {{ ultima_carga(ref('bronze_fipezap_locacao')) }}
