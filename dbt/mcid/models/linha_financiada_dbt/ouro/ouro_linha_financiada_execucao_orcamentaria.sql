{{ config(materialized='table') }}

select
    *,
    coalesce(orcamento_atualizado, 0) - coalesce(pagamentos_totais, 0) as saldo_orcamentario_estimado,
    case
        when periodicidade = 'Anual' and despesas_pagas is null
            then 'Orçamento disponível; execução financeira mensal não disponível nesta fonte'
        when despesas_pagas is not null or restos_a_pagar_pagos is not null
            then 'Execução financeira disponível'
        else 'Cobertura insuficiente'
    end as cobertura_execucao,
    current_timestamp as dt_gold
from {{ ref('prata_linha_financiada_execucao_orcamentaria') }}
