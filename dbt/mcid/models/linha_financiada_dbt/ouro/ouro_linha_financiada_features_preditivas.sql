{{ config(materialized='table') }}

-- Conjunto de atributos para treino/validação temporal. Não publica previsão
-- sem modelo validado; evita apresentar uma extrapolação como resultado de IA.

with mensal as (
    select
        competencia_contratacao,
        fonte_recurso,
        segmento_linha_financiada,
        sum(quantidade_contratos) as quantidade_contratos,
        sum(valor_financiamento) as valor_financiamento,
        sum(valor_desconto_fgts + valor_desconto_ogu) as valor_descontos
    from {{ ref('ouro_linha_financiada_resumo_mensal') }}
    group by 1, 2, 3
)
select
    *,
    lag(quantidade_contratos, 1) over w as contratos_mes_anterior,
    lag(quantidade_contratos, 12) over w as contratos_ano_anterior,
    lag(valor_financiamento, 1) over w as valor_mes_anterior,
    lag(valor_financiamento, 12) over w as valor_ano_anterior,
    avg(quantidade_contratos) over (
        partition by fonte_recurso, segmento_linha_financiada
        order by competencia_contratacao rows between 3 preceding and 1 preceding
    ) as media_movel_contratos_3m,
    avg(valor_financiamento) over (
        partition by fonte_recurso, segmento_linha_financiada
        order by competencia_contratacao rows between 3 preceding and 1 preceding
    ) as media_movel_valor_3m,
    extract(month from competencia_contratacao)::integer as mes_sazonal,
    extract(quarter from competencia_contratacao)::integer as trimestre_sazonal,
    current_timestamp as dt_gold
from mensal
window w as (
    partition by fonte_recurso, segmento_linha_financiada
    order by competencia_contratacao
)
