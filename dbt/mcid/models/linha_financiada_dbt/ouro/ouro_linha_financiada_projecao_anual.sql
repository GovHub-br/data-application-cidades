{{ config(materialized='table') }}

-- Projeção determinística reproduzível do relatório semanal. A fonte disponível
-- é mensal fechada: por isso não anualizamos mês parcial. Os meses restantes
-- são estimados pela média dos dois últimos meses anteriores ao mês de referência.

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
),

com_medias as (
    select
        *,
        avg(quantidade_contratos) over janela_2_meses as media_2m_contratos,
        avg(valor_financiamento) over janela_2_meses as media_2m_financiamento,
        avg(valor_descontos) over janela_2_meses as media_2m_descontos
    from mensal
    window janela_2_meses as (
        partition by fonte_recurso, segmento_linha_financiada
        order by competencia_contratacao
        rows between 2 preceding and 1 preceding
    )
),

referencia as (
    select distinct on (fonte_recurso, segmento_linha_financiada)
        competencia_contratacao as competencia_referencia,
        fonte_recurso,
        segmento_linha_financiada,
        media_2m_contratos,
        media_2m_financiamento,
        media_2m_descontos
    from com_medias
    order by fonte_recurso, segmento_linha_financiada, competencia_contratacao desc
),

acumulado_ano as (
    select
        r.competencia_referencia,
        r.fonte_recurso,
        r.segmento_linha_financiada,
        r.media_2m_contratos,
        r.media_2m_financiamento,
        r.media_2m_descontos,
        sum(m.quantidade_contratos) as quantidade_contratos_realizada_ano,
        sum(m.valor_financiamento) as valor_financiamento_realizado_ano,
        sum(m.valor_descontos) as valor_descontos_realizado_ano
    from referencia r
    join mensal m
      on m.fonte_recurso = r.fonte_recurso
     and m.segmento_linha_financiada = r.segmento_linha_financiada
     and extract(year from m.competencia_contratacao) = extract(year from r.competencia_referencia)
     and m.competencia_contratacao <= r.competencia_referencia
    group by 1, 2, 3, 4, 5, 6
)

select
    competencia_referencia,
    fonte_recurso,
    segmento_linha_financiada,
    extract(year from competencia_referencia)::integer as ano_referencia,
    extract(month from competencia_referencia)::integer as meses_realizados,
    12 - extract(month from competencia_referencia)::integer as meses_a_projetar,
    quantidade_contratos_realizada_ano,
    valor_financiamento_realizado_ano,
    valor_descontos_realizado_ano,
    media_2m_contratos,
    media_2m_financiamento,
    media_2m_descontos,
    coalesce(media_2m_contratos, 0) * (12 - extract(month from competencia_referencia)::integer) as quantidade_contratos_projetada_restante,
    coalesce(media_2m_financiamento, 0) * (12 - extract(month from competencia_referencia)::integer) as valor_financiamento_projetado_restante,
    coalesce(media_2m_descontos, 0) * (12 - extract(month from competencia_referencia)::integer) as valor_descontos_projetado_restante,
    quantidade_contratos_realizada_ano + coalesce(media_2m_contratos, 0) * (12 - extract(month from competencia_referencia)::integer) as quantidade_contratos_projetada_ano,
    valor_financiamento_realizado_ano + coalesce(media_2m_financiamento, 0) * (12 - extract(month from competencia_referencia)::integer) as valor_financiamento_projetado_ano,
    valor_descontos_realizado_ano + coalesce(media_2m_descontos, 0) * (12 - extract(month from competencia_referencia)::integer) as valor_descontos_projetado_ano,
    'Média dos dois meses anteriores; meses fechados disponíveis no MinIO'::text as metodo_projecao,
    current_timestamp as dt_gold
from acumulado_ano
