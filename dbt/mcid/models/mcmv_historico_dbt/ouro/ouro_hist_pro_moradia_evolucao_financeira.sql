{{ config(materialized="table") }}

-- OURO — série mensal de desembolso acumulado por contrato Pró-Moradia
-- (change criar-ouro-serie-historica-frentes-novas, design.md D4). Grão:
-- (codigo_contrato, mes).
--
-- Parte de prata_hist_pro_moradia_desembolso_mensal (grão já
-- deduplicado por competência — soma de transações duplicadas dentro do
-- mesmo snapshot resolvida na prata, ver comentário no próprio model) e
-- calcula a soma corrida (window function) por contrato — NÃO recalcula o
-- acumulado direto da bronze, para não reprocessar a mesma correção de
-- dedup já resolvida na prata.
--
-- mes vem de dte_ano/dte_mes_ref (texto cru na prata, ex. '2024'/'01') —
-- confirmado no dado real desta implementação.
--
-- valor_contratado/uf/municipio/nome_empreendimento vêm de
-- prata_hist_pro_moradia_contrato via left join (todo cod_contrato de
-- desembolso já existe no universo Pró-Moradia — a prata de desembolso é
-- filtrada por inner join com esta mesma tabela).
{% set desembolso = ref('prata_hist_pro_moradia_desembolso_mensal') %}
{% set contrato = ref('prata_hist_pro_moradia_contrato') %}

with

    desembolso_mes as (
        select
            d.frente_mcmv,
            d.cod_contrato as codigo_contrato,
            make_date(
                cast(d.dte_ano as integer), cast(d.dte_mes_ref as integer), 1
            ) as mes,
            d.valor_liberado as valor_liberado_mes,
            c.valor_contratado,
            c.uf,
            c.municipio,
            c.nome_empreendimento
        from {{ desembolso }} as d
        left join {{ contrato }} as c on d.cod_contrato = c.codigo_contrato
    ),

    acumulado as (
        select
            *,
            sum(valor_liberado_mes) over (
                partition by codigo_contrato order by mes
            ) as valor_acumulado
        from desembolso_mes
    )

select
    frente_mcmv,
    codigo_contrato,
    mes,
    uf,
    municipio,
    nome_empreendimento,
    valor_contratado,
    valor_liberado_mes,
    valor_acumulado,
    -- nullif no denominador: percentual fica NULL para contrato com
    -- valor_contratado nulo ou zero, sem erro de divisão por zero.
    valor_acumulado / nullif(valor_contratado, 0) as percentual_acumulado_contratado
from acumulado
order by codigo_contrato, mes
