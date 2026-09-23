{{ config(materialized="table") }}

-- Gold: Orçamento × Contratação (Pró-Moradia) — o que o FGTS reservou para o programa em cada
-- ano e região, ao lado do que foi efetivamente contratado com orçamento daquele ano.
-- Grão: ano de orçamento × região.
--
-- O orçamento só existe para os anos com prestação de contas no lake (2021–2024 em 2026-09);
-- a contratação vai de 1995 em diante. O full join mantém os dois lados: ano sem orçamento
-- mostra só o contratado, e orçamento sem contrato mostra a reserva não usada.

with
    orcamento as (
        select * from {{ ref("prata_pro_moradia_orcamento") }}
    ),

    contratado as (
        select
            ano_orcamento,
            regiao,
            count(*) as qt_contratos,
            count(*) filter (where ic_contrato_vigente) as qt_contratos_vigentes,
            sum(valor_contratado) as valor_contratado,
            sum(valor_contratado) filter (where ic_contrato_vigente) as valor_contratado_vigente,
            sum(quantidade_uh) filter (where ic_contrato_vigente) as quantidade_uh
        from {{ ref("ouro_pro_moradia_ficha_contrato") }}
        where ano_orcamento is not null and regiao is not null
        group by ano_orcamento, regiao
    )

select
    coalesce(o.ano_orcamento, c.ano_orcamento) as ano_orcamento,
    coalesce(o.regiao, c.regiao) as regiao,
    o.orcamento_original,
    o.orcamento_final,
    o.orcamento_alocado,
    coalesce(c.qt_contratos, 0) as qt_contratos,
    coalesce(c.qt_contratos_vigentes, 0) as qt_contratos_vigentes,
    coalesce(c.valor_contratado, 0) as valor_contratado,
    coalesce(c.valor_contratado_vigente, 0) as valor_contratado_vigente,
    coalesce(c.quantidade_uh, 0) as quantidade_uh,
    case
        when o.orcamento_final > 0
        then round(coalesce(c.valor_contratado, 0) / o.orcamento_final * 100, 2)
    end as percentual_orcamento_contratado,
    o.ano_orcamento is not null as ic_tem_orcamento
from orcamento o
full outer join contratado c
    on c.ano_orcamento = o.ano_orcamento
    and c.regiao = o.regiao
