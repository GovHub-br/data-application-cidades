{{ config(materialized="table") }}

-- Gold: Evolução Financeira (Pró-Moradia) — série mensal de liberações por contrato, com o
-- acumulado e o percentual sobre o contratado. Grão: contrato × mês.
--
-- O acumulado começa na primeira liberação do ARQUIVO (a série do canal começa em 2000), então
-- para contrato anterior a 2000 é um piso — ver `ic_serie_financeira_incompleta` na ficha.

with
    serie as (
        select * from {{ ref("prata_pro_moradia_desembolso_mensal") }}
    ),

    contrato as (
        select contrato, uf, regiao, tipo_intervencao, valor_contratado, ic_serie_financeira_incompleta
        from {{ ref("ouro_pro_moradia_ficha_contrato") }}
    )

select
    s.contrato,
    s.dt_competencia as mes,
    c.uf,
    c.regiao,
    c.tipo_intervencao,
    s.qt_lancamentos,
    s.vr_liberado as vr_liberado_mes,
    s.vr_estornado as vr_estornado_mes,
    sum(s.vr_liberado) over (
        partition by s.contrato order by s.dt_competencia
        rows between unbounded preceding and current row
    ) as vr_acumulado,
    case
        when coalesce(c.valor_contratado, 0) > 0
        then round(
            sum(s.vr_liberado) over (
                partition by s.contrato order by s.dt_competencia
                rows between unbounded preceding and current row
            ) / c.valor_contratado * 100, 2
        )
    end as percentual_acumulado_contratado,
    c.ic_serie_financeira_incompleta
from serie s
join contrato c on c.contrato = s.contrato
