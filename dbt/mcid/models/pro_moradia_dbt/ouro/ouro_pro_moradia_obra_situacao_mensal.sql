{{ config(materialized="table") }}

-- Gold: Quantos contratos do Pró-Moradia estavam em cada situação de obra, mês a mês, e quantos
-- entraram nela no mês. Réplica da pergunta do Rural "hoje só chega o dado final": a execução de
-- obra do Canal FGTS já é uma série mensal. Grão: mês × UF × situação da obra.

with
    med as (
        select
            e.cod_contrato,
            date_trunc('month', e.dt_avaliacao)::date as mes,
            coalesce(e.situacao_obra, 'Sem situação') as situacao_obra,
            lag(e.situacao_obra) over (partition by e.cod_contrato order by e.dt_avaliacao) as situacao_anterior,
            e.percentual_obra_realizado
        from {{ ref("prata_pro_moradia_execucao_obra") }} e
        where not coalesce(e.ic_avaliacao_futura, false)
    ),

    c as (select cod_contrato, uf, valor_contratado from {{ ref("prata_pro_moradia_contrato") }})

select
    m.mes,
    coalesce(c.uf, 'ND') as uf,
    {{ regiao_da_uf("c.uf") }} as regiao,
    m.situacao_obra,
    count(*) as qt_contratos,
    coalesce(sum(c.valor_contratado), 0) as valor_contratado,
    round(avg(m.percentual_obra_realizado), 1) as percentual_medio_realizado,
    count(*) filter (where m.situacao_anterior is not null and m.situacao_anterior is distinct from m.situacao_obra) as qt_entraram_no_mes
from med m
left join c on c.cod_contrato = m.cod_contrato
group by 1, 2, 3, 4
