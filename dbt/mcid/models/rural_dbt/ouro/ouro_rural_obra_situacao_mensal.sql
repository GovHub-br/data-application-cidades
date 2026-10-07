{{ config(materialized="table") }}

-- Gold: Quantas obras do Rural estavam em cada situação, mês a mês, e quantas ENTRARAM nela
-- naquele mês. Responde "hoje só chega o dado final": mostra a trajetória que existe no lake.
-- Grão: fonte × mês × UF × situação.
--
-- Duas fontes, sem misturar: a obra mensal do Novo Rural (estágio do layout, dez/2025 em diante)
-- e o andamento GEHIS da CAIXA (desde jul/2021, só APFs do Rural).

with
    op as (select apf, uf, quantidade_uh_contratadas from {{ ref("prata_rural_operacao_eo") }}),

    monit as (
        select
            'Obra mensal Novo Rural (estágio)' as fonte,
            s.dt_referencia,
            s.apf,
            s.estagio_obra as situacao,
            s.estagio_mes_anterior as situacao_anterior
        from {{ ref("prata_rural_obra_mensal_serie") }} s
    ),

    gehis as (
        select
            'GEHIS CAIXA (andamento)' as fonte,
            g.dt_referencia,
            g.apf,
            g.situacao_obra as situacao,
            lag(g.situacao_obra) over (partition by g.apf order by g.dt_referencia) as situacao_anterior
        from {{ ref("prata_rural_gehis_andamento_obra") }} g
        where g.frente_mcmv = 'Rural'
    ),

    tudo as (
        select * from monit
        union all
        select * from gehis
    )

select
    t.fonte,
    t.dt_referencia,
    coalesce(o.uf, 'ND') as uf,
    {{ regiao_da_uf("o.uf") }} as regiao,
    coalesce(t.situacao, 'Sem situação') as situacao,
    count(*) as qt_obras,
    coalesce(sum(o.quantidade_uh_contratadas), 0) as uh_contratadas,
    count(*) filter (where t.situacao_anterior is distinct from t.situacao and t.situacao_anterior is not null) as qt_entraram_no_mes
from tudo t
left join op o on o.apf = t.apf
group by 1, 2, 3, 4, 5
