{{ config(materialized="table", alias="gold_execucao_fisica_financeira_chart") }}

-- Gold: Execução Física × Financeira (chart) — as duas curvas na mesma série mensal
-- O full outer join junta meses que existem só num dos lados, e o LOCF arrasta o
-- último valor válido para os meses vazios (senão a linha despencaria para 0%).
with
    financeira as (
        select apf, mes, pct_executado_financeiro
        from {{ ref("far_silver_evolucao_financeira") }}
    ),

    fisica as (
        select
            apf,
            date_trunc('month', dt_alteracao_situacao) as mes_fisica,
            max(pct_obra_realizada) as pct_obra_realizada
        from {{ ref("far_silver_obra_mensal") }}
        where dt_alteracao_situacao is not null
        group by 1, 2
    ),

    empreendimento as (
        select apf, municipio, uf, empreendimento_nome
        from {{ ref("far_silver_empreendimento") }}
    ),

    base as (
        select
            coalesce(f.apf, o.apf) as apf,
            to_char(coalesce(f.mes, o.mes_fisica), 'YYYY-MM-DD') as mes,
            o.pct_obra_realizada,
            f.pct_executado_financeiro
        from financeira f
        full outer join fisica o on f.apf = o.apf and f.mes = o.mes_fisica
    ),

    -- LOCF passo 1: o count() acumulado cria um grupo por valor não nulo
    base_grp as (
        select
            apf,
            mes,
            pct_obra_realizada,
            pct_executado_financeiro,
            count(pct_obra_realizada) over (partition by apf order by mes) as grp_fisica,
            count(pct_executado_financeiro) over (
                partition by apf order by mes
            ) as grp_financeira
        from base
    ),

    -- LOCF passo 2: dentro do grupo, o primeiro valor é o último válido arrastado
    base_preenchida as (
        select
            apf,
            mes,
            first_value(pct_obra_realizada) over (
                partition by apf, grp_fisica order by mes
            ) as pct_obra_realizada,
            first_value(pct_executado_financeiro) over (
                partition by apf, grp_financeira order by mes
            ) as pct_executado_financeiro
        from base_grp
    )

select
    b.apf,
    concat(
        b.apf, ' - ', e.municipio, '/', e.uf, ' - ', upper(e.empreendimento_nome)
    ) as apf_municipio_empreendimento,
    b.mes,
    coalesce(b.pct_obra_realizada, 0.0) as pct_obra_realizada,
    coalesce(b.pct_executado_financeiro, 0.0) as pct_executado_financeiro
from base_preenchida b
left join empreendimento e on b.apf = e.apf
