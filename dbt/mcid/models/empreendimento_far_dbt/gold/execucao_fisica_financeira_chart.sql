{{ config(materialized="table") }}

with
    financeira as (
        select
            apf,
            mes,
            pct_executado_financeiro
        from {{ ref("evolucao_financeira") }}
    ),

    fisica as (
        select
            apf,
            date_trunc('month', dt_alteracao_situacao) as mes_fisica,
            max(pct_obra_realizada) as pct_obra_realizada
        from {{ ref("obra_mensal") }}
        where dt_alteracao_situacao is not null
        group by 1, 2
    ),

    empreendimento as (
        select
            apf,
            municipio,
            uf,
            empreendimento_nome
        from {{ ref("empreendimento") }}
    ),

    base as (
        select
            coalesce(f.apf, o.apf) as apf,
            to_char(coalesce(f.mes, o.mes_fisica), 'YYYY-MM-DD') as mes,
            o.pct_obra_realizada,
            f.pct_executado_financeiro
        from financeira f
        full outer join fisica o
            on f.apf = o.apf
            and f.mes = o.mes_fisica
    ),

    -- Cria grupos para cada vez que um valor NÃO NULO aparece (técnica para LOCF no Postgres)
    base_grp as (
        select
            apf,
            mes,
            pct_obra_realizada,
            pct_executado_financeiro,
            count(pct_obra_realizada) over (partition by apf order by mes) as grp_fisica,
            count(pct_executado_financeiro) over (partition by apf order by mes) as grp_financeira
        from base
    ),

    -- Preenche os valores nulos com o primeiro valor de cada grupo (que é o último valor válido arrastado)
    -- A partir de agora, se o físico parou em 50% no mês 1 e no mês 2 só o financeiro subiu, o gráfico ainda exibirá o físico estável em 50% no mês 2, evitando aquelas quedas de linha indesejadas para 0%.
    base_preenchida as (
        select
            apf,
            mes,
            first_value(pct_obra_realizada) over (partition by apf, grp_fisica order by mes) as pct_obra_realizada,
            first_value(pct_executado_financeiro) over (partition by apf, grp_financeira order by mes) as pct_executado_financeiro
        from base_grp
    )

select
    b.apf,
    concat(
        b.apf,
        ' - ', 
        e.municipio, '/', e.uf, 
        ' - ', 
        upper(e.empreendimento_nome)
    ) as apf_municipio_empreendimento,
    b.mes,
    coalesce(b.pct_obra_realizada, 0.0) as pct_obra_realizada,
    coalesce(b.pct_executado_financeiro, 0.0) as pct_executado_financeiro
from base_preenchida b
left join empreendimento e
    on b.apf = e.apf

