{{ config(materialized="table") }}

-- Gold: Acompanhamento de Trabalho Social (Rural)
-- Consolida os dados do Plano de Trabalho Social (PTS) de Caixa e BB, permitindo comparar execução física com social.

with
    ts_caixa as (
        select
            apf,
            'CAIXA' as agente_financeiro,
            situacao_ts,
            vr_global_ts,
            vr_desembolsado_ts,
            percentual_execucao_ts,
            percentual_obra
        from {{ ref("silver_trabalho_social_caixa") }}
    ),

    ts_bb as (
        select
            apf,
            'BB' as agente_financeiro,
            situacao_ts,
            vr_global_ts,
            vr_desembolsado_ts,
            percentual_execucao_ts,
            percentual_obra
        from {{ ref("silver_trabalho_social_bb") }}
    ),

    ts_union as (
        select * from ts_caixa
        union all
        select * from ts_bb
    ),

    fichas as (
        select
            apf,
            nome_empreendimento,
            municipio,
            uf,
            programa,
            quantidade_uh
        from {{ ref("gold_ficha_empreendimento") }}
    )

select
    f.apf,
    f.nome_empreendimento,
    f.municipio,
    f.uf,
    f.programa,
    f.quantidade_uh,
    t.agente_financeiro,
    upper(coalesce(t.situacao_ts, 'NÃO INFORMADO')) as situacao_trabalho_social,

    -- Valores — sem coalesce para 0. Empreendimento sem registro de PTS não é um
    -- empreendimento com PTS de R$ 0 e 0% executado: é um sobre o qual não há informação.
    t.vr_global_ts as valor_global_ts,
    t.vr_desembolsado_ts as valor_desembolsado_ts,

    -- Execuções
    t.percentual_execucao_ts as percentual_execucao_ts,
    t.percentual_obra as percentual_obra_ts_reportado,

    -- Regra de Negócio: Defasagem físico-social (margem de 10%)
    -- 'Ritmo Alinhado' era o `else` de um CASE com NULL nos dois lados: sem os dois
    -- percentuais não há defasagem a medir, e afirmar alinhamento é inventar.
    case
        when t.percentual_obra is null or t.percentual_execucao_ts is null
            then 'Sem Informação'
        when t.percentual_obra > t.percentual_execucao_ts + 10 then 'Trabalho Social Atrasado'
        when t.percentual_obra < t.percentual_execucao_ts - 10 then 'Trabalho Social Adiantado'
        else 'Ritmo Alinhado'
    end as ritmo_social_fisico

from ts_union t
inner join fichas f on t.apf = f.apf
