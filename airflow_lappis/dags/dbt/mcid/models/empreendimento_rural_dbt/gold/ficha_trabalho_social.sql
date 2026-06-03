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
        from {{ ref("trabalho_social_caixa") }}
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
        from {{ ref("trabalho_social_bb") }}
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
        from {{ ref("ficha_empreendimento_rural") }}
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
    
    -- Valores
    coalesce(t.vr_global_ts, 0.00) as valor_global_ts,
    coalesce(t.vr_desembolsado_ts, 0.00) as valor_desembolsado_ts,
    
    -- Execuções
    coalesce(t.percentual_execucao_ts, 0.00) as percentual_execucao_ts,
    coalesce(t.percentual_obra, 0.00) as percentual_obra_ts_reportado,

    -- Regra de Negócio: Defasagem físico-social (margem de 10%)
    case
        when t.percentual_obra > (t.percentual_execucao_ts + 10) then 'Trabalho Social Atrasado'
        when t.percentual_obra < (t.percentual_execucao_ts - 10) then 'Trabalho Social Adiantado'
        else 'Ritmo Alinhado'
    end as ritmo_social_fisico

from ts_union t
inner join fichas f on t.apf = f.apf
