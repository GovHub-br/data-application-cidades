{{ config(materialized="table") }}

-- Silver: Empreendimento FAR — Visão unificada
-- Reúne dados cadastrais, contratuais e status físico-financeiro de cada APF.

with
    cadastro as (
        select * from {{ ref("cadastro_pj") }}
    ),
    
    -- Consolidado pode ter mais de uma proposta para o mesmo ID, pegamos a mais recente
    consolidado as (
        select * from (
            select *,
                   row_number() over(partition by id_proposta order by dt_movimento desc, dt_protocolo desc) as rn
            from {{ ref("consolidado") }}
            where id_proposta is not null
        ) t where rn = 1
    ),
    
    obra as (
        select * from {{ ref("obra_mensal") }}
    ),
    
    caixa as (
        select * from {{ ref("dados_prioritarios_caixa") }}
    )

select
    c.apf,

    -- 1. Dados do Empreendimento
    coalesce(cons.portaria_contratacao, 'Não informada') as portaria_selecao,
    cons.proponente_cnpj,
    cons.proponente_nome,
    cons.tomador_cnpj,
    cons.tomador_nome,
    c.empreendimento_nome,
    c.qt_uh as quantidade_uh,
    cons.municipio,
    cons.uf,
    cons.co_originacao,
    cons.co_tipo_demanda,
    coalesce(cx.situacao, 'Situação não mapeada') as situacao_empreendimento,
    c.co_tipo_edificacao as codigo_tipologia,
    case c.co_tipo_edificacao
        when 1 then 'Casa Térrea'
        when 2 then 'Apartamento'
        when 3 then 'Sobrado'
        when 4 then 'Misto'
        else 'Não Informado'
    end as tipologia,
    -- Heurística baseada em 3.3 pessoas por família
    floor(c.qt_uh * 3.3)::int as pessoas_atendidas,

    -- 2. Dados do Contrato
    -- O valor total do investimento engloba FAR + Contrapartidas
    coalesce(c.vr_total_investimento, 0.0) as valor_contratado,
    c.agente_financeiro,
    c.dt_contratacao,
    coalesce(cx.valor_aporte_adicional, 0.0) as valor_aporte_adicional,
    case
        when coalesce(c.qt_uh, 0) > 0 then c.vr_total_investimento / c.qt_uh
        else 0.0
    end as valor_por_uh,

    -- 3. Evolução Física
    coalesce(o.pct_obra_realizada, cx.pct_execucao, 0.0) as percentual_execucao_fisica,
    o.dt_conclusao_obra,
    o.dt_entrega,
    cx.dt_previsao_entrega,

    -- 4. Evolução Financeira
    coalesce(cx.valor_desembolsado, 0.0) as valor_desembolsado,
    case
        when coalesce(c.vr_total_investimento, 0.0) > 0 
        then (coalesce(cx.valor_desembolsado, 0.0) / c.vr_total_investimento) * 100
        else 0.0
    end as percentual_execucao_financeira

from cadastro c
left join consolidado cons on c.id_proposta = cons.id_proposta
left join obra o on c.apf = o.apf
left join caixa cx on c.apf = cx.apf
