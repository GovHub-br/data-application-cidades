{{ config(materialized="table") }}

-- Silver: Empreendimento Rural — Visão unificada
-- Consolida os dados cadastrais, contratuais, andamento físico-financeiro de Caixa e BB.

with
    snh as (
        select * from {{ ref("prioritarios_snh") }}
    ),
    
    caixa as (
        select * from {{ ref("prioritarios_caixa") }}
    ),
    
    bb as (
        select * from {{ ref("prioritarios_bb") }}
    ),
    
    cad_pj as (
        select * from {{ ref("cadastro_pj_rural") }}
    ),

    pnhr_caixa as (
        select * from {{ ref("int_pnhr_caixa") }}
    ),

    pnhr_bb as (
        select * from {{ ref("int_pnhr_bb") }}
    )

select
    s.apf,
    
    -- Agente Financeiro e Modalidade
    s.agente_financeiro,
    s.modalidade,
    coalesce(s.ic_novo_mcmv, (cpj.apf is not null), false) as ic_novo_mcmv,

    -- Nomes e Entidades
    upper(coalesce(
        cpj.empreendimento_nome,
        s.empreendimento_nome,
        cx.empreendimento_nome,
        b.empreendimento_nome,
        pcx.empreendimento_nome,
        pbb.empreendimento_nome
    )) as empreendimento_nome,

    upper(coalesce(
        cpj.eo_nome,
        pcx.eo_nome,
        pbb.eo_nome
    )) as entidade_organizadora_nome,

    coalesce(
        cpj.eo_cnpj,
        pcx.eo_cnpj,
        pbb.eo_cnpj
    ) as entidade_organizadora_cnpj,

    upper(coalesce(
        s.construtora_nome,
        s.construtora_nome
    )) as construtora_nome,

    s.construtora_cnpj,

    -- Localização
    s.municipio,
    s.uf,
    s.estado_nome,
    s.regiao,
    s.cod_ibge,

    -- Situação
    coalesce(
        s.situacao,
        cx.situacao,
        b.situacao,
        pcx.situacao_obra,
        pbb.situacao_obra
    ) as situacao_empreendimento,

    coalesce(
        s.situacao_detalhamento,
        cx.situacao_detalhamento,
        b.situacao_detalhamento
    ) as detalhamento_situacao,

    -- UHs
    coalesce(
        s.uh_contratadas,
        cx.uh_contratadas,
        b.uh_contratadas,
        cpj.qt_uh_contratadas,
        pcx.qt_unidades,
        pbb.qt_unidades,
        0
    ) as quantidade_uh_contratadas,

    coalesce(
        s.uh_entregues,
        cx.uh_entregues,
        b.uh_entregues,
        cpj.qt_uh_concluidas,
        pcx.qt_unidades_entregues,
        pbb.qt_unidades_entregues,
        0
    ) as quantidade_uh_entregues,

    coalesce(
        s.uh_vigentes,
        cx.uh_vigentes,
        b.uh_vigentes,
        0
    ) as quantidade_uh_vigentes,

    coalesce(
        s.uh_distratadas,
        cx.uh_distratadas,
        b.uh_distratadas,
        0
    ) as quantidade_uh_distratadas,

    -- Valores Contratuais e KPIs
    coalesce(
        s.valor_contratado,
        cx.valor_contratado,
        b.valor_contratado,
        cpj.vr_investimento_total,
        pcx.vr_investimento,
        pbb.vr_investimento,
        0.00
    ) as valor_contratado,

    coalesce(
        s.valor_aporte_adicional,
        cx.valor_aporte_adicional,
        b.valor_aporte_adicional,
        cpj.vr_aporte,
        0.00
    ) as valor_aporte_adicional,

    coalesce(
        s.valor_desembolsado,
        cx.valor_desembolsado,
        b.valor_desembolsado,
        cpj.vr_liberado,
        pcx.vr_liberado,
        pbb.vr_liberado,
        0.00
    ) as valor_desembolsado,

    -- Execução física (%)
    coalesce(
        s.percentual_execucao_fisica,
        cx.percentual_execucao_fisica,
        b.percentual_execucao_fisica,
        cpj.percentual_obra_realizada,
        pcx.percentual_execucao_fisica,
        pbb.percentual_execucao_fisica,
        0.00
    ) as percentual_execucao_fisica,

    -- Coordenadas
    coalesce(s.latitude, cx.latitude, b.latitude) as latitude,
    coalesce(s.longitude, cx.longitude, b.longitude) as longitude,

    -- Datas
    coalesce(
        s.dt_contratacao,
        cx.dt_contratacao,
        b.dt_contratacao,
        cpj.dt_contratacao,
        pcx.dt_contrato,
        pbb.dt_contrato
    ) as dt_contratacao,

    coalesce(
        s.dt_previsao_entrega,
        cx.dt_previsao_entrega,
        b.dt_previsao_entrega
    ) as dt_previsao_entrega,

    coalesce(
        s.dt_termino,
        cpj.dt_conclusao,
        pcx.dt_conclusao_obra,
        pbb.dt_conclusao_obra
    ) as dt_conclusao_obra

from snh s
left join caixa cx on s.apf = cx.apf and s.agente_financeiro = 'CAIXA'
left join bb b on s.apf = b.apf and s.agente_financeiro = 'BB'
left join cad_pj cpj on s.apf = cpj.apf
left join pnhr_caixa pcx on s.apf = pcx.apf and s.agente_financeiro = 'CAIXA'
left join pnhr_bb pbb on s.apf = pbb.apf and s.agente_financeiro = 'BB'
