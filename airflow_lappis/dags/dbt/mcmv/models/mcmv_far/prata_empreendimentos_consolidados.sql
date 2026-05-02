with base_prioritarios as (

    select * from {{ ref('bronze_empreendimentos_caixa_bb') }}

),

base_operacional as (

    select * from {{ ref('bronze_int000_far_caixa_bb') }}

)

select
    p.uf,
    p.apf,
    p.nome_empreendimento,

    -- STATUS
    p.situacao_do_empreendimento,
    o.situacao_obra,
    o.situacao_retomada,

    -- PORTARIA
    o.portaria_selecao,

    -- DATAS
    p.dt_contratacao,
    o.dt_inicio_obra,
    o.dt_termino_obra,
    p.dt_previsao_entrega,

    -- EXECUÇÃO FÍSICA
    coalesce(o.percentual_execucao, p.percentual_exec) as percentual_execucao,

    -- UHs
    p.uh_contratadas,
    p.uh_entregues,
    p.uh_vigentes,
    p.uh_distratadas,
    o.uh_concluidas,
    o.uh_ociosas,

    -- FINANCEIRO
    p.vl_contratado,
    p.vl_aporte_adicional,
    o.vr_liberado,
    o.vl_contrapartida,

    -- CONTROLE
    p.fonte as fonte_prioritaria,
    o.fonte as fonte_operacional

from base_prioritarios p

left join base_operacional o
    on p.apf = o.apf