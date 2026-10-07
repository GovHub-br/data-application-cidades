{{ config(materialized="table") }}

-- Prata: Situação da obra do Novo Rural MÊS A MÊS (série), com os códigos do layout decodificados.
-- Fonte: bronze_shpt_obra_mensal_rural (todas as remessas MONIT_MOV_OBRA_RURAL_MENSAL, dez/2025
-- em diante) + seed dominio_rural_obra (descrições do layout MONIT_MOV_OBRA_RURAL_LAYOUT).
-- Grão: APF × mês de referência.
--
-- A prata_rural_obra_mensal guarda só o último retrato; esta guarda todos, para medir a
-- trajetória ("hoje só chega o dado final"). Remessas de 202602 vieram duplicadas (uma cópia
-- arquivada por engano na pasta do FAR): fica uma linha por APF e mês, a do arquivo mais
-- recente pelo nome.

with
    bruto as (
        select
            {{ target.schema }}.normalize_apf(nu_apf::text) as apf,
            dt_referencia::date as dt_referencia,
            {{ parse_int('co_situacao_operacao::text') }} as co_situacao_operacao,
            {{ parse_int('co_andamento_operacao::text') }} as co_andamento_operacao,
            {{ parse_numeric('pc_obra_prevista::text', 'numeric(6,2)') }} as percentual_obra_prevista,
            {{ parse_numeric('pc_obra_realizada::text', 'numeric(6,2)') }} as percentual_obra_realizada,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_alteracao_situacao::text), '')) as dt_alteracao_situacao,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_paralisacao::text), '')) as dt_paralisacao,
            nullif({{ parse_int('co_classificacao_paralisados::text') }}, 0) as co_classificacao_paralisados,
            nullif({{ parse_int('co_classificacao_nao_retomada::text') }}, 0) as co_classificacao_nao_retomada,
            nullif({{ parse_int('co_motivo_distrato_empreendimento::text') }}, 0) as co_motivo_distrato,
            nullif(trim({{ target.schema }}.corrigir_mojibake(no_detalhe_paralisacao_retomada::text)), '') as detalhe_paralisacao,
            {{ target.schema }}.parse_date_br(nullif(trim(dt_previsao_entrega_do_empreendimento::text), '')) as dt_previsao_entrega,
            source_file::text as arquivo_de_origem
        from {{ ref("bronze_shpt_obra_mensal_rural") }}
        where nullif(trim(nu_apf::text), '') is not null
    ),

    unico as (
        select distinct on (apf, dt_referencia) *
        from bruto
        order by apf, dt_referencia, regexp_replace(arquivo_de_origem, '^.*/', '') desc
    ),

    dominio as (
        select campo, codigo, descricao, estagio from {{ ref("dominio_rural_obra") }}
    )

select
    u.apf,
    u.dt_referencia,
    u.co_situacao_operacao,
    coalesce(ds.descricao, 'Código não documentado') as situacao_operacao,
    coalesce(ds.estagio, 'Não documentado') as estagio_obra,
    u.co_andamento_operacao,
    da.descricao as andamento_operacao,
    u.percentual_obra_prevista,
    u.percentual_obra_realizada,
    u.percentual_obra_realizada - u.percentual_obra_prevista as desvio_cronograma_pp,
    u.dt_alteracao_situacao,
    u.dt_paralisacao,
    u.co_classificacao_paralisados,
    dp.descricao as motivo_paralisacao,
    u.co_classificacao_nao_retomada,
    dn.descricao as motivo_nao_retomada,
    u.co_motivo_distrato,
    dd.descricao as motivo_distrato,
    u.detalhe_paralisacao,
    u.dt_previsao_entrega,
    -- situação no mês anterior da MESMA APF, para contar mudanças de estágio
    lag(coalesce(ds.estagio, 'Não documentado')) over (partition by u.apf order by u.dt_referencia) as estagio_mes_anterior,
    u.arquivo_de_origem
from unico u
left join dominio ds on ds.campo = 'co_situacao_operacao' and ds.codigo = u.co_situacao_operacao
left join dominio da on da.campo = 'co_andamento_operacao' and da.codigo = u.co_andamento_operacao
left join dominio dp on dp.campo = 'co_classificacao_paralisados' and dp.codigo = u.co_classificacao_paralisados
left join dominio dn on dn.campo = 'co_classificacao_nao_retomada' and dn.codigo = u.co_classificacao_nao_retomada
left join dominio dd on dd.campo = 'co_motivo_distrato_empreendimento' and dd.codigo = u.co_motivo_distrato
