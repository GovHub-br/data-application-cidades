{{ config(materialized="table") }}

-- Prata: natureza, destino e pactuação de cada empreendimento FAR contratado (um por APF).
-- O cadastro PJ (MONIT) dá tipologia, demanda, contrapartidas por ente, equipamentos e
-- aporte/suplementação; o consolidado HIS dá o que foi declarado na proposta: portaria,
-- originação, demanda no enquadramento, qualificação do terreno, elevador e entorno.
with
    dominio as (select campo, codigo, rotulo from {{ ref("dominio_far_perguntas") }}),

    cad as (
        select
            {{ var('schema_udfs') }}.normalize_apf(nu_apf) as apf,
            nullif(trim(no_identificacao_proposta), '') as id_proposta,
            lpad(nullif(trim(co_tipo_edificacao), ''), 3, '0') as co_tipo_edificacao,
            lpad(nullif(trim(co_tipo_administracao), ''), 3, '0') as co_tipo_administracao,
            lpad(nullif(trim(co_linha_atendimento), ''), 3, '0') as co_linha_atendimento,
            lpad(nullif(trim(co_tipo_demanda), ''), 3, '0') as co_tipo_demanda,
            lpad(nullif(trim(co_tipo_empreendimento), ''), 3, '0') as co_tipo_empreendimento,
            lpad(nullif(trim(co_regime_propriedade), ''), 3, '0') as co_regime_propriedade,
            lpad(nullif(trim(co_ente_publico_proponente), ''), 3, '0') as co_ente_proponente,
            lpad(nullif(trim(co_tipo_ente_publico_contrapartida), ''), 3, '0') as co_ente_contrapartida,
            lpad(nullif(trim(co_operacao_aporte_retomada), ''), 3, '0') as co_operacao_aporte,
            trim(ic_terreno_doado) = 'S' as ic_terreno_doado,
            trim(ic_previsao_area_comercial) = 'S' as ic_area_comercial,
            trim(ic_substiuicao_construtora) = 'S' as ic_substituicao_construtora,
            {{ parse_numeric("nu_area_terreno") }} as area_terreno_m2,
            {{ parse_financial_value("vr_terreno") }} as vr_terreno,
            {{ parse_financial_value("vr_emprestimo_far") }} as vr_far,
            {{ parse_financial_value("vr_total_investimento") }} as vr_total_investimento,
            {{ parse_financial_value("vr_contrapartida_municipio_terreno") }} as vr_cp_municipio_terreno,
            {{ parse_financial_value("vr_contrapartida_municipio_infra") }} as vr_cp_municipio_infra,
            {{ parse_financial_value("vr_contrapartida_complementar_municipio") }} as vr_cp_municipio_complementar,
            {{ parse_financial_value("vr_contrapartida_estado_terreno") }} as vr_cp_estado_terreno,
            {{ parse_financial_value("vr_contrapartida_estado_infra") }} as vr_cp_estado_infra,
            {{ parse_financial_value("vr_contrapartida_complementar_estado") }} as vr_cp_estado_complementar,
            {{ parse_financial_value("vr_contrapartida_outras") }} as vr_cp_outras,
            {{ parse_financial_value("vr_outras_contrapartida_ente_publico") }} as vr_cp_outras_ente_publico,
            {{ parse_financial_value("vr_total_contrapartidas") }} as vr_total_contrapartidas,
            {{ parse_financial_value("vr_total_contrapartidas_retomadas") }} as vr_total_contrapartidas_retomada,
            {{ parse_financial_value("vr_equipamentos_publicos") }} as vr_equipamentos_publicos,
            {{ parse_financial_value("vr_equipamento_uso_comum") }} as vr_equipamento_uso_comum,
            {{ parse_financial_value("vr_infraestrutura_incidente") }} as vr_infraestrutura_incidente,
            {{ parse_financial_value("vr_contratado_aporte_suplementacao") }} as vr_aporte_suplementacao,
            {{ var('schema_udfs') }}.parse_date_br(dt_contratacao_aporte_suplementacao) as dt_aporte_suplementacao
        from {{ ref("bronze_shpt_monit_cad_pj_far_mensal") }}
    ),

    -- O consolidado repete a proposta a cada movimento; fica o retrato mais recente.
    proposta as (
        select *
        from (
            select
                nullif(trim(no_identificacao_proposta), '') as id_proposta,
                nullif(trim(nu_portaria_contratacao), '') as portaria_contratacao,
                lpad(nullif(trim(co_originacao_empreendimento), ''), 3, '0') as co_originacao,
                lpad(nullif(trim(co_tipo_de_demanda), ''), 3, '0') as co_demanda_enquadramento,
                lpad(nullif(trim(co_tipo_demanda_emp_contratacao), ''), 3, '0') as co_demanda_contratacao,
                -- A da contratação quase sempre vem vazia; cai para a do enquadramento.
                lpad(coalesce(nullif(trim(co_qualificacao_terreno_contratacao), ''), nullif(trim(co_qualificacao_terreno), '')), 3, '0') as co_qualificacao_terreno,
                case trim(ic_previsao_elevador) when 'S' then true when 'N' then false end as ic_previsao_elevador,
                trim(ic_dados_alterados_pos_contratacao) = 'S' as ic_alterado_pos_contratacao,
                nullif(trim(no_campo_alterado_contratacao), '') as campos_alterados_contratacao,
                trim(ic_terreno_area_urbana_consolidada) = 'S' as ic_area_urbana_consolidada,
                trim(ic_energia_eletrica_iluminacao_publica) = 'S' as ic_entorno_energia,
                trim(ic_agua_potavel) = 'S' as ic_entorno_agua,
                trim(ic_rede_esgoto_coleta_lixo) = 'S' as ic_entorno_esgoto_lixo,
                trim(ic_via_pavimentada) = 'S' as ic_entorno_pavimentacao,
                trim(ic_drenagem_pluvial) = 'S' as ic_entorno_drenagem,
                trim(ic_educacao_infantil) = 'S' as ic_entorno_educacao_infantil,
                trim(ic_ensino_fundamental_ciclo_1) = 'S' as ic_entorno_fundamental_1,
                trim(ic_ensino_fundamental_ciclo_2) = 'S' as ic_entorno_fundamental_2,
                trim(ic_unidade_saude_ubs) = 'S' as ic_entorno_ubs,
                trim(ic_unidade_protecao_social_cras) = 'S' as ic_entorno_cras,
                trim(ic_comercio_servicos_cotidiano) = 'S' as ic_entorno_comercio,
                row_number() over (
                    partition by trim(no_identificacao_proposta)
                    order by {{ var('schema_udfs') }}.parse_date_br(dt_movimento) desc nulls last, hr_movimento desc
                ) as rn
            from {{ ref("bronze_shpt_his_mcidades_consolidado") }}
            where nullif(trim(nu_apf), '') is not null
        ) t
        where rn = 1
    )

select
    cad.apf,
    cad.id_proposta,
    coalesce(p.portaria_contratacao, 'Não informada') as portaria_contratacao,
    coalesce(og.rotulo, 'Não informada') as originacao,
    coalesce(td.rotulo, 'Não informada') as tipo_demanda,
    coalesce(de.rotulo, 'Código ' || p.co_demanda_enquadramento, 'Não informada') as demanda_enquadramento,
    coalesce(dc.rotulo, 'Código ' || p.co_demanda_contratacao, 'Não informada') as demanda_contratacao,
    -- Mudou se enquadramento, contratação e monitoramento não declaram a mesma demanda.
    case
        when p.id_proposta is null then null
        else p.co_demanda_enquadramento is distinct from p.co_demanda_contratacao
            or p.co_demanda_contratacao is distinct from cad.co_tipo_demanda
    end as ic_demanda_mudou,
    coalesce(te.rotulo, 'Não informada') as tipo_edificacao,
    coalesce(tem.rotulo, 'Não informado') as tipo_empreendimento,
    coalesce(rp.rotulo, 'Não informado') as regime_propriedade,
    coalesce(la.rotulo, 'Não informada') as linha_atendimento,
    coalesce(ta.rotulo, 'Não informada') as tipo_administracao,
    coalesce(ep.rotulo, 'Não informado') as ente_proponente,
    coalesce(ec.rotulo, 'Não informado') as ente_contrapartida,
    coalesce(op.rotulo, 'Sem aporte/suplementação') as operacao_aporte,
    coalesce(qt.rotulo, 'Código ' || p.co_qualificacao_terreno, 'Não informada') as qualificacao_terreno,
    p.ic_previsao_elevador,
    p.ic_alterado_pos_contratacao,
    p.campos_alterados_contratacao,
    cad.ic_terreno_doado,
    cad.ic_area_comercial,
    cad.ic_substituicao_construtora,
    p.ic_area_urbana_consolidada,
    p.ic_entorno_energia,
    p.ic_entorno_agua,
    p.ic_entorno_esgoto_lixo,
    p.ic_entorno_pavimentacao,
    p.ic_entorno_drenagem,
    p.ic_entorno_educacao_infantil,
    p.ic_entorno_fundamental_1,
    p.ic_entorno_fundamental_2,
    p.ic_entorno_ubs,
    p.ic_entorno_cras,
    p.ic_entorno_comercio,
    cad.area_terreno_m2,
    cad.vr_terreno,
    cad.vr_far,
    cad.vr_total_investimento,
    cad.vr_cp_municipio_terreno,
    cad.vr_cp_municipio_infra,
    cad.vr_cp_municipio_complementar,
    cad.vr_cp_estado_terreno,
    cad.vr_cp_estado_infra,
    cad.vr_cp_estado_complementar,
    cad.vr_cp_outras,
    cad.vr_cp_outras_ente_publico,
    cad.vr_total_contrapartidas,
    cad.vr_total_contrapartidas_retomada,
    cad.vr_equipamentos_publicos,
    cad.vr_equipamento_uso_comum,
    cad.vr_infraestrutura_incidente,
    cad.vr_aporte_suplementacao,
    cad.dt_aporte_suplementacao
from cad
left join proposta p on p.id_proposta = cad.id_proposta
left join dominio td on td.campo = 'tipo_demanda' and td.codigo = cad.co_tipo_demanda
left join dominio de on de.campo = 'tipo_demanda' and de.codigo = p.co_demanda_enquadramento
left join dominio dc on dc.campo = 'tipo_demanda' and dc.codigo = p.co_demanda_contratacao
left join dominio te on te.campo = 'tipo_edificacao' and te.codigo = cad.co_tipo_edificacao
left join dominio tem on tem.campo = 'tipo_empreendimento' and tem.codigo = cad.co_tipo_empreendimento
left join dominio rp on rp.campo = 'regime_propriedade' and rp.codigo = cad.co_regime_propriedade
left join dominio la on la.campo = 'linha_atendimento' and la.codigo = cad.co_linha_atendimento
left join dominio ta on ta.campo = 'tipo_administracao' and ta.codigo = cad.co_tipo_administracao
left join dominio ep on ep.campo = 'ente_publico' and ep.codigo = cad.co_ente_proponente
left join dominio ec on ec.campo = 'ente_publico' and ec.codigo = cad.co_ente_contrapartida
left join dominio op on op.campo = 'operacao_aporte' and op.codigo = cad.co_operacao_aporte
left join dominio og on og.campo = 'originacao' and og.codigo = p.co_originacao
left join dominio qt on qt.campo = 'qualificacao_terreno' and qt.codigo = p.co_qualificacao_terreno
