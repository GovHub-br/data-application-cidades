{{ config(materialized="table") }}

-- Ouro: um empreendimento FAR contratado por linha, com natureza e destino, panorama da
-- contratação, execução com sinal de atraso ou paralisação, estoque retomado/invadido e
-- os contratos das famílias já assinados. Alimenta o painel de perguntas do FAR.
--
-- O sinal de paralisação antecipa o que a origem só marca quando a situação muda:
-- obra iniciada, não concluída e com o percentual físico parado há 3 meses ou mais.
with
    dominio as (select campo, codigo, rotulo from {{ ref("dominio_far_perguntas") }}),

    serie as (
        select
            apf,
            mes,
            pct_obra_realizada,
            desvio_fisico,
            pct_obra_realizada is distinct from lag(pct_obra_realizada) over (partition by apf order by mes) as ic_mudou
        from {{ ref("prata_far_serie_mensal") }}
        where pct_obra_realizada is not null
    ),

    ritmo as (
        select
            apf,
            max(mes) as mes_ultima_medicao,
            max(mes) filter (where ic_mudou) as mes_ultimo_avanco,
            (array_agg(desvio_fisico order by mes desc))[1] as desvio_fisico,
            (array_agg(pct_obra_realizada order by mes desc))[1]
                - coalesce((array_agg(pct_obra_realizada order by mes desc))[4], 0) as avanco_3_meses
        from serie
        group by apf
    ),

    familias as (
        select
            apf,
            count(*) as qt_contratos_familias,
            count(*) filter (where ic_ativo) as qt_contratos_ativos,
            count(*) filter (where ic_inadimplente) as qt_contratos_inadimplentes
        from {{ ref("prata_far_contrato_pf") }}
        where geracao = 'Novo MCMV'
        group by apf
    ),

    base as (
        select
            e.apf,
            e.empreendimento_nome as nome_empreendimento,
            e.apf || ' - ' || e.municipio || '/' || e.uf || ' - ' || upper(e.empreendimento_nome) as apf_municipio_empreendimento,
            e.municipio,
            e.uf,
            e.agente_financeiro,
            e.dt_contratacao,
            extract(year from e.dt_contratacao)::int as ano_contratacao,
            e.portaria_selecao as portaria,
            n.portaria_contratacao,
            n.originacao,
            n.tipo_demanda,
            n.demanda_enquadramento,
            n.demanda_contratacao,
            n.ic_demanda_mudou,
            n.tipo_edificacao,
            n.tipo_empreendimento,
            n.regime_propriedade,
            n.linha_atendimento,
            n.ente_proponente,
            n.qualificacao_terreno,
            n.ic_terreno_doado,
            n.ic_previsao_elevador,
            n.ic_area_comercial,
            n.ic_substituicao_construtora,
            n.ic_area_urbana_consolidada,
            n.ic_entorno_energia,
            n.ic_entorno_agua,
            n.ic_entorno_esgoto_lixo,
            n.ic_entorno_pavimentacao,
            n.ic_entorno_drenagem,
            n.ic_entorno_educacao_infantil,
            n.ic_entorno_fundamental_1,
            n.ic_entorno_fundamental_2,
            n.ic_entorno_ubs,
            n.ic_entorno_cras,
            n.ic_entorno_comercio,
            e.quantidade_uh as qt_uh,
            e.valor_contratado,
            e.valor_por_uh,
            e.valor_desembolsado,
            e.percentual_execucao_fisica,
            e.percentual_execucao_financeira,
            n.vr_far,
            n.vr_terreno,
            n.vr_total_contrapartidas,
            n.vr_total_contrapartidas_retomada,
            n.vr_equipamentos_publicos,
            n.vr_equipamento_uso_comum,
            n.vr_aporte_suplementacao,
            n.operacao_aporte,
            coalesce(so.situacao_obra, 'NÃO INFORMADA') as situacao_obra,
            o.dt_paralisacao,
            cp.rotulo as motivo_paralisacao,
            mc.rotulo as motivo_cancelamento,
            e.dt_previsao_entrega,
            e.dt_entrega,
            coalesce(o.qt_uh_ociosas_retomadas, 0) as qt_uh_ociosas_retomadas,
            coalesce(o.qt_uh_invadidas, 0) as qt_uh_invadidas,
            coalesce(o.qt_uh_alienadas, 0) as qt_uh_alienadas,
            coalesce(o.qt_uh_a_alienar, 0) as qt_uh_a_alienar,
            r.mes_ultima_medicao,
            r.desvio_fisico,
            r.avanco_3_meses,
            (
                extract(year from age(r.mes_ultima_medicao, coalesce(r.mes_ultimo_avanco, r.mes_ultima_medicao))) * 12
                + extract(month from age(r.mes_ultima_medicao, coalesce(r.mes_ultimo_avanco, r.mes_ultima_medicao)))
            )::int as meses_sem_avanco,
            coalesce(f.qt_contratos_familias, 0) as qt_contratos_familias,
            coalesce(f.qt_contratos_ativos, 0) as qt_contratos_familias_ativos,
            coalesce(f.qt_contratos_inadimplentes, 0) as qt_contratos_familias_inadimplentes
        from {{ ref("prata_far_empreendimento") }} e
        left join {{ ref("prata_far_empreendimento_natureza") }} n on n.apf = e.apf
        left join {{ ref("prata_far_obra_mensal") }} o on o.apf = e.apf
        left join {{ ref("far_situacao_obra") }} so on so.co_situacao_obra = o.co_situacao_obra
        left join dominio cp on cp.campo = 'classificacao_paralisado' and cp.codigo = lpad(o.co_classificacao_paralisado::text, 3, '0')
        left join dominio mc on mc.campo = 'motivo_cancelamento' and mc.codigo = lpad(o.co_motivo_cancelamento::text, 3, '0')
        left join ritmo r on r.apf = e.apf
        left join familias f on f.apf = e.apf
    )

select
    *,
    case
        when situacao_obra like 'CONCLU%' or percentual_execucao_fisica >= 100 then 'Concluída'
        when situacao_obra like 'CANCELADA%' then 'Cancelada ou distratada'
        when situacao_obra like '%PARALISAD%' or dt_paralisacao is not null then 'Paralisada (informada)'
        when situacao_obra like '%ATRASAD%' then 'Atrasada (informada)'
        when coalesce(percentual_execucao_fisica, 0) > 0 and meses_sem_avanco >= 3 then 'Sinal de paralisação: 3+ meses sem avanço'
        when desvio_fisico <= -10 then 'Ritmo abaixo do previsto'
        when situacao_obra like 'NÃO INICIADA%' or coalesce(percentual_execucao_fisica, 0) = 0 then 'Não iniciada'
        else 'Em ritmo'
    end as alerta_execucao
from base
