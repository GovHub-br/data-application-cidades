{{ config(materialized="table") }}

-- Gold: Histórico e capacidade de execução de cada TOMADOR do Pró-Moradia (1995 em diante).
-- Réplica, para o Pró-Moradia, das perguntas do Rural sobre a entidade organizadora: histórico
-- de execução, se terminou o que contratou, o que tem em aberto e se recontratou com pendência.
-- Grão: tomador (código do Canal FGTS).
--
-- Não há dado de "habilitação" do tomador (análise de capacidade de pagamento, CAUC, PVL): o que
-- existe é o histórico de execução dos contratos.

with
    c as (
        select f.*, p.tomador_cnpj
        from {{ ref("ouro_pro_moradia_ficha_contrato") }} f
        left join {{ ref("prata_pro_moradia_contrato") }} p on p.cod_contrato = f.cod_contrato
        where f.tomador_codigo is not null
    ),

    rec as (
        select tomador_codigo,
               count(*) filter (where ic_contratou_com_pendencia) as qt_contratos_com_pendencia_anterior,
               max(qt_anteriores_pendentes) as max_anteriores_pendentes
        from {{ ref("ouro_pro_moradia_tomador_recontratacao") }}
        group by 1
    ),

    t as (
        select
            tomador_codigo,
            max(tomador_nome) as tomador_nome,
            max(tomador_esfera) as tomador_esfera,
            max(tomador_cnpj) as tomador_cnpj,
            max(uf) as uf,
            count(*) as qt_contratos,
            count(*) filter (where ic_contrato_vigente) as qt_vigentes,
            count(*) filter (where status_execucao_simplificado = 'Concluído') as qt_concluidos,
            count(*) filter (where status_execucao_simplificado = 'Em Andamento') as qt_em_andamento,
            count(*) filter (where status_execucao_simplificado = 'Paralisado') as qt_paralisados,
            count(*) filter (where status_execucao_simplificado = 'Não Iniciado') as qt_nao_iniciados,
            count(*) filter (where status_execucao_simplificado = 'Cancelado ou distratado') as qt_cancelados_distratados,
            count(*) filter (where status_execucao_simplificado = 'Sem Informação') as qt_sem_informacao,
            count(*) filter (where tipo_intervencao = 'Urbanização') as qt_urbanizacao,
            count(*) filter (where tipo_intervencao = 'Provisão habitacional') as qt_provisao,
            coalesce(sum(quantidade_uh), 0) as uh_financiadas,
            coalesce(sum(quantidade_uh) filter (where status_execucao_simplificado = 'Concluído'), 0) as uh_contratos_concluidos,
            coalesce(sum(populacao_beneficiada), 0) as populacao_beneficiada,
            coalesce(sum(valor_contratado), 0) as valor_contratado,
            coalesce(sum(vr_desembolsado), 0) as valor_desembolsado,
            round(avg(percentual_execucao_fisica) filter (where status_execucao_simplificado in ('Em Andamento', 'Paralisado')), 1) as percentual_medio_obra_abertos,
            min(dt_assinatura) as dt_primeiro_contrato,
            max(dt_assinatura) as dt_ultimo_contrato,
            count(distinct ano_orcamento) as qt_anos_orcamento,
            string_agg(distinct identificador_selecao, '; ') as selecoes
        from c
        group by tomador_codigo
    )

select
    t.tomador_codigo,
    t.tomador_nome,
    t.tomador_esfera,
    t.tomador_cnpj,
    t.uf,
    t.qt_contratos,
    t.qt_vigentes,
    t.qt_concluidos,
    t.qt_em_andamento,
    t.qt_paralisados,
    t.qt_nao_iniciados,
    t.qt_cancelados_distratados,
    t.qt_sem_informacao,
    t.qt_urbanizacao,
    t.qt_provisao,
    t.uh_financiadas,
    t.uh_contratos_concluidos,
    t.populacao_beneficiada,
    t.valor_contratado,
    t.valor_desembolsado,
    t.percentual_medio_obra_abertos,
    t.dt_primeiro_contrato,
    t.dt_ultimo_contrato,
    t.qt_anos_orcamento,
    t.selecoes,
    t.qt_em_andamento + t.qt_paralisados + t.qt_nao_iniciados as qt_abertos,
    case
        when t.qt_contratos - t.qt_cancelados_distratados > 0
        then round(100.0 * t.qt_concluidos / (t.qt_contratos - t.qt_cancelados_distratados), 1)
    end as percentual_contratos_concluidos,
    coalesce(r.qt_contratos_com_pendencia_anterior, 0) as qt_contratos_com_pendencia_anterior,
    coalesce(r.max_anteriores_pendentes, 0) as max_anteriores_pendentes,
    case
        when t.qt_paralisados > 0 then 'Com obra paralisada'
        when t.qt_em_andamento + t.qt_nao_iniciados > 0 then 'Em execução'
        when t.qt_concluidos > 0 then 'Concluiu tudo o que contratou'
        else 'Sem informação suficiente'
    end as situacao_execucao_tomador
from t
left join rec r on r.tomador_codigo = t.tomador_codigo
