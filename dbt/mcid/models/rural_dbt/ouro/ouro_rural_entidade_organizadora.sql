{{ config(materialized="table") }}

-- Gold: Histórico e capacidade de execução de cada ENTIDADE ORGANIZADORA do Rural (PNHR e
-- Novo Rural, 2009 em diante). Responde: histórico de execução da EO, se entregou o que
-- contratou, o que tem em aberto, se recontratou com pendência e se foi selecionada em 2018.
-- Grão: EO (CNPJ).
--
-- Não há histórico de HABILITAÇÃO: nenhuma base traz pedido, decisão ou motivo. O que existe é o
-- histórico de EXECUÇÃO, que é o que esta tabela mostra.

with
    op as (select * from {{ ref("prata_rural_operacao_eo") }} where eo_cnpj is not null),
    rec as (select * from {{ ref("ouro_rural_entidade_recontratacao") }}),
    sel as (
        select entidade_organizadora_cnpj as eo_cnpj, count(*) as qt_selecionados_portaria_162,
               sum(quantidade_uh_selecionadas) as uh_selecionadas_portaria_162
        from {{ ref("prata_rural_selecao_portaria_162") }}
        where entidade_organizadora_cnpj is not null
        group by 1
    ),

    eo as (
        select
            eo_cnpj,
            (array_agg(eo_nome order by dt_contratacao desc nulls last))[1] as eo_nome,
            string_agg(distinct fonte_eo, '; ') as fonte_eo,
            count(*) as qt_operacoes,
            count(*) filter (where ic_novo_mcmv) as qt_operacoes_novo_mcmv,
            count(*) filter (where status_operacao = 'Concluída') as qt_concluidas,
            count(*) filter (where status_operacao = 'Em obra') as qt_em_obra,
            count(*) filter (where status_operacao = 'Paralisada') as qt_paralisadas,
            count(*) filter (where status_operacao = 'Não iniciada') as qt_nao_iniciadas,
            count(*) filter (where status_operacao = 'Distratada') as qt_distratadas,
            coalesce(sum(quantidade_uh_contratadas), 0) as uh_contratadas,
            coalesce(sum(quantidade_uh_entregues), 0) as uh_entregues,
            coalesce(sum(quantidade_uh_distratadas), 0) as uh_distratadas,
            round(avg(percentual_execucao_fisica) filter (where status_operacao in ('Em obra', 'Paralisada')), 1) as percentual_medio_obra_abertas,
            coalesce(sum(valor_contratado), 0) as valor_contratado,
            coalesce(sum(valor_desembolsado), 0) as valor_desembolsado,
            min(dt_contratacao) as dt_primeira_contratacao,
            max(dt_contratacao) as dt_ultima_contratacao,
            count(distinct extract(year from dt_contratacao)) as qt_anos_com_contratacao,
            count(distinct uf) as qt_ufs,
            string_agg(distinct uf, ', ' order by uf) as ufs
        from op
        group by eo_cnpj
    ),

    r as (
        select eo_cnpj,
               count(*) filter (where ic_contratou_com_pendencia) as qt_contratos_com_pendencia_anterior,
               max(qt_anteriores_pendentes) as max_anteriores_pendentes
        from rec group by 1
    )

select
    eo.eo_cnpj,
    eo.eo_nome,
    eo.fonte_eo,
    eo.qt_operacoes,
    eo.qt_operacoes_novo_mcmv,
    eo.qt_concluidas,
    eo.qt_em_obra,
    eo.qt_paralisadas,
    eo.qt_nao_iniciadas,
    eo.qt_distratadas,
    eo.uh_contratadas,
    eo.uh_entregues,
    eo.uh_distratadas,
    eo.percentual_medio_obra_abertas,
    eo.valor_contratado,
    eo.valor_desembolsado,
    eo.dt_primeira_contratacao,
    eo.dt_ultima_contratacao,
    eo.qt_anos_com_contratacao,
    eo.qt_ufs,
    eo.ufs,
    case
        when eo.uh_contratadas - eo.uh_distratadas > 0
        then round(100.0 * eo.uh_entregues / (eo.uh_contratadas - eo.uh_distratadas), 1)
    end as percentual_uh_entregues,
    eo.qt_em_obra + eo.qt_paralisadas + eo.qt_nao_iniciadas as qt_abertas,
    coalesce(r.qt_contratos_com_pendencia_anterior, 0) as qt_contratos_com_pendencia_anterior,
    coalesce(r.max_anteriores_pendentes, 0) as max_anteriores_pendentes,
    coalesce(s.qt_selecionados_portaria_162, 0) as qt_selecionados_portaria_162,
    coalesce(s.uh_selecionadas_portaria_162, 0) as uh_selecionadas_portaria_162,
    case
        when eo.qt_paralisadas > 0 then 'Com obra paralisada'
        when eo.qt_em_obra + eo.qt_nao_iniciadas > 0 then 'Em execução'
        when eo.uh_entregues >= eo.uh_contratadas - eo.uh_distratadas then 'Entregou tudo o que contratou'
        else 'Concluída com entrega parcial'
    end as situacao_execucao_eo
from eo
left join r on r.eo_cnpj = eo.eo_cnpj
left join sel s on s.eo_cnpj = eo.eo_cnpj
