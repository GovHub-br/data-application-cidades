{{ config(materialized="table") }}

-- Gold: Em que etapa está cada proposta do FNHIS Sub-50 — da apresentação à obra — e como o
-- termo de compromisso mudou entre os retratos mensais do TransfereGov.
-- Grão: proposta apresentada (7.121).
--
-- Etapas, em ordem: 1 fora no enquadramento → 2 fora na seleção → 3 selecionada sem termo →
-- 4 termo anulado/rescindido → 5 termo em execução sem obra na SNH → 6 em obra (SNH) →
-- 7 prestação de contas. O trâmite INTERNO do MCID (SEI + planilha) não chega ao banco; quando
-- chegar, entra entre as etapas 3 e 5.
-- Execução: na SNH as operações FNHIS estão todas com 0% e R$ 0 desembolsado (set/2026); o
-- empenho do TransfereGov é hoje o melhor sinal de andamento.

with
    prop as (select * from {{ ref("prata_fnhis_propostas") }}),
    termo as (select * from {{ ref("prata_fnhis_termo_compromisso") }}),
    ficha as (select * from {{ ref("ouro_fnhis_ficha_termo_compromisso") }}),

    traj as (
        select
            numero_proposta_transferegov,
            count(*) as qt_retratos,
            min(dt_retrato) as dt_primeiro_retrato,
            max(dt_retrato) as dt_ultimo_retrato,
            count(distinct situacao_instrumento) as qt_situacoes_instrumento,
            (array_agg(situacao_instrumento order by dt_retrato) filter (where situacao_instrumento is not null))[1] as situacao_instrumento_inicial,
            max(dt_retrato) filter (where situacao_instrumento_retrato_anterior is not null
                                     and situacao_instrumento is distinct from situacao_instrumento_retrato_anterior) as dt_ultima_mudanca_situacao,
            (array_agg(situacao_instrumento_retrato_anterior order by dt_retrato desc)
                filter (where situacao_instrumento_retrato_anterior is not null
                        and situacao_instrumento is distinct from situacao_instrumento_retrato_anterior))[1] as situacao_instrumento_anterior,
            (array_agg(valor_empenhado_acumulado order by dt_retrato))[1] as valor_empenhado_primeiro_retrato
        from {{ ref("prata_fnhis_termo_compromisso_mensal") }}
        group by 1
    ),

    base as (
        select
            p.numero_proposta,
            p.uf,
            p.municipio,
            p.cod_ibge,
            p.proponente_esfera,
            p.quantidade_uh,
            p.resultado_selecao,
            p.ic_selecionada,
            p.ic_proposta_de_risco,
            t.numero_proposta_transferegov,
            t.situacao_contratacao,
            t.situacao_instrumento,
            t.ic_instrumento_ativo,
            t.dt_assinatura,
            t.valor_repasse,
            t.valor_empenhado_acumulado,
            f.ic_contrato_snh_identificado,
            f.id_operacao_snh,
            f.situacao_obra_snh,
            f.percentual_execucao_fisica,
            f.uh_entregues,
            f.valor_desembolsado,
            f.ic_ente_regular,
            tr.qt_retratos,
            tr.dt_primeiro_retrato,
            tr.situacao_instrumento_inicial,
            tr.situacao_instrumento_anterior,
            tr.dt_ultima_mudanca_situacao,
            tr.qt_situacoes_instrumento,
            t.valor_empenhado_acumulado - tr.valor_empenhado_primeiro_retrato as variacao_empenho_na_serie
        from prop p
        left join termo t on t.numero_proposta = p.numero_proposta
        left join ficha f on f.numero_proposta_transferegov = t.numero_proposta_transferegov
        left join traj tr on tr.numero_proposta_transferegov = t.numero_proposta_transferegov
    )

select
    b.numero_proposta,
    b.uf,
    b.municipio,
    b.cod_ibge,
    b.proponente_esfera,
    b.quantidade_uh,
    b.resultado_selecao,
    b.ic_proposta_de_risco,
    b.numero_proposta_transferegov,
    b.situacao_contratacao,
    b.situacao_instrumento,
    b.ic_instrumento_ativo,
    b.dt_assinatura,
    b.valor_repasse,
    b.valor_empenhado_acumulado,
    b.ic_contrato_snh_identificado,
    b.id_operacao_snh,
    b.situacao_obra_snh,
    b.percentual_execucao_fisica,
    b.uh_entregues,
    b.valor_desembolsado,
    b.ic_ente_regular,
    b.qt_retratos,
    b.dt_primeiro_retrato,
    b.situacao_instrumento_inicial,
    b.situacao_instrumento_anterior,
    b.dt_ultima_mudanca_situacao,
    b.qt_situacoes_instrumento,
    b.variacao_empenho_na_serie,
    case
        when b.resultado_selecao = 'Não enquadrada' then 1
        when not coalesce(b.ic_selecionada, false) then 2
        when b.numero_proposta_transferegov is null or b.situacao_instrumento is null then 3
        when b.situacao_instrumento in ('Convênio Anulado', 'Convênio Rescindido') then 4
        when b.situacao_instrumento ~* 'presta..o de contas' then 7
        when coalesce(b.percentual_execucao_fisica, 0) > 0 or coalesce(b.valor_desembolsado, 0) > 0 then 6
        else 5
    end as etapa_ordem,
    case
        when b.resultado_selecao = 'Não enquadrada' then 'Fora no enquadramento'
        when not coalesce(b.ic_selecionada, false) then concat('Fora na seleção - ', b.resultado_selecao)
        when b.numero_proposta_transferegov is null or b.situacao_instrumento is null then 'Selecionada - sem termo formalizado'
        when b.situacao_instrumento in ('Convênio Anulado', 'Convênio Rescindido') then 'Termo anulado/rescindido'
        when b.situacao_instrumento ~* 'presta..o de contas' then 'Prestação de contas'
        when coalesce(b.percentual_execucao_fisica, 0) > 0 or coalesce(b.valor_desembolsado, 0) > 0 then 'Em obra (SNH)'
        else 'Termo em execução - sem obra registrada'
    end as etapa_atual
from base b
