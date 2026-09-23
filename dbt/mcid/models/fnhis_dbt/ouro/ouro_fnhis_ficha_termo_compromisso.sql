{{ config(materialized="table") }}

-- Gold: Ficha do Termo de Compromisso (FNHIS Sub-50)
-- Uma linha por termo, juntando os três lados do ciclo: a SELEÇÃO (proposta, UH, IBGE), o
-- INSTRUMENTO no TransfereGov (repasse, empenho, situação) e a EXECUÇÃO acompanhada pela SNH
-- (situação da obra, UH entregues, desembolso). Mais a regularidade do município no SNHIS.
--
-- Termo × contrato SNH não têm chave comum. O vínculo é por município (IBGE de 6 dígitos) e
-- valor de repasse, desempatado pela data de assinatura; só entra vínculo 1:1 — se sobrar mais
-- de um candidato de qualquer lado, fica sem vínculo em vez de escolher no chute. Conferido
-- em 2026-09: 1.184 dos 1.224 contratos casam por município + valor.

with
    termo as (
        select * from {{ ref("prata_fnhis_termo_compromisso") }}
    ),

    proposta as (
        select * from {{ ref("prata_fnhis_propostas") }}
    ),

    ente as (
        select * from {{ ref("prata_fnhis_regularidade_entes") }}
    ),

    snh as (
        select * from {{ ref("prata_fnhis_prioritarios_snh") }}
    ),

    base as (
        select
            t.*,
            p.cod_ibge,
            p.cod_ibge_6,
            p.quantidade_uh,
            p.proponente_esfera,
            p.situacao_proposta as situacao_selecao,
            p.ic_proposta_de_risco
        from termo t
        left join proposta p on p.numero_proposta = t.numero_proposta
    ),

    candidato as (
        select
            b.numero_proposta_transferegov,
            s.id_operacao_snh,
            b.dt_assinatura = s.dt_contratacao as ic_mesma_data
        from base b
        join snh s
            on s.cod_ibge_6 = b.cod_ibge_6
            and s.valor_contratado = b.valor_repasse
    ),

    -- quando algum candidato do termo tem a mesma data, só esses valem
    preferido as (
        select *
        from (
            select c.*, bool_or(ic_mesma_data) over (partition by numero_proposta_transferegov) as ic_termo_tem_mesma_data
            from candidato c
        ) x
        where ic_mesma_data or not ic_termo_tem_mesma_data
    ),

    vinculo as (
        select
            numero_proposta_transferegov,
            id_operacao_snh,
            case when ic_mesma_data then 'município + valor + data' else 'município + valor' end as criterio_vinculo_snh
        from (
            select
                p.*,
                count(*) over (partition by numero_proposta_transferegov) as n_por_termo,
                count(*) over (partition by id_operacao_snh) as n_por_contrato
            from preferido p
        ) x
        where n_por_termo = 1 and n_por_contrato = 1
    )

select
    -- Identificação
    b.numero_proposta_transferegov,
    b.numero_proposta,
    b.codigo_programa,
    'Novo MCMV FNHIS Sub-50' as programa,

    -- Localização
    b.uf,
    {{ regiao_da_uf("b.uf") }} as regiao,
    b.municipio,
    b.cod_ibge,
    concat(b.municipio, '/', b.uf) as municipio_uf,
    concat(b.numero_proposta_transferegov, ' - ', b.municipio, '/', b.uf) as termo_municipio,

    -- Proponente
    b.proponente_nome,
    b.proponente_esfera,
    b.objeto,

    -- Seleção
    b.situacao_selecao,
    b.ic_proposta_de_risco,
    b.quantidade_uh,
    floor(b.quantidade_uh * 3.3)::int as pessoas_atendidas,

    -- Instrumento (TransfereGov)
    b.situacao_contratacao,
    b.situacao_proposta,
    b.situacao_instrumento,
    b.ic_instrumento_ativo,
    b.dt_assinatura,
    b.valor_repasse,
    b.valor_contrapartida,
    b.valor_investimento,
    case
        when coalesce(b.quantidade_uh, 0) > 0 then round(b.valor_repasse / b.quantidade_uh, 2)
    end as valor_repasse_por_uh,
    b.valor_empenhado_acumulado,
    case
        when b.valor_repasse > 0 then round(b.valor_empenhado_acumulado / b.valor_repasse * 100, 2)
    end as percentual_empenhado,

    -- Regularidade do município no SNHIS
    e.ic_ente_regular,
    e.situacao_ente,
    e.situacao_plano_habitacional,
    e.situacao_lei_fundo,
    e.situacao_lei_conselho,

    -- Execução (SNH)
    v.id_operacao_snh is not null as ic_contrato_snh_identificado,
    v.criterio_vinculo_snh,
    s.id_operacao_snh,
    s.cod_operacao_agente,
    s.agente_financeiro,
    s.situacao as situacao_obra_snh,
    s.percentual_execucao_fisica,
    s.uh_entregues,
    s.valor_desembolsado,
    s.dt_previsao_termino,
    s.dt_termino,
    case
        when not b.ic_instrumento_ativo then 'Sem instrumento ativo'
        when s.id_operacao_snh is null then 'Sem acompanhamento SNH'
        when s.situacao ~* 'conclu|entregue' or s.percentual_execucao_fisica >= 100 then 'Concluído'
        when s.situacao ~* 'n.o iniciad' or coalesce(s.percentual_execucao_fisica, 0) = 0 then 'Não Iniciado'
        when s.situacao ~* 'paralisad' then 'Paralisado'
        else 'Em Andamento'
    end as status_execucao_simplificado,

    -- Linhagem
    b.dt_consulta,
    s.dt_referencia as dt_referencia_snh
from base b
left join vinculo v on v.numero_proposta_transferegov = b.numero_proposta_transferegov
left join snh s on s.id_operacao_snh = v.id_operacao_snh
left join ente e on e.cod_ibge = b.cod_ibge
