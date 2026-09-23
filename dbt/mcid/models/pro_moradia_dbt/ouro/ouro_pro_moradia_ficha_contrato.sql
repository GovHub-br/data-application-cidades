{{ config(materialized="table") }}

-- Gold: Ficha do Contrato (Pró-Moradia)
-- Uma linha por contrato, com as regras de negócio finais: tipo de intervenção, status de
-- execução, execução física × financeira e paralisação. Consome as pratas do produto.
--
-- Física: vale a última avaliação mensal já MEDIDA (não o cronograma futuro); sem ela, a
-- posição do cadastro do empreendimento. A fonte fica ao lado do número.
--
-- Financeira: o canal não informa o estoque desembolsado, só a série de liberações desde 2000.
-- `vr_desembolsado` é a soma da série, e por isso é um PISO para contrato anterior a 2000 —
-- `ic_serie_financeira_incompleta` marca esses casos para o painel não ler 0% como "nada pago".

with
    contrato as (
        select * from {{ ref("prata_pro_moradia_contrato") }}
    ),

    ultima_medicao as (
        select distinct on (cod_contrato)
            cod_contrato,
            dt_avaliacao,
            percentual_obra_realizado,
            percentual_obra_previsto,
            situacao_obra_codigo,
            situacao_obra,
            dt_ultima_vistoria
        from {{ ref("prata_pro_moradia_execucao_obra") }}
        where not ic_avaliacao_futura
        order by cod_contrato, dt_avaliacao desc
    ),

    financeiro as (
        select
            cod_contrato,
            sum(vr_liberado) as vr_desembolsado,
            min(dt_competencia) as dt_primeiro_desembolso,
            max(dt_competencia) as dt_ultimo_desembolso,
            count(*) as qt_meses_com_desembolso
        from {{ ref("prata_pro_moradia_desembolso_mensal") }}
        group by cod_contrato
    ),

    paralisacao as (
        select * from {{ ref("prata_pro_moradia_paralisacao") }}
    ),

    calculado as (
        select
            c.*,
            m.dt_avaliacao as dt_ultima_avaliacao,
            m.situacao_obra_codigo,
            m.situacao_obra,
            m.dt_ultima_vistoria,
            m.percentual_obra_previsto,
            coalesce(m.percentual_obra_realizado, c.percentual_obra_posicao) as pct_fisico,
            case
                when m.percentual_obra_realizado is not null then 'execucao_obra_mensal'
                when c.percentual_obra_posicao is not null then 'posicao_empreendimento'
            end as fonte_percentual_fisico,
            f.vr_desembolsado,
            f.dt_primeiro_desembolso,
            f.dt_ultimo_desembolso,
            coalesce(f.qt_meses_com_desembolso, 0) as qt_meses_com_desembolso,
            case
                when coalesce(c.valor_contratado, 0) > 0 and f.vr_desembolsado is not null
                then round(f.vr_desembolsado / c.valor_contratado * 100, 2)
            end as pct_financeiro,
            p.cod_contrato is not null as ic_paralisada,
            p.dias_sem_evolucao,
            p.faixa_paralisacao,
            p.motivo_paralisacao,
            p.dt_previsao_conclusao
        from contrato c
        left join ultima_medicao m on m.cod_contrato = c.cod_contrato
        left join financeiro f on f.cod_contrato = c.cod_contrato
        left join paralisacao p on p.cod_contrato = c.cod_contrato
    )

select
    -- Identificação
    contrato,
    cod_contrato,
    cod_contrato_dv,
    cod_empreendimento,
    'Pró-Moradia' as programa,
    tipo_intervencao,
    modalidade_codigo,
    modalidade,
    empreendimento_nome,
    objeto,

    -- Tomador e acompanhamento
    tomador_codigo,
    tomador_nome,
    tomador_esfera,
    agente_financeiro,
    uor,

    -- Localização
    uf,
    {{ regiao_da_uf("uf") }} as regiao,
    municipio,
    cod_ibge,
    concat(municipio, '/', uf) as municipio_uf,
    concat(contrato, ' - ', municipio, '/', uf, ' - ', coalesce(empreendimento_nome, objeto)) as contrato_municipio_empreendimento,

    -- Contrato
    situacao_contrato_codigo,
    situacao_contrato,
    ic_contrato_vigente,
    ano_orcamento,
    dt_assinatura,
    ic_pac,
    identificador_selecao,

    -- Escopo
    quantidade_uh_financiadas as quantidade_uh,
    populacao_beneficiada,
    empregos_gerados,

    -- Valores
    valor_contratado,
    valor_investimento,
    valor_contrapartida,
    case
        when coalesce(quantidade_uh_financiadas, 0) > 0
        then round(valor_investimento / quantidade_uh_financiadas, 2)
    end as valor_investimento_por_uh,

    -- Execução física
    pct_fisico as percentual_execucao_fisica,
    fonte_percentual_fisico,
    percentual_obra_previsto,
    situacao_obra_codigo,
    situacao_obra,
    dt_ultima_avaliacao,
    dt_ultima_vistoria,
    dt_inicio_obra,
    dt_termino_obra,

    -- Regra de negócio: status de execução. A ordem importa: contrato que deixou de existir
    -- não está "não iniciado", e obra paralisada com 40% não está "em andamento".
    case
        when not ic_contrato_vigente or situacao_obra_codigo in ('0', 'D') then 'Cancelado ou distratado'
        when situacao_obra_codigo in ('6', '8', 'E') or pct_fisico >= 100 then 'Concluído'
        when ic_paralisada or situacao_obra_codigo in ('4', 'H') then 'Paralisado'
        when situacao_obra_codigo in ('5', 'G') or pct_fisico = 0 then 'Não Iniciado'
        when pct_fisico > 0 then 'Em Andamento'
        else 'Sem Informação'
    end as status_execucao_simplificado,

    -- Execução financeira
    vr_desembolsado,
    pct_financeiro as percentual_execucao_financeira,
    dt_primeiro_desembolso,
    dt_ultimo_desembolso,
    qt_meses_com_desembolso,
    coalesce(dt_assinatura < date '2000-01-01', false) as ic_serie_financeira_incompleta,

    -- Regra de negócio: ritmo físico × financeiro, com a mesma tolerância de 5 pontos do Rural
    case
        when pct_fisico is null or pct_financeiro is null then 'Sem Informação'
        when pct_financeiro - pct_fisico > 5 then 'Desembolso Adiantado'
        when pct_fisico - pct_financeiro > 5 then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as ritmo_fisico_financeiro,

    -- Paralisação
    ic_paralisada,
    dias_sem_evolucao,
    faixa_paralisacao,
    motivo_paralisacao,
    dt_previsao_conclusao,

    -- Linhagem
    remessa_canal_fgts,
    dt_remessa
from calculado
