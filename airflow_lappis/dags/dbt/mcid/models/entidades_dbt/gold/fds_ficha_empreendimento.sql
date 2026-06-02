{{ config(materialized="table") }}

with
    silver as (
        select * from {{ ref("fds_empreendimento") }}
    )

select
    -- Identificadores
    apf,
    
    -- Nomes formatados
    upper(empreendimento_nome) as nome_empreendimento,
    eo_cnpj as cnpj_eo,
    upper(eo_nome) as nome_eo,
    agente_financeiro,

    -- Escopo e Tipologia
    quantidade_uh,
    pessoas_atendidas,
    tipologia,
    co_regime_obra,
    co_modalidade,
    
    -- Localização
    municipio,
    uf,
    concat(municipio, '/', uf) as municipio_uf,
    gps_lat_grau,
    gps_lat_minuto,
    gps_lat_segundo,
    gps_long_grau,
    gps_long_minuto,
    gps_long_segundo,

    -- Concatenação: APF - Municipio/UF - Nome Empreendimento (para filtros)
    concat(
        apf,
        ' - ', 
        municipio, '/', uf, 
        ' - ', 
        upper(empreendimento_nome)
    ) as apf_municipio_empreendimento, 
    
    -- Status e Alertas
    situacao_gefus,
    case
        when situacao_gefus = 'PARALISADA' then 'Paralisada'
        when situacao_gefus = 'NAO_INICIADA_CLAUSULA_SUSPENSIVA' then 'Cláusula Suspensiva'
        when percentual_execucao_fisica = 0 then 'Não Iniciada'
        when percentual_execucao_fisica = 100 then 'Concluída'
        else 'Em Andamento'
    end as semaforo_alerta,
    
    case
        when dt_paralisacao is not null then 'Sim'
        else 'Não'
    end as indicativo_paralisacao,

    -- Valores Contratuais e Financeiros
    valor_contratado,
    valor_financiamento_fds,
    valor_contrapartidas,
    valor_por_uh,
    dt_contratacao,

    -- Evolução Financeira
    valor_desembolsado,
    percentual_execucao_financeira,
    
    -- Evolução Física
    percentual_obra_prevista,
    percentual_execucao_fisica,
    dt_inicio_obra,
    dt_previsao_conclusao,
    dt_conclusao_obra,
    
    -- Divergência
    divergencia_fisico_financeira,
    case
        when divergencia_fisico_financeira > 5 then 'Desembolso Adiantado'
        when divergencia_fisico_financeira < -5 then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as status_ritmo_obra,

    -- Entregas e Alienação
    qt_uh_alienadas,
    pct_entrega,
    dt_entrega,
    
    case
        when pct_entrega = 100 then 'Totalmente Entregue'
        when pct_entrega > 0 then 'Entrega Parcial'
        else 'Não Entregue'
    end as status_entrega,

    -- Trabalho Social
    pct_execucao_ts

from silver
