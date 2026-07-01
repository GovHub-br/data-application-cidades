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
    regexp_replace(eo_cnpj, '^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$', '\1.\2.\3/\4-\5') as cnpj_eo_formatado,
    upper(eo_nome) as nome_eo,
    agente_financeiro,

    -- Escopo e Tipologia
    quantidade_uh,
    pessoas_atendidas,
    tipologia,
    
    -- Tradução de Códigos de Negócio (FDS)
    co_regime_obra,
    case co_regime_obra
        when 1 then 'Empreitada Global'
        when 2 then 'Autogestão'
        else 'Outro (' || coalesce(co_regime_obra::text, 'N/A') || ')'
    end as regime_obra,
    
    co_modalidade,
    case co_modalidade
        when 1 then 'Aquisição de Terreno e Projeto'
        when 2 then 'Elaboração de Projeto'
        when 3 then 'Produção de Unidades Novas'
        when 4 then 'Requalificação'
        else 'Outro (' || coalesce(co_modalidade::text, 'N/A') || ')'
    end as modalidade,
    
    -- Localização
    municipio,
    uf,
    concat(municipio, '/', uf) as municipio_uf,
    concat('BR-', uf) as iso_3166_2,
    case
        when uf in ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') then 'Norte'
        when uf in ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') then 'Nordeste'
        when uf in ('DF', 'GO', 'MT', 'MS') then 'Centro-Oeste'
        when uf in ('ES', 'MG', 'RJ', 'SP') then 'Sudeste'
        when uf in ('PR', 'RS', 'SC') then 'Sul'
        else 'Não Informado'
    end as regiao_nome,
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
