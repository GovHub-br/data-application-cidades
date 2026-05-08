{{ config(materialized="table") }}

with
    base_silver as (
        select * from {{ ref("empreendimento") }}
    )

select
    -- Identificadores
    apf,
    portaria_selecao,
    
    -- Nomes formatados
    upper(empreendimento_nome) as nome_empreendimento,
    proponente_cnpj,
    upper(proponente_nome) as nome_proponente,
    tomador_cnpj,
    upper(tomador_nome) as nome_tomador,
    agente_financeiro,

    -- Escopo e Tipologia
    quantidade_uh,
    pessoas_atendidas,
    tipologia,
    
    -- Localização
    municipio,
    uf,
    concat(municipio, '/', uf) as municipio_uf,

    -- Concatenação: APF - Municipio/UF - Nome Empreendimento
    -- para busca na Dashboard
    concat(
        apf,
        ' - ', 
        municipio, '/', uf, 
        ' - ', 
        upper(empreendimento_nome)
    ) as apf_municipio_empreendimento, 
    
    -- Originação e Demanda
    case co_originacao
        when 1 then 'SELEÇÃO'
        when 2 then 'CALAMIDADE/EMERGÊNCIA'
        else 'OUTRO (' || coalesce(co_originacao::text, 'N/A') || ')'
    end as metodo,
    
    case co_tipo_demanda
        when 1 then 'CADASTRO HABITACIONAL'
        when 2 then 'MOVIMENTOS SOCIAIS'
        when 3 then 'CALAMIDADE'
        when 4 then 'OBRA PÚBLICA'
        when 5 then 'REASSENTAMENTO'
        when 6 then 'DOAÇÃO/CESSÃO DE TERRENO'
        when 7 then 'OUTROS'
        else 'NÃO IDENTIFICADO (' || coalesce(co_tipo_demanda::text, 'N/A') || ')'
    end as demanda,
    
    -- Status do Projeto
    situacao_empreendimento,
    case
        when percentual_execucao_fisica = 100 then 'Concluído'
        when percentual_execucao_fisica = 0 then 'Não Iniciado'
        when percentual_execucao_fisica > 0 and percentual_execucao_fisica < 100 then 'Em Andamento'
        else 'Status Desconhecido'
    end as status_execucao_simplificado,

    -- Valores Contratuais e KPIs
    valor_contratado,
    valor_aporte_adicional,
    valor_por_uh,
    dt_contratacao,

    -- Evolução e Entregas
    percentual_execucao_fisica,
    dt_previsao_entrega,
    dt_conclusao_obra,
    dt_entrega,
    
    -- Regra de Negócio: Atraso na Entrega
    case
        when situacao_empreendimento != 'ENTREGUE' and dt_previsao_entrega < current_date
        then 'Em Atraso'
        when situacao_empreendimento = 'ENTREGUE'
        then 'Entregue'
        else 'Dentro do Prazo'
    end as status_prazo,

    -- Evolução Financeira
    valor_desembolsado,
    percentual_execucao_financeira,
    
    -- Regra de Negócio: Ritmo Físico-Financeiro
    -- Compara se o desembolso está aderente à execução física (margem de 5%)
    case
        when percentual_execucao_financeira > (percentual_execucao_fisica + 5)
        then 'Desembolso Adiantado'
        when percentual_execucao_financeira < (percentual_execucao_fisica - 5)
        then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as ritmo_fisico_financeiro

from base_silver
