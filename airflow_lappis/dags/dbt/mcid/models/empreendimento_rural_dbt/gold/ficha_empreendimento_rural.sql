{{ config(materialized="table") }}

-- Gold: Ficha do Empreendimento (Rural)
-- Consome silver/empreendimento e injeta regras de negócio finais como status, prazos e ritmo físico-financeiro.

with
    base_silver as (
        select * from {{ ref("empreendimento_rural") }}
    )

select
    -- Identificadores
    apf,
    agente_financeiro,
    case when ic_novo_mcmv then 'NOVO MCMV RURAL' else 'PNHR (HISTÓRICO)' end as programa,
    
    -- Nomes
    upper(empreendimento_nome) as nome_empreendimento,
    upper(entidade_organizadora_nome) as nome_entidade_organizadora,
    entidade_organizadora_cnpj,
    upper(construtora_nome) as nome_construtora,
    construtora_cnpj,

    -- Escopo e Tipologia
    quantidade_uh_contratadas as quantidade_uh,
    quantidade_uh_entregues,
    quantidade_uh_vigentes,
    floor(quantidade_uh_contratadas * 3.3)::int as pessoas_atendidas,
    case when ic_novo_mcmv then 'Novo MCMV Rural' else 'PNHR (Rural)' end as tipologia,
    
    -- Localização
    municipio,
    uf,
    concat(municipio, '/', uf) as municipio_uf,
    concat(
        apf,
        ' - ', 
        municipio, '/', uf, 
        ' - ', 
        upper(empreendimento_nome)
    ) as apf_municipio_empreendimento,

    -- Coordenadas
    latitude,
    longitude,
    
    -- Status do Projeto
    situacao_empreendimento,
    case
        when percentual_execucao_fisica = 100 or situacao_empreendimento in ('CONCLUÍDO E ENTREGUE', 'CONCLUIDA', 'ENTREGUE') then 'Concluído'
        when percentual_execucao_fisica = 0 then 'Não Iniciado'
        when percentual_execucao_fisica > 0 and percentual_execucao_fisica < 100 then 'Em Andamento'
        else 'Status Desconhecido'
    end as status_execucao_simplificado,

    -- Valores Contratuais
    valor_contratado,
    valor_aporte_adicional,
    case
        when quantidade_uh_contratadas > 0 then round((valor_contratado / quantidade_uh_contratadas), 2)
        else 0.00
    end as valor_por_uh,
    dt_contratacao,

    -- Evolução e Prazos
    percentual_execucao_fisica,
    dt_previsao_entrega,
    dt_conclusao_obra,
    
    -- Regra de Negócio: Atraso na Entrega
    case
        when situacao_empreendimento in ('CONCLUÍDO E ENTREGUE', 'CONCLUIDA', 'ENTREGUE') then 'Entregue'
        when dt_previsao_entrega < current_date then 'Em Atraso'
        else 'Dentro do Prazo'
    end as status_prazo,

    -- Evolução Financeira
    valor_desembolsado,
    case
        when valor_contratado > 0 then round((valor_desembolsado / valor_contratado) * 100, 2)
        else 0.00
    end as percentual_execucao_financeira,
    
    -- Regra de Negócio: Ritmo Físico-Financeiro (margem de 5%)
    case
        when (case when valor_contratado > 0 then (valor_desembolsado / valor_contratado) * 100 else 0.00 end) > (percentual_execucao_fisica + 5)
        then 'Desembolso Adiantado'
        when (case when valor_contratado > 0 then (valor_desembolsado / valor_contratado) * 100 else 0.00 end) < (percentual_execucao_fisica - 5)
        then 'Desembolso Atrasado'
        else 'Ritmo Equilibrado'
    end as ritmo_fisico_financeiro

from base_silver
