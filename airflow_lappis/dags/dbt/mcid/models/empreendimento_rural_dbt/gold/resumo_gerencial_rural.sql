{{ config(materialized="table") }}

-- Gold: Resumo Gerencial (Rural)
-- Visão agregada contendo os KPIs consolidados para os gráficos gerenciais de pizza e barra.

with
    base_fichas as (
        select * from {{ ref("ficha_empreendimento_rural") }}
    )

select
    agente_financeiro,
    programa,
    situacao_empreendimento,
    tipologia,
    status_execucao_simplificado,
    status_prazo,
    
    -- Métricas Agregadas
    count(apf) as total_empreendimentos,
    sum(quantidade_uh) as total_uhs,
    sum(quantidade_uh_entregues) as total_uhs_entregues,
    sum(pessoas_atendidas) as total_pessoas_atendidas,
    
    -- Métricas Financeiras
    sum(valor_contratado) as somatorio_valor_contratado,
    sum(valor_desembolsado) as somatorio_valor_desembolsado,
    sum(valor_aporte_adicional) as somatorio_valor_aporte,
    
    -- Médias
    avg(percentual_execucao_fisica) as media_execucao_fisica,
    avg(valor_por_uh) as media_valor_por_uh

from base_fichas
group by
    agente_financeiro,
    programa,
    situacao_empreendimento,
    tipologia,
    status_execucao_simplificado,
    status_prazo
