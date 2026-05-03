{{ config(materialized="view") }}

with
    base_fichas as (
        select * from {{ ref("ficha_empreendimento") }}
    )

select
    situacao_empreendimento,
    tipologia,
    status_execucao_simplificado,
    status_prazo,
    
    -- Métricas Agregadas
    count(apf) as total_empreendimentos,
    sum(quantidade_uh) as total_uhs,
    sum(pessoas_atendidas) as total_pessoas_atendidas,
    
    -- Métricas Financeiras
    sum(valor_contratado) as somatorio_valor_contratado,
    sum(valor_desembolsado) as somatorio_valor_desembolsado,
    
    -- Médias
    avg(percentual_execucao_fisica) as media_execucao_fisica,
    avg(valor_por_uh) as media_valor_por_uh

from base_fichas
group by
    situacao_empreendimento,
    tipologia,
    status_execucao_simplificado,
    status_prazo
