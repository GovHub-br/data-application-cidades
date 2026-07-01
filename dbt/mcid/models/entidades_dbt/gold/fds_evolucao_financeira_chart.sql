{{ config(materialized="table") }}

-- Gold: Gráfico de Evolução Financeiro-Física FDS
-- Objetivo: Alimentar o Superset com a série temporal de desembolsos.
-- Como o FDS não possui histórico físico mensal, a execução física
-- (percentual_execucao_fisica) é projetada de forma estática para 
-- acompanhamento do valor atual contra a curva histórica de desembolso.

with
    evolucao_mensal as (
        select * from {{ ref("fds_evolucao_financeira") }}
    ),

    ficha as (
        select 
            apf,
            apf_municipio_empreendimento,
            percentual_execucao_fisica
        from {{ ref("fds_ficha_empreendimento") }}
    )

select
    m.apf,
    m.eo_cnpj as cnpj_eo,
    upper(m.eo_nome) as nome_eo,
    f.apf_municipio_empreendimento,
    to_char(m.mes, 'YYYY-MM-DD') as mes,
    
    -- Agrupamentos temporais para o Superset
    to_char(date_trunc('year', m.mes), 'YYYY-MM-DD') as ano,
    to_char(date_trunc('quarter', m.mes), 'YYYY-MM-DD') as trimestre,
    
    -- Valores liberados no período
    m.vr_liberado_mes as valor_liberado_mensal,
    
    -- Componentes
    m.vr_pago_obra_mes as valor_obra_mensal,
    m.vr_pago_terreno_mes as valor_terreno_mensal,
    m.vr_pago_pts_mes as valor_ts_mensal,
    m.vr_pago_projeto_mes as valor_projeto_mensal,
    m.vr_pago_incc_mes as valor_incc_mensal,
    
    -- Acumulado (já pre-calculado na silver usando Window Functions)
    m.vr_acumulado as valor_desembolsado_acumulado,
    
    -- KPIs Financeiros
    m.pct_executado_financeiro as percentual_execucao_financeira,
    
    -- KPIs Físicos (Estático/Atual Projetado)
    coalesce(f.percentual_execucao_fisica, 0.0) as percentual_execucao_fisica,
    
    -- Dimensões geográficas herdadas
    m.municipio,
    m.uf

from evolucao_mensal m
left join ficha f
    on m.apf = f.apf
