{{ config(materialized="table") }}

-- Gold: Gráfico de Evolução Financeira FDS
-- Objetivo: Alimentar o Superset com a série temporal de desembolsos.
-- Pode ser agrupado por Entidade Organizadora (CNPJ) ou Empreendimento.

with
    evolucao_mensal as (
        select * from {{ ref("fds_evolucao_financeira") }}
    )

select
    apf,
    eo_cnpj as cnpj_eo,
    upper(eo_nome) as nome_eo,
    mes,
    
    -- Agrupamentos temporais para o Superset
    date_trunc('year', mes) as ano,
    date_trunc('quarter', mes) as trimestre,
    
    -- Valores liberados no período
    vr_liberado_mes as valor_liberado_mensal,
    
    -- Componentes
    vr_pago_obra_mes as valor_obra_mensal,
    vr_pago_terreno_mes as valor_terreno_mensal,
    vr_pago_pts_mes as valor_ts_mensal,
    vr_pago_projeto_mes as valor_projeto_mensal,
    vr_pago_incc_mes as valor_incc_mensal,
    
    -- Acumulado (já pre-calculado na silver usando Window Functions)
    vr_acumulado as valor_desembolsado_acumulado,
    
    -- KPIs
    pct_executado_financeiro,
    
    -- Dimensões geográficas herdadas
    municipio,
    uf

from evolucao_mensal
