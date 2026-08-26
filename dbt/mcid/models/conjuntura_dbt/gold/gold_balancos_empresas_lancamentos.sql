{{ config(materialized="table") }}

with
    t_1t26 as (
        select nome_empresa, lancamento, dt_ingest, dt_silver
        from {{ ref("silver_balancos_empresas") }}
        where ano_balanco = 2026 and trimestre_balanco = 1
    ),

    t_4t25 as (
        select nome_empresa, lancamento
        from {{ ref("silver_balancos_empresas") }}
        where ano_balanco = 2025 and trimestre_balanco = 4
    ),

    t_1t25 as (
        select nome_empresa, lancamento
        from {{ ref("silver_balancos_empresas") }}
        where ano_balanco = 2025 and trimestre_balanco = 1
    ),

    acum_26 as (
        select nome_empresa, sum(lancamento) as lancamento_12m_26
        from {{ ref("silver_balancos_empresas") }}
        where
            (ano_balanco = 2025 and trimestre_balanco in (2, 3, 4))
            or (ano_balanco = 2026 and trimestre_balanco = 1)
        group by nome_empresa
    ),

    acum_25 as (
        select nome_empresa, sum(lancamento) as lancamento_12m_25
        from {{ ref("silver_balancos_empresas") }}
        where
            (ano_balanco = 2024 and trimestre_balanco in (2, 3, 4))
            or (ano_balanco = 2025 and trimestre_balanco = 1)
        group by nome_empresa
    ),

    acum_24 as (
        select nome_empresa, sum(lancamento) as lancamento_12m_24
        from {{ ref("silver_balancos_empresas") }}
        where
            (ano_balanco = 2023 and trimestre_balanco in (2, 3, 4))
            or (ano_balanco = 2024 and trimestre_balanco = 1)
        group by nome_empresa
    )

select
    atual.nome_empresa,
    round(
        ((atual.lancamento::numeric / nullif(q4.lancamento, 0)) - 1) * 100, 0
    ) as variacao_4t25,
    round(
        ((atual.lancamento::numeric / nullif(t25.lancamento, 0)) - 1) * 100, 0
    ) as variacao_1t25,
    round(
        ((a26.lancamento_12m_26::numeric / nullif(a25.lancamento_12m_25, 0)) - 1) * 100, 0
    ) as variacao_12m_26_25,
    round(
        ((a25.lancamento_12m_25::numeric / nullif(a24.lancamento_12m_24, 0)) - 1) * 100, 0
    ) as variacao_12m_25_24,
    {{ add_metadata_timestamps("gold") }}
from t_1t26 atual
left join t_4t25 q4 on atual.nome_empresa = q4.nome_empresa
left join t_1t25 t25 on atual.nome_empresa = t25.nome_empresa
left join acum_26 a26 on atual.nome_empresa = a26.nome_empresa
left join acum_25 a25 on atual.nome_empresa = a25.nome_empresa
left join acum_24 a24 on atual.nome_empresa = a24.nome_empresa
order by
    case
        atual.nome_empresa
        when 'MRV'
        then 1
        when 'Cury'
        then 2
        when 'Tenda'
        then 3
        when 'Direcional'
        then 4
        when 'Pacaembu'
        then 5
        when 'Plano & Plano'
        then 6
    end
