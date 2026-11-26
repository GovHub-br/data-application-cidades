{{ config(materialized='table') }}

-- Novo CAGED, serviços especializados para construção (divisão CNAE 43): série mensal desde
-- jan/2024 a partir dos microdados do PDET
-- (prata_conjuntura_novo_caged_pdet), no lugar do painel Power BI que saiu do ar em 06/2026.
--
-- Os microdados não trazem o estoque. Como no painel (estoque = estoque anterior + saldo,
-- conferido: 1.204.418 + 810 = 1.205.228 em jun/26 nos edifícios), o estoque sai do estoque
-- de dez/2023 do painel somado aos saldos. `variacao` é o saldo sobre o estoque anterior,
-- a mesma medida que o painel publicava.
with
    ancora as (
        -- estoque de dez/2023 = estoque de jan/2024 menos o saldo de jan/2024, no painel
        select estoque::numeric - saldo::numeric as estoque_inicial
        from {{ ref('bronze_novo_caged_servicos_especializados') }}
        where ano::int = 2024 and mes::int = 1
    ),

    serie as (
        select
            p.data_referencia,
            p.admitidos,
            p.desligados,
            p.saldo,
            a.estoque_inicial + sum(p.saldo) over (order by p.data_referencia) as estoque
        from {{ ref('prata_conjuntura_novo_caged_pdet') }} p
        cross join ancora a
        where p.recorte = 'servicos_especializados'
    )

select
    extract(year from data_referencia)::int as ano,
    extract(month from data_referencia)::int as mes,
    admitidos::numeric as admitidos,
    desligados::numeric as desligados,
    saldo::numeric as saldo,
    estoque::numeric as estoque,
    saldo::numeric / nullif(estoque - saldo, 0) as variacao,
    current_timestamp as dt_ingest
from serie
