{{ config(materialized='table') }}

-- INCC-M (FGV): série do Sinduscon-PR (índice e variações publicados) até onde ela vai,
-- e depois disso o índice estendido pelas variações mensais do SGS/BACEN (série 7456).
--
-- Por que encadear aqui: o Sinduscon-PR parou de atualizar a planilha em 07/2026, e as
-- fontes que seguem vivas (SGS e o FGV Dados gratuito) publicam só a variação % do mês,
-- sem número-índice. O índice dos meses novos é o último índice publicado multiplicado
-- pelas variações acumuladas; var no ano e em 12 meses saem dele no ouro, como antes.
-- Decisão do Lucas em 26/11/2026. Se o Sinduscon voltar, a série dele tem precedência.
with
    sinduscon as (
        select
            mes::date as mes,
            nullif(nullif(indice::text, ''), '...')::numeric as indice,
            nullif(nullif(var_mes::text, ''), '...')::numeric as var_mes,
            nullif(nullif(var_ano::text, ''), '...')::numeric as var_ano,
            nullif(nullif(var_12_meses::text, ''), '...')::numeric as var_12_meses
        from {{ ref('bronze_fgv_incc_m') }}
        where {{ ultima_carga(ref('bronze_fgv_incc_m')) }}
    ),

    ancora as (
        select mes, indice
        from sinduscon
        where indice is not null
        order by mes desc
        limit 1
    ),

    sgs as (
        select
            to_date(b.data, 'DD/MM/YYYY') as mes,
            nullif(btrim(b.valor), '')::numeric as var_mes
        from {{ ref('bronze_bacen_financiamentos_imobiliarios') }} b
        where
            b.tipo = 'incc_m_var_mes_pct'
            and {{ ultima_carga(ref('bronze_bacen_financiamentos_imobiliarios')) }}
    ),

    encadeada as (
        select
            s.mes,
            a.indice * exp(sum(ln(1 + s.var_mes / 100)) over (order by s.mes)) as indice,
            s.var_mes,
            null::numeric as var_ano,
            null::numeric as var_12_meses
        from sgs s
        cross join ancora a
        where s.mes > a.mes
    )

select mes, indice, var_mes, var_ano, var_12_meses
from sinduscon
union all
select mes, indice, var_mes, var_ano, var_12_meses
from encadeada
