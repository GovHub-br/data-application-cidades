{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: INCC-M — número índice e variações mensais.
-- Página 6 (Preços). Fonte: FGV (automatizado).
--
-- O XLSX da FGV também traz duas variações prontas. Elas são preservadas com
-- nomes explícitos para auditoria, mas as métricas de uso analítico são
-- recalculadas a partir do número-índice: isso elimina ambiguidade de rótulo
-- entre "no ano" e "12 meses" e torna a regra verificável.

with serie as (
    select
        mes,
        indice,
        var_mes,
        var_ano as var_fonte_no_ano,
        var_12_meses as var_fonte_12_meses,
        lag(indice, 12) over (order by mes) as indice_mes_ano_anterior
    from {{ ref('silver_continuo_fgv_incc_m') }}
),

dezembro_ano_anterior as (
    select
        extract(year from mes)::int + 1 as ano_referencia,
        indice as indice_dezembro_ano_anterior
    from serie
    where extract(month from mes) = 12
),

com_referencia_dezembro as (
    select
        s.*,
        d.indice_dezembro_ano_anterior
    from serie s
    left join dezembro_ano_anterior d
        on d.ano_referencia = extract(year from s.mes)::int
)

select
    mes,
    indice,
    var_mes,
    indice / nullif(indice_dezembro_ano_anterior, 0) - 1 as var_ano,
    indice / nullif(indice_mes_ano_anterior, 0) - 1 as var_12_meses,
    var_fonte_no_ano,
    var_fonte_12_meses
from com_referencia_dezembro
order by mes desc
