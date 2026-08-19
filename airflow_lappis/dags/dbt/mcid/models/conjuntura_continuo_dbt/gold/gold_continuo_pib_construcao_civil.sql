{{ config(materialized='table') }}

-- Gold do conjuntura contínuo: PIB da construção civil em % de crescimento,
-- direto da API do IBGE (agregado de taxas de variação trimestrais), por
-- trimestre. Apoio/comparação à gold_continuo_pib_construcao_civil_pct
-- (fonte MANUAL, oficial do boletim). A silver traz 4 variáveis de taxa (%)
-- por período (6561/6562/6563/6564) — aqui elas são pivotadas em colunas;
-- somar `valor` direto (como antes) misturava as 4 taxas num total sem
-- sentido. Obs.: a coluna trim/trim aqui costuma divergir um pouco do
-- boletim por revisão de dessazonalização feita pelo IBGE a cada trimestre;
-- as acumuladas tendem a bater.

with base as (
    select * from {{ ref('silver_continuo_ibge_pib_construcao_civil') }}
    where localidade_id = 1
)

select
    periodo,
    data_referencia,
    extract(year from data_referencia)::int                          as ano,
    max(case when variavel_id = 6564 then valor end)                 as var_trim_trim_anterior,
    max(case when variavel_id = 6561 then valor end)                 as var_trim_mesmo_trim_ano_anterior,
    max(case when variavel_id = 6563 then valor end)                 as var_acumulada_ano,
    max(case when variavel_id = 6562 then valor end)                 as var_acumulada_4_trimestres
from base
group by periodo, data_referencia
order by data_referencia desc
