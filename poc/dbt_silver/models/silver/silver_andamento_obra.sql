{{ config(materialized='table') }}

-- Silver em dbt-POSTGRES lendo a bronze que vive como parquet no MinIO.
--
-- Repare no que este arquivo NÃO tem: nada de DuckDB, nada de s3://, nada de encoding ou
-- delimitador. `distinct on`, CTE, window function, `::`, ILIKE, `regexp_replace(...,'g')`
-- e a materialização como tabela são idênticos ao que os models do mcid já fazem hoje.
--
-- DUAS RESTRIÇÕES MEDIDAS (ver README, seção da silver): quando uma query toca a view do
-- pg_duckdb, ela é executada INTEIRA no DuckDB. Então NÃO funcionam aqui:
--   * operadores exclusivos do Postgres, como `~*`  -> usar ILIKE / upper()
--   * FUNÇÕES definidas no Postgres (os UDFs `parse_date_br`, `normalize_apf`)
-- Na prática isso não dói nesta arquitetura, porque o trabalho desses UDFs — transformar
-- texto cru em tipo — já foi feito na bronze. A silver recebe DATE e INTEGER prontos.

with base as (

    select
        anomes,
        nu_apf,
        dt_prevista_conclusao,
        dt_prevista_inauguracao,
        situacao_obra,
        _source_file,
        _ingested_at
    from {{ source('bronze', 'caixa_andamento_obra') }}
    where nu_apf is not null

),

-- Último registro de cada APF por mês: os extratos se sobrepõem entre semanas.
deduplicado as (

    select distinct on (anomes, nu_apf)
        anomes,
        nu_apf,
        dt_prevista_conclusao,
        dt_prevista_inauguracao,
        situacao_obra,
        _source_file
    from base
    order by anomes, nu_apf, _ingested_at desc, _source_file desc

)

select
    anomes,
    nu_apf,
    dt_prevista_conclusao,
    dt_prevista_inauguracao,
    upper(trim(situacao_obra)) as situacao_obra,
    case
        when situacao_obra ilike '%normal%'    then 'NORMAL'
        when situacao_obra ilike '%atrasad%'   then 'ATRASADA'
        when situacao_obra ilike '%paralisad%' then 'PARALISADA'
        when situacao_obra ilike '%conclu%'    then 'CONCLUIDA'
        else 'OUTROS'
    end as situacao_categoria,
    (dt_prevista_conclusao < current_date) as conclusao_vencida,
    _source_file
from deduplicado
