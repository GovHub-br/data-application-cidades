{{ config(materialized='ephemeral') }}

-- Bronze dos microdados do Novo CAGED (PDET), seção F: ~400 mil movimentações por mês,
-- ~14 milhões desde 2024. Por isso é `ephemeral`: copiar tudo para o Postgres a cada
-- execução não se paga, e a prata agrega direto no parquet pelo pg_duckdb.
--
-- O pg_duckdb não expõe as colunas de `select * from read_parquet(...)` fora de uma
-- tabela materializada (só por `r['coluna']`), então aqui vão as colunas que a prata usa
-- e a linhagem; as demais estão no contrato (contracts/snh/novo_caged/pdet.odcs.yaml).
-- Precisou de outra coluna: acrescente aqui.
select
    r['competenciamov'] as competenciamov,
    r['secao'] as secao,
    r['subclasse'] as subclasse,
    r['saldomovimentacao'] as saldomovimentacao,
    r['_source_file'] as _source_file,
    r['_load_id'] as _load_id
from {{ fonte_lake('novo_caged_pdet', 'lake_staging') }} r
