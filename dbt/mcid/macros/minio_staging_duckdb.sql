{% macro assert_duckdb_staging_only() %}
    {% if execute and target.type != 'duckdb' %}
        {{ exceptions.raise_compiler_error(
            "mcmv_silver_dbt deve ser executado somente com target DuckDB lendo MinIO staging/. "
            ~ "Use --target staging_duckdb e substitua fontes Postgres por read_minio_staging_parquet()."
        ) }}
    {% endif %}
{% endmacro %}

{% macro minio_staging_uri(object_name) -%}
    's3://{{ env_var("MINIO_BUCKET", "data-lake-mcid") }}/staging/{{ object_name }}'
{%- endmacro %}

{% macro read_minio_staging_parquet(object_name) -%}
    read_parquet({{ minio_staging_uri(object_name) }}, union_by_name = true)
{%- endmacro %}

{#
    Envelopa o corpo do modelo (dialeto DuckDB nativo, incluindo
    read_minio_staging_parquet[_series], union all by name, funções
    escalares DuckDB como regexp_extract/strptime) para rodar dentro de
    duckdb.query() no target `prod` (adapter dbt-postgres + extensão
    pg_duckdb). Fora do `select *` mais externo aqui gerado, colunas do
    resultado só são acessíveis via r['coluna'] com CAST — por isso todo o
    corpo do modelo deve ficar dentro do {% call %}, sem lógica adicional
    no nível do dbt fora dele. Ver design.md (D4) da change
    migrar-leitura-staging-pg-duckdb.
#}
{% macro duckdb_query() -%}
select * from duckdb.query($duckdb_query${{ caller() }}$duckdb_query$) r
{%- endmacro %}
