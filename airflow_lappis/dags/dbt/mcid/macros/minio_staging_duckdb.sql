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
