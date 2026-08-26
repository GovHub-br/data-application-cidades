{% macro read_minio_staging_parquet_series(glob_pattern) -%}
    read_parquet(
        {{ minio_staging_uri(glob_pattern) }},
        union_by_name = true,
        filename = true
    )
{%- endmacro %}
