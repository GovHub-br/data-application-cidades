{#
    Dentro do envelope duckdb_query() o SQL roda no DuckDB do pg_duckdb, que enxerga o
    Postgres como o catálogo `pgduckdb` (o nome do banco do ref() não existe lá).
#}
{% macro rel_pgduckdb(rel) -%} pgduckdb.{{ rel.schema }}.{{ rel.identifier }} {%- endmacro %}
