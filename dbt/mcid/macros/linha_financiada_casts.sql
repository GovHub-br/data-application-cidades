{#
  Conversões tolerantes compartilhadas pela Linha Financiada.
  Os modelos precisam executar tanto no DuckDB local quanto no PostgreSQL
  materializado. As expressões abaixo preservam o mesmo resultado sem levar
  dados do banco de volta ao computador de execução.
#}

{% macro linha_financiada_data_iso(expressao) %}
  {% if target.type == 'postgres' %}
    case
      when nullif(trim(({{ expressao }})::text), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        then ({{ expressao }})::date
    end
  {% else %}
    try_cast({{ expressao }}::text as date)
  {% endif %}
{% endmacro %}


{% macro linha_financiada_data_formato(expressao, formato_duckdb, formato_postgres, padrao_postgres) %}
  {% if target.type == 'postgres' %}
    case
      when nullif(trim(({{ expressao }})::text), '') ~ '{{ padrao_postgres }}'
        then to_timestamp(trim(({{ expressao }})::text), '{{ formato_postgres }}')::date
    end
  {% else %}
    try_strptime({{ expressao }}::text, '{{ formato_duckdb }}')::date
  {% endif %}
{% endmacro %}


{% macro linha_financiada_numero(expressao) %}
  {% if target.type == 'postgres' %}
    case
      when nullif(trim(({{ expressao }})::text), '') ~ '^-?[0-9]+([.,][0-9]+)?$'
        then replace(trim(({{ expressao }})::text), ',', '.')::numeric
    end
  {% else %}
    try_cast(replace({{ expressao }}::text, ',', '.') as numeric)
  {% endif %}
{% endmacro %}
