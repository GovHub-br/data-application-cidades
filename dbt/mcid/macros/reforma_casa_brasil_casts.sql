{#
  Conversões tolerantes do produto Reforma Casa Brasil.

  As fontes chegam como texto na Bronze e os modelos devem rodar tanto no
  DuckDB de desenvolvimento quanto no PostgreSQL com pg_duckdb. As macros
  evitam funções exclusivas de um motor e mantêm a transformação no banco.
#}

{% macro reforma_casa_brasil_data(expressao) %}
  {% if target.type == 'postgres' %}
    case
      when nullif(trim(({{ expressao }})::text), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        then trim(({{ expressao }})::text)::date
      when nullif(trim(({{ expressao }})::text), '') ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
        then to_date(trim(({{ expressao }})::text), 'DD/MM/YYYY')
    end
  {% else %}
    coalesce(
      {{ parse_hist_date(expressao) }},
      try_strptime(nullif(trim({{ expressao }}::varchar), ''), '%d/%m/%Y')::date
    )
  {% endif %}
{% endmacro %}


{% macro reforma_casa_brasil_numero(expressao) %}
  {% if target.type == 'postgres' %}
    case
      when nullif(trim(({{ expressao }})::text), '') ~ '^[-+]?[0-9]+([.,][0-9]+)?$'
        then replace(trim(({{ expressao }})::text), ',', '.')::numeric
      when nullif(trim(({{ expressao }})::text), '') ~ '^[-+]?[0-9]{1,3}(\.[0-9]{3})+(,[0-9]+)?$'
        then replace(replace(trim(({{ expressao }})::text), '.', ''), ',', '.')::numeric
    end
  {% else %}
    {{ parse_hist_numeric(expressao) }}
  {% endif %}
{% endmacro %}


{% macro reforma_casa_brasil_double(expressao) %}
  ({{ reforma_casa_brasil_numero(expressao) }})::double precision
{% endmacro %}


{% macro reforma_casa_brasil_bigint(expressao) %}
  round({{ reforma_casa_brasil_numero(expressao) }})::bigint
{% endmacro %}


{% macro reforma_casa_brasil_data_arquivo(expressao) %}
  {% if target.type == 'postgres' %}
    case
      when substring(({{ expressao }})::text from '(20[0-9]{2}(0[1-9]|1[0-2])([0-2][0-9]|3[0-1]))')
        is not null
      then to_date(
        substring(({{ expressao }})::text from '(20[0-9]{2}(0[1-9]|1[0-2])([0-2][0-9]|3[0-1]))'),
        'YYYYMMDD'
      )
    end
  {% else %}
    try_strptime(
      nullif(regexp_extract({{ expressao }}::varchar, '(20[0-9]{2}(0[1-9]|1[0-2])([0-2][0-9]|3[0-1]))', 1), ''),
      '%Y%m%d'
    )::date
  {% endif %}
{% endmacro %}
