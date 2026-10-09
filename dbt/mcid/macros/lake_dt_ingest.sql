{#
    Data e hora da ingestão de uma linha do lake, tirada do caminho do arquivo.

    A ingestão nova (plugins/ingestion) grava cada execução em
    `<dataset>/<AAAA-MM-DD>/<HHMMSS>/`, no horário de Brasília, e o `latest/`
    guarda a cópia com esse mesmo sufixo. O `filename` que o `fonte_lake` expõe
    traz então a partição em qualquer modo de carga; aqui ela vira timestamp
    (sem fuso, horário de Brasília), que é o `dt_ingest` das pratas.

    Uso na prata:
        select ..., {{ lake_dt_ingest() }} as dt_ingest
        from {{ ref('bronze_x') }}

    A regex roda igual no Postgres e no DuckDB.
#}
{% macro lake_dt_ingest(coluna='filename') -%}
    cast(
        regexp_replace(
            {{ coluna }},
            '^.*/([0-9]{4}-[0-9]{2}-[0-9]{2})/([0-9]{2})([0-9]{2})([0-9]{2})/[^/]*$',
            '\1 \2:\3:\4'
        ) as timestamp
    )
{%- endmacro %}
