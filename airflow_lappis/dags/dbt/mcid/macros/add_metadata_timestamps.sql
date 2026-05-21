{% macro add_metadata_timestamps(layer, has_ingest_date=true) %}

    {%- if layer == 'silver' %}
        {%- if has_ingest_date %}
            dt_ingest,
        {%- else %}
            NULL::timestamp AS dt_ingest,
        {%- endif %}
        current_timestamp AS dt_silver

    {%- elif layer == 'gold' %}
        {%- if has_ingest_date %}
            dt_ingest,
        {%- else %}
            NULL::timestamp AS dt_ingest,
        {%- endif %}
        dt_silver,
        current_timestamp AS dt_gold

    {%- else %}
        {{ exceptions.raise_compiler_error(
            "Camada inválida: '" ~ layer ~ "'. Use 'silver' ou 'gold'."
        ) }}
    {%- endif %}

{% endmacro %}