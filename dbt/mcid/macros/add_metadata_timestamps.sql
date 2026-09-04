{% macro add_metadata_timestamps(layer, has_ingest_date=true) %}

    {%- if layer == "silver" %}
        {%- if has_ingest_date %} dt_ingest,
        {%- else %} null::timestamp as dt_ingest,
        {%- endif %}
        current_timestamp as dt_silver

    {%- elif layer == "gold" %}
        {%- if has_ingest_date %} dt_ingest,
        {%- else %} null::timestamp as dt_ingest,
        {%- endif %}
        dt_silver,
        current_timestamp as dt_gold

    {%- else %}
        {{
            exceptions.raise_compiler_error(
                "Camada inválida: '" ~ layer ~ "'. Use 'silver' ou 'gold'."
            )
        }}
    {%- endif %}

{% endmacro %}
