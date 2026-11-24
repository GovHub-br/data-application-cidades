{#
    Filtro da prata sobre uma bronze que guarda todas as cargas (ADR-0005): fica só
    a carga mais recente, porque cada carga da fonte é completa.

    Uso:
        from {{ ref('bronze_fgv_icst') }}
        where {{ ultima_carga(ref('bronze_fgv_icst')) }}
#}
{% macro ultima_carga(relacao) -%}
    _load_id = (select max(_load_id) from {{ relacao }})
{%- endmacro %}
