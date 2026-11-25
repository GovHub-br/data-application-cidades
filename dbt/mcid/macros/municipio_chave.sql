-- Chave de comparação de nome de município: corrige mojibake, tira acento e tudo que não é letra
-- ou dígito, em maiúsculas. Serve para casar município quando falta o código IBGE.
{% macro municipio_chave(coluna) -%}
    nullif(regexp_replace(upper(translate(
        {{ var('schema_udfs') }}.corrigir_mojibake({{ coluna }}::text),
        'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇáàâãäéèêëíìîïóòôõöúùûüç',
        'AAAAAEEEEIIIIOOOOOUUUUCaaaaaeeeeiiiiooooouuuuc'
    )), '[^A-Z0-9]', '', 'g'), '')
{%- endmacro %}
