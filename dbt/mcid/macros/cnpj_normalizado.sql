{#
    CNPJ só com dígitos, sempre com 14 posições.

    As bases do Rural trazem o CNPJ como número em parte das linhas, e nelas o
    zero à esquerda se perde ("1234567000189" em vez de "01234567000189").
    Comparado como texto, o mesmo CNPJ com e sem o zero parecia de entidades
    diferentes. Aqui:
      - tira tudo que não é dígito;
      - completa com zeros à esquerda até 14 posições;
      - devolve nulo quando não sobra dígito ou quando só há zeros ("0",
        "00000000000000"): isso é marcador de ausência na origem, não CNPJ.
#}
{% macro cnpj_normalizado(coluna) -%}
    nullif(
        lpad(nullif(regexp_replace({{ coluna }}::text, '[^0-9]', '', 'g'), ''), 14, '0'),
        '00000000000000'
    )
{%- endmacro %}

{#- Dígitos do CNPJ como vieram da origem (sem normalizar), para mostrar o valor recebido. -#}
{% macro cnpj_digitos_origem(coluna) -%}
    nullif(regexp_replace({{ coluna }}::text, '[^0-9]', '', 'g'), '')
{%- endmacro %}
