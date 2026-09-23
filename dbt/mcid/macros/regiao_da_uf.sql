{#-
    Região geográfica (IBGE) de uma sigla de UF.

    O mesmo CASE já existia inline na ouro do FDS; os produtos Pró-Moradia e FNHIS precisam
    dele em várias ouros, e a região é o nível em que o orçamento do FGTS é distribuído — um
    erro de digitação numa cópia faria o orçamento de uma UF cair na região errada.

        {{ regiao_da_uf('uf') }} as regiao
-#}
{% macro regiao_da_uf(coluna) -%}
    case
        when {{ coluna }} in ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') then 'Norte'
        when {{ coluna }} in ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') then 'Nordeste'
        when {{ coluna }} in ('DF', 'GO', 'MT', 'MS') then 'Centro-Oeste'
        when {{ coluna }} in ('ES', 'MG', 'RJ', 'SP') then 'Sudeste'
        when {{ coluna }} in ('PR', 'RS', 'SC') then 'Sul'
    end
{%- endmacro %}
