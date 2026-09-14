-- strip_float_text(col): limpa o sufixo ".0" de um identificador que passou por
-- `int -> float -> str` a montante ("3550308" -> 3550308.0 -> "3550308.0").
--
-- Só age em valores que casam o padrão de INTEIRO PURO seguido de ".0"
-- (`-?[0-9]+[.]0+`): remove o `.0+` final. Valor não numérico é devolvido como
-- está (apenas trim); vazio/whitespace vira NULL.
--
-- NÃO usar em coluna de VALOR numérico/monetário — para essas, os parsers de
-- macros/parse_hist_numeric.sql (`parse_hist_double`) já leem "2113.0" como
-- número. Esta macro é só para coluna de IDENTIFICADOR que deve seguir texto
-- (código IBGE, código de empreendimento, CNPJ). Ver a change
-- testes-data-quality-dbt e models/docs/varredura-sufixo-float-texto.md.
--
-- Portável DuckDB/Postgres: `similar to` + `regexp_replace` com âncora `$`.
{% macro strip_float_text(col) -%}
    case
        when {{ col }} is null then null
        when trim(cast({{ col }} as varchar)) = '' then null
        when trim(cast({{ col }} as varchar)) similar to '-?[0-9]+[.]0+'
        then regexp_replace(trim(cast({{ col }} as varchar)), '[.]0+$', '')
        else trim(cast({{ col }} as varchar))
    end
{%- endmacro %}
