{#
    Portaria do Trabalho Social no formato único "número/ano" (ex.: 21/2014).

    Valores que chegam nas planilhas de acompanhamento (jul/2026):
      - BB grava só o número: "21", "168", "464";
      - CAIXA grava número e ano: "21-2014", "168-2013", "464-2018", "75-2025";
      - CAIXA FAR tem a categoria "anterior 168-2013" (contratos regidos por norma anterior
        à Portaria 168/2013, sem dizer qual);
      - campo vazio vem como "", "None", "nan" ou "null".

    Regras:
      - vazio vira nulo (a portaria não foi informada);
      - "anterior ... 168" vira "anterior à 168/2013";
      - número sem ano recebe o ano da portaria de TS correspondente
        (21 -> 2014, 168 -> 2013, 464 -> 2018, 75 -> 2025);
      - "número-ano" ou "número/ano" vira "número/ano"; ano com 3 dígitos ("21/014") vira 2014;
      - qualquer outro formato passa como veio, para não esconder valor novo
        (o teste accepted_values da prata acusa).
#}
{% macro portaria_ts_normalizada(coluna) -%}
    case
        when {{ coluna }} is null
            or upper(trim({{ coluna }}::text)) in ('', 'NONE', 'NAN', 'NULL', '-')
            then null
        when trim({{ coluna }}::text) ~* '^anterior.*168'
            then 'anterior à 168/2013'
        when trim({{ coluna }}::text) ~ '^\d+\s*[-/]\s*\d{4}$'
            then (regexp_match(trim({{ coluna }}::text), '^(\d+)'))[1]
                || '/' || (regexp_match(trim({{ coluna }}::text), '(\d{4})$'))[1]
        -- ano digitado com 3 dígitos ("21/014", jan/2025): completa com o 2 do milênio
        when trim({{ coluna }}::text) ~ '^\d+\s*[-/]\s*0\d{2}$'
            then (regexp_match(trim({{ coluna }}::text), '^(\d+)'))[1]
                || '/2' || (regexp_match(trim({{ coluna }}::text), '(\d{3})$'))[1]
        when trim({{ coluna }}::text) ~ '^\d+(\.0)?$'
            then case (regexp_match(trim({{ coluna }}::text), '^(\d+)'))[1]
                when '21' then '21/2014'
                when '168' then '168/2013'
                when '464' then '464/2018'
                when '75' then '75/2025'
                else trim({{ coluna }}::text)
            end
        else trim({{ coluna }}::text)
    end
{%- endmacro %}
