-- Datas do Canal FGTS (pacote semanal `MC<aaaammdd>.zip`), que chegam no
-- formato AMERICANO com hora colada: `05/29/26 00:00:00` = 29/05/2026.
--
-- `parse_date_br` NÃO serve aqui: ela cobre DD/MM/YYYY, YYYYMMDD e YYYY-MM-DD,
-- e devolve null para este layout. Trocar uma pela outra inverte dia e mês em
-- silêncio nos 12 primeiros dias de cada mês — erro que não aparece em
-- contagem nenhuma.
--
-- O ano de dois dígitos segue a regra do Postgres: 00-69 vira 2000s, 70-99 vira
-- 1900s. A série do pacote cabe inteira nessa janela.
{% macro parse_data_canal_fgts(col) -%}
    {%- set t = "trim(cast(" ~ col ~ " as varchar))" -%}
    case
        when {{ col }} is null or {{ t }} = ''
        then null
        when {{ t }} ~ '^\d{2}/\d{2}/\d{2}( |$)'
        then to_date(substring({{ t }} from 1 for 8), 'MM/DD/YY')
        when {{ t }} ~ '^\d{2}/\d{2}/\d{4}'
        then to_date(substring({{ t }} from 1 for 10), 'MM/DD/YYYY')
        when {{ t }} ~ '^\d{4}-\d{2}-\d{2}'
        then substring({{ t }} from 1 for 10)::date
        else null
    end
{%- endmacro %}


-- Datas em DD/MM/YYYY do relatório de operações paralisadas — é o único
-- arquivo do pacote que usa o formato brasileiro, e por isso não passa pelo
-- `parse_data_canal_fgts`.
{% macro parse_data_ddmmaaaa(col) -%}
    {%- set t = "trim(cast(" ~ col ~ " as varchar))" -%}
    case
        when {{ col }} is null or {{ t }} = ''
        then null
        when {{ t }} ~ '^\d{2}/\d{2}/\d{4}$'
        then to_date({{ t }}, 'DD/MM/YYYY')
        else null
    end
{%- endmacro %}


-- Competência AAAAMM -> primeiro dia do mês.
--
-- A validação não é zelo: `tab_execucoes_obras` traz competências ilegíveis como
-- `000000`, e `to_date` as aceita sem reclamar, devolvendo datas de dois mil anos
-- atrás que envenenam qualquer `max()`. Só passa 19xx/20xx com mês 01-12.
{% macro parse_competencia(col) -%}
    {%- set t = "trim(cast(" ~ col ~ " as varchar))" -%}
    case
        when {{ t }} ~ '^(19|20)\d{2}(0[1-9]|1[0-2])$'
        then to_date({{ t }}, 'YYYYMM')
        else null
    end
{%- endmacro %}


-- Datas em ISO (`2026-04-30`). Existem no mesmo arquivo que usa DD/MM/AAAA: no
-- relatório de paralisadas, `dt_previsao_conclusao_objeto` vem em DD/MM/AAAA e
-- `dt_ultimo_bm` vem em ISO. O arquivo mistura os dois, então cada coluna precisa do
-- seu parser.
{% macro parse_data_iso(col) -%}
    {%- set t = "trim(cast(" ~ col ~ " as varchar))" -%}
    case
        when {{ col }} is null or {{ t }} = ''
        then null
        when {{ t }} ~ '^\d{4}-\d{2}-\d{2}'
        then substring({{ t }} from 1 for 10)::date
        else null
    end
{%- endmacro %}


-- Competência MM/AA -> primeiro dia do mês.
--
-- É o formato de `data_base` no relatório de paralisadas, onde `08/26` significa
-- agosto de 2026: o mês de referência do relatório, não um dia. Passar isso por um
-- parser de DD/MM/AAAA zera a coluna inteira e deixa qualquer comparação de prazo
-- sem régua.
{% macro parse_competencia_mm_aa(col) -%}
    {%- set t = "trim(cast(" ~ col ~ " as varchar))" -%}
    case
        when {{ t }} ~ '^(0[1-9]|1[0-2])/\d{2}$'
        then to_date({{ t }}, 'MM/YY')
        else null
    end
{%- endmacro %}
