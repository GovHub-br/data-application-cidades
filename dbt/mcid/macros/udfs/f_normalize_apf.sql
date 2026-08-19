-- UDF para normalizar APF para formato canônico de 8 dígitos
-- O APF aparece em 3 formatos diferentes nas tabelas:
--   GFAR consolidado: "0626780-03" (com traço)
--   CAIXA/CAD_PJ:     "62678003"   (8 dígitos)
--   Financeiro mensal: "626780"     (6 dígitos, sem dígitos verificadores)
-- Esta função normaliza todos para o formato de 8 dígitos sem traço.
{% macro create_f_normalize_apf() %}

    create or replace function {{ target.schema }}.normalize_apf(in_text text)
    returns text
    as
        $$
        select
            case
                when in_text is null or trim(in_text) = '' then null
                -- Remove traços e zeros à esquerda (formato GFAR "0626780-03" -> "62678003")
                when in_text like '%-%'
                then lpad(replace(in_text, '-', ''), 8, '0')
                -- Se já tem 8+ dígitos, pegar os 8 últimos (remove zeros à esquerda extras como "0064659798")
                when length(regexp_replace(in_text, '[^0-9]', '', 'g')) >= 8
                then right(regexp_replace(in_text, '[^0-9]', '', 'g'), 8)
                -- Se tem menos de 8 dígitos (formato financeiro "626780"), pad com zeros
                else lpad(regexp_replace(in_text, '[^0-9]', '', 'g'), 8, '0')
            end
    $$
    language sql
    ;

{% endmacro %}
