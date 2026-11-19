-- normalize_apf(col): forma canônica de 8 dígitos do número de APF, para os
-- modelos que leem a staging MinIO via DuckDB.
--
-- Porta a UDF Postgres `{{ target.schema }}.normalize_apf`
-- (macros/udfs/f_normalize_apf.sql) para SQL puro DuckDB-safe. A lógica é a
-- MESMA da UDF — não "corrigir" aqui: as silvers de FAR/FDS já dependem deste
-- comportamento para casar as fontes (contrato só cresce, D6 da change
-- `migracao-bronze-minio-mcmv`).
--
-- Formatos de entrada cobertos (iguais aos da UDF):
-- GFAR consolidado  : "626780-03"  (6 díg. + traço + DV → 8 díg. sem traço)
-- CAIXA / CAD_PJ     : "62678003"   (8 díg. → os 8 à direita)
-- Financeiro mensal  : "626780"     (6 díg., sem DV → pad à esquerda: "00626780")
-- Rural INT065/INT057: nu_apf_com_dv (mesmo shape do GFAR; validar em 4.2)
{% macro normalize_apf(col) -%}
    {%- set t = "trim(cast(" ~ col ~ " as varchar))" -%}
    {%- set digits = "regexp_replace(" ~ t ~ ", '[^0-9]', '', 'g')" -%}
    case
        when {{ col }} is null or {{ t }} = ''
        then null
        when {{ t }} like '%-%'
        then lpad(replace({{ t }}, '-', ''), 8, '0')
        when length({{ digits }}) >= 8
        then right({{ digits }}, 8)
        else lpad({{ digits }}, 8, '0')
    end
{%- endmacro %}
