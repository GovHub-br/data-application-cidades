{#-
  Teste genérico: sinaliza quando o conjunto de colunas de um modelo diverge do
  retrato versionado no seed `colunas_esperadas`.

  As famílias do eixo histórico têm 2-3 gerações de schema e o bronze faz
  `select *` do glob de Parquet: uma coluna nova numa entrega entra sozinha,
  uma coluna que some vira `null` no `union_by_name` — e ninguém é avisado.
  Este teste é o aviso.

  Compara as colunas da RELAÇÃO JÁ MATERIALIZADA (não o glob cru — assim roda
  offline, depois do `dbt run`, sem depender de introspecção da staging) com as
  linhas do seed para aquele `modelo`. Colunas de auditoria adicionadas pelos
  corpos bronze (`macros/historico/corpos_bronze.sql`) são ignoradas.

  Nível: `warn`. Uma coluna a mais numa entrega não deve travar o build — deve
  gerar uma decisão: cataloga no seed (`seeds/data_quality/colunas_esperadas.csv`)
  ou trata como ruído conhecido. O seed nasce do `drift_schema.csv` do change
  `modelo-carga-drift-schema` (ou do retrato inicial das tabelas materializadas).

  Uso no schema.yml (nível de model):

      models:
        - name: bronze_dhist_serie_entrada_bb
          data_tests:
            - colunas_esperadas:
                config:
                  severity: warn
-#}
{% macro test_colunas_esperadas(model) %}

{%- set audit = [
    "fonte_familia", "fonte_interface", "source_file", "report_date_parsed",
    "dt_referencia", "dt_ingest", "hash_linha", "agente_arquivo",
    "prioridade_reentrega", "dt_evento", "qt_uh_entregues_evento",
] -%}

{%- set atuais = [] -%}
{%- if execute -%}
    {%- for c in adapter.get_columns_in_relation(model) -%}
        {%- if (c.name | lower) not in audit -%}
            {%- do atuais.append(c.name | lower) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

with
    esperado as (
        select lower(trim(cast(coluna as varchar))) as coluna
        from {{ ref("colunas_esperadas") }}
        where modelo = '{{ model.identifier }}'
    ),
    atual as (
        {%- if atuais | length > 0 %}
        {%- for c in atuais %}
        select '{{ c }}' as coluna{% if not loop.last %}
        union all{% endif %}
        {%- endfor %}
        {%- else %}
        select cast(null as varchar) as coluna where 1 = 0
        {%- endif %}
    )

select 'nova nao catalogada' as status, atual.coluna
from atual
left join esperado using (coluna)
where esperado.coluna is null

union all

select 'removida' as status, esperado.coluna
from esperado
left join atual using (coluna)
where atual.coluna is null

{% endmacro %}
