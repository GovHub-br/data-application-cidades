{#-
  Teste genérico: retorna as linhas em que `abs(total - Σ parcelas) > tolerancia`.

  Dois usos no eixo (glossario-valores-financeiros.md, D7):
    - `serie_executiva`: total = `valor_investimento`,
      parcelas = [`valor_financiamento`, `valor_contrapartidas`, `subsidio_total`].
    - modelos de liberação por frente: total = `vr_liberado`,
      parcelas = componentes (obra, terreno, PTS, INCC, …) + o residual
      `valor_desembolsado_componente_outros`.

  Regras:
    - NULL numa parcela conta como 0 (`coalesce`).
    - Só testa linhas com `total` não nulo E ao menos uma parcela não nula
      (linha sem nenhuma decomposição não é violação — é ausência de dado).
    - `tolerancia` é ABSOLUTA, em R$ (ex.: 1.00 para "fecha ao centavo",
      1000.00 para folga de arredondamento de origem).
    - Severidade típica: `warn`.

  Uso no schema.yml (nível de model):

      data_tests:
        - reconcilia_decomposicao:
            arguments:
              total_column: valor_investimento
              parcela_columns: [valor_financiamento, valor_contrapartidas, subsidio_total]
              tolerancia: 1000.00
            config:
              severity: warn
-#}
{% macro test_reconcilia_decomposicao(model, total_column, parcela_columns, tolerancia) %}

{%- set soma_expr = [] -%}
{%- for p in parcela_columns -%}
    {%- do soma_expr.append('coalesce(' ~ p ~ ', 0)') -%}
{%- endfor -%}
{%- set alguma_parcela = [] -%}
{%- for p in parcela_columns -%}
    {%- do alguma_parcela.append(p ~ ' is not null') -%}
{%- endfor -%}

with base as (
    select
        {{ total_column }} as total,
        {{ soma_expr | join(' + ') }} as soma_parcelas
    from {{ model }}
    where {{ total_column }} is not null
        and ({{ alguma_parcela | join(' or ') }})
)

select
    total,
    soma_parcelas,
    round(total - soma_parcelas, 2) as diferenca,
    {{ tolerancia }} as tolerancia,
    count(*) as ocorrencias
from base
where abs(total - soma_parcelas) > {{ tolerancia }}
group by total, soma_parcelas
order by abs(total - soma_parcelas) desc

{% endmacro %}
