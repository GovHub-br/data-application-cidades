{#-
  Teste genérico: falha se a completude de uma coluna cai abaixo de um limiar.

  `not_null` puro serve para chave e auditoria — onde ausência é sempre erro.
  Para campo de negócio de base histórica, ausência PARCIAL é esperada (uma
  geração de schema não trazia a coluna, a frente não se aplica...). O que
  interessa é: "esta coluna não pode ficar abaixo de X% de preenchimento".

  Completude = linhas com valor / total de linhas. "Com valor" exclui NULL e
  string vazia/só-espaço (placeholder textual comum no histórico).

  Severidade: use `error` para campo obrigatório de contrato (o limiar vem do
  artefato de campos obrigatórios), `warn` para completude informativa.

  Uso no schema.yml:

      columns:
        - name: valor_contratado
          data_tests:
            - completude_minima:
                arguments:
                  min_pct: 0.90
                config:
                  severity: warn
-#}
{% macro test_completude_minima(model, column_name, min_pct) %}

with medida as (
    select
        count(*) as n_total,
        count(
            case
                when {{ column_name }} is not null
                    and trim(cast({{ column_name }} as varchar)) <> ''
                then 1
            end
        ) as n_preenchido
    from {{ model }}
)

select
    n_total,
    n_preenchido,
    n_preenchido::double / nullif(n_total, 0) as completude,
    {{ min_pct }} as min_pct_exigido
from medida
where n_total > 0
    and n_preenchido::double / n_total < {{ min_pct }}

{% endmacro %}
