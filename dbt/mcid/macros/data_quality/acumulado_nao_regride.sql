{#-
  Teste genérico: retorna as linhas em que uma coluna de ACUMULADO cai vs. a
  observação anterior da mesma partição (na ordem dada).

  Colunas como `quantidade_uh_entregues`, `quantidade_uh_concluidas` e
  `valor_desembolsado` são acumuladas ao longo do tempo por APF — deveriam ser
  monotônicas não-decrescentes. Uma queda mês-a-mês é defeito de origem
  (recontagem, troca de fonte, snapshot parcial). Este teste só LISTA as quedas;
  não filtra nem corrige nada — a linha "ruim" permanece na silver.

  Nível: `warn` (nesta change não há threshold de erro). Change:
  enriquecer-quantidades-uh-e-sinais-obra-historico (D7).

  Uso no schema.yml (nível de coluna):

      columns:
        - name: quantidade_uh_entregues
          data_tests:
            - acumulado_nao_regride:
                arguments:
                  partition_by: apf
                  order_by: dt_referencia
                config:
                  severity: warn
-#}
{% macro test_acumulado_nao_regride(model, column_name, partition_by, order_by) %}

with ordenado as (
    select
        {{ partition_by }} as particao,
        {{ order_by }} as ordem,
        {{ column_name }} as valor,
        lag({{ column_name }}) over (
            partition by {{ partition_by }} order by {{ order_by }}
        ) as valor_anterior
    from {{ model }}
)

select
    particao,
    ordem,
    valor,
    valor_anterior,
    valor_anterior - valor as queda
from ordenado
where
    valor is not null
    and valor_anterior is not null
    and valor < valor_anterior
order by queda desc

{% endmacro %}
