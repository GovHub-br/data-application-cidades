{#
    Teste genérico: a combinação de colunas é única no modelo (grão composto, ex.: APF × mês).
    O projeto não usa dbt_utils; este é o equivalente de unique_combination_of_columns.

        data_tests:
          - chave_composta_unica:
              arguments:
                colunas: [apf, dt_referencia]
#}
{% test chave_composta_unica(model, colunas) %}
select {{ colunas | join(', ') }}, count(*) as qt
from {{ model }}
group by {{ colunas | join(', ') }}
having count(*) > 1
{% endtest %}
