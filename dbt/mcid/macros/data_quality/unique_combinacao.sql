{#-
  Teste genérico: falha se a COMBINAÇÃO de colunas não for única.

  O `unique` nativo do dbt só olha uma coluna. Grão composto — "uma linha por APF por
  mês", "uma linha por APF por agente financeiro" — precisa deste. Equivale ao
  dbt_utils.unique_combination_of_columns, escrito aqui porque o projeto não usa
  dbt_utils e não vale puxar o pacote inteiro por um teste.

  Declarar o grão como teste não é burocracia: é o que pega o join que multiplicou
  linhas. Uma gold que deveria ter uma linha por empreendimento e passa a ter duas
  dobra todo somatório do dashboard, silenciosamente.

  Uso no schema.yml (nível de model, não de coluna):

      models:
        - name: evolucao_financeira_rural
          data_tests:
            - unique_combinacao:
                arguments:
                  colunas: ["apf", "mes"]
-#}
{% macro test_unique_combinacao(model, colunas) %}

{%- set lista = colunas | join(", ") -%}

select {{ lista }}, count(*) as linhas
from {{ model }}
group by {{ lista }}
having count(*) > 1
order by linhas desc

{% endmacro %}
