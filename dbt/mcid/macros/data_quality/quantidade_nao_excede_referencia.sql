{#-
  Teste genérico: retorna as linhas em que `column` é maior que
  `reference * fator`. Análogo a `desembolso_nao_excede_contratado`, mas para
  quantidades (UH) e comparação linha-a-linha (não só o último snapshot).

  `quantidade_uh_entregues` / `quantidade_uh_concluidas` não deveriam passar de
  `quantidade_uh` (contratadas). Quando passam, é defeito de origem (recontagem,
  troca de escopo, erro de digitação). Só LISTA; não filtra nem corrige.

  `fator` (default 1.0) dá folga: `1.0` = estrito. Nível: `warn`. Change:
  enriquecer-quantidades-uh-e-sinais-obra-historico (D7).

  Comparação só entre valores presentes: linha com `column` ou `reference` NULL
  nunca é listada.

  Uso no schema.yml (nível de coluna):

      columns:
        - name: quantidade_uh_entregues
          data_tests:
            - quantidade_nao_excede_referencia:
                arguments:
                  reference: quantidade_uh
                  fator: 1.0
                config:
                  severity: warn
-#}
{% macro test_quantidade_nao_excede_referencia(model, column_name, reference, fator=1.0) %}

select
    {{ column_name }} as valor,
    {{ reference }} as referencia,
    {{ fator }} as fator_limite,
    {{ column_name }} - {{ reference }} as excedente
from {{ model }}
where
    {{ column_name }} is not null
    and {{ reference }} is not null
    and {{ column_name }} > {{ reference }} * {{ fator }}
order by excedente desc

{% endmacro %}
