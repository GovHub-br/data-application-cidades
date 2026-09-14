{#-
  Teste genérico: no ÚLTIMO snapshot por APF de uma silver histórica por frente,
  retorna os APF em que `valor_desembolsado > valor_contratado * fator`.

  "Último snapshot" = maior `dt_referencia` do APF. Só considera APF com
  `valor_contratado > 0` e `valor_desembolsado` não nulo.

  Dois níveis (glossario-valores-financeiros.md, D7) — declarar o teste duas
  vezes no modelo:
    - `warn`: sem `ratio_minimo` -> fator = 1 + `tolerancia` (folga pequena).
    - `error`: com `ratio_minimo` (ex.: 2.0) -> pega só os casos grosseiros
      (Rural tem APF com desembolso 9× o contratado).
  FAR/FDS declaram só o `warn`.

  Uso no schema.yml (nível de model):

      data_tests:
        - desembolso_nao_excede_contratado:
            arguments: { tolerancia: 0.01 }
            config: { severity: warn }
        - desembolso_nao_excede_contratado:      # só Rural
            arguments: { ratio_minimo: 2.0 }
            config: { severity: error }
-#}
{% macro test_desembolso_nao_excede_contratado(
    model,
    tolerancia=0.01,
    ratio_minimo=none,
    apf_column='apf',
    data_column='dt_referencia',
    contratado_column='valor_contratado',
    desembolsado_column='valor_desembolsado'
) %}

{%- set fator = ratio_minimo if ratio_minimo is not none else (1.0 + tolerancia) -%}

with ultimo as (
    select
        {{ apf_column }} as apf,
        {{ contratado_column }} as valor_contratado,
        {{ desembolsado_column }} as valor_desembolsado,
        row_number() over (
            partition by {{ apf_column }}
            order by {{ data_column }} desc nulls last
        ) as rn
    from {{ model }}
    where {{ apf_column }} is not null
)

select
    apf,
    valor_contratado,
    valor_desembolsado,
    round(valor_desembolsado / nullif(valor_contratado, 0), 3) as ratio,
    {{ fator }} as fator_limite
from ultimo
where rn = 1
    and valor_contratado > 0
    and valor_desembolsado is not null
    and valor_desembolsado > valor_contratado * {{ fator }}
order by ratio desc

{% endmacro %}
