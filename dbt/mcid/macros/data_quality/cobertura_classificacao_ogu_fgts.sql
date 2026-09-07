{#-
  Teste genérico: falha quando a fração de valor com classificação OGU/FGTS
  preenchida cai abaixo de `min_pct`.

  Cobertura = Σ valor com `linha_ogu_fgts` classificada / Σ valor total, no
  nível de agregação escolhido (o `gold_serie_mensal` tem GROUPING SETS
  nacional/regiao/uf — somar todos triplica; o teste fixa um nível).

  "Classificada" = `linha_ogu_fgts` não nulo e diferente do rótulo de ausência
  (`Nao classificada`). A família `bext` reporta só `subsidio_total` sem split
  FGTS/OGU -> fica sem classificação e derruba a cobertura; o `warn` é o sinal
  disso, e o glossário registra o impacto no backtest do piloto #118.

  Severidade típica: `warn`.

  Uso no schema.yml (nível de model):

      data_tests:
        - cobertura_classificacao_ogu_fgts:
            arguments:
              min_pct: 0.40
              valor_column: valor_investimento_acumulado
            config:
              severity: warn
-#}
{% macro test_cobertura_classificacao_ogu_fgts(
    model,
    min_pct,
    valor_column='valor_investimento_acumulado',
    classificacao_column='linha_ogu_fgts',
    rotulo_ausencia='Nao classificada',
    nivel_column='nivel_agregacao',
    nivel_valor='nacional'
) %}

with medida as (
    select
        sum({{ valor_column }}) as valor_total,
        sum(
            case
                when {{ classificacao_column }} is not null
                    and {{ classificacao_column }} <> '{{ rotulo_ausencia }}'
                then {{ valor_column }}
            end
        ) as valor_classificado
    from {{ model }}
    where {{ nivel_column }} = '{{ nivel_valor }}'
        and {{ valor_column }} is not null
)

select
    valor_total,
    valor_classificado,
    valor_classificado::double / nullif(valor_total, 0) as cobertura,
    {{ min_pct }} as min_pct_exigido
from medida
where valor_total > 0
    and valor_classificado::double / valor_total < {{ min_pct }}

{% endmacro %}
