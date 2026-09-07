{#-
  Teste genérico: sinaliza `(dt_referencia, uf)` com mais de uma `fonte_familia`
  de GRÃO DIFERENTE no `gold_serie_mensal`.

  `gold_serie_mensal` põe todas as famílias lado a lado; `bases_relatorio_executivo`
  e `min_cidades` (grão empreendimento) sobrepõem `bext` (grão contrato) em
  2014–2018. Somar valor/UH entre elas no mesmo mês é dupla contagem de grãos
  distintos. Um mart consumidor precisa filtrar por `prioridade_familia`.

  Este teste não conserta nada — é um lembrete que falha em `warn` enquanto a
  janela de sobreposição existir (é característica do dado, não bug). Lista os
  meses/UF e as famílias concorrentes.

  Roda no nível de agregação `uf` por padrão (o gold tem GROUPING SETS).

  Uso no schema.yml (nível de model):

      data_tests:
        - soma_nao_cruza_familia:
            config:
              severity: warn
-#}
{% macro test_soma_nao_cruza_familia(
    model,
    data_column='dt_referencia',
    uf_column='uf',
    familia_column='fonte_familia',
    grao_column='grao_familia',
    nivel_column='nivel_agregacao',
    nivel_valor='uf'
) %}

with por_janela as (
    select
        {{ data_column }} as dt_referencia,
        {{ uf_column }} as uf,
        count(distinct {{ familia_column }}) as n_familias,
        count(distinct {{ grao_column }}) as n_graos,
        string_agg(distinct {{ familia_column }} || ' (' || {{ grao_column }} || ')', ', ') as familias
    from {{ model }}
    where {{ nivel_column }} = '{{ nivel_valor }}'
    group by {{ data_column }}, {{ uf_column }}
)

select
    dt_referencia,
    uf,
    n_familias,
    n_graos,
    familias
from por_janela
where n_familias > 1 and n_graos > 1
order by dt_referencia, uf

{% endmacro %}
