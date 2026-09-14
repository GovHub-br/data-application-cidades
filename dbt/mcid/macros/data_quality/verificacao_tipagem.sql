{#-
  Teste genérico de coluna: falha se o `data_type` materializado da coluna
  diverge do tipo canônico do escopo.

  Referência de tipos: `models/mcmv_historico_dbt/docs/inventario-tipagem-silver-gold.md`.

  A comparação é por STRING EXATA contra `information_schema.columns.data_type`.
  Os `tipo_esperado` usam os nomes que o DuckDB reporta (modo A / build local):

      BIGINT · INTEGER · VARCHAR · DOUBLE · BOOLEAN · DATE
      DECIMAL(15,2) · DECIMAL(38,2) · TIMESTAMP WITH TIME ZONE

  Numa materialização futura no Postgres (modos B/C) os nomes mudam
  (`bigint`, `character varying`, `numeric`, `double precision`,
  `timestamp with time zone`...) e os `tipo_esperado` precisam de uma revisão —
  decisão "strings DuckDB" da change `verificar-tipagem-silver-gold-historico`.

  Resolve a tabela pelo próprio relation do modelo (`model.schema` /
  `model.identifier`) — nada de schema hard-coded.

  Uso no schema.yml (nível de coluna):

      columns:
        - name: uh_contratadas
          data_tests:
            - verificacao_tipagem:
                arguments: { tipo_esperado: BIGINT }
-#}
{% macro test_verificacao_tipagem(model, column_name, tipo_esperado) %}

select
    '{{ model.schema }}.{{ model.identifier }}' as nome_tabela,
    '{{ column_name }}' as nome_coluna,
    '{{ tipo_esperado }}' as tipo_esperado,
    data_type as tipo_real
from information_schema.columns
where
    table_schema = '{{ model.schema }}'
    and table_name = '{{ model.identifier }}'
    and column_name = '{{ column_name }}'
    and data_type <> '{{ tipo_esperado }}'

{% endmacro %}
