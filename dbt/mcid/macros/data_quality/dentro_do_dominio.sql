{#-
  Teste genérico: sinaliza valores de uma coluna que não constam num seed de
  domínio de referência.

  É o `accepted_values` nativo, mas com a lista de valores válidos VERSIONADA
  num seed em vez de embutida no schema.yml. Serve quando o domínio é grande,
  evolui, ou é compartilhado com o subprojeto de tratamento (mapa de status,
  frentes) — uma única fonte de verdade.

  Nunca descarta nem corrige: só lista o que está fora, em `warn`. Onde o
  domínio é pequeno e estável (`frente_mcmv` com 3 valores), prefira o
  `accepted_values` nativo inline.

  Comparação: case-insensitive e sem espaços nas bordas dos dois lados.

  Uso no schema.yml:

      columns:
        - name: status_operacional
          data_tests:
            - dentro_do_dominio:
                arguments:
                  seed: dominio_status
                  seed_column: valor_canonico
                config:
                  severity: warn
-#}
{% macro test_dentro_do_dominio(model, column_name, seed, seed_column) %}

with
    dominio as (
        select distinct lower(trim(cast({{ seed_column }} as varchar))) as valor_ok
        from {{ ref(seed) }}
    ),
    coluna as (
        select
            {{ column_name }} as valor_bruto,
            lower(trim(cast({{ column_name }} as varchar))) as valor_norm,
            count(*) as ocorrencias
        from {{ model }}
        where {{ column_name }} is not null
            and trim(cast({{ column_name }} as varchar)) <> ''
        group by 1, 2
    )

select coluna.valor_bruto, coluna.ocorrencias
from coluna
left join dominio on coluna.valor_norm = dominio.valor_ok
where dominio.valor_ok is null
order by coluna.ocorrencias desc

{% endmacro %}
