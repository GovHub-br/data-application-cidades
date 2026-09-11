{#-
  Teste genérico: falha quando o conjunto de colunas da tabela difere do declarado.

  A bronze lê o parquet com `select *`, então uma coluna que a origem adiciona ou remove
  entra na tabela em silêncio. Some uma coluna que ninguém usa e nada quebra hoje —
  quebra meses depois, no model que passar a precisar dela. Chega uma coluna nova e
  ninguém fica sabendo que a origem mudou de layout.

  O teste falha nos dois sentidos: coluna nova não declarada e coluna declarada ausente.
  Quando a mudança for legítima, atualize a lista `colunas` no schema.yml — é a aceitação
  explícita do novo layout.

  Nasce como `warn`, não `error`: layout novo na origem não é motivo para derrubar a DAG,
  mas fica gravado em lake._dbt_log pelo on-run-end. Um schema.yml pode sobrescrever com
  `config: severity: error` onde a quebra tiver que ser dura.

  Uso no schema.yml:

      models:
        - name: bronze_shpt_monit_cad_pj_far_mensal
          tests:
            - sem_drift_de_colunas:
                colunas: [nu_apf, dt_movimento, ...]
-#}
{% test sem_drift_de_colunas(model, colunas) %}

{{ config(severity="warn") }}

    with
        esperadas as (
            {% for coluna in colunas -%}
                select '{{ coluna }}' as coluna
                {% if not loop.last %}
                    union all
                {% endif %}
            {% endfor -%}
        ),

        atuais as (
            select column_name as coluna
            from information_schema.columns
            where
                table_schema = '{{ model.schema }}'
                and table_name = '{{ model.identifier }}'
        )

    select coluna, 'coluna_nova_nao_declarada' as divergencia
    from atuais
    where coluna not in (select coluna from esperadas)

    union all

    select coluna, 'coluna_declarada_ausente' as divergencia
    from esperadas
    where coluna not in (select coluna from atuais)

{% endtest %}
