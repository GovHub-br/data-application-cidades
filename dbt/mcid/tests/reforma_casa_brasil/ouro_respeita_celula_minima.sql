-- Dimensões sensíveis só podem ser publicadas em células com ao menos dez
-- contratos, mitigando reidentificação por combinações raras. Acesso e
-- resultado só existem após a staging protegida do CadÚnico; portanto o teste
-- verifica dinamicamente apenas as Golds que já foram materializadas.
{% set modelos = [
    'ouro_reforma_casa_brasil_acesso_dash',
    'ouro_reforma_casa_brasil_implementacao_dash',
    'ouro_reforma_casa_brasil_monitoramento_recursos_dash',
    'ouro_reforma_casa_brasil_resultado_dash'
] %}
{% set consultas = [] %}

{% if execute %}
  {% for nome in modelos %}
    {% set relacao = adapter.get_relation(
        database=target.database,
        schema='ouro',
        identifier=nome
    ) %}
    {% if relacao is not none %}
      {% do consultas.append(
        "select '" ~ nome ~ "' as tabela, quantidade_contratos from "
        ~ relacao ~ " where quantidade_contratos < 10"
      ) %}
    {% endif %}
  {% endfor %}
{% endif %}

{% if consultas %}
  {{ consultas | join('\nunion all\n') }}
{% else %}
  select cast(null as varchar) as tabela, cast(null as bigint) as quantidade_contratos
  where false
{% endif %}
