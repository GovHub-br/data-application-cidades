{#-
  rural_portaria_por_tipo(alias): cláusula `cross join lateral (values ...)` que desdobra uma linha por
  APF da prata_rural_vinculo_operacao em três, uma por tipo de portaria (Seleção, Autorização de
  contratação, Trabalho Social), com o rótulo usado no filtro único de portaria do painel do Rural.
  Quando a APF não tem portaria daquele tipo, o rótulo diz por quê (ex. "Seleção 2023 sem autorização
  carregada"), para a APF não sumir do painel quando se escolhe um tipo.
  Usada pela ouro_rural_painel_portaria e pela ouro_rural_curva_s, para os rótulos serem os mesmos.
-#}
{% macro rural_portaria_por_tipo(alias) -%}
cross join lateral (
    values
        ('Seleção'::text,
         coalesce({{ alias }}.portaria_selecao, 'Sem portaria de seleção identificada'),
         {{ alias }}.situacao_portaria_selecao,
         ({{ alias }}.portaria_selecao is not null)),
        ('Autorização de contratação'::text,
         coalesce({{ alias }}.portaria_autorizacao, {{ alias }}.situacao_portaria_autorizacao),
         {{ alias }}.situacao_portaria_autorizacao,
         ({{ alias }}.portaria_autorizacao is not null)),
        ('Trabalho Social'::text,
         coalesce({{ alias }}.portaria_ts, {{ alias }}.situacao_portaria_ts),
         {{ alias }}.situacao_portaria_ts,
         ({{ alias }}.portaria_ts is not null))
) as tp(tipo_portaria, portaria, situacao_portaria, ic_tem_portaria)
{%- endmacro %}
