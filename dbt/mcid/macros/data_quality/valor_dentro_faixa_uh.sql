{#-
  Teste genérico: sinaliza as linhas cujo valor POR UNIDADE HABITACIONAL
  (`valor_column / uh_column`) cai fora do intervalo `[piso, teto]` definido num
  seed de faixa de plausibilidade.

  O seed (`seeds/data_quality/faixa_valor_uh.csv`) tem as colunas
  `frente, faixa, valor_col, piso, teto`. Uma linha por (frente × faixa ×
  coluna-de-valor). Os limites são calibrados na distribuição observada e
  revisáveis com a área de negócio (ver seeds/data_quality/README.md).

  Regras:
    - Só testa linhas com `uh_column > 0` e `valor_column` não nulo.
    - Só testa linhas cuja combinação de chave EXISTE no seed para esta
      `valor_column`; sem faixa definida = sem teste (não é violação).
    - Comparação de chave case-insensitive e sem espaços nas bordas.
    - Severidade típica: `warn` (limiar de plausibilidade, não de contrato).

  `seed_key_columns` mapeia coluna-do-modelo -> coluna-do-seed. Ex.: no
  `serie_executiva` a frente vem de `frente_mcmv` e a faixa de `faixa`.

  Uso no schema.yml (nível de model):

      data_tests:
        - valor_dentro_faixa_uh:
            arguments:
              valor_column: valor_investimento
              uh_column: uh_contratadas
              seed: faixa_valor_uh
              seed_key_columns:
                frente_mcmv: frente
                faixa: faixa
            config:
              severity: warn
-#}
{% macro test_valor_dentro_faixa_uh(model, valor_column, uh_column, seed, seed_key_columns) %}

{%- set join_conds = [] -%}
{%- for model_col, seed_col in seed_key_columns.items() -%}
    {%- do join_conds.append(
        "lower(trim(cast(m." ~ model_col ~ " as varchar))) = lower(trim(cast(s." ~ seed_col ~ " as varchar)))"
    ) -%}
{%- endfor -%}

with
    faixa as (
        select
            {%- for model_col, seed_col in seed_key_columns.items() %}
            {{ seed_col }},
            {%- endfor %}
            cast(piso as double) as piso,
            cast(teto as double) as teto
        from {{ ref(seed) }}
        where lower(trim(cast(valor_col as varchar))) = lower('{{ valor_column }}')
    ),
    medido as (
        select
            m.*,
            {{ valor_column }}::double / nullif({{ uh_column }}, 0) as valor_por_uh
        from {{ model }} m
        where {{ uh_column }} > 0
            and {{ valor_column }} is not null
    )

select
    {%- for model_col in seed_key_columns.keys() %}
    m.{{ model_col }},
    {%- endfor %}
    m.{{ valor_column }} as valor,
    m.{{ uh_column }} as uh,
    round(m.valor_por_uh, 2) as valor_por_uh,
    s.piso,
    s.teto,
    count(*) as ocorrencias
from medido m
join faixa s on {{ join_conds | join(' and ') }}
where m.valor_por_uh < s.piso or m.valor_por_uh > s.teto
group by
    {%- for model_col in seed_key_columns.keys() %}
    m.{{ model_col }},
    {%- endfor %}
    m.{{ valor_column }}, m.{{ uh_column }}, m.valor_por_uh, s.piso, s.teto
order by ocorrencias desc

{% endmacro %}
