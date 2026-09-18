{#
  Dimensão temporal padrão do conjuntura contínuo.

  Por que existe: até 2026-08-30 cada modelo derivava tempo do seu jeito —
  `ano` era `double precision` em uns e `integer` em outros, `trimestre` era
  o texto `'2T'` em três golds e o inteiro `2` num quarto, e 31 dos 36
  silvers não tinham trimestre nenhum. O resultado foi que a dimensão passou
  a ser reconstruída fora do medalhão: 21 datasets virtuais do Superset
  faziam aritmética de `ano*4 + trimestre` no próprio SQL do dashboard.

  O contrato abaixo é único e vale da silver em diante:

    data_referencia  date   primeiro dia do período
    ano              int
    mes              int    mês do dado (mensal) ou 1º mês do trimestre
    trimestre        int    1..4
    periodo          text   '2026-03' (mensal) ou '1T2026' (trimestral)
    edicao           text   '1T2026' — a edição do boletim que contém a linha

  `edicao` é o que o filtro do Superset usa. Para dado mensal é o trimestre
  que contém o mês; para dado trimestral é o próprio período. Com ela no
  modelo, o dashboard filtra por igualdade em vez de recalcular tempo.
#}

{% macro periodo_trimestral(ano, trimestre) %}
    ({{ trimestre }}::int::text || 'T' || {{ ano }}::int::text)
{% endmacro %}


{% macro indice_trimestre(ano, trimestre) %}
    ({{ ano }}::int * 4 + {{ trimestre }}::int)
{% endmacro %}


{% macro indice_mes(ano, mes) %}
    ({{ ano }}::int * 12 + {{ mes }}::int)
{% endmacro %}


{#
  A partir de uma data. Use em qualquer modelo que já tenha `data_referencia`
  ou equivalente — é o caminho preferido, porque a data é a única forma que
  não sofre de ordenação lexicográfica ('4T2025' > '1T2026' como texto).
#}
{% macro dimensao_temporal(data, mensal=true) %}
    {{ data }}::date                                              as data_referencia,
    extract(year    from {{ data }}::date)::int                   as ano,
    extract(month   from {{ data }}::date)::int                   as mes,
    extract(quarter from {{ data }}::date)::int                   as trimestre,
    {% if mensal -%}
    to_char({{ data }}::date, 'YYYY-MM')                          as periodo,
    {%- else -%}
    (extract(quarter from {{ data }}::date)::int::text || 'T'
     || extract(year from {{ data }}::date)::int::text)           as periodo,
    {%- endif %}
    (extract(quarter from {{ data }}::date)::int::text || 'T'
     || extract(year from {{ data }}::date)::int::text)           as edicao
{% endmacro %}


{#
  A partir do texto '1T2026'. Só para modelos cuja origem é a planilha manual,
  que não traz data. `left`/`right` bastam porque o formato é fixo em 6
  caracteres — se algum dia variar, o `nullif` evita virar data inválida
  silenciosa.
#}
{% macro dimensao_temporal_do_periodo(periodo) %}
    make_date(
        right({{ periodo }}, 4)::int,
        (left({{ periodo }}, 1)::int - 1) * 3 + 1,
        1
    )                                                             as data_referencia,
    right({{ periodo }}, 4)::int                                  as ano,
    (left({{ periodo }}, 1)::int - 1) * 3 + 1                     as mes,
    left({{ periodo }}, 1)::int                                   as trimestre,
    {{ periodo }}                                                 as periodo,
    {{ periodo }}                                                 as edicao
{% endmacro %}
