{#
    Silver de uma família dos dados históricos: o de-para (de_para_dhist) aplicado sobre a
    bronze, com tipos e o contexto do log do cleansed.

    - Cada coluna da silver é o primeiro valor não vazio entre as origens (o cleansed usa
      texto vazio, não nulo), com o tipo aplicado por try_cast. O cleansed já escreveu os
      valores de um jeito só (ponto decimal, datas AAAA-MM-DD, sem ".0"), então o cast é
      direto; o teste dhist_prata_tipagem_sem_perda confere que nenhum valor se perdeu nele.
    - `ibge`: o código com 7 dígitos; o de 6 (sem o dígito verificador) é completado pelo
      seed ibge_municipio. O valor da fonte fica em `<nome>_fonte` (o "0" do painel SNH é
      código de ausente e fica nulo).
    - `ano_mes`: AAAAMM na fonte, vira o 1º dia do mês.
    - `situacao`: o texto da fonte e a situação canônica (`<nome>_canonica`), pelos seeds
      data_quality/dominio_status (compartilhado) e dados_historicos/dominio_status_extra.
    - Contexto pelo log, ligado pela `saida`: `competencia` (1º dia do mês) e `data_corte`.
    - O grão é o da fonte: uma linha da silver por linha do cleansed (`saida`,
      `_linha_origem`). Agregar é da gold.
#}
{% macro dhist_origem(origens) -%}
    coalesce(
        {%- for o in origens %}nullif(trim(b."{{ o }}"), ''){{ ', ' if not loop.last }}{% endfor -%}
    )
{%- endmacro %}

{% macro dhist_tipo(expr, tipo) -%}
    {%- if tipo == 'inteiro' -%} try_cast({{ expr }} as bigint)
    {%- elif tipo == 'ano' -%} try_cast({{ expr }} as integer)
    {%- elif tipo == 'valor' -%} try_cast({{ expr }} as decimal(20, 4))
    {%- elif tipo == 'percentual' -%} try_cast({{ expr }} as decimal(12, 6))
    {%- elif tipo == 'coordenada' -%} try_cast({{ expr }} as double)
    {%- elif tipo == 'data' -%} try_cast(try_cast({{ expr }} as timestamp) as date)
    {%- elif tipo == 'ano_mes' -%} try_cast(try_strptime({{ expr }}, '%Y%m') as date)
    {%- else -%} {{ expr }}
    {%- endif -%}
{%- endmacro %}

{% macro prata_dhist(familia) %}
    {%- set colunas = de_para_dhist()[familia]['colunas'] -%}
    with

        b as (select * from {{ ref('bronze_dhist_' ~ familia) }}),

        log as (
            select saida, competencia, data_corte
            from {{ ref('bronze_dhist_cleansed_log') }}
            where status = 'tratado'
        )

    select
        b.familia,
        b.saida,
        b._linha_origem,
        b._source_file as arquivo_raw,
        try_cast(log.competencia || '-01' as date) as competencia,
        log.data_corte,
        {%- for c in colunas %}
            {%- set origem = dhist_origem(c.origens) %}
            {%- if c.tipo == 'ibge' %}
                case
                    when length({{ origem }}) = 7
                    then {{ origem }}
                    when length({{ origem }}) = 6
                    then cast(ibge_{{ loop.index }}.cod_ibge as varchar)
                end as "{{ c.nome }}",
                {{ origem }} as "{{ c.nome }}_fonte"
            {%- elif c.tipo == 'situacao' %}
                {{ origem }} as "{{ c.nome }}",
                status_{{ loop.index }}.situacao_canonica as "{{ c.nome }}_canonica"
            {%- else %} {{ dhist_tipo(origem, c.tipo) }} as "{{ c.nome }}"
            {%- endif %}
            {{- ',' if not loop.last }}
        {%- endfor %}
    from b
    left join log on b.saida = log.saida
    {%- for c in colunas %}
        {%- if c.tipo == 'ibge' %}
            left join
                {{ ref('ibge_municipio') }} as ibge_{{ loop.index }}
                on substr(cast(ibge_{{ loop.index }}.cod_ibge as varchar), 1, 6)
                = {{ dhist_origem(c.origens) }}
        {%- elif c.tipo == 'situacao' %}
            left join
                (
                    select distinct
                        strip_accents(lower(trim(valor_bruto))) as chave, situacao_canonica
                    from
                        (
                            select valor_bruto, situacao_canonica
                            from {{ ref('dominio_status') }}
                            union all
                            select valor_bruto, situacao_canonica
                            from {{ ref('dominio_status_extra') }}
                        )
                ) as status_{{ loop.index }}
                on status_{{ loop.index }}.chave
                = strip_accents(lower({{ dhist_origem(c.origens) }}))
        {%- endif %}
    {%- endfor %}
{% endmacro %}

{#- As famílias que já têm silver (as que estão no de-para). -#}
{% macro familias_prata_dhist() %} {{ return(de_para_dhist().keys() | list | sort) }} {% endmacro %}
