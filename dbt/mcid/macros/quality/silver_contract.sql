{#
  Contrato genérico de qualidade para models Silver.

  A macro retorna uma linha por violação, no formato exigido por testes dbt.
  Ela não consulta conteúdo fora do model testado e não seleciona valores de
  negócio: os detalhes devolvidos são somente o nome da regra ou da coluna.
#}
{% test silver_contract(
    model,
    expected_columns=[],
    allow_additional_columns=true,
    required_columns=[],
    not_null_columns=[],
    unique_key=[],
    freshness_column=none,
    freshness_days=none,
    min_completeness={},
    expected_data_types={},
    accepted_values={},
    value_patterns={},
    numeric_ranges={},
    trimmed_text_columns=[]
) %}

    {% set checks = [] %}
    {% set schema = model.schema %}
    {% set identifier = model.identifier %}

    {# Convenção técnica: só nomes seguros chegam à camada de consumo. #}
    {% do checks.append(
        "select 'nome_de_coluna_invalido'::text as regra, 'estrutura do model'::text as detalhe "
        ~ "from information_schema.columns where table_schema = '" ~ schema ~ "' "
        ~ "and table_name = '" ~ identifier ~ "' and (column_name like 'unnamed%' "
        ~ "or column_name ~ '[^a-z0-9_]' or column_name !~ '^[a-z]')"
    ) %}

    {#
      expected_columns descreve o layout contratado. required_columns mantém a
      semântica de interface mínima: uma coluna pode existir no layout e não
      ser obrigatória para todos os consumidores.
    #}
    {% for column in expected_columns %}
        {% do checks.append(
            "select 'coluna_do_layout_ausente'::text as regra, 'estrutura do model'::text as detalhe "
            ~ "where not exists (select 1 from information_schema.columns "
            ~ "where table_schema = '" ~ schema ~ "' and table_name = '" ~ identifier ~ "' "
            ~ "and column_name = '" ~ column ~ "')"
        ) %}
    {% endfor %}

    {% if expected_columns and not allow_additional_columns %}
        {% set expected_columns_sql = [] %}
        {% for column in expected_columns %}
            {% do expected_columns_sql.append("'" ~ (column | replace("'", "''")) ~ "'") %}
        {% endfor %}
        {% do checks.append(
            "select 'coluna_fora_do_layout'::text as regra, 'estrutura do model'::text as detalhe "
            ~ "from information_schema.columns where table_schema = '" ~ schema ~ "' "
            ~ "and table_name = '" ~ identifier ~ "' and column_name not in ("
            ~ (expected_columns_sql | join(', ')) ~ ") limit 1"
        ) %}
    {% endif %}

    {% for column in required_columns %}
        {% do checks.append(
            "select 'coluna_obrigatoria_ausente'::text as regra, '" ~ column ~ "'::text as detalhe "
            ~ "where not exists (select 1 from information_schema.columns "
            ~ "where table_schema = '" ~ schema ~ "' and table_name = '" ~ identifier ~ "' "
            ~ "and column_name = '" ~ column ~ "')"
        ) %}
    {% endfor %}

    {% for column in not_null_columns %}
        {% do checks.append(
            "select 'valor_obrigatorio_ausente'::text as regra, '" ~ column ~ "'::text as detalhe "
            ~ "from " ~ model ~ " where \"" ~ column ~ "\" is null limit 1"
        ) %}
    {% endfor %}

    {% if unique_key %}
        {% set unique_key_sql = [] %}
        {% for column in unique_key %}
            {% do unique_key_sql.append('"' ~ column ~ '"') %}
        {% endfor %}
        {% do checks.append(
            "select 'chave_duplicada'::text as regra, 'chave declarada'::text as detalhe "
            ~ "from (select 1 from " ~ model ~ " group by " ~ (unique_key_sql | join(', '))
            ~ " having count(*) > 1 limit 1) as duplicidade"
        ) %}
    {% endif %}

    {% if freshness_column and freshness_days is not none %}
        {% do checks.append(
            "select 'frescor_insuficiente'::text as regra, '" ~ freshness_column ~ "'::text as detalhe "
            ~ "where coalesce((select max(\"" ~ freshness_column ~ "\")::date from " ~ model ~ "), "
            ~ "current_date - interval '" ~ (freshness_days + 1) ~ " day') "
            ~ "< current_date - interval '" ~ freshness_days ~ " day'"
        ) %}
    {% endif %}

    {% for column, threshold in min_completeness.items() %}
        {% do checks.append(
            "select 'completude_insuficiente'::text as regra, '" ~ column ~ "'::text as detalhe "
            ~ "from (select coalesce(avg(case when \"" ~ column ~ "\" is not null then 1.0 else 0.0 end), 0) "
            ~ "as completude from " ~ model ~ ") as medida where completude < " ~ threshold
        ) %}
    {% endfor %}

    {% for column, expected_type in expected_data_types.items() %}
        {% do checks.append(
            "select 'tipo_incompativel'::text as regra, 'estrutura do model'::text as detalhe "
            ~ "from information_schema.columns where table_schema = '" ~ schema ~ "' "
            ~ "and table_name = '" ~ identifier ~ "' and column_name = '" ~ column ~ "' "
            ~ "and lower(data_type) <> lower('" ~ (expected_type | replace("'", "''")) ~ "')"
        ) %}
    {% endfor %}

    {% for column, allowed in accepted_values.items() %}
        {% set allowed_sql = [] %}
        {% for allowed_value in allowed %}
            {% do allowed_sql.append("'" ~ (allowed_value | string | replace("'", "''")) ~ "'") %}
        {% endfor %}
        {% if allowed_sql %}
            {% do checks.append(
                "select 'valor_fora_do_dominio'::text as regra, 'dominio declarado'::text as detalhe "
                ~ "from " ~ model ~ " where \"" ~ column ~ "\" is not null and \"" ~ column ~ "\"::text not in ("
                ~ (allowed_sql | join(', ')) ~ ") limit 1"
            ) %}
        {% endif %}
    {% endfor %}

    {% for column, pattern in value_patterns.items() %}
        {% do checks.append(
            "select 'padrao_de_valor_invalido'::text as regra, 'padrao declarado'::text as detalhe "
            ~ "from " ~ model ~ " where \"" ~ column ~ "\" is not null and \"" ~ column ~ "\"::text !~ '"
            ~ (pattern | replace("'", "''")) ~ "' limit 1"
        ) %}
    {% endfor %}

    {% for column, bounds in numeric_ranges.items() %}
        {% set conditions = [] %}
        {% if bounds.get('min') is not none %}
            {% do conditions.append('\"' ~ column ~ '\"::numeric < ' ~ bounds.get('min')) %}
        {% endif %}
        {% if bounds.get('max') is not none %}
            {% do conditions.append('\"' ~ column ~ '\"::numeric > ' ~ bounds.get('max')) %}
        {% endif %}
        {% if conditions %}
            {% do checks.append(
                "select 'valor_fora_da_faixa'::text as regra, 'faixa declarada'::text as detalhe "
                ~ "from " ~ model ~ " where \"" ~ column ~ "\" is not null and ("
                ~ (conditions | join(' or ')) ~ ") limit 1"
            ) %}
        {% endif %}
    {% endfor %}

    {% for column in trimmed_text_columns %}
        {% do checks.append(
            "select 'texto_sem_normalizacao'::text as regra, 'padrao textual'::text as detalhe "
            ~ "from " ~ model ~ " where \"" ~ column ~ "\" is not null and \"" ~ column ~ "\"::text <> btrim(\""
            ~ column ~ "\"::text) limit 1"
        ) %}
    {% endfor %}

    {% if checks %}
        {% for check in checks %}
            select * from (
                {{ check }}
            ) as verificacao_{{ loop.index }}
            {{ "union all" if not loop.last }}
        {% endfor %}
    {% else %}
        select null::text as regra, null::text as detalhe where false
    {% endif %}

{% endtest %}


{#
  Reconcilia duas versões da mesma série (por exemplo, fonte antiga e fonte
  substituta). A configuração informa a chave de comparação, o pareamento de
  medidas e uma tolerância absoluta. O resultado nunca retorna chaves nem
  valores de negócio, apenas a regra que falhou.
#}
{% test silver_reconciliation(
    model,
    compare_model,
    key_columns=[],
    measures={},
    tolerance=0
) %}

    {% if not key_columns or not measures %}
        select null::text as regra, null::text as detalhe where false
    {% else %}
        {% set key_columns_sql = [] %}
        {% for column in key_columns %}
            {% do key_columns_sql.append('"' ~ column ~ '"') %}
        {% endfor %}
        {% set first_key = key_columns[0] %}
        {% set local_measures = [] %}
        {% set external_measures = [] %}
        {% set difference_checks = [] %}

        {% for local_column, external_column in measures.items() %}
            {% do local_measures.append('sum(coalesce("' ~ local_column ~ '"::numeric, 0)) as "' ~ local_column ~ '"') %}
            {% do external_measures.append('sum(coalesce("' ~ external_column ~ '"::numeric, 0)) as "' ~ local_column ~ '"') %}
            {% do difference_checks.append('abs(l."' ~ local_column ~ '" - r."' ~ local_column ~ '") > ' ~ tolerance) %}
        {% endfor %}

        with base_atual as (
            select
                {{ key_columns_sql | join(', ') }},
                {{ local_measures | join(', ') }}
            from {{ model }}
            group by {{ key_columns_sql | join(', ') }}
        ),
        base_comparada as (
            select
                {{ key_columns_sql | join(', ') }},
                {{ external_measures | join(', ') }}
            from {{ compare_model }}
            group by {{ key_columns_sql | join(', ') }}
        ),
        chaves_divergentes as (
            select
                'chave_ausente_em_uma_fonte'::text as regra,
                'chave de reconciliação'::text as detalhe
            from base_atual l
            full outer join base_comparada r using ({{ key_columns_sql | join(', ') }})
            where l."{{ first_key }}" is null or r."{{ first_key }}" is null
            limit 1
        ),
        medidas_divergentes as (
            select
                'medida_divergente_entre_fontes'::text as regra,
                'métrica de reconciliação'::text as detalhe
            from base_atual l
            inner join base_comparada r using ({{ key_columns_sql | join(', ') }})
            where {{ difference_checks | join(' or ') }}
            limit 1
        )
        select * from chaves_divergentes
        union all
        select * from medidas_divergentes
    {% endif %}

{% endtest %}


{# Teste separado para que segurança seja a única regra bloqueante inicial. #}
{% test sem_coluna_sensivel(model) %}

    select
        'nome_de_coluna_sensivel'::text as regra,
        'coluna sensível identificada'::text as detalhe
    from information_schema.columns
    where table_schema = '{{ model.schema }}'
      and table_name = '{{ model.identifier }}'
      and {{ expressao_sensivel('column_name') }}

{% endtest %}
