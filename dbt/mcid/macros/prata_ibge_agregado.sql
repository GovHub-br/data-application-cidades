{#
    Prata de um agregado do IBGE (API v3). As 13 pratas do IBGE são iguais: cada uma é
    `{{ prata_ibge_agregado('bronze_ibge_<tabela>') }}`.

    A staging é texto, como a API entrega (ADR-0002); a tipagem que a DAG fazia em pandas
    mora aqui:
    - ids viram bigint; com várias classificações juntas por `|`, o id fica nulo;
    - nomes de localidade, classificação e categoria em maiúsculas e sem bordas;
    - `valor` usa ponto decimal na API v3: não se remove o ponto. `...`, `-`, `X` e vazio
      viram nulo;
    - `data_referencia` é o 1º dia do período `AAAAMM`; período anual fica nulo.
    A bronze guarda todas as cargas; cada carga traz a janela pedida inteira, fica a última.
#}
{% macro prata_ibge_agregado(bronze) %}
    select
        periodo,
        case
            when length(periodo) = 6 then strptime(periodo || '01', '%Y%m%d')::date
        end as data_referencia,
        case when variavel_id ~ '^[0-9]+$' then variavel_id::bigint end as variavel_id,
        variavel_nome as variavel,
        unidade,
        case when localidade_id ~ '^[0-9]+$' then localidade_id::bigint end as localidade_id,
        upper(trim(localidade_nome)) as localidade,
        case
            when classificacao_id ~ '^[0-9]+$' then classificacao_id::bigint
        end as classificacao_id,
        upper(trim(classificacao_nome)) as classificacao,
        case when categoria_id ~ '^[0-9]+$' then categoria_id::bigint end as categoria_id,
        upper(trim(categoria_nome)) as categoria,
        case
            when trim(valor) ~ '^-?[0-9]+(\.[0-9]+)?$' then trim(valor)::double precision
        end as valor,
        left(_ingested_at, 19)::timestamp as dt_ingest
    from {{ ref(bronze) }}
    where {{ ultima_carga(ref(bronze)) }}
{% endmacro %}
