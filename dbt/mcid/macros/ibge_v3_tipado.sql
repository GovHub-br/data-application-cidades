{#
    Tipagem do achatamento da API v3 do IBGE (conversor `ibge_v3` da ingestão).

    A staging guarda texto: uma linha por (variável, localidade, classificação,
    categoria, período), com o valor como a API mandou. Este macro devolve as
    mesmas colunas e tipos que o parquet antigo tinha, quando a DAG tipava em
    pandas, para as pratas do IBGE não mudarem:

    - ids como inteiro (vazio ou composto, `1|2`, vira nulo, como o `to_numeric`);
    - nomes de localidade, classificação e categoria em maiúsculas, sem espaço
      nas pontas; `classificacao_nome`/`categoria_nome` viram `classificacao` e
      `categoria`;
    - `data_referencia` = período `AAAAMM` + dia 1 (período anual fica nulo);
    - `valor` numérico; `...`, `-`, `X` e afins viram nulo. A API usa PONTO como
      decimal e não tem separador de milhar: o ponto fica.
    - `dt_ingest` = partição da ingestão (`lake_dt_ingest`).

    Uso na prata:
        from ({{ ibge_v3_tipado(ref('bronze_ibge_sinapi')) }}) as bronze
#}
{% macro ibge_v3_tipado(relacao) -%}
select
    case when variavel_id ~ '^[0-9]+$' then variavel_id::bigint end          as variavel_id,
    variavel_nome,
    case when localidade_id ~ '^[0-9]+$' then localidade_id::bigint end      as localidade_id,
    upper(trim(localidade_nome))                                             as localidade_nome,
    case when classificacao_id ~ '^[0-9]+$' then classificacao_id::bigint end as classificacao_id,
    upper(trim(classificacao_nome))                                          as classificacao,
    case when categoria_id ~ '^[0-9]+$' then categoria_id::bigint end        as categoria_id,
    upper(trim(categoria_nome))                                              as categoria,
    unidade,
    periodo,
    case when periodo ~ '^[0-9]{6}$'
         then to_date(periodo || '01', 'YYYYMMDD')::timestamp end            as data_referencia,
    case when trim(valor) ~ '^-?[0-9]+(\.[0-9]+)?$'
         then trim(valor)::double precision end                              as valor,
    {{ lake_dt_ingest() }}                                                   as dt_ingest
from {{ relacao }}
{%- endmacro %}
