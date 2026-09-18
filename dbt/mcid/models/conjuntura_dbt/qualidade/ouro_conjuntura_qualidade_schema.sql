{{ config(materialized='incremental', unique_key=['model', 'coluna', 'visto_em']) }}

-- Retrato do schema de cada model da prata e da ouro: uma linha por coluna,
-- com tipo e data da medição.
--
-- Responde ao item 5 do checklist ("identificação de colunas que deixaram de
-- existir e novas colunas"). É INCREMENTAL de propósito: cada execução
-- acrescenta o retrato do dia, e o histórico é o que permite comparar. Um
-- model `table` sobrescreveria e a gente perderia justamente a informação
-- que interessa — o que mudou.
--
-- A comparação entre retratos fica em `ouro_conjuntura_qualidade_schema_drift`.

{% set relacoes = relacoes_do_produto(
    'conjuntura_dbt',
    camadas=['prata', 'ouro'],
    excluir_prefixo='ouro_conjuntura_qualidade',
) %}

select
    table_schema::text            as schema_dado,
    table_name::text              as model,
    -- Nome mascarado quando a coluna carrega identificador de pessoa: este
    -- model é da ouro e chega ao Superset. O rótulo é estável, então a
    -- detecção de drift continua funcionando sem publicar o nome real.
    {{ mascarar_coluna_sensivel('column_name') }}::text as coluna,
    data_type::text               as tipo,
    ordinal_position::int         as posicao,
    current_date                  as visto_em
from information_schema.columns
{% if relacoes %}
where (table_schema, table_name) in (
    {% for r in relacoes -%}
    ('{{ r["schema"] }}', '{{ r["tabela"] }}'){{ ", " if not loop.last }}
    {%- endfor %}
)
{% else %}
where false
{% endif %}

{% if is_incremental() %}
  and current_date > (select coalesce(max(visto_em), '1900-01-01'::date) from {{ this }})
{% endif %}
