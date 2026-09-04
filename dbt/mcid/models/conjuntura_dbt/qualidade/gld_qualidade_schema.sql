{{ config(materialized='incremental', schema='conjuntura', unique_key=['model','coluna','visto_em']) }}

-- Retrato do schema de cada model silver/gold: uma linha por coluna, com tipo
-- e data da medição.
--
-- Responde ao item 5 do checklist ("identificação de colunas que deixaram de
-- existir e novas colunas"). É INCREMENTAL de propósito: cada execução
-- acrescenta o retrato do dia, e o histórico é o que permite comparar. Um
-- model `table` sobrescreveria e a gente perderia justamente a informação
-- que interessa — o que mudou.
--
-- A comparação entre retratos fica em `gld_qualidade_schema_drift`.

select
    table_schema::text            as schema_dado,
    table_name::text              as model,
    -- Nome mascarado quando a coluna carrega identificador de pessoa: este
    -- model é gold e chega ao Superset. O rótulo é estável, então a detecção
    -- de drift continua funcionando sem publicar o nome real.
    {{ mascarar_coluna_sensivel('column_name') }}::text as coluna,
    data_type::text               as tipo,
    ordinal_position::int         as posicao,
    current_date                  as visto_em
from information_schema.columns
where table_schema in ('conjuntura', 'conjuntura', 'conjuntura')
  and table_name not like 'gold_qualidade%'

{% if is_incremental() %}
  and current_date > (select coalesce(max(visto_em), '1900-01-01'::date) from {{ this }})
{% endif %}
