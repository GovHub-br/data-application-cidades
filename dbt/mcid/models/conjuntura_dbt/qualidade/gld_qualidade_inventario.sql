{{ config(materialized='table', schema='conjuntura') }}

-- Inventário dos models do conjuntura: camada, materialização e volume.
--
-- Item 8 do checklist ("identificar quais seguem full-refresh e quais são
-- incrementais"). Hoje a resposta é simples e vale registrar: **tudo é
-- `table`, ou seja, full-refresh puro** — as únicas exceções são os próprios
-- models de qualidade (`gld_qualidade_schema` é incremental porque o
-- histórico É o dado).
--
-- Isso tem uma consequência boa: como nada é incremental, nenhum layout novo
-- entra "por baixo" — toda execução reconstrói a partir da bronze, então
-- mudança de estrutura na origem aparece de imediato, e não meses depois
-- misturada com dado velho.

select
    case
        when table_schema like '%_bronze' then 'bronze'
        when table_schema like '%_silver' then 'silver'
        else 'gold'
    end                                   as camada,
    table_schema::text                    as schema_dado,
    table_name::text                      as model,
    case table_type
        when 'BASE TABLE' then 'table (full-refresh)'
        when 'VIEW'       then 'view'
        else lower(table_type)
    end                                   as materializacao,
    (xpath(
        '/row/cnt/text()',
        query_to_xml(
            format('select count(*) as cnt from %I.%I', table_schema, table_name),
            false, true, ''
        )
    ))[1]::text::bigint                   as linhas,
    current_timestamp                     as medido_em
from information_schema.tables
where table_schema in ('conjuntura', 'conjuntura', 'conjuntura')
order by 1, 3
