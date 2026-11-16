-- Validação 1 (bronze): as colunas de cada bronze são exatamente a união das colunas que
-- o log do cleansed registra para as saídas tratadas da família (fora as técnicas).
-- Devolve a coluna que sobra ou falta de um dos lados.
with

    esperadas as (
        select distinct familia, unnest(colunas) as coluna
        from {{ ref('bronze_dhist_cleansed_log') }}
        where status = 'tratado'
    ),

    na_bronze as (
        {% for f in familias_dhist() %}
            {%- set rel = ref('bronze_dhist_' ~ f) %}
            select '{{ f }}' as familia, column_name as coluna
            from information_schema.columns
            where table_schema = '{{ rel.schema }}' and table_name = '{{ rel.identifier }}'
            {{ 'union all' if not loop.last }}
        {% endfor %}
    )

select
    coalesce(e.familia, b.familia) as familia,
    coalesce(e.coluna, b.coluna) as coluna,
    e.coluna is null as so_na_bronze,
    b.coluna is null as so_no_log
from esperadas as e
full outer join na_bronze as b on e.familia = b.familia and e.coluna = b.coluna
where (e.coluna is null or b.coluna is null)
    and coalesce(e.coluna, b.coluna) not in (
        'familia', 'saida', 'source_file', '_linha_origem', '_source_file', '_source_hash'
    )
