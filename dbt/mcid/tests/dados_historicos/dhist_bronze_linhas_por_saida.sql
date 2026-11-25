-- Validação 1 (bronze): cada saída tratada do cleansed está inteira na bronze da sua
-- família, e nada além disso (versões substituídas e painéis excluídos não entram).
-- Devolve as saídas em que a contagem não bate.
{% call duckdb_query() %}
with

    bronze as (
        {% for f in familias_dhist() %}
            select '{{ f }}' as familia, saida, count(*) as linhas
            from {{ rel_pgduckdb(ref('bronze_dhist_' ~ f)) }}
            group by saida {{ 'union all' if not loop.last }}
        {% endfor %}
    ),

    log as (
        select familia, saida, linhas_reais
        from {{ rel_pgduckdb(ref('bronze_dhist_cleansed_log')) }}
        where status = 'tratado'
    )

select
    coalesce(b.familia, l.familia) as familia,
    coalesce(b.saida, l.saida) as saida,
    b.linhas as linhas_bronze,
    l.linhas_reais as linhas_log
from bronze as b
full outer join log as l on b.familia = l.familia and b.saida = l.saida
where b.linhas is distinct from l.linhas_reais
{% endcall %}
