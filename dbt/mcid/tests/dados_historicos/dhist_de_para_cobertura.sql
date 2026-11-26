-- Validação 2 (silver): toda coluna do cleansed de uma família com silver está no de-para,
-- como origem de alguma coluna ou em `descartar`. Coluna nova num layout que ninguém
-- mapeou aparece aqui, em vez de sumir calada na silver.
{% call duckdb_query() %}
with

    mapeadas(familia, coluna) as (
        values
            {%- set linhas = [] %}
            {%- for f in familias_prata_dhist() %}
                {%- set dp = de_para_dhist()[f] %}
                {%- for c in dp['colunas'] %}
                    {% for o in c.origens %}
                        {% do linhas.append("('" ~ f ~ "', '" ~ o ~ "')") %}
                    {% endfor %}
                {% endfor %}
                {%- for o in dp['descartar'] %}
                    {% do linhas.append("('" ~ f ~ "', '" ~ o ~ "')") %}
                {% endfor %}
            {%- endfor %}
            {{ linhas | join(',\n        ') }}
    ),

    no_log as (
        select distinct familia, unnest(colunas) as coluna
        from {{ rel_pgduckdb(ref('bronze_dhist_cleansed_log')) }}
        where status = 'tratado' and familia in ('{{ familias_prata_dhist() | join("', '") }}')
    )

select l.familia, l.coluna
from no_log as l
left join mapeadas as m on l.familia = m.familia and l.coluna = m.coluna
where m.coluna is null
{% endcall %}
