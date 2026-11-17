-- Toda coordenada normalizada (`latitude_br` e `longitude_br`) cai dentro do Brasil, ilhas
-- oceânicas incluídas. Devolve família, coluna e a contagem fora da faixa.
{% call duckdb_query() %}
{%- set partes = [] %}
{%- for f in familias_prata_dhist() %}
    {%- for c in de_para_dhist()[f]["colunas"] if c.tipo == "latitude_br" %}
        {%- do partes.append(
            "select '"
            ~ f
            ~ "' as familia, '"
            ~ c.nome
            ~ "' as coluna, count(*) as n from "
            ~ rel_pgduckdb(ref("prata_dhist_" ~ f))
            ~ ' where not ("'
            ~ c.nome
            ~ '" between -34 and 5.5 and "'
            ~ c.par
            ~ '" between -74.2 and -28.8)'
        ) %}
    {%- endfor %}
{%- endfor %}
select * from ({{ partes | join(" union all ") }}) as t where n > 0
{% endcall %}
