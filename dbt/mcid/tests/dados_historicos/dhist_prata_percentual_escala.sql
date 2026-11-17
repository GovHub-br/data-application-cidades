-- FR-006: todo percentual de escala por arquivo (`percentual_auto`) sai em [0, 100].
-- Devolve família, coluna e a contagem fora da faixa.
{% call duckdb_query() %}
{%- set partes = [] %}
{%- for f in familias_prata_dhist() %}
    {%- for c in de_para_dhist()[f]['colunas'] if c.tipo == 'percentual_auto' %}
        {%- do partes.append(
            "select '"
            ~ f
            ~ "' as familia, '"
            ~ c.nome
            ~ "' as coluna, count(*) as n from "
            ~ rel_pgduckdb(ref("prata_dhist_" ~ f))
            ~ ' where "'
            ~ c.nome
            ~ '" < 0 or "'
            ~ c.nome
            ~ '" > 100'
        ) %}
    {%- endfor %}
{%- endfor %}
{#- PNHR de 2015-09: valor da fonte sem vizinho em 2015-10 fica nulo na silver (research R2). #}
{%- if "pnhr_bb" in familias_prata_dhist() %}
    {%- do partes.append(
        "select 'pnhr_bb' as familia, 'percentual_execucao_fisica sem vizinho' as coluna, count(*) as n from "
        ~ rel_pgduckdb(ref("prata_dhist_pnhr_bb"))
        ~ " as p join "
        ~ rel_pgduckdb(ref("bronze_dhist_pnhr_bb"))
        ~ " as b using (saida, _linha_origem)"
        ~ " where p.competencia = date '2015-09-01' and nullif(trim(b.pc_acm_obra_rlzd), '') is not null"
        ~ " and p.percentual_execucao_fisica is null"
    ) %}
{%- endif %}
{% if partes %} select * from ({{ partes | join("\nunion all\n") }}) where n > 0
{% else %} select null as familia, null as coluna, 0 as n where false
{%- endif %}
{% endcall %}
