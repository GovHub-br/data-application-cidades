-- FR-007/FR-008: as colunas da silver com nome de dado pessoal só têm valor mascarado.
-- Devolve família, coluna e a contagem de valores com cara de CPF em claro (11 dígitos ou
-- ddd.ddd.ddd-dd); nunca o valor.
{% call duckdb_query() %}
{%- set pii = "(cpf|(^|_)nis(_|$)|(^|_)pis(_|$)|pasep|nome_(benef|mutu|titular|comprador|conjuge|pessoa)|nascimento|telefone|e_?mail)" %}
{%- set partes = [] %}
{%- for f in familias_prata_dhist() %}
    {%- for c in de_para_dhist()[f]['colunas'] if modules.re.search(pii, c.nome) %}
        {%- do partes.append(
            "select '"
            ~ f
            ~ "' as familia, '"
            ~ c.nome
            ~ "' as coluna, count(*) as n from "
            ~ rel_pgduckdb(ref("prata_dhist_" ~ f))
            ~ ' where regexp_matches(trim("'
            ~ c.nome
            ~ "\"), '^(\\d{11}|\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2})$')"
        ) %}
    {%- endfor %}
{%- endfor %}
{% if partes %} select * from ({{ partes | join("\nunion all\n") }}) where n > 0
{% else %} select null as familia, null as coluna, 0 as n where false
{%- endif %}
{% endcall %}
