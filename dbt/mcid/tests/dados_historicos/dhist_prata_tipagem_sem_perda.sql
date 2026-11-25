-- Validação 2 (silver): nenhum valor preenchido na fonte vira nulo (ou é arredondado) na
-- tipagem. Para cada família e coluna tipada do de-para, devolve quantos valores se
-- perderam e um exemplo. Código IBGE: código de 6 ou 7 dígitos na fonte sem correspondente de
-- 7 dígitos (o "0" do SNH é ausente e o código malformado, como o 49650 da entrega BB PJ de
-- 2015-01, é erro da fonte: nenhum dos dois é perda). Data 1899-12-30 é o zero do Excel, data
-- vazia na fonte: também não é perda.
{% call duckdb_query() %}
{%- set partes = [] %}
{%- for f in familias_prata_dhist() %}
    {%- for c in de_para_dhist()[f]["colunas"] %}
        {%- set origem = dhist_origem(c.origens) %}
        {%- if c.tipo == "ibge" %}
            {%- do partes.append(
                "select '"
                ~ f
                ~ "' as familia, '"
                ~ c.nome
                ~ "' as coluna, 'sem_ibge_7' as perda, "
                ~ 'count(*) as n, min("'
                ~ c.nome
                ~ '_fonte") as exemplo from '
                ~ rel_pgduckdb(ref("prata_dhist_" ~ f))
                ~ ' where "'
                ~ c.nome
                ~ '_fonte" is not null and "'
                ~ c.nome
                ~ '" is null'
                ~ ' and regexp_matches("'
                ~ c.nome
                ~ "_fonte\", '^\\d{6,7}$')"
            ) %}
        {%- elif c.tipo in [
            "inteiro",
            "ano",
            "valor",
            "percentual",
            "percentual_auto",
            "coordenada",
            "latitude_br",
            "longitude_br",
            "valor_calculado",
            "data",
            "data_excel",
            "ano_mes",
        ] %}
            {%- do partes.append(
                "select '"
                ~ f
                ~ "' as familia, '"
                ~ c.nome
                ~ "' as coluna, 'nulo_no_cast' as perda, "
                ~ "count(*) as n, min("
                ~ origem
                ~ ") as exemplo from "
                ~ rel_pgduckdb(ref("bronze_dhist_" ~ f))
                ~ " as b"
                ~ " where "
                ~ origem
                ~ " is not null and "
                ~ dhist_tipo(origem, c.tipo)
                ~ " is null"
                ~ (" and not starts_with(" ~ origem ~ ", '1899-12-30')" if c.tipo in ["data", "data_excel"] else "")
            ) %}
            {%- if c.tipo in ["valor", "percentual", "percentual_auto"] %}
                {%- do partes.append(
                    "select '"
                    ~ f
                    ~ "' as familia, '"
                    ~ c.nome
                    ~ "' as coluna, 'arredondado' as perda, "
                    ~ "count(*) as n, min("
                    ~ origem
                    ~ ") as exemplo from "
                    ~ rel_pgduckdb(ref("bronze_dhist_" ~ f))
                    ~ " as b"
                    ~ " where regexp_matches("
                    ~ origem
                    ~ ", '\\.\\d{"
                    ~ (5 if c.tipo == "valor" else 7)
                    ~ ",}$')"
                ) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
{%- endfor %}
select *
from ({{ partes | join("\nunion all\n") }})
where n > 0
{% endcall %}
