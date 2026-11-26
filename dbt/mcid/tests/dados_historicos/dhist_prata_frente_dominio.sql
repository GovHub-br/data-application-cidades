-- Frente do MCMV na silver histórica: todo valor de produto, modalidade ou subprograma
-- preenchido tem tradução no seed dados_historicos/dominio_frente (inclusive os que ficam
-- fora da habitação, que estão no seed sem frente). Devolve família, coluna e o valor sem
-- tradução. Também confere que a frente só tem os valores do vocabulário, que cada código
-- do BB tem uma tradução só e que a frente por chave tem uma linha por chave.
{%- set partes = [] %}
{%- for f, fr in frente_dhist().items() %}
    {%- if fr.get("coluna") %}
        {%- do partes.append(
            "select '" ~ f ~ "' as familia, '" ~ fr["coluna"] ~ "' as coluna, \""
            ~ fr["coluna"] ~ "\"::text as valor, count(*) as n from "
            ~ rel_pgduckdb(ref("prata_dhist_" ~ f))
            ~ " where nullif(trim(\"" ~ fr["coluna"] ~ "\"::text), '') is not null"
            ~ " and strip_accents(lower(trim(\"" ~ fr["coluna"] ~ "\"::text))) not in ("
            ~ "select strip_accents(lower(trim(valor_bruto))) from "
            ~ rel_pgduckdb(ref("dominio_frente")) ~ ") group by 1, 2, 3"
        ) %}
    {%- endif %}
    {%- do partes.append(
        "select '" ~ f ~ "', 'frente_mcmv', frente_mcmv, count(*) from "
        ~ rel_pgduckdb(ref("prata_dhist_" ~ f))
        ~ " where frente_mcmv not in ('FAR', 'Entidades', 'Rural', 'SUB50', 'Financiada')"
        ~ " group by 1, 2, 3"
    ) %}
{%- endfor %}
{%- do partes.append(
    "select 'dominio_frente_codigo', coluna, codigo, count(*) from "
    ~ rel_pgduckdb(ref("dominio_frente_codigo")) ~ " group by 1, 2, 3 having count(*) > 1"
) %}
{%- do partes.append(
    "select 'prata_dhist_frente_por_chave', tipo_chave, chave, count(*) from "
    ~ rel_pgduckdb(ref("prata_dhist_frente_por_chave")) ~ " group by 1, 2, 3 having count(*) > 1"
) %}
{% call duckdb_query() %} {{ partes | join("\nunion all\n") }} {% endcall %}
