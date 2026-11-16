-- Validação 2 (silver): toda situação de obra preenchida tem situação canônica (pelos seeds
-- dominio_status e dominio_status_extra). Devolve o valor sem correspondência.
{%- set partes = [] %}
{%- for f in familias_prata_dhist() %}
    {%- for c in de_para_dhist()[f]['colunas'] if c.tipo == 'situacao' %}
        {%- do partes.append(
            "select '" ~ f ~ "' as familia, \"" ~ c.nome ~ "\" as valor, count(*) as n from "
            ~ ref('prata_dhist_' ~ f) ~ " where \"" ~ c.nome ~ "\" is not null and \""
            ~ c.nome ~ "_canonica\" is null group by 1, 2"
        ) %}
    {%- endfor %}
{%- endfor %}
{{ partes | join('\nunion all\n') }}
