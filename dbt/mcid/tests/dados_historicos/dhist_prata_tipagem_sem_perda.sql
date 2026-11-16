-- Validação 2 (silver): nenhum valor preenchido na fonte vira nulo (ou é arredondado) na
-- tipagem. Para cada família e coluna tipada do de-para, devolve quantos valores se
-- perderam e um exemplo. Código IBGE: valor da fonte sem correspondente de 7 dígitos (fora
-- o "0", código de ausente).
{%- set partes = [] %}
{%- for f in familias_prata_dhist() %}
    {%- for c in de_para_dhist()[f]['colunas'] %}
        {%- set origem = dhist_origem(c.origens) %}
        {%- if c.tipo == 'ibge' %}
            {%- do partes.append(
                "select '" ~ f ~ "' as familia, '" ~ c.nome ~ "' as coluna, 'sem_ibge_7' as perda, "
                ~ "count(*) as n, min(\"" ~ c.nome ~ "_fonte\") as exemplo from " ~ ref('prata_dhist_' ~ f)
                ~ " where \"" ~ c.nome ~ "_fonte\" is not null and \"" ~ c.nome ~ "\" is null"
                ~ " and not regexp_matches(\"" ~ c.nome ~ "_fonte\", '^0+$')"
            ) %}
        {%- elif c.tipo in ['inteiro', 'ano', 'valor', 'percentual', 'coordenada', 'data', 'ano_mes'] %}
            {%- do partes.append(
                "select '" ~ f ~ "' as familia, '" ~ c.nome ~ "' as coluna, 'nulo_no_cast' as perda, "
                ~ "count(*) as n, min(" ~ origem ~ ") as exemplo from " ~ ref('bronze_dhist_' ~ f) ~ " as b"
                ~ " where " ~ origem ~ " is not null and " ~ dhist_tipo(origem, c.tipo) ~ " is null"
            ) %}
            {%- if c.tipo in ['valor', 'percentual'] %}
                {%- do partes.append(
                    "select '" ~ f ~ "' as familia, '" ~ c.nome ~ "' as coluna, 'arredondado' as perda, "
                    ~ "count(*) as n, min(" ~ origem ~ ") as exemplo from " ~ ref('bronze_dhist_' ~ f) ~ " as b"
                    ~ " where regexp_matches(" ~ origem ~ ", '\\.\\d{' ~ (7 if c.tipo == 'percentual' else 5) ~ ',}$')"
                ) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
{%- endfor %}
select * from (
    {{ partes | join('\nunion all\n') }}
)
where n > 0
