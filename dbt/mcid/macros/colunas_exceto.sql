{#
    Lista as colunas de uma relação, menos as que forem excluídas.

    Serve para o caso "quero `select *` mas sem estas aqui" — que o Postgres
    não tem nativamente e que, sem macro, obriga a enumerar dezenas de colunas
    na mão (e a manter essa lista sincronizada para sempre).

    Uso:
        select {{ colunas_exceto('manual_conjuntura', 'dados_trimestrais',
                                 ['unnamed_115', 'unnamed_116']) }}
        from conjuntura.bnz_manual_dados_trimestrais
#}
{% macro colunas_exceto(schema_dado, tabela, excluir=[]) %}
    {%- if execute -%}
        {%- set consulta -%}
            select column_name
            from information_schema.columns
            where table_schema = '{{ schema_dado }}' and table_name = '{{ tabela }}'
            order by ordinal_position
        {%- endset -%}
        {%- set todas = run_query(consulta).columns[0].values() -%}
        {%- set mantidas = todas | reject('in', excluir) | list -%}
        {%- if not mantidas -%}
            {{ exceptions.raise_compiler_error(
                "colunas_exceto: nenhuma coluna sobrou em " ~ schema_dado ~ "." ~ tabela) }}
        {%- endif -%}
        {%- for c in mantidas %}"{{ c }}"{{ ", " if not loop.last }}{% endfor -%}
    {%- else -%}
        *
    {%- endif -%}
{% endmacro %}
