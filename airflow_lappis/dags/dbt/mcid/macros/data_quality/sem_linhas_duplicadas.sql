{#
    Teste generico: nenhuma linha inteiramente duplicada no model.

    Aplique no nivel do model, em schema.yml:

        - name: silver_poc_contratos
          tests:
            - sem_linhas_duplicadas

    Cada linha devolvida e uma combinacao de valores que aparece mais de uma
    vez, com a contagem de ocorrencias.

    POR QUE UM TESTE, E NAO UM `select distinct` NO MODEL
    Duplicata inteira quase nunca vem da origem: vem de fan-out de join -- uma
    tabela de dominio com codigo repetido, ou uma tabela filha com varias
    linhas por pai. Um distinct no final esconde isso e o numero volta a
    aparecer errado mais tarde, em uma agregacao. O teste falha e aponta o
    problema onde ele nasceu.

    Quando a duplicacao for legitima e conhecida, a saida nao e o distinct: e
    declarar o grao correto do model e deduplicar de proposito, com
    row_number() na chave, ou agregar.
#}

{% test sem_linhas_duplicadas(model) %}

    {%- set colunas = adapter.get_columns_in_relation(model) -%}
    {%- set nomes = [] -%}
    {%- for coluna in colunas -%}
        {%- do nomes.append(adapter.quote(coluna.name)) -%}
    {%- endfor -%}

    {%- if nomes | length == 0 -%}
        select 1 as sem_colunas where false
    {%- else -%}

    select
        {{ nomes | join(',\n        ') }},
        count(*) as qtd_ocorrencias
    from {{ model }}
    group by
        {{ nomes | join(',\n        ') }}
    having count(*) > 1

    {%- endif -%}

{% endtest %}
