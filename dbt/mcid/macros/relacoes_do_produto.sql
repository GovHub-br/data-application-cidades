{#
    Lista as tabelas de um produto no banco, lidas do GRAFO do dbt.

    Por que pelo grafo e não por nome: na arquitetura de três schemas, `bronze`,
    `prata` e `ouro` são COMPARTILHADOS entre far, fds, rural e conjuntura. Não
    dá para dizer "tudo que está no schema `ouro`" — isso varreria os quatro
    domínios. E filtrar por prefixo do nome erra na bronze, onde o nome carrega
    a origem (`bronze_ibge_sinapi`, `bronze_shpt_monit_...`) e não o domínio.

    O grafo sabe exatamente quais models são de qual pasta, então o recorte é o
    próprio projeto se declarando — não uma heurística que envelhece. Foi
    heurística de nome que apodreceu no conjuntura v1: filtros escritos para os
    schemas `*_bronze`/`*_silver` continuaram no lugar depois da unificação,
    deixaram de filtrar, e passaram a varrer a bronze inteira em silêncio.

    `camadas` restringe às camadas informadas (ex.: `['prata', 'ouro']`).
    `excluir_prefixo` tira os models cujo nome começa com o prefixo — usado
    pelos próprios models de qualidade, para não se perfilarem.

    Devolve lista de dicionários: `{schema, tabela, camada, materializacao}`.
#}
{% macro relacoes_do_produto(pasta, camadas=none, excluir_prefixo=none) %}

    {%- set achadas = [] -%}
    {%- if execute -%}
        {%- for no in graph.nodes.values() -%}
            {%- if no.resource_type == 'model' and no.path.startswith(pasta ~ '/') -%}
                {%- set partes = no.path.split('/') -%}
                {%- set camada = partes[1] -%}
                {%- if (camadas is none or camada in camadas)
                       and (excluir_prefixo is none
                            or not no.alias.startswith(excluir_prefixo)) -%}
                    {%- do achadas.append({
                        "schema": no.schema,
                        "tabela": no.alias,
                        "camada": camada,
                        "materializacao": no.config.materialized,
                    }) -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {{ return(achadas | sort(attribute='tabela')) }}

{% endmacro %}
