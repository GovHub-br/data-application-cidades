{#
    Escolhe, entre várias fontes que medem a MESMA coisa, o valor da fonte com a data de
    referência mais recente.

    Um `coalesce` com ordem de fonte fixa responde "quem é o primeiro que não é nulo", e
    a pergunta certa é "quem mediu por último". As duas coincidem só por sorte: o
    snapshot do SNH tem a melhor cobertura e costuma vir na frente, mas pode estar meses
    atrás dos feeds mensais da CAIXA e do MONIT.

    Fonte sem data de referência (o cadastro PJ, por exemplo) entra com data nula e o
    `nulls last` a deixa por último: ela é usada só quando é a única que tem valor. É o
    comportamento desejado — melhor um número sem data do que nenhum número.

    Empate de data é resolvido pela ordem em que os candidatos foram declarados, para o
    resultado não depender do plano de execução.

    Uso:
        {{ valor_mais_recente([
            ('s.percentual_execucao_fisica', 's.dt_referencia',  'prioritarios_snh'),
            ('cx.percentual_execucao_fisica', 'cx.dt_movimento', 'prioritarios_caixa'),
        ]) }} as percentual_execucao_fisica,

        {{ valor_mais_recente([...], retornar='fonte') }} as fonte_execucao_fisica,
        {{ valor_mais_recente([...], retornar='data')  }} as dt_referencia_execucao_fisica,

    `retornar='fonte'` e `retornar='data'` devolvem a procedência do MESMO valor — é o que
    permite o dashboard dizer "100% (CAIXA, ref. 31/07/2026)" em vez de só "100%".
#}

{% macro valor_mais_recente(candidatos, tipo="numeric", retornar="valor") %}
    {%- set valores = [] -%}
    {%- set datas = [] -%}
    {%- set fontes = [] -%}
    {%- set ordens = [] -%}
    {%- for c in candidatos -%}
        {%- do valores.append(c[0]) -%}
        {%- do datas.append(c[1]) -%}
        {%- do fontes.append("'" ~ c[2] ~ "'") -%}
        {%- do ordens.append(loop.index) -%}
    {%- endfor -%}
    {%- if retornar == "valor" -%}
        {%- set devolve = "c.valor" -%}
    {%- elif retornar == "fonte" -%}
        {%- set devolve = "c.fonte" -%}
    {%- elif retornar == "data" -%}
        {%- set devolve = "c.dt" -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "valor_mais_recente: retornar deve ser 'valor', 'fonte' ou 'data', não '"
            ~ retornar ~ "'"
        ) }}
    {%- endif -%}
    (
        select {{ devolve }}
        from unnest(
            array[{{ valores | join(", ") }}]::{{ tipo }}[],
            array[{{ datas | join(", ") }}]::date[],
            array[{{ fontes | join(", ") }}]::text[],
            array[{{ ordens | join(", ") }}]::int[]
        ) as c(valor, dt, fonte, ord)
        where c.valor is not null
        order by c.dt desc nulls last, c.ord
        limit 1
    )
{% endmacro %}
