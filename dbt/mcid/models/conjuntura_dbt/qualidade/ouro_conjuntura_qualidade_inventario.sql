{{ config(materialized='table') }}

-- Inventário dos models do conjuntura: camada, materialização e volume.
--
-- Item 8 do checklist ("identificar quais seguem full-refresh e quais são
-- incrementais"). Hoje a resposta é simples e vale registrar: **tudo é
-- `table`, ou seja, full-refresh puro** — a exceção é o próprio
-- `ouro_conjuntura_qualidade_schema`, incremental porque o histórico É o dado.
--
-- Isso tem uma consequência boa: como nada é incremental, nenhum layout novo
-- entra "por baixo" — toda execução reconstrói a partir da bronze, então
-- mudança de estrutura na origem aparece de imediato, e não meses depois
-- misturada com dado velho.
--
-- A camada e a materialização vêm do grafo do dbt, não do nome nem do schema:
-- `bronze`/`prata`/`ouro` são compartilhados com far, fds e rural, e a bronze
-- do conjuntura nomeia a origem (`bronze_ibge_sinapi`), não o domínio.
--
-- O grafo diz o que PERTENCE ao produto; o catálogo diz o que já foi
-- MATERIALIZADO. O inventário é a interseção: model declarado mas ainda não
-- construído fica de fora em vez de derrubar a execução — inclusive este, na
-- primeira vez que roda.

{% set relacoes = relacoes_do_produto('conjuntura_dbt') %}

{% if relacoes %}
with declarados (camada, schema_dado, model, materializacao) as (
    values
    {% for r in relacoes -%}
    (
        '{{ r["camada"] }}'::text,
        '{{ r["schema"] }}'::text,
        '{{ r["tabela"] }}'::text,
        {% if r["materializacao"] == 'table' -%}
            'table (full-refresh)'::text
        {%- else -%}
            '{{ r["materializacao"] }}'::text
        {%- endif %}
    ){{ "," if not loop.last }}
    {% endfor %}
)

select
    d.camada,
    d.schema_dado,
    d.model,
    d.materializacao,
    (xpath(
        '/row/cnt/text()',
        query_to_xml(
            format('select count(*) as cnt from %I.%I', d.schema_dado, d.model),
            false, true, ''
        )
    ))[1]::text::bigint as linhas,
    current_timestamp   as medido_em
from declarados as d
join information_schema.tables as t
    on t.table_schema = d.schema_dado
   and t.table_name = d.model
order by d.camada, d.model
{% else %}
select
    null::text as camada, null::text as schema_dado, null::text as model,
    null::text as materializacao, null::bigint as linhas,
    current_timestamp as medido_em
where false
{% endif %}
