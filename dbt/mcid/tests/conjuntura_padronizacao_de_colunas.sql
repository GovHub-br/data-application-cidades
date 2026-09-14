-- Impõe a convenção de nomes de coluna dos models do conjuntura.
--
-- Itens 1 e 2 do checklist de qualidade. A convenção existia na prática mas
-- não estava escrita nem verificada, então nada impedia uma coluna
-- `Valor Total` ou `unnamed_115` de entrar na silver e vazar pro dashboard.
--
-- Regras:
--   a) snake_case: minúsculas, dígitos e `_`, começando por letra
--   b) sem acento ou cedilha (quebra referência no Superset e em SQL solto)
--   c) sem `unnamed_*` — é lixo de importação de planilha, não dado
--
-- A bronze fica DE FORA de propósito: ela é espelho fiel da origem, e a
-- origem não segue a nossa convenção. É exatamente na prata que a
-- padronização tem que acontecer.
--
-- O recorte vem do grafo do dbt, não do schema: `prata` e `ouro` são
-- compartilhados com far, fds e rural, então "todo o schema" varreria os
-- quatro domínios.

{% set relacoes = relacoes_do_produto(
    'conjuntura_dbt',
    camadas=['prata', 'ouro'],
) %}

select
    table_schema as schema_dado,
    table_name   as model,
    column_name  as coluna,
    case
        when column_name like 'unnamed%'                   then 'lixo de importacao de planilha'
        when column_name ~ '[^a-z0-9_]'                    then 'fora do snake_case (maiuscula, acento ou simbolo)'
        when column_name !~ '^[a-z]'                       then 'nao comeca com letra'
    end as problema
from information_schema.columns
{% if relacoes %}
where (table_schema, table_name) in (
    {% for r in relacoes -%}
    ('{{ r["schema"] }}', '{{ r["tabela"] }}'){{ ", " if not loop.last }}
    {%- endfor %}
)
{% else %}
where false
{% endif %}
  and (
        column_name like 'unnamed%'
     or column_name ~ '[^a-z0-9_]'
     or column_name !~ '^[a-z]'
  )
