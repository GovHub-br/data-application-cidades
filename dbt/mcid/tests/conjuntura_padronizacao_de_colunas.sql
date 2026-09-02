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
-- origem não segue a nossa convenção. É exatamente na silver que a
-- padronização tem que acontecer.

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
where table_schema in ('conjuntura_silver', 'conjuntura_mart')
  and (
        column_name like 'unnamed%'
     or column_name ~ '[^a-z0-9_]'
     or column_name !~ '^[a-z]'
  )
