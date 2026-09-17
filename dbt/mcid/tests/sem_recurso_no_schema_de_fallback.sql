-- Falha se algum recurso materializar no schema de fallback do dbt.
--
-- O `schema` do `profiles.yml` é obrigatório e serve de destino para todo
-- recurso que não declara o seu: model sem `+schema` no `dbt_project.yml`,
-- snapshot sem `target_schema`, seed sem `+schema`. Em operação normal nada
-- cai ali, porque todos declaram — então QUALQUER tabela aqui é um esquecimento.
--
-- Por que isso importa: enquanto o fallback apontou para um schema real, o
-- extravio ficava invisível. Foi assim que seis models de uma branch não
-- mesclada materializaram 980 MB em `conjuntura` e sobreviveram um mês sem
-- ninguém notar, e que cópias das UDFs se espalharam por `cidades` e
-- `empreendimento_far`. Com um schema dedicado e vazio, a pergunta "tem algo
-- extraviado?" vira `count(*)` em vez de conferir tabela por tabela.
--
-- O teste lê `target.schema`, e não um nome fixo: se o fallback mudar, ele
-- acompanha. Consequência desejada: apontar o fallback de volta para um schema
-- real faz este teste falhar em massa, que é o aviso certo.

select
    table_schema as schema_de_fallback,
    table_name   as recurso_extraviado
from information_schema.tables
where table_schema = '{{ target.schema }}'
