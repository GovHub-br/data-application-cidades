-- Falha o build se coluna com identificador de pessoa chegar a qualquer
-- camada persistida do contínuo.
--
-- Esta é a garantia que NÃO depende da anonimização a montante ter
-- funcionado: se o mascaramento falhar lá atrás e um `nu_cpf_cgc_mutuario`
-- vazar para a silver, o build para aqui — antes de virar dashboard,
-- documentação ou export.
--
-- A Bronze é uma projeção mínima dos campos usados no banco; portanto, ela
-- também é verificada. A Raw imutável no MinIO não faz parte deste teste.
--
-- Padrões em `macros/coluna_sensivel.sql`.

select
    table_schema as schema_dado,
    table_name   as model,
    column_name  as coluna
from information_schema.columns
where table_schema in (
    'conjuntura',
    'conjuntura',
    'conjuntura'
)
  and {{ expressao_sensivel('column_name') }}
