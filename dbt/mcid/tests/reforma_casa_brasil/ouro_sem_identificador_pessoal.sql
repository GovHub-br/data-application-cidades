-- A Gold é destinada a consumo analítico e não pode expor chaves de junção,
-- documentos ou identificadores diretos, mesmo pseudonimizados.
select table_schema, table_name, column_name
from information_schema.columns
where table_schema = 'ouro'
  and table_name like 'ouro_reforma_casa_brasil_%'
  and regexp_matches(
      lower(column_name),
      '(^|_)(cpf|cnpj|nis|pis|nome|apelido|endereco|logradouro|cep|codigo_familiar|id_pessoa|token|hmac)($|_)'
  )
