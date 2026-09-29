-- As Golds da Linha Financiada não podem expor identificadores pessoais.
select table_schema, table_name, column_name
from information_schema.columns
where table_schema = 'ouro'
  and table_name like 'ouro_linha_financiada_%'
  and regexp_matches(
      lower(column_name),
      '(^|_)(cpf|cnpj|nis|pis|nome_pessoa|nome_beneficiario|endereco|logradouro|cep|data_nascimento)($|_)'
  )
