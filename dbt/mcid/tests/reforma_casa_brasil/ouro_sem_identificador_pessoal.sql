-- A Gold é destinada a consumo analítico e não pode expor chaves de junção,
-- documentos ou identificadores diretos, mesmo pseudonimizados.
-- Roda depois das ouros que vigia (macros/depende_de.sql); ouro nova do domínio entra aqui.
{{ depende_de([
        'ouro_reforma_casa_brasil_acesso_dash',
        'ouro_reforma_casa_brasil_cobertura_perguntas_dash',
        'ouro_reforma_casa_brasil_implementacao_dash',
        'ouro_reforma_casa_brasil_resultado_dash',
    ]) }}
select table_schema, table_name, column_name
from information_schema.columns
where
    table_schema = 'ouro'
    and table_name like 'ouro_reforma_casa_brasil_%'
    and
    {{ casa_regex(
        'lower(column_name)',
        '(^|_)(cpf|cnpj|nis|pis|nome|apelido|endereco|logradouro|cep|codigo_familiar|id_pessoa|token|hmac)($|_)'
    ) }}
