-- Dimensões sensíveis só podem ser publicadas em células com ao menos dez
-- contratos, mitigando reidentificação por combinações raras.
select 'acesso' as tabela, quantidade_contratos
from {{ ref('ouro_reforma_casa_brasil_acesso_dash') }}
where quantidade_contratos < 10

union all

select 'implementacao' as tabela, quantidade_contratos
from {{ ref('ouro_reforma_casa_brasil_implementacao_dash') }}
where quantidade_contratos < 10

union all

select 'resultado' as tabela, quantidade_contratos
from {{ ref('ouro_reforma_casa_brasil_resultado_dash') }}
where quantidade_contratos < 10
