-- O enriquecimento pelo CadÚnico não pode multiplicar contratos.
select id_contrato, count(*) as quantidade
from {{ ref('prata_reforma_casa_brasil_acesso') }}
group by id_contrato
having count(*) <> 1
