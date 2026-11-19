select id_contrato_linha_financiada, count(*) as quantidade
from {{ ref('prata_linha_financiada_contrato') }}
group by 1
having count(*) <> 1
