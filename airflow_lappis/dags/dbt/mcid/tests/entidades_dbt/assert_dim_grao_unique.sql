select
    id_empreendimento,
    apf
from {{ ref("dim_empreendimento") }}
group by id_empreendimento, apf
having count(*) > 1
