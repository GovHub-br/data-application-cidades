select
    id_empreendimento
from {{ ref("dim_empreendimento") }}
group by id_empreendimento
having count(*) filter (where apf_ancora) <> 1
