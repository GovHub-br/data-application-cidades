select
    id_empreendimento,
    apf
from {{ ref("prata_fds_dim_empreendimento") }}
group by id_empreendimento, apf
having count(*) > 1
