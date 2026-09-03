select
    id_empreendimento
from {{ ref("silver_fds_dim_empreendimento") }}
group by id_empreendimento
having count(*) filter (where apf_ancora) <> 1
