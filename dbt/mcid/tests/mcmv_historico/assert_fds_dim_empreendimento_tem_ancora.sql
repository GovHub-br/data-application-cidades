select
    id_empreendimento
from {{ ref("prata_fds_historico_dim_empreendimento") }}
group by id_empreendimento
having count(*) filter (where apf_ancora) <> 1
