select
    apf
from {{ ref("silver_fds_dim_empreendimento") }}
group by apf
having count(distinct id_empreendimento || '|' || fase_empreendimento) > 1
