select
    apf
from {{ ref("prata_fds_historico_dim_empreendimento") }}
group by apf
having count(distinct id_empreendimento || '|' || fase_empreendimento) > 1
