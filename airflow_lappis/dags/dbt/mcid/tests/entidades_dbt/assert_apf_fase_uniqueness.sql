select
    apf
from {{ ref("dim_empreendimento") }}
group by apf
having count(distinct id_empreendimento || '|' || fase_empreendimento) > 1
