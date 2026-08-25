select
    c.apf
from {{ ref("fds_cadastro_pj") }} c
left join {{ ref("dim_empreendimento") }} d on c.apf = d.apf
where d.apf is null
