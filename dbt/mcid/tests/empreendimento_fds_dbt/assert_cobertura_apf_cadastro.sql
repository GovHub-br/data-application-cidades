select
    c.apf
from {{ ref("bronze_fds_cadastro_pj") }} c
left join {{ ref("silver_atual_dim_empreendimento") }} d on c.apf = d.apf
where d.apf is null
