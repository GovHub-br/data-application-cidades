-- Teste singular: o grão da prata_dhist_snh_apf_mes deve ser
-- (agente_financeiro, apf, dt_referencia). Retorna linhas apenas se a
-- deduplicação por APF tiver falhado (duplicidade 2x da origem não neutralizada).

select
    agente_financeiro,
    apf,
    dt_referencia,
    count(*) as n_linhas
from {{ ref("prata_dhist_snh_apf_mes") }}
group by agente_financeiro, apf, dt_referencia
having count(*) > 1
