-- Teste singular: o grão da silver_reloginho_snh_apf_mes deve ser
-- (agente_financeiro, apf, dt_referencia). Retorna linhas apenas se a
-- deduplicação por APF tiver falhado (duplicidade 2x da origem não neutralizada).

select
    agente_financeiro,
    apf,
    dt_referencia,
    count(*) as n_linhas
from {{ ref("silver_reloginho_snh_apf_mes") }}
group by agente_financeiro, apf, dt_referencia
having count(*) > 1
