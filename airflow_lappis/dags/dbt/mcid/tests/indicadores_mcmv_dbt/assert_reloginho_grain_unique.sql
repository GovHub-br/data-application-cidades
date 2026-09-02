-- Teste singular: o grao de indicadores_reloginho deve ser (agente_financeiro,
-- dt_referencia). Retorna linhas apenas se houver duplicidade de grao.

select
    agente_financeiro,
    dt_referencia,
    count(*) as n_linhas
from {{ ref("indicadores_reloginho") }}
group by agente_financeiro, dt_referencia
having count(*) > 1
