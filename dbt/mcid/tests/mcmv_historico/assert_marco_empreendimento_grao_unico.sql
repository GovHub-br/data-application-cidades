-- ouro_dhist_marco_empreendimento: 1 linha por (frente_mcmv, id_empreendimento).
-- Falha se algum empreendimento aparecer mais de uma vez.
--
-- Change: enriquecer-datas-acompanhamento-historico (D), tarefa 5.4.
select
    frente_mcmv,
    id_empreendimento,
    count(*) as n
from {{ ref('ouro_dhist_marco_empreendimento') }}
group by frente_mcmv, id_empreendimento
having count(*) > 1
