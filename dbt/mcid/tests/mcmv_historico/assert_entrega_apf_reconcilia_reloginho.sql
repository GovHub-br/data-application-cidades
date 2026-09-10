-- A espinha prata_dhist_entrega_apf e a silver do reloginho
-- prata_dhist_snh_entregas_mes leem os MESMOS bronzes de evento e aplicam
-- a MESMA dedup por hash de conteudo. Os totais de UH entregues devem bater.
-- Falha se a diferenca absoluta passar de 0.
--
-- Change: enriquecer-datas-acompanhamento-historico (B), tarefa 5.1.
with
    espinha as (
        select sum(uh_entregues_acumulada) as uh
        from {{ ref('prata_dhist_entrega_apf') }}
    ),
    reloginho as (
        select sum(uh_entregues_evento_mes) as uh
        from {{ ref('prata_dhist_snh_entregas_mes') }}
    )
select
    espinha.uh as uh_espinha,
    reloginho.uh as uh_reloginho,
    espinha.uh - reloginho.uh as diff
from espinha, reloginho
where espinha.uh is distinct from reloginho.uh
