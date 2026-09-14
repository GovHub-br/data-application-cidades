{{ config(enabled=false) }}

-- DESABILITADO nesta arquitetura: o contra-lado da reconciliacao,
-- `prata_historico_snh_entregas_mes`, e do dominio reloginho
-- (indicadores_mcmv_dbt), que nao foi trazido para este repositorio. Sem ele o
-- `ref()` abaixo nao resolve e o projeto inteiro falha no parse. Reativar
-- (remover este config) quando o reloginho entrar na arquitetura nova.
-- A espinha prata_historico_entrega_apf e a silver do reloginho
-- prata_historico_snh_entregas_mes leem os MESMOS bronzes de evento e aplicam
-- a MESMA dedup por hash de conteudo. Os totais de UH entregues devem bater.
-- Falha se a diferenca absoluta passar de 0.
--
-- Change: enriquecer-datas-acompanhamento-historico (B), tarefa 5.1.
with
    espinha as (
        select sum(uh_entregues_acumulada) as uh
        from {{ ref('prata_historico_entrega_apf') }}
    ),
    reloginho as (
        select sum(uh_entregues_evento_mes) as uh
        from {{ ref('prata_historico_snh_entregas_mes') }}
    )
select
    espinha.uh as uh_espinha,
    reloginho.uh as uh_reloginho,
    espinha.uh - reloginho.uh as diff
from espinha, reloginho
where espinha.uh is distinct from reloginho.uh
