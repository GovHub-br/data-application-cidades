-- Coerencia valor <-> proveniencia nas silvers historicas por frente:
-- toda linha com dt_entrega_uh preenchida DEVE ter dt_entrega_uh_fonte;
-- toda linha com dt_entrega_uh nula DEVE ter dt_entrega_uh_fonte nula.
--
-- Change: enriquecer-datas-acompanhamento-historico (B), tarefa 5.3.
with
    consolidado as (
        select frente_mcmv, apf, dt_referencia, dt_entrega_uh, dt_entrega_uh_fonte
        from {{ ref('prata_far_historico_empreendimento') }}
        union all
        select frente_mcmv, apf, dt_referencia, dt_entrega_uh, dt_entrega_uh_fonte
        from {{ ref('prata_fds_historico_empreendimento') }}
        union all
        select frente_mcmv, apf, dt_referencia, dt_entrega_uh, dt_entrega_uh_fonte
        from {{ ref('prata_rural_historico_empreendimento') }}
    )
select *
from consolidado
where (dt_entrega_uh is not null) <> (dt_entrega_uh_fonte is not null)
