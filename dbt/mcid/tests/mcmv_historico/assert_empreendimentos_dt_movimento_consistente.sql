-- Valida que a data de movimento informada pela fonte (dt_movimento) e
-- consistente com o mes de referencia extraido do nome do arquivo
-- (dt_referencia). Esperado: mesmo mes. Falha se houver divergencia.
--
-- fonte_serie = 'obra_mensal' fica FORA: o snapshot MONIT_MOV_OBRA do mes M e
-- gerado no inicio de M+1, entao dt_movimento cai legitimamente no mes seguinte
-- ao de referencia (a convencao de nome-por-mes vale so p/ GEFUS/SNH). Change:
-- consolidar-schemas-historico-reloginho.
--
-- Uniao direta das 3 silvers por frente — o helper consolidado
-- silver_mcmv_historico_empreendimento foi aposentado na convencao
-- 2026-09-04 (cada frente materializa em schema proprio).
with
    consolidado as (
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_far') }}
        union all
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_fds') }}
        union all
        select *
        from {{ ref('silver_mcmv_historico_empreendimento_rural') }}
    )

select
    fonte_tabela,
    count(*) as divergencias
from consolidado
where dt_movimento is not null
  and fonte_serie <> 'obra_mensal'
  and date_trunc('month', dt_movimento) <> date_trunc('month', dt_referencia)
group by fonte_tabela
having count(*) > 0
