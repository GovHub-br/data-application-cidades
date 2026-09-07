{{ config(materialized="table") }}

-- BRONZE — evolução de obra por empreendimento MCMV, frente Rural / PNHR
-- (MONIT_MOV_OBRA_RURAL_MENSAL_YYYYMM), cópia fiel.
--
-- Seleção pela frente NO NOME DO ARQUIVO (`MONIT_MOV_OBRA_RURAL_MENSAL_`);
-- alguns arquivos RURAL de 202602 estão misfiled sob `Novo MCMV - FAR/`.
-- Janela real: 202512 → 202607. Schema como o do FDS (`dh_movimento`,
-- `co_situacao_operacao`), mas com `no_detalhe_paralisacao_retomada` e
-- `dt_previsao_conclusao_obra_retomada` / `dt_previsao_entrega_do_empreendimento`.
--
-- Demais responsabilidades e origem do corpo: ver
-- bronze_mcmv_historico_obra_mensal_far.sql. Change:
-- enriquecer-quantidades-uh-e-sinais-obra-historico (D4).
{{ bronze_obra_mensal('OBRA_RURAL') }}
