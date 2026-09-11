{{ config(materialized="table") }}

-- BRONZE — evolução de obra por empreendimento MCMV, frente FDS / Entidades
-- (MONIT_MOV_OBRA_FDS_MENSAL_YYYYMM), cópia fiel.
--
-- Seleção pela frente NO NOME DO ARQUIVO (`MONIT_MOV_OBRA_FDS_MENSAL_`): os
-- arquivos FDS de 202602+ estão fisicamente sob `Novo MCMV - FAR/` (misfiled)
-- e mesmo assim entram aqui. Janela real: 202512 → 202607. Schema DIVERGE do
-- FAR (`dh_movimento`, `co_situacao_operacao` + `co_andamento_operacao`,
-- `detalhe_paralisacao_retomada`, `dt_legalizacao_reg`, `dt_prev_entrega_emprend`).
--
-- Demais responsabilidades e origem do corpo: ver
-- bronze_shpt_obra_mensal_far.sql. Change:
-- enriquecer-quantidades-uh-e-sinais-obra-historico (D4).
{{ bronze_obra_mensal('OBRA_FDS') }}
