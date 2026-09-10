{{ config(materialized="table") }}

-- BRONZE — evolução de obra por empreendimento MCMV, frente FAR
-- (MONIT_MOV_OBRA_FAR_MENSAL_YYYYMM), cópia fiel.
--
-- Empilha os snapshots datados `_MENSAL_` sob
-- `staging/sharepoint/Novo MCMV - */` (glob recursivo; seleção pela frente NO
-- NOME DO ARQUIVO — arquivos FDS/RURAL misfiled sob a pasta FAR não entram
-- aqui). `_LAYOUT_` (dicionário de campos), `_SEMANAL_` e `_DIARIO_` ficam de
-- fora. Janela real: 202512 → 202607 (~820-880 linhas/mês). NÃO há histórico
-- de obra mensal antes de 202512.
--
-- Responsabilidade da camada: uma linha por linha de origem (grão da fonte:
-- 1 linha / nu_apf / arquivo); colunas preservadas como vieram, sem tipagem;
-- dt_referencia do sufixo `_YYYYMM`; auditoria source_file, frente_mcmv,
-- dt_ingest, hash_linha. A dedup por (apf, dt_referencia) e a tipagem/mapa de
-- colunas ficam no braço `obra_mensal` das 3 pratas de frente.
--
-- Corpo e glob vêm do mapa de famílias (macros/historico/familias.sql). Change:
-- enriquecer-quantidades-uh-e-sinais-obra-historico (D4).
{{ bronze_obra_mensal('OBRA_FAR') }}
