{{ config(materialized="table") }}

-- BRONZE — série histórica semanal de contratos MCMV Classe Média (Faixa 3),
-- família `PMCMV_FAIXA3_MCID` do SFTP/GEFUS, cópia fiel.
--
-- Glob na staging: staging/sftp/fabrica/GEFUS/**/PMCMV_FAIXA3_MCID_*.parquet
-- (snapshots semanais 2025-07+, grão contrato PF/FGTS). Sem dedup nem filtro;
-- colunas cruas preservadas como vieram, sem tipagem (a tipagem/dedup/
-- mascaramento de PII ficam na prata prata_hist_classe_media_contrato).
--
-- Contém PII de mutuário (nu_cpf_cnpj_mutuario, no_mutuario,
-- dt_nascimento_mutuario) — retida aqui só para linhagem/auditoria; a prata
-- NÃO expõe essas 3 colunas (D4 da change frentes-restantes-mcmv-historico).
--
-- A fonte já traz uma coluna `dt_referencia` própria (texto DD/MM/YYYY, por
-- linha) que colide de nome com a auditoria do domínio — preservada como
-- `dt_referencia_origem_txt`; a auditoria `dt_referencia` é sempre a data do
-- NOME DO ARQUIVO (sufixo `_YYYY_MM_DD`).
--
-- Corpo e glob vêm do mapa de famílias (macros/historico/familias.sql).
-- change frentes-restantes-mcmv-historico.
{{ bronze_frente_gefus_semanal('PMCMV_FAIXA3_MCID') }}
