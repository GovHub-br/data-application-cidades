{{ config(materialized="table") }}

-- BRONZE — série histórica semanal de contratos Reforma Casa Brasil, família
-- `PMCMV_REFORMAS_MCID` do SFTP/GEFUS, cópia fiel.
--
-- Glob na staging: staging/sftp/fabrica/GEFUS/**/PMCMV_REFORMAS_MCID_*.parquet
-- (snapshots semanais, grão contrato PF/FGTS — mesmo desenho de
-- PMCMV_FAIXA3_MCID). Sem dedup nem filtro.
--
-- Contém PII de mutuário (nu_cpf_cnpj_mutuario, no_mutuario,
-- dt_nascimento_mutuario) — retida aqui só para linhagem/auditoria; a prata
-- NÃO expõe essas 3 colunas (D4 da change frentes-restantes-mcmv-historico).
--
-- A fonte já traz uma coluna `dt_referencia` própria que colide de nome com a
-- auditoria do domínio — preservada como `dt_referencia_origem_txt`; a
-- auditoria `dt_referencia` é sempre a data do NOME DO ARQUIVO.
--
-- Fonte complementar `reforma_casa_brasil_contratacao.parquet` (sharepoint,
-- consolidado) entra como bronze separada
-- (bronze_shpt_reforma_casa_brasil_contratacao) mas não é unida a esta —
-- mesmo espírito do D2 (MCMV Cidades): a prata desta frente usa só esta
-- fonte GEFUS.
--
-- Corpo e glob vêm do mapa de famílias (macros/historico/familias.sql).
-- change frentes-restantes-mcmv-historico.
{{ bronze_frente_gefus_semanal('PMCMV_REFORMAS_MCID') }}
