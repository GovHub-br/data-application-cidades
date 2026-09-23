{{ config(materialized="table") }}

-- BRONZE — consolidado transacional de contratos Reforma Casa Brasil,
-- `staging/sharepoint/reforma_casa_brasil_contratacao.parquet`, cópia fiel.
--
-- Arquivo único (sem glob por data). 51.512 linhas, schema quase idêntico ao
-- glob semanal GEFUS (bronze_sftp_reforma_casa_brasil) — mesmas colunas de
-- PII de mutuário (nu_cpf_cnpj_mutuario, no_mutuario, dt_nascimento_mutuario),
-- retidas aqui só para linhagem/auditoria.
--
-- Achado da verificação de schema pendente (task 1.1 da change
-- frentes-restantes-mcmv-historico): a fonte entra em escopo como bronze
-- fiel, mas NÃO é unida à prata desta mudança — a prata de Reforma Casa
-- Brasil usa só a fonte GEFUS semanal (mesmo espírito do D2, MCMV Cidades:
-- reconciliar 2 fontes complementares fica como follow-up).
--
-- A fonte já traz uma coluna `dt_referencia` própria (texto DD/MM/YYYY, por
-- linha) que colide de nome com a auditoria do domínio — preservada como
-- `dt_referencia_origem_txt`; a auditoria `dt_referencia` usa `_ingested_at`
-- (não há snapshot datado no nome do arquivo, ao contrário do glob GEFUS).
-- Corpo em macros/historico/corpos_bronze.sql.
-- change frentes-restantes-mcmv-historico.
{{ bronze_flat_shpt('sharepoint/reforma_casa_brasil_contratacao.parquet') }}
