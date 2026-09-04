{{ config(materialized="table") }}

-- BRONZE do reloginho MCMV (grupo A) — série mensal SNH.
--
-- Desde a change `separacao-silver-historico-por-frente` (D8) esta tabela é uma
-- VIEW fina sobre a bronze compartilhada
-- `bronze_mcmv_historico_empreendimento_snh`, que já:
-- * empilha todos os snapshots `historico_recente_*` de CAIXA e BB;
-- * exclui os fluxos de entrega por evento (%entrega%);
-- * deriva dt_referencia (nome do arquivo), agente_arquivo, prioridade_reentrega;
-- * carrega source_file, dt_ingest e hash_linha (mesma chave de hash de antes).
--
-- O contrato de saída é idêntico ao anterior — a silver do reloginho
-- (silver_reloginho_snh_apf_mes) e a reconciliação #66 não mudam. Os fluxos de
-- entrega por evento continuam em bronze_reloginho_snh_entregas_evento.
--
-- Target obrigatório: staging_duckdb (gating em dbt_project.yml).
select *
from {{ ref('bronze_mcmv_historico_empreendimento_snh') }}
