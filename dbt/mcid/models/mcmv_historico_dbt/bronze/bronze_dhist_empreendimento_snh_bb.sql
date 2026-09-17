{{ config(materialized="table") }}

-- BRONZE — serie mensal SNH de "dados prioritarios" por empreendimento MCMV,
-- agente BB, copia fiel. Fonte mais rica de historia por empreendimento
-- (2024-06+), com a coluna `modalidade` (FAR / Entidades / Rural) que
-- discrimina a frente.
--
-- Uma das 2 tabelas em que a bronze unica da SNH foi separada (D5 da change
-- pipeline-bronze-historica-destino-trocavel): uma tabela por agente. Os
-- schemas dos dois agentes divergem (o BB traz `uhs_contratadas`/
-- `uhs_entregues`; a CAIXA traz `dt_entrega`/`qt_uh_entregues`) — o que o
-- union_by_name da bronze unica escondia. Quem une as duas e a silver, com
-- coalesce_present por relacao.
--
-- Glob na staging: staging/dados_historicos/*ecente_*snh_pmcmv_dados_prioritarios_af_bb*.parquet
-- NAO entram aqui os fluxos de ENTREGA por evento (`*entrega*`) — filtrados
-- no corpo. O reloginho usa o acumulado `uh_entregues` do proprio snapshot
-- (decisao D6 da #130); os fluxos de evento sao fonte das bronzes
-- bronze_dhist_snh_entregas_evento_bb/_caixa.
--
-- Preserva os campos derivados que o reloginho usa: agente_arquivo,
-- prioridade_reentrega, dt_referencia (do NOME DO ARQUIVO), alem de
-- source_file, dt_ingest e hash_linha.
--
-- Corpo e glob vem do mapa de familias (macros/historico/familias.sql).
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`,
-- Postgres atachado em `prod_duckdb`.
{{ bronze_snh_empreendimento('BB') }}
