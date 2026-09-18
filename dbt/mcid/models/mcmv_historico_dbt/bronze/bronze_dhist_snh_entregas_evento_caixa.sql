{{ config(materialized="table") }}

-- BRONZE do reloginho (grupo A) — entregas por EVENTO (fluxo), agente CAIXA.
--
-- Uma das 2 tabelas em que a bronze unica de entregas foi separada (D5 da
-- change pipeline-bronze-historica-destino-trocavel): uma por agente. Os dois
-- lotes nomeiam as colunas de forma diferente (CAIXA: dt_entrega /
-- qt_uh_entregues; BB: dt_ass_doc / numero_de_unidades_entregues) — o corpo
-- resolve isso com coalesce_present_cols sobre as colunas reais do glob, e a
-- uniao dos dois agentes acontece na silver.
--
-- Complementa a serie mensal SNH (bronze_dhist_empreendimento_snh_*,
-- que traz o ACUMULADO). Enquanto aquela responde "quantas UH entregues ate o
-- mes X", esta responde "quantas UH foram entregues NO mes X" (fluxo),
-- necessario para o ritmo_recente e para o caminho alternativo do total de
-- entregas (decisao #5 da #130).
--
-- Glob na staging: staging/dados_historicos/*snh_pmcmv_dados_prioritarios_af_caixa_entregas.parquet
-- Responsabilidade desta camada: 1 linha por linha de origem, sem regra de
-- negocio; dt_referencia do nome do arquivo; helpers harmonizados (dt_evento,
-- qt_uh_entregues_evento) e hash_linha para a silver.
--
-- Corpo e glob vem do mapa de familias (macros/historico/familias.sql).
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`,
-- Postgres atachado em `prod_duckdb`.
{{ bronze_snh_entregas('CAIXA') }}
