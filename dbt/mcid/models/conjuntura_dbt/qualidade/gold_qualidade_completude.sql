{{ config(materialized='table', schema='conjuntura_continuo_mart') }}

-- Perfil de completude de todos os models silver e gold do conjuntura.
-- Uma linha por coluna, com % de preenchimento.
--
-- Responde ao item 7 do checklist de qualidade ("porcentagem de completude
-- das bases"). Colunas de metadado de ingestão (`dt_ingest`, `_source_*`)
-- ficam de fora — completude delas não diz nada sobre o dado.
--
-- Lido junto com `gold_qualidade_schema` (item 5), dá o retrato da saúde das
-- bases sem precisar abrir model por model.
--
-- Cobre apenas silver e gold — a bronze fica fora de propósito: ela espelha a
-- origem e pode conter coluna com identificador de pessoa, que não deve
-- aparecer em camada de consumo. Ver `macros/coluna_sensivel.sql`.

{{ perfil_completude(['conjuntura_continuo_silver', 'conjuntura_continuo_mart']) }}
