{{ config(materialized='table') }}

-- Bronze: Carta de Crédito Associativo (CCA) — Canal FGTS / GEAVO. Espelho da origem.
--
-- Caminho declarado em `sources.yml`; o nome do arquivo carrega a data da
-- remessa e muda a cada carga — atualizar lá, não aqui.

select * from {{ fonte_lake('geavo_cca_analitico') }}
