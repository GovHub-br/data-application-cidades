{{ config(materialized='table') }}

-- Bronze é ESPELHO da origem: `select *`, sem projeção e sem transformação.
-- Toda transformação — inclusive descartar coluna — acontece de bronze para
-- silver, já dentro do banco. Projetar coluna aqui é transformação disfarçada
-- e quebra o contrato da camada.
--
-- A proteção de dado pessoal NÃO depende desta camada:
--   - os valores já chegam anonimizados do pipeline a montante (conferido em
--     2026-08-30: nome e CEP como `***`, CPF em hash);
--   - os NOMES de coluna são mascarados na documentação por
--     `sanitizar_artefatos_dbt()` (ver `gerar_doc_pipeline.py`);
--   - `tests/conjuntura_sem_dado_sensivel.sql` impede que cheguem a
--     silver/gold, que são as camadas de consumo.

select * from {{ fonte_lake('geavo_fgts_pj') }}
