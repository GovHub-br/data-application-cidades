{{ config(materialized='table') }}

-- Bronze do conjuntura contínuo: Faixa 3 Fundo Social (GEFUS).
--
-- ⚠️ NÃO usa `select *`, e isso é deliberado. A origem tem 46 colunas,
-- entre elas `no_mutuario`, `nu_cpf_cnpj_mutuario`, `nu_pis`,
-- `dt_nascimento_mutuario` e `ed_cep_imovel_garantia`. Os valores chegam
-- anonimizados do pipeline (conferido em 2026-08-29: 137.491 de 137.491
-- linhas com nome `***`, zero CPF em claro), mas nem o NOME dessas colunas
-- deve ser materializado em camada persistida — é a mesma regra que levou à
-- minimização das bronzes do GEAVO.
--
-- Projeta os três campos que o indicador usa, e só eles. Se precisar de mais
-- algum, avaliar campo a campo — não voltar para `select *`.
--
-- ATENÇÃO: a remessa é semanal e o nome do arquivo carrega a data. O caminho
-- está em `sources.yml` (`meta.caminho`); atualizar lá, não aqui.

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

select * from {{ fonte_lake('gefus_fundo_social') }}
