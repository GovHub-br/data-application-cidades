{{ config(materialized='table') }}

-- Prata do conjuntura: empregos em serviços especializados (CNAE 43), do Novo CAGED.
-- Página 3, seção 4 (Empregos).
--
-- Reescrita em 2026-08-28 pra nova arquitetura: a bronze materializa o
-- parquet de staging e a prata TIPA. O parquet novo é espelho do raw e
-- traz tudo como texto — sem o cast aqui, o ouro quebra na hora de fazer
-- conta (`estoque - lag(estoque, 12)` dava "operator does not exist:
-- text - text").
--
-- `dt_ingest` sai de `_ingested_at`, a coluna de linhagem que a ingestão
-- nova grava em toda staging (a DAG não escreve mais `dt_ingest`).
-- A bronze guarda todas as cargas; cada carga traz a série inteira, então fica a última.
select
    ano::int as ano,
    mes::int as mes,
    admitidos::numeric as admitidos,
    desligados::numeric as desligados,
    saldo::numeric as saldo,
    estoque::numeric as estoque,
    variacao::numeric as variacao,
    _ingested_at as dt_ingest
from {{ ref('bronze_novo_caged_servicos_especializados') }}
where {{ ultima_carga(ref('bronze_novo_caged_servicos_especializados')) }}
