{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: empregos em total da construção (todas as divisões CNAE), do Novo CAGED.
-- Página 3, seção 4 (Empregos).
--
-- Reescrita em 2026-08-28 pra nova arquitetura: a bronze materializa o
-- parquet de staging e a silver TIPA. O parquet novo é espelho do raw e
-- traz tudo como texto — sem o cast aqui, o gold quebra na hora de fazer
-- conta (`estoque - lag(estoque, 12)` dava "operator does not exist:
-- text - text").

select
    ano::int                       as ano,
    mes::int                       as mes,
    admitidos::numeric             as admitidos,
    desligados::numeric            as desligados,
    saldo::numeric                 as saldo,
    estoque::numeric               as estoque,
    variacao::numeric              as variacao,
    _ingested_at                   as dt_ingest
from {{ ref('bronze_continuo_novo_caged_total') }}
