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
-- `dt_ingest` e não `_ingested_at`: a nossa DAG grava `dt_ingest`, e as
-- colunas `_source_file/_ingested_at/_source_hash` só aparecem quando outro
-- processo reescreve o parquet. `dt_ingest` existe nas duas formas, então é
-- a única que sobrevive a qualquer dos dois escritores.
--
-- Ingestão nova (plugins/ingestion, 10/2026): um arquivo por mês na staging
-- (`<AAAA-MM>.parquet`), com as medidas do Power BI já decodificadas do DSR e
-- com o nome que o painel dá. Ano e mês saem do nome do arquivo. Mês ainda não
-- publicado vem com as medidas nulas e fica de fora, como na ingestão antiga.

select
    regexp_replace(filename, '^.*/([0-9]{4})-([0-9]{2})\.parquet$', '\1')::int as ano,
    regexp_replace(filename, '^.*/([0-9]{4})-([0-9]{2})\.parquet$', '\2')::int as mes,
    "Admitidos"::numeric           as admitidos,
    "Desligados"::numeric          as desligados,
    "Saldo"::numeric               as saldo,
    "Estoque"::numeric             as estoque,
    "Variacao"::numeric            as variacao,
    {{ lake_dt_ingest() }}     as dt_ingest
from {{ ref('bronze_novo_caged_servicos_especializados') }}
where "Admitidos" is not null
