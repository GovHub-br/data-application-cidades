{{ config(materialized="table") }}

-- Bronze: Dados prioritários da CAIXA — TODAS as competências empilhadas, todas as
-- modalidades (a prata filtra FAR). Cada arquivo é um retrato do mês.
-- O padrão começa por `2` para casar só `<aaaamm>_SNH_...` e deixar de fora as
-- remessas `HISTORICO_RECENTE_<aaaamm>_SNH_...`.
-- Fonte: SFTP — sftp/fabrica/GEFUS/ (e GEFUS/ANTERIORES/)
{% set padrao = "s3://data-lake-mcid/staging/**/2*_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
