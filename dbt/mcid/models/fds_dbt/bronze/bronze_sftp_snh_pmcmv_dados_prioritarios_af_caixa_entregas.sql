{{ config(materialized="table") }}

-- Bronze: Entregas reportadas pela CAIXA, todas as modalidades — a prata filtra FDS.
-- Fonte: SFTP — sftp/fabrica/GEFUS/
{% set padrao = "s3://data-lake-mcid/staging/**/*_SNH_PMCMV_DADOS_PRIORITARIOS_AF_CAIXA_ENTREGAS.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
