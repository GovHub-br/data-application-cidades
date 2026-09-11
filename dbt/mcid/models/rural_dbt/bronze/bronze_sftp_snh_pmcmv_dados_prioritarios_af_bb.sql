{{ config(materialized="table") }}

-- Bronze: Dados prioritarios reportados pelo Banco do Brasil, todas as modalidades — a prata filtra RURAL. Mesmo layout do arquivo da CAIXA; o BB envia com um mes de atraso.
-- Fonte: SFTP — sftp/fabrica/GEFUS/
{% set padrao = "s3://data-lake-mcid/staging/**/*_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
