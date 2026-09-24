{{ config(materialized="table") }}

-- Bronze: Contratos FGTS do empreendimento — valor contratado, investimento, linha, modalidade e tomador. Uma linha por contrato.
-- Fonte: SFTP — sftp/caixa.geavo/GEAVO/ (pacote semanal `MC<aaaammdd>.zip`)
{% set padrao = "s3://data-lake-mcid/staging/**/*__MCidades_AO_1__tab_contratos_fgts.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
