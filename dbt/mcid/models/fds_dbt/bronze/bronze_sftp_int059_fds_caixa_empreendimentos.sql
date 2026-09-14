{{ config(materialized="table") }}

-- Bronze: INT059 — empreendimentos FDS reportados pela CAIXA.
-- Fonte: SFTP — sftp/fabrica/GEFUS/
{% set padrao = "s3://data-lake-mcid/staging/**/*INT059_MinisterioCidades_FDS_CAIXA_EMPREENDIMENTOS_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where
    cast(r['filename'] as varchar)
    = {{ arquivo_mais_recente(padrao, excluir=["VALIDACAO"]) }}
