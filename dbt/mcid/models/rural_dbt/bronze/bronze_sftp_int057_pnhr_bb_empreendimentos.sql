{{ config(materialized="table") }}

-- Bronze: Historico de empreendimentos PNHR no Banco do Brasil (integracao INT057). Nao traz nu_apf: o APF sai do numero do contrato. Tambem parado em 20241129.
-- Fonte: SFTP — sftp/fabrica/GEFUS/ANTERIORES/
{% set padrao = "s3://data-lake-mcid/staging/**/*INT057_MinisterioCidades_PNHR_BB_EMPREENDIMENTOS_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao, excluir=["VALIDACAO", "SUBSTITUIDO"]) }}
