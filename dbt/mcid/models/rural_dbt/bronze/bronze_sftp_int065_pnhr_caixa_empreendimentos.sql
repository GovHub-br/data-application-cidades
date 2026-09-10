{{ config(materialized="table") }}

-- Bronze: Historico de empreendimentos PNHR na CAIXA (integracao INT065). O feed para em 20241129: o PNHR e programa encerrado.
-- Fonte: SFTP — sftp/fabrica/GEFUS/ANTERIORES/
{% set padrao = "s3://data-lake-mcid/staging/**/*INT065_MinisterioCidades_PNHR_CAIXA_EMPREENDIMENTOS_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao, excluir=["VALIDACAO", "SUBSTITUIDO"]) }}
