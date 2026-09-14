{{ config(materialized="table") }}

-- Bronze: Liberacoes de recurso de CAIXA e BB (integracao INT055), de todos os programas — a prata filtra PNHR. Nao decompoe o valor por componente. Chega zipado do SFTP e e expandido para .TXT na entrada.
-- Fonte: SFTP — sftp/fabrica/GEFUS/ANTERIORES/
{% set padrao = "s3://data-lake-mcid/staging/**/*INT055_MinisterioCidades_LIBERACOES_CAIXA_BB_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao, excluir=["VALIDACAO"]) }}
