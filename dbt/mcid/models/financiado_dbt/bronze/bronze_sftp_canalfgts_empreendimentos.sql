{{ config(materialized="table") }}

-- Bronze: Empreendimentos do Canal FGTS — nome, localidade e unidades financiadas. Uma linha por empreendimento.
-- Fonte: SFTP — sftp/caixa.geavo/GEAVO/ (pacote semanal `MC<aaaammdd>.zip`)
{% set padrao = "s3://data-lake-mcid/staging/**/*__MCidades_AO_1__tab_empreendimentos.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
