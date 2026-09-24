{{ config(materialized="table") }}

-- Bronze: MCMV Cidades: contratos PF vinculados a emenda parlamentar, com o valor da contrapartida de parceria.
-- Fonte: SFTP — sftp/caixa.geavo/GEAVO/ (pacote semanal `MC<aaaammdd>.zip`)
{% set padrao = "s3://data-lake-mcid/staging/**/*MCMV_CIDADES_EMENDAS_*__mcmv_cidades_emendas.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
