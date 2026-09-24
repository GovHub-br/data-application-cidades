{{ config(materialized="table") }}

-- Bronze: Coordenadas do empreendimento em chave-valor: uma linha por componente de GPS (grau, minuto, segundo, hemisfério, datum).
-- Fonte: SFTP — sftp/caixa.geavo/GEAVO/ (pacote semanal `MC<aaaammdd>.zip`)
{% set padrao = "s3://data-lake-mcid/staging/**/*__MCidades_AO_1__tab_empreendimentos_gps.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
