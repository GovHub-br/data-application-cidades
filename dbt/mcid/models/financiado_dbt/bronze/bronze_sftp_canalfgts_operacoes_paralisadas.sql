{{ config(materialized="table") }}

-- Bronze: Operações paralisadas do setor público — dias sem evolução, motivo e entrave, em texto livre.
-- Fonte: SFTP — sftp/caixa.geavo/GEAVO/ (pacote semanal `MC<aaaammdd>.zip`)
{% set padrao = "s3://data-lake-mcid/staging/**/*__MCidades_AO_2__operacoes_paralisadas_fgts_setorpublico.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
