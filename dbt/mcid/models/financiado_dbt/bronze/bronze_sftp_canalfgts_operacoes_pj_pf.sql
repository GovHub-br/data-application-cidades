{{ config(materialized="table") }}

-- Bronze: Domínio: classificação PJ/PF por linha e objetivo — é o que separa os dois fluxos que o cartaz pediu para separar.
-- Fonte: SFTP — sftp/caixa.geavo/GEAVO/ (pacote semanal `MC<aaaammdd>.zip`)
{% set padrao = "s3://data-lake-mcid/staging/**/*__MCidades_AO_1__operacoespj_pf.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
