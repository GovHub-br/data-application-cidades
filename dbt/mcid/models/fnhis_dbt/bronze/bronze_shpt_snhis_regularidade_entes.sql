{{ config(materialized="table") }}

-- Bronze: Regularidade de estados e municípios no SNHIS: lei do fundo local de habitação (FLHIS), lei do conselho gestor, termo de adesão, plano habitacional e relatório de gestão. Condição para o ente receber recurso do FNHIS. Retrato semanal; a tabela guarda o mais recente.
-- Fonte: SHPT — sharepoint/SNHIS/ (extração semanal da CAIXA para o MCID)
{% set padrao = "s3://data-lake-mcid/staging/**/FNHIS_SEMANAL_MCID_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
