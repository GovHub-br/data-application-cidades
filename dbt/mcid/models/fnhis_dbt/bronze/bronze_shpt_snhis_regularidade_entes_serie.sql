{{ config(materialized="table") }}

-- Bronze: Regularidade de estados e municípios no SNHIS, SÉRIE inteira (todas as remessas
-- semanais).
-- Fonte: SHPT — sharepoint/SNHIS/ (extração semanal da CAIXA para o MCID)
--
-- Diferente da bronze_shpt_snhis_regularidade_entes, que guarda só o retrato mais recente, esta
-- empilha todos os arquivos FNHIS_SEMANAL_MCID_* da staging. O valor dela está em responder se o
-- ente estava regular NA DATA da seleção, e não só hoje. Cada remessa é um retrato completo; quem
-- escolhe uma data é a prata_fnhis_regularidade_serie.
{% set padrao = "s3://data-lake-mcid/staging/**/FNHIS_SEMANAL_MCID_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
