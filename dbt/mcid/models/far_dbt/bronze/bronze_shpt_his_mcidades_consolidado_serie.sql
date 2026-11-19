{{ config(materialized="table") }}

-- Bronze: Consolidado GFAR — TODAS as remessas semanais empilhadas.
-- Cada arquivo é a foto completa da base naquela semana, e propostas saem e voltam
-- entre remessas sem terem sido canceladas. Só empilhando dá para recuperar a última
-- versão de quem saiu. A data da remessa sai do nome (`_CONSOLIDADO_<aaaammdd>`) na
-- prata; inclui as remessas de 2025 em `Arquivados/`.
-- Fonte: SHPT — sharepoint/Novo MCMV - FAR/
{% set padrao = "s3://data-lake-mcid/staging/**/*HIS_MCIDADES_CONSOLIDADO_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
