{{ config(materialized="table") }}

-- Bronze: Anexo "Rural" da Portaria MCidades nº 162, de 27/02/2018 — empreendimentos do PNHR
-- selecionados naquele ciclo, com a entidade organizadora (CNPJ) e a quantidade de UH.
-- Fonte: SFTP — sftp/fabrica/GEFUS/FDS/Anexo_Portaria MCidades nº 162, de 27_02_2018__rural
--
-- É a única lista nominal de selecionados do Rural que existe no lake. `codigo_do_empreendimento`
-- casa com a APF do INT065 (PNHR CAIXA) em 772 de 1.287 linhas.
{% set padrao = "s3://data-lake-mcid/staging/**/Anexo_Portaria MCidades*162*rural.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
