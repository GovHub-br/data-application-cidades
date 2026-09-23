{{ config(materialized="table") }}

-- Bronze: Termos de compromisso do Novo MCMV FNHIS Sub-50 no TransfereGov: uma linha por proposta selecionada, com situação da contratação e do instrumento, valores de repasse, contrapartida e empenho. Retrato mensal; a tabela guarda o mais recente.
-- Fonte: SHPT — sharepoint/Novo MCMV - FNHIS Sub 50/ (painel mensal extraído do TransfereGov)
{% set padrao = "s3://data-lake-mcid/staging/**/FNHIS SUB 50 Painel_TG_*.parquet" %}

select *
from read_parquet('{{ padrao }}', filename => true, union_by_name => true) as r
where cast(r['filename'] as varchar) = {{ arquivo_mais_recente(padrao) }}
