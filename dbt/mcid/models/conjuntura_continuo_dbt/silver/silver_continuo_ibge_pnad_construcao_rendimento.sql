{{ config(materialized='table') }}

-- Silver do conjuntura contínuo: PNAD-C rendimento médio real construção x total (R$).
-- Página 3, seção 4. Fonte: IBGE via SIDRA (tabela 6391, var 5932, classif 888:
-- 47946=Total, 47949=Construção). Trimestre móvel. Full-refresh.

select *
from read_parquet('s3://data-lake-mcid/staging/ibge/pnad_construcao_rendimento.parquet')
