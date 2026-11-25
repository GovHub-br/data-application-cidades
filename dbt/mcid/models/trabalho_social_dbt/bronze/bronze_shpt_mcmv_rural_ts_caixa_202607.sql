{{ config(materialized="table", tags=["ts_lake"]) }}

-- Bronze: acompanhamento do Trabalho Social do MCMV Rural / PNHR pela CAIXA, remessa de jul/2026
-- (aba TS_AF_CAIXA_PNHR_RURAL da planilha MCMV_RURAL_TS_CAIXA_BB_202607). Cópia fiel do parquet.
-- Faltava entre as bronzes de jul/2026: a do Rural que existia era só a do BB.
select * from read_parquet('s3://data-lake-mcid/staging/sharepoint/Trabalho Social/MCMV_RURAL_TS_CAIXA_BB_ 202607__ts_af_caixa_pnhr_rural_07_26.parquet')
