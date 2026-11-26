select *
from
    read_parquet(
        's3://data-lake-mcid/snh/staging/sharepoint/Trabalho Social/MCMV_FDS_TS_CAIXA_ 202607.parquet'
    )
