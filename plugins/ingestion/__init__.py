"""Ingestão do cidades: fonte -> raw -> staging -> bronze.

    fonte --Extractor-->      raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/
          --FileConverter-->  staging/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/
          --dbt (read_parquet)-->  bronze no Postgres

A raw guarda o formato original; a staging, Parquet só com texto. Extração é
Strategy + Factory; conversão e carga são Template Method + Factory. Nada aqui
tipa ou renomeia coluna: isso é do dbt. Plano e decisões em
docs/refactor-ingestao.md e docs/ingestao/diagnostico.md.
"""
