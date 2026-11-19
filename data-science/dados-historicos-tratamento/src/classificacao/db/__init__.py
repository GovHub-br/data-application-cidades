"""Database access layer for the MCMV classification and treatment pipeline.

Modules:
- ``connection``: SQLAlchemy engine and session management.
- ``reader``: Read tables from ``dados_historicos`` schema as pd.DataFrame.
- ``writer``: Write treated DataFrames to ``dados_historicos_formatados`` schema.
"""
