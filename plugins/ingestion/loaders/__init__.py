"""Carga (Template Method + Factory): modos de carga e loaders para fora do dbt.

O bronze do Postgres é carregado pelo dbt (`fonte_lake`). Os loaders daqui cobrem o
que o dbt não alcança: Postgres sem MinIO e, na Fase 9, tabelas Iceberg.
"""
