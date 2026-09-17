{{ config(materialized="table") }}

-- BRONZE — serie historica mensal de empreendimentos MCMV, interface
-- INT059 do SFTP/GEFUS (FDS / Entidades), copia fiel.
--
-- Uma das 5 tabelas em que a bronze unica do SFTP foi separada (D5 da change
-- pipeline-bronze-historica-destino-trocavel): uma tabela por interface, em
-- vez de uma tabela larga e esparsa com union_by_name entre as 5. A separacao
-- por FRENTE continua sendo da silver — o discriminador e a interface.
--
-- Glob na staging: staging/sftp/fabrica/GEFUS/**/INT059_*.parquet
-- Responsabilidade da camada (models/docs/arquitetura-medalhao-mcid.md):
-- uma linha por linha de origem (sem dedup); colunas da fonte preservadas
-- como vieram; sem tipagem (a coercao fica na silver); dt_referencia
-- derivada do NOME DO ARQUIVO (mais confiavel que dt_movimento, ver
-- docs/entregas/issue-130-pendencias-encoding-canonicalizacao-sftp-minio.md);
-- auditoria: source_file, fonte_interface, dt_ingest, hash_linha.
--
-- Arquivos de reentrega (sufixo != _YYYYMMDD) e de VALIDACAO ficam de fora,
-- para nao duplicar APF x mes.
--
-- Corpo e glob vem do mapa de familias (macros/historico/familias.sql).
-- Destino conforme o target (D2): arquivo local em `staging_duckdb`,
-- Postgres atachado em `prod_duckdb`.
{{ bronze_gefus('INT059') }}
