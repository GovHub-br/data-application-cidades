# Issue #119 — Correcao de Arquitetura: DuckDB Lendo MinIO Staging

## Resumo

Foi ajustada a diretriz tecnica da camada silver: as tabelas silver do MCMV
devem ser geradas somente a partir dos arquivos do MinIO em `staging/`, usando
DuckDB como mecanismo de leitura dos Parquets.

As consultas anteriores em schemas Postgres como `sftp`, `__dados_brutos`,
`empreendimento_far` e `entidades_fds` devem ser tratadas apenas como
inventario/evidencia de validacao, nao como implementacao final da silver.

## Decisao

Fluxo correto:

```text
MinIO raw/
  -> MinIO staging/ parquet
  -> DuckDB read_parquet(...)
  -> dbt silver padronizada
  -> gold/marts
  -> Superset/dashboard
```

Regra de arquitetura:

- `raw/` preserva origem.
- `staging/` e a unica fonte autorizada para gerar silver.
- DuckDB deve ler `staging/*.parquet`.
- Postgres nao deve ser usado como fonte de leitura da silver MCMV.
- Postgres pode continuar como destino materializado/ambiente analitico, desde
  que a origem da transformacao seja MinIO `staging/`.

## Ajustes no Repositorio

- Adicionada dependencia `dbt-duckdb`.
- Criado target `staging_duckdb` no `profiles.yml`.
- Criadas macros:
  - `minio_staging_uri(object_name)`
  - `read_minio_staging_parquet(object_name)`
  - `assert_duckdb_staging_only()`
- `mcmv_silver_dbt` foi condicionado para ficar habilitado apenas quando o
  adapter ativo for DuckDB.

## Comando Esperado

```bash
cd airflow_lappis/dags/dbt/mcid
dbt run --target staging_duckdb --select mcmv_silver_dbt
```

Variaveis esperadas no ambiente:

- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET`
- `DUCKDB_MCID_PATH`

## Impacto na Entrega Anterior

Os documentos, inventarios, matrizes e evidencias continuam uteis para entender
frentes, campos, fontes e regras. Porem, as silvers materializadas anteriormente
a partir de Postgres nao devem ser consideradas a implementacao final aderente a
arquitetura.

## Proximo Passo

Refatorar cada modelo em `models/mcmv_silver_dbt/silver/<frente>/` para trocar:

```sql
from {{ source(...) }}
from {{ ref(...) }}
```

por:

```sql
from {{ read_minio_staging_parquet('<arquivo_staging>.parquet') }}
```

Mapeamento prioritario:

| Frente | Origem correta esperada |
|---|---|
| FAR | `staging/novo_mcmv_far_*.parquet` e dados prioritarios CAIXA/BB |
| Entidades/FDS | `staging/novo_mcmv_fds_*.parquet` |
| Rural/PNHR | `staging/novo_mcmv_rural_*.parquet` e serie semanal FAR/FDS/Rural |
| SUB50/FNHIS | `staging/novo_mcmv_fnhis_sub_50_*.parquet` |
| Classe Media/Faixa 3 | `staging/PMCMV_FAIXA3_MCID_*.parquet` |
| Reforma Casa Brasil | `staging/PMCMV_REFORMAS_MCID_*.parquet` |
| MCMV Cidades | arquivo staging correspondente ao snapshot Cidades |
| Conjuntura | staging de FGTS/financiamentos habitacionais |

## Status

Esta correcao deixa o projeto protegido contra execucao indevida via Postgres e
prepara o caminho correto DuckDB/MinIO. A refatoracao SQL fonte-a-fonte ainda
deve ser feita antes de considerar a silver final como aderente a arquitetura.
