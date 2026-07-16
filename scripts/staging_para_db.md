# `staging_para_db.py` — Ingestão Staging → Postgres (via DuckDB)

Quarta e última etapa do pipeline do data lake. Lê cada parquet de `staging/` **direto do
MinIO** (httpfs) e materializa uma tabela no Postgres usando a extensão `postgres` do DuckDB
(`ATTACH` + `CREATE TABLE AS`) — os dados **não passam pelo Python**.

> **Ordem importa:** rode **depois** do `raw_para_staging.py --apply`, que é quem popula
> `staging/`.

---

## 1. O que faz

- Para cada `staging/<...>.parquet`, cria/substitui uma tabela em `sftp.<nome>`.
- **Todas as colunas como TEXT** — mantém a decisão do staging (tudo string); a tipagem
  (datas/números) fica para o dbt. A carga nunca falha por inferência de tipo errada.
- Preserva as **colunas de linhagem** do staging (`_source_file`, `_ingested_at`,
  `_source_hash`) na tabela final.
- **Verifica** a carga: `count(*)` no Postgres tem que bater com o nº de linhas do parquet
  (senão a tabela não é dada como carregada).

### Por que DuckDB
Lê o parquet do MinIO e escreve no Postgres num único `CREATE TABLE AS`, sem materializar os
dados na memória do processo Python — o que importa nos arquivos de ~2 GB / 10 M linhas.

### Tipagem: TEXT vs `character varying`
O DuckDB mapeia `VARCHAR` para `character varying` **sem limite de tamanho**, que no Postgres
é funcionalmente idêntico a `text` (mesmo armazenamento, sem truncagem). É o que aparece no
`information_schema`; não há nada a corrigir.

---

## 2. Como funciona (arquitetura)

Para cada objeto em `staging/`:

1. **Metadados**: lê só o *footer* do parquet (nº de linhas, colunas, `source_hash`,
   `source_file`) — não baixa o arquivo.
2. **Idempotência**: se `(staging_key, source_hash)` já está `loaded` no controle e sem
   `--force` → `skipped_already`.
3. **Nome da tabela**: derivado da staging key, snake_case ASCII, com o caminho inteiro
   (`staging/dados_historicos/bb_2015_csv.parquet` → `dados_historicos_bb_2015_csv`), porque
   `staging/` espelha o layout de `raw/` e subpastas diferentes têm nomes de arquivo iguais.
4. **Identificadores**: nomes de tabela/coluna acima de **63 bytes** (limite do Postgres) são
   truncados com sufixo de hash, que preserva a unicidade em vez de colidir silenciosamente.
5. **Carga**: `DROP TABLE IF EXISTS` + `CREATE TABLE AS SELECT` **na mesma transação** — os
   leitores enxergam a tabela antiga até o commit; se a carga falha, faz rollback e a tabela
   antiga continua de pé.
6. **Verificação** + registro no controle e na auditoria.

---

## 3. Configuração (`.env`)

| Variável | Obrigatória | Descrição |
|---|---|---|
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` | sim | Conexão MinIO |
| `DB_DW_HOST_MCID`, `DB_DW_PORT_MCID`, `DB_DW_USER_MCID`, `DB_DW_PASSWORD_MCID`, `DB_DW_DBNAME_MCID` | sim | Postgres de destino |
| `SFTP_SCHEMA` | não (default `sftp`) | Schema de destino e do controle |
| `STAGING_PREFIX` | não (default `staging/`) | Prefixo de entrada |
| `DUCKDB_MEMORY_LIMIT` | não (default `4GB`) | Limite de memória do DuckDB |
| `DUCKDB_THREADS` | não (default `4`) | Threads do DuckDB |
| `LAKE_TMPDIR` / `MASKING_TMPDIR` | recomendado | Dir de *spill* do DuckDB. **Não use `/tmp`** se for tmpfs (RAM). |

### Dependências
`duckdb` (nova, já em `pyproject.toml`) + `pyarrow`, `boto3`, `psycopg2`, `pandas`,
`python-dotenv`, já usadas pelo projeto. O DuckDB instala/carrega as extensões `httpfs` e
`postgres` na primeira execução (precisa de saída para a internet uma vez).

Importa `lake_utils.py`, então rode com `scripts/` acessível.

---

## 4. Uso

Por padrão roda em **DRY-RUN**: não grava nada no Postgres, só reporta o que faria.

```bash
# dry-run: quais tabelas seriam criadas, com quantas linhas
python scripts/staging_para_db.py

# um arquivo específico
python scripts/staging_para_db.py --pattern PMCMV_FAIXA3 --apply

# EFETIVAR — recomendado fatiar, menores primeiro
python scripts/staging_para_db.py --apply --max-size-mb 50
python scripts/staging_para_db.py --apply
```

### Flags

| Flag | Efeito |
|---|---|
| `--apply` | Cria/substitui as tabelas no Postgres. Sem ela = dry-run. |
| `--force` | Recarrega parquets já materializados. |
| `--limit N` | Processa no máximo N objetos. |
| `--pattern STR` | Só objetos cuja key contém `STR`. |
| `--prefix P` | Prefixo a varrer (default `staging/`). |
| `--max-size-mb N` | Pula objetos maiores que N MB. |
| `--memory-limit` | Limite de memória do DuckDB (default `4GB`). |
| `--threads` | Threads do DuckDB (default `4`). |

---

## 5. Status de saída

| Status | Significado |
|---|---|
| `loaded` | Tabela criada/substituída e verificada (com `--apply`). |
| `dry_run` | Prévia; nada gravado. |
| `skipped_already` | Já carregado (mesmo `source_hash`). |
| `skipped_empty` | Parquet sem colunas. |
| `error` | Falhou; rollback, tabela anterior intacta. |

**`error` deve ser 0.**

---

## 6. Controle e auditoria

**Tabela de controle** `sftp._staging_to_dw_log` (criada automaticamente),
`UNIQUE(staging_key, source_hash)`: `staging_key, target_table, source_file, source_format,
source_hash, n_linhas, n_colunas, status, error_message, created_at`.

`source_file`/`source_format` vêm dos metadados do parquet e dizem de qual arquivo e formato
de origem (csv/txt/xlsx) aquela tabela saiu — o mesmo dataset chega do SFTP em mais de uma
extensão, e cada uma vira sua própria tabela (`x_csv`, `x_txt`).

**Auditoria parquet** por execução em `audit/db/execution_id=<uuid>/part-0.parquet`
(+ cópia local `scripts/auditoria_db_<uuid>.parquet`).

```sql
-- panorama da última carga
select status, count(*), sum(n_linhas) from sftp._staging_to_dw_log group by 1;

-- de qual formato cada tabela saiu
select target_table, source_format, n_linhas
from sftp._staging_to_dw_log where status = 'loaded' order by 1;
```

---

## 7. Logs do pipeline

Uma tabela de controle por etapa, todas no schema `sftp`:

| Etapa | Script | Tabela |
|---|---|---|
| SFTP → MinIO | `sftp_para_minio.py` | `_ingest_minio_log` |
| Mascaramento | `mascarar_minio.py` | `_masking_log` |
| Raw → Staging | `raw_para_staging.py` | `_raw_staging_log` |
| Staging → DW | `staging_para_db.py` | `_staging_to_dw_log` |

---

## 8. Recomendações operacionais

- **Sempre dry-run antes do `--apply`** e confira os nomes de tabela gerados.
- **Fatiar** por tamanho; se algo falhar no meio, o já carregado não se perde (idempotência
  por `source_hash`).
- Ajuste `--memory-limit` ao que a máquina tem; o DuckDB faz *spill* para `LAKE_TMPDIR`.
- Rode **depois** do `raw_para_staging.py --apply` (ver aviso no topo).
