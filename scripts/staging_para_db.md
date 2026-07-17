# `staging_para_db.py` — Ingestão Staging → Postgres (via pg_duckdb)

Quarta e última etapa do pipeline do data lake. Para cada parquet de `staging/`, dispara um
`DROP TABLE` + `CREATE TABLE AS ... FROM read_parquet('s3://...')` **direto no Postgres**
via `psycopg2` — é a extensão `pg_duckdb` (motor DuckDB embarcado no Postgres) que lê o
objeto do MinIO usando o secret S3 já configurado no servidor. Os dados **não passam pelo
processo Python**; o script só decide quais parquets carregar (metadados lidos localmente,
via footer do parquet) e envia o SQL.

> **Ordem importa:** rode **depois** do `raw_para_staging.py --apply`, que é quem popula
> `staging/`.

> **Pré-requisito:** `pg_duckdb` instalado/ativo no Postgres, `duckdb.postgres_role`
> apontando para o mesmo usuário de `DB_DW_USER_MCID`, e um secret S3
> (`duckdb.create_simple_secret`) já criado numa sessão desse usuário — setup feito uma vez
> na VM, fora do escopo deste script. Sem isso, `read_parquet` falha com erro de
> permissão/credenciais (`No credentials are provided` ou `duckdb.postgres_role`).

---

## 1. O que faz

- Para cada `staging/<...>.parquet`, cria/substitui uma tabela em `sftp.<nome>`.
- **Todas as colunas como TEXT** — mantém a decisão do staging (tudo string); a tipagem
  (datas/números) fica para o dbt. A carga nunca falha por inferência de tipo errada.
- Preserva as **colunas de linhagem** do staging (`_source_file`, `_ingested_at`,
  `_source_hash`) na tabela final.
- **Verifica** a carga: `count(*)` no Postgres tem que bater com o nº de linhas do parquet
  (senão a tabela não é dada como carregada).

### Por que pg_duckdb
O `CREATE TABLE AS` roda inteiro dentro do Postgres — leitura do MinIO e escrita na tabela
são a mesma operação, sem passar pelo processo Python nem por outro serviço intermediário.
Importa nos arquivos de ~2 GB / 10 M linhas: nada disso é materializado na memória do script.

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
| `DUCKDB_MEMORY_LIMIT` | não (default `4GB`) | `SET duckdb.max_memory` na sessão (pg_duckdb), *best-effort* |
| `DUCKDB_THREADS` | não (default `4`) | `SET duckdb.threads` na sessão (pg_duckdb), *best-effort* |

`DUCKDB_MEMORY_LIMIT`/`DUCKDB_THREADS` (ou `--memory-limit`/`--threads`) ajustam GUCs do
`pg_duckdb` só para a conexão desta execução — se a role não tiver permissão para alterá-los,
o script loga um aviso e segue a carga normalmente (não é motivo para abortar).

### Dependências
`pyarrow`, `boto3`, `psycopg2`, `pandas`, `python-dotenv` — já usadas pelo projeto. **Não
depende mais do pacote Python `duckdb`**: quem lê o parquet é o `pg_duckdb` dentro do
Postgres (ver pré-requisito no topo deste documento); o Python só lê o footer do parquet
via `pyarrow` (metadados) e dispara o SQL via `psycopg2`.

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
| `--memory-limit` | `duckdb.max_memory` na sessão do pg_duckdb (default `4GB`). |
| `--threads` | `duckdb.threads` na sessão do pg_duckdb (default `4`). |

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
- Ajuste `--memory-limit`/`--threads` ao que o Postgres da VM tem disponível — é o mesmo
  processo que atende outras conexões; `duckdb.max_memory` alto demais pode competir por RAM
  com o resto do servidor. *Spill* em disco (`duckdb.temporary_directory`) é config do
  servidor, fora do escopo deste script.
- Rode **depois** do `raw_para_staging.py --apply` (ver aviso no topo).
