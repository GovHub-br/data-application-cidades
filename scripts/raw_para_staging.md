# `raw_para_staging.py` — Conversão Raw → Staging (Parquet)

Terceira etapa do pipeline do data lake (MinIO). Lê cada objeto de `raw/` (CSV/TXT/XLSX),
detecta separador e encoding, e grava a versão colunar em **Parquet** em `staging/`, para a
etapa seguinte (Staging → DB via dbt/DuckDB) consumir dados uniformes e eficientes.

> **Ordem importa:** rode **depois** do `mascarar_minio.py --apply`. Staging lê `raw/`
> como está; se rodar antes do mascaramento, o parquet conterá PII.

---

## 1. O que faz

- **Pré-processa cada arquivo** para descobrir o **separador** (`;`, `|`, tab, `,`) e o
  **encoding** (utf-8 ou cp1252) — `raw/` é heterogêneo (arquivos de fontes/sistemas diferentes).
- Converte para **Parquet** (compressão snappy), **todas as colunas como string**.
- **Normaliza os nomes de coluna** para snake_case ASCII (dedup) e guarda o mapa
  original→normalizado nos metadados do parquet e na tabela de controle.
- **Descarta colunas de padding** (sem nome e 100% vazias) — ver §1.1.
- Adiciona **colunas de linhagem**: `_source_file`, `_ingested_at`, `_source_hash`.

### Formatos de entrada

A staging **espelha a estrutura de pastas do `raw/`**, trocando o arquivo de origem por
parquet: `raw/sftp/fabrica/GEFUS/foo.csv` → `staging/sftp/fabrica/GEFUS/foo.parquet`. Vale
para o `raw/` inteiro, não só para `sftp/`.

A extensão de origem **não** entra no nome. `foo.csv` e `foo.txt` são o mesmo dado em dois
formatos e viram um único `foo.parquet`; quem garante que só um deles chega até aqui é o
descarte de gêmeos abaixo.

O descarte de gêmeos roda sobre a listagem **completa**, antes dos filtros
`--pattern` / `--only-ext` / `--max-size-mb`. Ou seja: **o conjunto final da staging é o
mesmo independente de como a execução foi fatiada** — os filtros só decidem que parte dele é
construída agora. É o que torna seguro carregar o lake em etapas (`--only-ext xlsx`, depois
`csv`, depois `mdb`, depois `txt`). Num recorte, o objeto que perdeu para outro formato
aparece como `skipped_duplicado` e não é convertido — quem o converte é a execução do formato
vencedor.

### Pastas ignoradas

`PASTAS_IGNORADAS` (topo do script) lista pastas de primeiro nível de `raw/` que nunca viram
parquet, mesmo com `--pattern`/`--only-ext` vazios. Hoje só `dados_historicos/`: já tem
tratamento próprio de um membro da equipe para ciência de dados, e gerar parquet full-text por
cima duplicaria o trabalho e criaria uma segunda versão divergente do mesmo dado. A pasta
continua sendo mascarada normalmente pelo `mascarar_minio.py` — a exclusão é só da conversão
para staging/.

O filtro roda antes do descarte de gêmeos: se um objeto da pasta ignorada entrasse na lista,
ele poderia vencer o gêmeo de outra pasta pelo mesmo *stem* e o dado bom sairia como
`skipped_duplicado` sem nunca virar parquet.

### Gêmeos: um objeto por nome no lake inteiro

O mesmo dado aparece no lake em mais de um formato (`202601_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB`
existe em `.txt` e `.xlsx` com **1289 linhas × 40 colunas em ambos**) e, desde que o `raw/`
ganhou estrutura de pastas, também em mais de um lugar — `Base_PF_FGTS_20260107.txt` está em
`caixa.geavo/GEAVO/` e em `fabrica/GEFUS/`, e a pasta `ANTERIORES/` do SFTP guarda cópia de
arquivos da pasta corrente. Não é economia de espaço: é não gerar N tabelas do mesmo dado.

A identidade do gêmeo é o **nome do arquivo, sem extensão e sem pasta**. Para cada nome
sobrevive **um único objeto no lake inteiro**, por esta ordem:

1. **formato suportado antes de não-suportado** — senão um `.xls` (que o script não lê)
   descartaria o `.csv` equivalente e o dado sumiria da staging;
2. **texto antes de planilha** (`.csv`, `.txt` → `.xlsx`, `.xls`): o mascaramento reescreve
   texto byte a byte em transporte latin-1, sem tocar em nenhuma coluna não-alvo, enquanto
   no `.xlsx` ele reescreve a pasta de trabalho e perde o cache de fórmulas;
3. `.csv` antes de `.txt`, `.xlsx` antes de `.xls`;
4. empate (mesma extensão em pasta ou caixa diferente, `GEAVO/FOO.TXT` e `GEFUS/foo.txt`) →
   **menor key** na ordem lexicográfica, para a escolha não variar entre execuções.

Os descartados entram na auditoria com status `skipped_duplicado` — nada some em silêncio.
Medido em produção: **2.795 objetos → 2.646 mantidos, 149 descartados**, sem nenhuma colisão
de destino.

| Formato | Saída |
|---|---|
| CSV / TXT | 1 parquet (`staging/<pastas>/<nome>.parquet`) |
| XLSX | 1 parquet por aba (`staging/<pastas>/<nome>__<aba>.parquet`; aba única → sem sufixo) |
| **MDB / ACCDB** (Access) | **1 parquet por tabela** (`staging/<pastas>/<nome>__<tabela>.parquet`) |
| XLS legado, ZIP, JSON | `skipped_unsupported` |

Marcadores de pasta (objetos de 0 byte com a key terminando em `/`, criados pelo console S3)
são ignorados na varredura.

### 1.1 Colunas de padding

Planilhas exportadas do Excel trazem colunas vazias até o limite da grade: há arquivos no lake
com **16.385 colunas** para 35 colunas reais (`2024_08_SNH_PMCMV_DADOS_PRIORITARIOS_AF_BB.csv`),
o que inflava o parquet e o schema no warehouse.

A regra é conservadora: descarta a coluna só se ela for **sem nome (`Unnamed:`) E 100% vazia**.
Coluna anônima que tenha qualquer valor é **preservada** (listas de validação do Excel moram
nesse tipo de coluna). A varredura extra do conteúdo só acontece quando o header tem mais de
`LIMITE_COLS_SEM_NOME` (10) colunas anônimas — arquivos normais não pagam nada por isso.

Efeito no arquivo acima: 16.385 → 38 colunas, 5,1 MB → 99 KB, mesmas 1.288 linhas.

**`.mdb`**: lidos via **`mdbtools`** (binário do sistema, não é lib Python). O `mdb-export`
já entrega CSV vírgula em **UTF-8**, então esse caminho não usa a detecção de encoding/dialeto
— vai direto para o mesmo conversor streaming do CSV. Uma falha numa tabela não derruba as
outras (aquela part vira `error`, as demais seguem). São 128 arquivos / ~105 GB no lake
(famílias `MCidades_AO_1/2/3`, `CCI_CCA`, `AF`).


### Detecção de encoding (pt-BR)
`charset_normalizer` puro confunde cp1252 com cp1250/latin2 nos dados brasileiros (`0xE3` vira
`ă` em vez de `ã`). Como staging **decodifica** o texto (diferente do mascaramento, que preserva
bytes), a heurística aqui é **utf-8 ou cp1252**: sample só-ASCII ou não-utf-8 → cp1252 (nunca
falha em bytes acentuados que apareçam depois); sample com utf-8 multibyte válido → utf-8.
Cobre praticamente todos os arquivos de governo pt-BR.

---

## 2. Como funciona (arquitetura)

Para cada objeto em `raw/`:

1. **Amostra** 64 KB (HTTP Range) → detecta encoding + dialeto (separador/quote/lineterm).
   Sem dialeto tabular → `skipped_no_header` (fixed-width, arquivos de teste).
2. **Idempotência**: md5 do objeto; se `(raw_key, source_hash)` já `success` no controle e sem
   `--force` → `skipped_already`.
3. **CSV/TXT** (streaming): `pandas.read_csv(dtype=str, na_filter=False, chunksize=N, ...)` com
   o separador/encoding detectados. 1º chunk define o schema (header normalizado + linhagem);
   cada chunk vira um row group via `pyarrow.parquet.ParquetWriter` → **memória constante**
   mesmo nos arquivos de ~2 GB / 10 M linhas.
4. **XLSX**: cada aba não-vazia vira um parquet. Aba única → `staging/<pastas>/<nome>.parquet`;
   múltiplas abas → `staging/<pastas>/<nome>__<aba>.parquet`.
5. **Verificação**: nº de linhas do parquet == nº de linhas lidas (senão `error`, não sobe).
6. **Upload** para `staging/<...>.parquet` + registro no controle e na auditoria.

`.xls` legado e `.zip` → `skipped_unsupported` (tratar à parte). Arquivos de lock `~$*` ignorados.

---

## 3. Configuração (`.env`)

| Variável | Obrigatória | Descrição |
|---|---|---|
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` | sim | Conexão MinIO |
| `DB_DW_HOST_MCID`, `DB_DW_PORT_MCID`, `DB_DW_USER_MCID`, `DB_DW_PASSWORD_MCID`, `DB_DW_DBNAME_MCID` | sim | Postgres (tabela de controle) |
| `STAGING_PREFIX` | não (default `staging/`) | Prefixo de saída |
| `MASKING_PREFIX` | não (default `raw/`) | Prefixo de entrada |
| `LAKE_TMPDIR` / `MASKING_TMPDIR` | recomendado | Dir de temporários. **Não use `/tmp`** se for tmpfs (RAM). ~5 GB livres p/ os arquivos grandes. |

### Dependências
Todas já usadas pelo projeto: `pandas`, `pyarrow`, `openpyxl`, `boto3`, `psycopg2`,
`python-dotenv`. Sem novas deps.

O script importa `lake_utils.py` (módulo compartilhado com detecção de encoding/dialeto,
normalização de header e helpers S3), então rode-o com `scripts/` acessível
(`.venv/bin/python scripts/raw_para_staging.py`).

---

## 4. Uso

Por padrão roda em **DRY-RUN**: grava em `staging_dryrun/` (não em `staging/`). Use `--apply`.

```bash
# dry-run fatiado por formato
.venv/bin/python scripts/raw_para_staging.py --only-ext csv --max-size-mb 50
.venv/bin/python scripts/raw_para_staging.py --only-ext xlsx
.venv/bin/python scripts/raw_para_staging.py --only-ext txt

# um arquivo específico (reprocessa)
.venv/bin/python scripts/raw_para_staging.py --pattern PMCMV_FAIXA3 --force

# EFETIVAR (grava em staging/) — recomendado fatiar, menores primeiro
.venv/bin/python scripts/raw_para_staging.py --apply --only-ext csv
.venv/bin/python scripts/raw_para_staging.py --apply --only-ext xlsx
.venv/bin/python scripts/raw_para_staging.py --apply --only-ext txt
```

### Flags

| Flag | Efeito |
|---|---|
| `--apply` | Grava em `staging/`. Sem ela = dry-run (`staging_dryrun/`). |
| `--force` | Reprocessa objetos já convertidos. |
| `--limit N` | Processa no máximo N objetos. |
| `--pattern STR` | Só objetos cuja key contém `STR`. |
| `--only-ext csv,txt` | Restringe às extensões dadas. |
| `--max-size-mb N` | Pula objetos maiores que N MB. |
| `--chunksize N` | Linhas por chunk na leitura CSV/TXT (default 200k). |
| `--bad-lines {skip,count,error}` | Linhas mal-formadas: `skip` (rápido, engine C, sem contagem), `count` (engine Python, conta), `error` (falha o arquivo). Default `skip`. |
| `--prefix P` | Prefixo a varrer (default `raw/`). |

---

## 5. Status de saída

| Status | Significado |
|---|---|
| `converted` | Convertido e gravado em `staging/` (com `--apply`). |
| `dry_run` | Prévia gravada em `staging_dryrun/` (sem `--apply`). |
| `skipped_already` | Já convertido (mesmo `source_hash`). |
| `skipped_no_header` | Não-tabular / sem separador detectável. |
| `skipped_unsupported` | `.xls` / `.zip` — tratar à parte. |
| `error` | Falhou; nada gravado. Investigar e re-rodar com `--pattern`. |

**`error` deve ser 0.** XLSX com múltiplas abas gera uma "part" (linha de status) por aba.

---

## 6. Controle e auditoria

**Tabela de controle** `lake._staging_log` (criada automaticamente),
`UNIQUE(raw_key, staging_key, source_hash)`: `raw_key, staging_key, source_hash, n_linhas,
n_colunas, n_bad_lines, encoding, delimiter, column_map (JSONB), status, error_message,
created_at`.

O `staging_key` entra na chave porque **XLSX e MDB geram N parts** (uma por aba/tabela) com o
mesmo par `(raw_key, source_hash)`; com a chave antiga o upsert sobrescrevia e sobrava só 1
registro por arquivo (ex.: 4 linhas no banco para 32 parts reais). Não afetava os dados nem a
idempotência — só a observabilidade.

> ⚠️ **Deploy**: `CREATE TABLE IF NOT EXISTS` **não migra** tabela já existente. Num ambiente
> com o `_staging_log` antigo, o DDL vira no-op e a constraint velha continua valendo — é
> preciso um `ALTER TABLE` explícito:
> ```sql
> ALTER TABLE lake._staging_log DROP CONSTRAINT _staging_log_raw_key_source_hash_key;
> ALTER TABLE lake._staging_log ADD CONSTRAINT _staging_log_raw_key_staging_key_source_hash_key
>       UNIQUE (raw_key, staging_key, source_hash);
> ```

**Auditoria parquet** por execução em `audit/staging/execution_id=<uuid>/part-0.parquet`
(+ cópia local `scripts/auditoria_staging_<uuid>.parquet`).

**Metadados do parquet** de saída (key-value): `source_file`, `source_hash`, `encoding`,
`delimiter`, `column_map` (e `sheet` para XLSX).

```sql
SELECT status, count(*), sum(n_linhas) FROM lake._staging_log GROUP BY status;
```

---

## 7. Layout no MinIO

```
raw/<pastas>/<nome>.csv|txt|xlsx|mdb           (entrada)
staging/<pastas>/<nome>.parquet                (saída — espelha as pastas, sem a extensão)
staging/<pastas>/<nome>__<aba>.parquet         (XLSX multi-aba: 1 parquet por aba)
staging/<pastas>/<nome>__<tabela>.parquet      (MDB: 1 parquet por tabela)
staging_dryrun/<...>.parquet          (prévia do dry-run)
audit/staging/execution_id=.../       (auditoria)
```

---

## 8. Recomendações operacionais

- **Sempre dry-run antes do `--apply`** e confira uma amostra dos parquets (schema, acentos,
  linhagem, contagem de linhas).
- **Fatiar** por formato/tamanho; se algo falhar no meio, o já convertido não se perde
  (idempotência por `source_hash`).
- Em máquina com pouca RAM, garanta `LAKE_TMPDIR` num disco com espaço; a conversão é
  streaming (memória constante), mas os arquivos grandes têm I/O intenso.
- Rode **depois** do mascaramento (ver aviso no topo).
