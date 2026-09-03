# staging_para_bronze.py

Terceira etapa do pipeline do data lake: leva os parquets de `staging/` (MinIO) para
tabelas no Postgres, na camada **bronze**.

```
raw/  ->  staging/ (parquet full-text)  ->  bronze (Postgres)  ->  silver  ->  gold
        raw_para_staging.py           staging_para_bronze.py        dbt
```

## 1. Por que é dirigido por família

Os outros scripts do lake são generalistas: varrem o `raw/` inteiro. Este não. Só um
subconjunto pequeno da `staging/` precisa virar tabela — o que alimenta modelo dbt —, então
a carga é declarada em `bronze_familias.yml` e `--familia` é obrigatório.

Uma **família** é um recorte de negócio: um nome, um schema de destino e a lista de
parquets que viram tabela. Cada objeto tem `tabela` mais **uma** das duas formas de origem:

```yaml
empreendimento_far:
  schema: empreendimento_far
  objetos:
    # (a) key exata — dado que é um arquivo só, sem versões
    - staging_key: staging/sharepoint/novo_mcmv_far_consolidado.parquet
      tabela: bronze_consolidado

    # (b) padrão — dado que chega periodicamente; vence a DATA mais recente
    - padrao: "staging/*MONIT_CAD_PJ_FAR_MENSAL_*.parquet"
      data_regex: "MONIT_CAD_PJ_FAR_MENSAL_(\\d{6})"
      tabela: bronze_cad_pj_mensal
```

Adicionar dado novo à bronze = adicionar uma entrada no YAML. Nada no `.py` muda.

### Escolha da versão (`padrao`)

O `*` do `fnmatch` casa `/`, então `staging/*NOME_*` acha o objeto em qualquer profundidade.
Isso é proposital: o descarte de gêmeos do `raw_para_staging.py` mantém um objeto por nome
no lake inteiro, então o arquivo canônico pode estar em `sharepoint/` ou em `sftp/`
dependendo de quem chegou primeiro — fixar a pasta quebraria a resolução.

A data sai do **nome do arquivo**, não da key, porque as versões ficam espalhadas em pastas
diferentes (`Novo MCMV - FAR/` e `.../Arquivados/...`) e aí o caminho pesaria mais que a
data na ordenação. Sem `data_regex`, usa o último grupo de 6 a 8 dígitos do nome; declare o
regex (com **um** grupo de captura) quando o default errar — por exemplo em
`MONIT_CAD_PJ_FAR_MENSAL_202607_041913`, onde o default pegaria o sufixo de hora em vez da
competência.

O arquivo escolhido aparece no log de cada execução e fica gravado em
`lake._bronze_log.staging_key`, então dá para auditar qual versão gerou cada carga.

Hoje a carga é sempre **a versão mais recente** (full refresh). Empilhar todas as datas numa
série histórica é uma evolução possível — os arquivos antigos continuam no lake, nada é
descartado por esta escolha.

## 2. Uso

```bash
python scripts/staging_para_bronze.py --listar                        # famílias declaradas
python scripts/staging_para_bronze.py --familia empreendimento_far    # dry-run (default)
python scripts/staging_para_bronze.py --familia empreendimento_far --apply
python scripts/staging_para_bronze.py --familia empreendimento_far --apply --force
```

| flag | efeito |
|---|---|
| `--familia NOME` | qual família carregar (obrigatório) |
| `--listar` | lista as famílias e sai |
| `--apply` | grava no Postgres; sem a flag, roda em dry-run |
| `--force` | recarrega objetos já materializados (ignora idempotência) |
| `--memory-limit` | `duckdb.max_memory` da sessão (default `4GB`, env `DUCKDB_MEMORY_LIMIT`) |
| `--threads` | `duckdb.threads` da sessão (default `4`, env `DUCKDB_THREADS`) |

## 3. Como a carga funciona

Quem lê o parquet é o **pg_duckdb**, dentro do Postgres — o dado não passa pelo processo
Python. O script só decide o que carregar e dispara:

```sql
DROP TABLE IF EXISTS empreendimento_far."bronze_consolidado";
CREATE TABLE empreendimento_far."bronze_consolidado" AS
SELECT CAST(r['co_tipo_registro'] AS VARCHAR) AS "co_tipo_registro", ...
FROM read_parquet('s3://data-lake-mcid/staging/sharepoint/novo_mcmv_far_consolidado.parquet') AS r;
```

Dois detalhes que não são óbvios:

- **`r['coluna']`**: o pg_duckdb devolve o `read_parquet()` como um registro opaco para o
  parser do Postgres. Referenciar `coluna` direto dá `column ... does not exist`; tem que
  ser via alias da função + acesso por chave.
- **DROP + CREATE na mesma transação**: é o que garante que não há como duplicar linha
  (o oposto do `INSERT` incremental) e que leitores enxergam a tabela antiga até o commit.
  Se a carga falhar, o rollback deixa a tabela antiga de pé.

Depois da carga, o script compara `count(*)` da tabela com o `num_rows` do footer do
parquet e falha se divergirem.

## 4. Decisões

- **Tudo TEXT.** Mantém a decisão da staging. A tipagem (datas, números, booleanos) é
  responsabilidade da silver do dbt. Assim a carga nunca quebra por inferência de tipo.
- **Full refresh no lugar de upsert.** Um upsert de verdade exigiria chave declarada e
  catálogo. Enquanto não há Iceberg, o full refresh entrega o mesmo resultado final para
  snapshot completo, sem duplicar.
- **snake_case nas colunas.** Passa por `normalizar_colunas` (a mesma do
  `raw_para_staging.py`) e trunca em 63 bytes. Parquet que veio de lá passa incólume; a
  normalização vale para parquets de outros caminhos e dá uma base estável para comparar
  schemas entre cargas.
  Ressalva: as colunas de linhagem (`_source_file`, `_ingested_at`, `_source_hash`)
  preservam o underscore inicial — `norm_header` o removeria.
- **Drift de schema avisa, não bloqueia.** Se a tabela já existe e o conjunto de colunas
  mudou, sai um `WARNING` com o que entrou e o que saiu, e o registro vai para
  `colunas_novas`/`colunas_sumidas` na tabela de controle. A carga segue (é full refresh),
  mas a mudança fica rastreada.

## 5. Idempotência

Tabela de controle `lake._bronze_log`, com `UNIQUE (familia, staging_key, source_hash)`.

O que decide se um objeto é pulado é o par **(origem, destino)** — quatro campos:

| Campo | Por que está na chave |
|---|---|
| `staging_key` | qual parquet |
| `source_hash` | metadado do parquet, vem do arquivo em `raw/` |
| `staging_etag` | o mesmo `raw/` gera parquet diferente quando o `raw_para_staging.py` muda |
| `target_table` | `schema.tabela` de destino |

Sem o `target_table` a idempotência olharia só a origem: renomear a tabela ou mudar o
schema no YAML deixa o arquivo intacto, e a carga pularia tudo com `skipped_already`
deixando o destino novo **vazio, em silêncio**. Use `--force` para recarregar de todo jeito.

Statuses: `loaded`, `dry_run`, `error`, `skipped_already`, `skipped_empty`.

## 6. Layout no MinIO

```
staging/<pastas>/<nome>.parquet          (entrada, declarada no YAML)
audit/bronze/execution_id=<uuid>/        (auditoria de cada execução)
```

## 7. Pré-requisito

Feito uma vez na VM, fora do escopo do script: `pg_duckdb` instalado e ativo no Postgres, e
um secret S3 (`duckdb.create_simple_secret`) criado numa sessão do usuário de
`DB_DW_USER_MCID`. Sem isso o `read_parquet` falha com erro de credenciais.

Se a role não puder alterar `duckdb.max_memory`/`duckdb.threads`, o script avisa uma vez e
segue com os defaults do servidor — não é motivo para abortar.

## 8. Ordem no pipeline

Rode **depois** do `raw_para_staging.py --apply`, que é quem popula a `staging/`. E ele,
por sua vez, depois do `mascarar_minio.py --apply`.
