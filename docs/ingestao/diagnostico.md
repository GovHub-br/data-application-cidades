# Diagnóstico da ingestão: Fase 0 do refactor raw → staging → bronze

Este documento é a entrega da Fase 0 do guia [`docs/refactor-ingestao.md`](../refactor-ingestao.md). Ele mapeia como a ingestão do cidades funciona hoje e como cada peça vira uma classe nova. Também registra as decisões tomadas e os desvios em relação ao guia.

- **Base analisada:** `origin/main` em `88b08cd` (2026-10-08).
- **Fora do escopo:** código de produção, que não foi alterado nesta fase.

## 0. Decisões tomadas na Fase 0

| Tema | Decisão |
|---|---|
| Escopo | Só o cidades. As DAGs do IPEA/MIR saem do refactor e não recebem mais suporte (lista na seção 2). |
| PoC | A `govhub-data-lakehouse` publicada só tem `extractors/` (que devolve `pa.Table` e usa singleton) e `core/storage`. Não tem `converters/`, `loaders/`, `raw/landing.py`, `layout.py` nem `pipeline/steps.py`. Implementamos pela especificação do guia. Da PoC vêm só o registry do `ExtractorFactory` e a interface do `StorageBackend`, ambos sem singleton. |
| Branch (9.4) | `refactor/ingestao-raw-staging-bronze` a partir da `main`. A `refactor/template-method-ingestao-lake` foi apagada: estava 477 commits atrás e lia tudo em DataFrame. |
| Carga do bronze | **Inegociável:** o bronze continua `select * from read_parquet(...)`, executado pelo dbt (`fonte_lake` nos `bronze_*`). A linhagem no OpenMetadata (`scripts/governance/sincronizar_lake.py`) depende disso. |
| LoadMode | Vive no dbt: `overwrite` = `table`, `merge` = `incremental` com `unique_key`, `append` = `incremental`. A família Loader em Python fica só com o `PostgresCopyLoader`, para prod sem MinIO (Fase 7). |
| Normalização de nomes | Na prata do dbt. A staging mantém o cabeçalho original. O converter só trata cabeçalho vazio (`column_<n>`), repetido (`<nome>_2`) e BOM. |
| Colunas técnicas (9.2) | `dt_ingest` + `_source_file`. `_source_file` vem do `filename => true` do `read_parquet`. `dt_ingest` é derivado dos segmentos `<AAAA-MM-DD>/<HHMMSS>` do caminho, na prata, pelo macro `lake_dt_ingest()`. Validado no piloto da Dotação: para o `overwrite` ter a data no caminho, o `latest/` guarda a cópia em `latest/<AAAA-MM-DD>/<HHMMSS>/` (decisão do Lucas, 09/10/2026). |
| Layout por data | `raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/<arquivo original>`, e a staging espelha (`…/part-<n>.parquet`). Data e hora = `run_after` da run no fuso America/Sao_Paulo (o dia da ingestão), nunca o `run_id`. Execuções no mesmo dia ficam em subpastas de horário; vale sempre a **última ingestão**. |
| Drift | Fase 8 do guia: drift estrutural raw → staging no converter e drift de dado bronze → prata no dbt. |
| Iceberg | Fase 9 do guia, a última de implementação: primeiro tudo funciona sem Iceberg. Catálogo-alvo Apache Polaris (REST do Iceberg, metadados no Postgres via JDBC). Documentação passa a ser a Fase 10. |
| Lakehouse | Toda adaptação deve deixar o caminho aberto para Iceberg, Delta e Hudi (seção 11). |
| Schedule | DAG migrada usa cron literal no decorator e deixa de usar o `get_dynamic_schedule`, que faz `Variable.get` no parse e viola a regra 7. |
| Providers | Cada extrator é um adaptador fino sobre um **hook** do provider (`HttpHook`, `S3Hook`, `SFTPHook`, `ImapHook`). Operators de transferência estão vetados (ver seção 7). As credenciais viram Airflow Connections (`AIRFLOW_CONN_*`). |
| Storage | `StorageBackend` próprio (local + s3), com o s3 sobre `S3Hook`, sem `ObjectStoragePath`/`s3fs`. |
| IMAP | Entra o `apache-airflow-providers-imap`. O `ImapHook.download_mail_attachments` grava o anexo em disco. |
| Fan-out | Sem uso no cidades: todas as 31 DAGs com fan-out por IDs do banco são do IPEA. |

## 1. Como a ingestão do cidades funciona hoje

### 1.1 Conjuntura (16 DAGs)

Cada DAG chama um `cliente_*` que busca o dado e o transforma em pandas (`fetch_and_transform_*`, `transformar_resposta`). Com o resultado, a DAG grava quatro destinos:

1. **Upsert no Postgres** via `ClientPostgresDB.insert_data` (`execute_values`, `ON CONFLICT`, f-string em SQL, lista inteira em memória), em schemas de domínio: `fgv`, `bacen`, `ibge`, `abecip`, `fipe`, `mrv`, `infomoney`, `novo_caged`. **Nenhum modelo dbt lê esses schemas.**
2. **`raw/<fonte>/<dado>.<ext>`** (full-refresh). Em MRV, `credito_pib`, SIDRA e `ibge`, a "raw" guarda os **registros já transformados**, não o payload da fonte.
3. **`raw/<fonte>/fallback_json/<dado>.json`**, uma cópia de conveniência.
4. **`staging/<fonte>/<dado>.parquet`**, 100% texto (`ingestor_lake._todas_colunas_texto`), full-refresh.

O `bronze_*` do dbt lê esse parquet via `fonte_lake` → `read_parquet`. Quem orquestra é o `conjuntura_dag`: dispara as ingestões por `TriggerDagRunOperator` e depois roda o `conjuntura_dbt` pelo Cosmos.

**Achado: perda de histórico nas extrações parciais.**
- ~~O `bacen_sgs_ingest_dag` extrai só 13 pontos~~ **(corrigido em 2026-10-08, testando contra a API real):** o SGS **ignora** `ultimos` passado como parâmetro de query, que é como o `cliente_bacen` chama, e devolve a série inteira (IPCA, série 433: 560 pontos, desde 01/1980). Para séries mensais, a DAG atual já traz o histórico completo, e o bronze não perde nada. O formato que de fato limita é o de caminho, `/dados/ultimos/N`. Séries **diárias** recusam a chamada sem janela (HTTP 406: exigem `dataInicial`, no máximo 10 anos) e só aceitam o formato de caminho.
- O `infomoney_imob` contorna o problema relendo `infomoney.acoes_imob` do Postgres para regravar a staging. Usa o Postgres como acumulador.
- A perda vale para `ibge` (`-20`) e `ibge_pnad_construcao_sidra` (`last 12`), cuja janela está no caminho e é respeitada pela API, e possivelmente para o MRV (só o trimestre mais recente). Infomoney (`compact`, os últimos 100 pregões, pela documentação da Alpha Vantage) ainda não foi conferido contra a API.
- **Esse é o caso do `merge`:** `incremental` com `unique_key` no dbt sobre todas as runs da staging. O Postgres de domínio deixa de ser necessário como acumulador.

### 1.2 Tesouro Gerencial MCid (3 DAGs)

Anexo de e-mail via IMAP (`cliente_email`): TSV com encoding detectado por `chardet` e linhas de cabeçalho puladas. Vai para `siafi.*` no Postgres. Só o `dotacao_execucao_outras_fontes_mcid` também grava a raw e a staging, lida pela `bronze_siafi_dotacao_execucao`.

### 1.3 Lake MCid (SFTP)

- `sftp_ingest_dag` → `scripts/sftp_para_minio.py`. Grava `raw/sftp/<pasta>/…` com controle incremental em `lake._ingest_minio_log`, e trata zip multivolume.
- `minio_transform_dag` faz duas etapas:
  - `mascarar_minio.py` **reescreve a raw in-place** (PII);
  - `raw_para_staging.py` converte para Parquet texto em streaming, normaliza nomes, descarta colunas de padding, descarta gêmeos e acrescenta `_source_file`, `_ingested_at` e `_source_hash`.
- As bronzes de far, fds, rural e reforma leem por `read_parquet(glob, filename => true, union_by_name => true)`.

### 1.4 Outros

- **`abecip_instituicoes_ingest_dag`:** lê `raw/abecip/<AAAA-MM>/` (gravada por outro time) e reescreve a staging. Na prática já é um conversor.
- **SharePoint:** `staging/sharepoint/*`, usado pela `linha_financiada_dbt`, não tem escritor na `main`.

### 1.5 Violações das regras do guia no código atual

| Regra | Onde |
|---|---|
| 1. Nada em memória | Todos os clientes montam `list[dict]`/DataFrame inteiro; `insert_data` com `execute_values`. |
| 2. Raw imutável | `mascarar_minio.py` reescreve a raw; MRV/`credito_pib`/SIDRA/`ibge` gravam registros transformados como raw; raw full-refresh sem run. |
| 3. Conversão não tipa | Já cumprida na staging. Mas os clientes renomeiam e transformam antes da raw. |
| 6. DAG sem lógica de dados | Tesouro MCid (parse de TSV na DAG), `ibge_ingest_dag` (achatamento), `abecip_instituicoes`. |
| 7. Sem consulta no parse | `ibge_ingest_dag` (`Variable.get` no topo), e todas as DAGs via `get_dynamic_schedule`. |
| 8. `run_id` sanitizado | Não se aplica hoje: os caminhos não têm run. |

## 2. Escopo

| Dentro (cidades): 24 DAGs de ingestão | Fora (IPEA/MIR, sem suporte) |
|---|---|
| **Conjuntura (16):** `abecip` ×3, `bacen` ×2, `fgv` ×2, `fipe`, `ibge` ×2, `infomoney`, `mrv` ×2, `novo_caged` ×3 | `siape` (14), `siafi` (3), `siorg` (3), `sisbolsas` |
| **Tesouro MCid (3):** `dotacao_execucao_outras_fontes_mcid`, `empenho_emendas_parlamentares`, `orcamento_mcid_por_acao` | `transfere_gov` (9), `transferegov_emendas` (12) |
| **Lake MCid (2):** `sftp_ingest_dag`, `minio_transform_dag` | `compras_gov` (6), `pncp` (2), `dados_abertos` (2), `sgac` |
| **Orquestração, não migra (3):** `conjuntura_dag`, `mcid_cosmos_dag`, `openmetadata_ingestion_dag` | `tesouro_gerencial` raiz e `mir` (9), `dashboard_servidores_dag` |

As DAGs de fora ficam como estão (nem migradas nem apagadas). Removê-las é outra decisão.

## 3. Fontes, formatos e LoadMode proposto

| DAG | Fonte / transporte | Formato nativo | Extração | Chave atual | LoadMode proposto |
|---|---|---|---|---|---|
| `incc_m_ingest_dag` | Sinduscon/FGV, HTTP | xlsx | completa | `mes` | `overwrite` |
| `icst_ingest_dag` | FGVDados, HTTP com login OutSystems + TLS legado | csv latin-1 `;` | completa | `mes` | `overwrite` |
| `fipezap_trimestral_ingest_dag` | FIPE, HTTP (URL fixa) | xlsx | completa | `data_referencia` | `overwrite` |
| `abecip_poupanca_trimestral_ingest_dag` | ABECIP, HTTP (link por scraping) | xlsx | completa | `data_referencia` | `overwrite` |
| `abecip_financiamentos_ingest_dag` | ABECIP, HTTP (link por scraping) | xlsx | completa | `data_referencia` | `overwrite` |
| `abecip_instituicoes_ingest_dag` | MinIO (`raw/abecip/<AAAA-MM>/`) | json | todas as competências | — | `overwrite` |
| `mrv` lançamentos / vendas | MRV RI, API mziq → link | xlsx | trimestre mais recente (a confirmar) | `periodo` | `merge (periodo)` a confirmar |
| `bacen_sgs_ingest_dag` | BACEN SGS, API | json | completa nas séries mensais (o `ultimos` em query é ignorado); diária exige janela | `tipo, data` | `overwrite` (série inteira); `merge (tipo, data)` só se alguma série diária entrar com janela |
| `bacen_credito_pib_ingest_dag` | BACEN, API | json | completa (a confirmar) | `data` | `overwrite` a confirmar |
| `ibge_ingest_dag` | IBGE agregados v3, API (config na Variable `IBGE_CONFIGURACOES`) | json aninhado | parcial (`-20`) | — | `merge` (chave a definir por agregado) |
| `ibge_pnad_construcao_sidra_ingest_dag` | SIDRA, API | json | parcial (`last 12`) | `periodo, categoria_id` | `merge (periodo, categoria_id)` |
| `infomoney_imob` | Alpha Vantage, API com `apikey` | json | parcial (`compact`) | `symbol, data_pregao` | `merge (symbol, data_pregao)` |
| `novo_caged` ×3 | PowerBI público, POST `querydata` | json DSR | completa (`obter_historico`) | `ano, mes` | `overwrite` |
| Tesouro MCid ×3 | IMAP, anexo | tsv, encoding variável | snapshot do relatório (a confirmar) | variável | `overwrite` a confirmar |
| `sftp_ingest_dag` | SFTP MCid | csv/txt/xlsx/mdb/zip | incremental por arquivo | — | por dataset (bronzes já existentes) |

Itens "a confirmar" se resolvem no PR de migração de cada DAG, com a justificativa na docstring.

## 4. Tabela "código atual → classe nova"

| Código atual | Classe nova | Observação |
|---|---|---|
| `cliente_bacen`, `cliente_bacen_imobiliario`, `cliente_ibge`, `cliente_ibge_sidra`, `cliente_infomoney`, `cliente_novo_caged` | `ApiExtractor` (`api`) | Sobre `HttpHook`: fonte pública declara `base_url` na DAG (Connection em memória, decisão do Lucas em 09/10/2026); Connection só onde há segredo. Retry tenacity, `stream`. Método e body (POST do PowerBI), paginação plugável, uma parte JSON por chamada. |
| `cliente_fgv` (INCC, ICST), `cliente_fipe`, `cliente_abecip`, `cliente_mrv` | `HttpFileExtractor` (`http_file`) | Sobre `HttpHook` com `stream`. URL fixa ou resolvedor genérico (`link_in_page` para a ABECIP, `latest_in_json_listing` para o catálogo da MZ/MRV). O login OutSystems e o TLS legado do ICST são um fluxo `http_session` declarado na DAG (passos genéricos, `LegacyTlsAdapter`). |
| `cliente_email` | `EmailAttachmentExtractor` (`email`) | Sobre `ImapHook.download_mail_attachments` (disco). |
| leitura de `raw/abecip/<AAAA-MM>/` em `abecip_instituicoes` | `ObjectStorageExtractor` (`object_storage`) | Sobre `S3Hook`, copia os objetos de um prefixo. |
| `cliente_sftp` + `scripts/sftp_para_minio.py` | `SftpExtractor` (`sftp`) | Sobre `SFTPHook`, mantém o incremental `lake._ingest_minio_log` e os zips. |
| `upload_raw_bytes`/`upload_raw_json` (`cliente_minio`) | `land` (`storage/landing.py`) | Sobe cada parte para `raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/` e apaga a cópia local. |
| `ClienteMinio` (boto3) | `S3StorageBackend` sobre `S3Hook` | O `ClienteMinio` continua só para os scripts legados do lake. |
| `ingestor_lake` (`IngestorLake`, `registros_para_staging_parquet`) | `FileConverter` + modelos | Template Method `convert()`: baixa → `_read()` em batches → `ParquetWriter` → sobe. |
| `scripts/raw_para_staging.py` (CSV/TXT/XLSX/MDB) | `CsvConverter`, `TxtConverter`, `XlsxConverter`, `MdbConverter` | Reaproveita `lake_utils` (`detectar_encoding`, `mdb_*`). A normalização de nomes **sai** do converter e vai para a prata. |
| decodificação DSR em `cliente_novo_caged` | `PowerBiDsrConverter` (implementado na Fase 6: máscaras `Ø`/`R`) | O DSR é formato estrutural (como o mdb), impraticável de desempacotar em SQL. A confirmar. |
| `fetch_and_transform_*`, `transformar_resposta`, renomes, `dt_ingest` nos clientes | Prata do dbt | Tipagem e renome só no dbt. |
| `ClientPostgresDB.insert_data` / `create_table_if_not_exists` / `alter_table` | Removido do caminho do cidades | O bronze é do dbt. `PostgresCopyLoader` (sobre `PostgresHook.copy_expert`) só na Fase 7. |
| `ClientPostgresDB` (consultas) | Mantido | Dashboards e DAGs do IPEA. |
| `get_dynamic_schedule` | Cron literal nas DAGs migradas | Mantido nas não migradas. |

Os `cliente_*` do cidades são removidos na Fase 6, quando nenhuma DAG os usar. Os do IPEA ficam intocados.

## 5. Estrutura do código novo e modelo de DAG

### 5.1 Pastas

```
plugins/ingestion/
├── layout.py                  # raw_prefix / staging_prefix(domain, dataset, ingested_at), safe_segment
├── dataset.py                 # DatasetSpec (domain, dataset, extractor, converter, load_mode, keys)
├── storage/                   # base_storage, storage_registry, landing (pouso + _SUCCESS),
│                              # models/{local_storage, s3_storage}
├── extractors/                # base_extractor (Extractor, RawFile), config_extractor,
│                              # extractor_registry, extractor_errors,
│                              # models/{api, http_file, email, object_storage, sftp}
├── converters/                # base_converter (FileConverter), config_converter,
│                              # converter_registry, converter_errors,
│                              # models/{csv, txt, json, xlsx, mdb, parquet, powerbi_dsr}
├── loaders/                   # load_types (LoadMode, LoadResult); base_loader, registry e
│                              # models/postgres_copy_loader só na Fase 7
└── pipeline/steps.py          # extract_to_raw, convert_to_staging

tests/ingestion/
├── conftest.py                # sys.path p/ plugins/, tmp_path, LocalStorageBackend, HTTP local
├── {storage,extractors,converters,loaders,pipeline}/test_contract.py
└── dags/test_<dag>.py         # um por DAG migrada
```

- **Caminhos no lake:**
  - `raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/<arquivo original>`;
  - `staging/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/part-<n>.parquet`.
  - Exemplo: `raw/ibge/sinapi/2026-10-08/060000/sinapi.json`.
  - O `<domain>` reaproveita o segmento atual (`fgv`, `bacen`, `ibge`…).
  - A ordem lexicográfica de `<AAAA-MM-DD>/<HHMMSS>` é a ordem cronológica, então "última ingestão" é o maior caminho.
- **Disco local:** o `work_dir` fica sob `LAKE_TMPDIR`.

### 5.2 Modelo de DAG migrada

- **Identidade:** mesmo `dag_id` e mesmo arquivo, porque o `conjuntura_dag` dispara por `dag_id`.
- **Schedule:** cron literal.
- **Docstring:** declara fonte, extração e LoadMode, com a justificativa. No `merge`, avisa que deleções na fonte não se propagam.
- **Configuração:** `DatasetSpec` no topo do arquivo, só com literais.
- **Credenciais:** só por `conn_id`, resolvido em runtime pelo hook.
- **Tasks:** duas, `extract_to_raw >> convert_to_staging`, que só chamam `pipeline/steps.py` e trocam o prefixo (string) por XCom.
- **Carga no bronze:** continua no dbt.
- **Teste em `tests/ingestion/dags/`:** cobre import, `dag_id`, cron, tags, tarefas, dependência, `DatasetSpec` e chamadas aos steps.

## 6. Infra e convenções

- **Airflow:** 3.3.2 (`apache/airflow:3.3.2-python3.11`), `LocalExecutor` standalone, DAGs em `airflow.sdk`. O `run_id` contém `:` e `+`, então passa pelo `safe_segment`.
- **Bibliotecas:**
  - pandas 3.0.5, pyarrow 25.0.1, numpy 2.4.6, duckdb 1.5.6 (só em scripts);
  - psycopg2 via `apache-airflow-providers-postgres` 7.0.2;
  - `mdbtools` na imagem.
- **Dependências:** `pyproject.toml` e `requirements.txt` precisam bater, porque `tests/test_dependencias_sincronizadas.py` reprova divergência. Entram:
  - os providers `http`, `amazon`, `sftp` (já na imagem oficial, faltam no venv dos testes);
  - `imap`;
  - eventualmente `ijson`.
- **Postgres:** o banco de **homologação** é **Postgres 15 com pg_duckdb 1.2.0** (confirmado pelo Lucas em 2026-10-08), então o `MERGE` nativo está disponível para o `PostgresCopyLoader` da Fase 7. A versão de produção fica a confirmar antes da Fase 7. A imagem local do compose é `postgres:17-alpine`, sem pg_duckdb.
- **Testes:** 8 arquivos em `tests/`, que fazem `sys.path.insert` à mão, sem `conftest` nem marcador `integration`.
- **CI:** `pytest tests` sem serviços, mais `lint-ci` só de SQL (`|| true`).
- **Lint local:** `make lint` roda black, ruff (E/F/W/C90, 90 colunas) e mypy estrito.
- **Commits:** Conventional Commits em português.

## 7. Estudo: providers do Airflow

Código lido na imagem `apache/airflow:3.2.2-python3.11`, mesma família da 3.3.2. A imagem oficial já traz amazon 9.29, http 6.0, sftp 5.8, ssh 5.0, ftp, common-io e postgres. **Não traz** o provider `imap` nem `s3fs`.

| Necessidade | Provider | Veredito |
|---|---|---|
| API JSON | `HttpOperator` devolve a resposta ao XCom (fere as regras 1 e 6). `HttpHook.run` aceita `stream`, Connection, `auth_type` e retry tenacity | `HttpHook` no `ApiExtractor` |
| Download de arquivo | `HttpToS3Operator` faz `response.content` + `load_bytes`: arquivo inteiro em memória | `HttpHook` (stream) no `HttpFileExtractor` |
| E-mail | `ImapAttachmentToS3Operator`: só o 1º anexo, bytes em memória. `ImapHook.download_mail_attachments` grava em disco | `ImapHook` (provider novo) |
| SFTP | `SFTPToS3Operator` faz streaming, mas é um arquivo por task, sem incremental. `SFTPHook` tem `retrieve_file` e `walktree` | `SFTPHook` no `SftpExtractor` |
| MinIO | `S3Hook`: `load_file` multipart, `download_file`, `list_keys`, `copy_object`, com `endpoint_url` na Connection | `S3StorageBackend` |
| Storage genérico | `ObjectStoragePath` (fsspec) exige `s3fs` | Descartado |
| Conversão para Parquet | Nenhum provider | Código nosso |
| Carga Postgres (Fase 7) | `PostgresHook.copy_expert` | `PostgresCopyLoader` |
| Disparo do dbt | `Asset` do core | Fora de escopo; evolução possível do `conjuntura_dag` |

## 8. Desvios do guia

- **§4 Loader para Postgres:** `PgDuckdbLoader` e `_ensure_table` em Python não existem. O papel deles é do dbt. O `LoadMode` mora no `fonte_lake`, declarado por fonte no `sources.yml` (`meta.load_mode` e `meta.keys`):
  - `overwrite` lê `latest/*/*/` (a cópia da última ingestão, com a partição de origem no caminho);
  - `append` lê todas as ingestões;
  - `merge` lê todas e fica com a mais recente por arquivo + chave.
  - Os três são **tabela recalculada** (`materialized='table'`, troca atômica), não incremental (decisão do Lucas).
  - As regras de validação são as mesmas de `ingestion.loaders.validate_load` e valem na compilação.
  - O `PostgresCopyLoader` fica para a Fase 7.
- **§3.3 / §4 Normalização:** acontece na prata, não no loader.
- **Fase 2:** sem `PostgresExtractor`, SOAP nem SQL Server, porque nenhuma fonte do cidades usa.
- **Converter extra:** `powerbi_dsr`, a confirmar.

## 9. Conflitos a decidir

| Conflito | Decidir antes da | Proposta |
|---|---|---|
| Layout por data × `meta.caminho` fixo do `fonte_lake` | Fase 3 | `meta.caminho` com glob `…/<dataset>/*/*/*.parquet`. Para `overwrite`, a staging retém só a última ingestão, e o `select *` continua puro. Para `merge`/`append`, retém todas, e o incremental deduplica a chave pela ingestão mais recente (maior `filename`). A raw guarda todas as ingestões. |
| `minio_transform_dag` varre a `raw/` inteira e converteria os novos prefixos | Fase 5 (piloto) | Excluir os prefixos dos datasets migrados da varredura até o SFTP migrar. |
| Raw imutável × mascaramento in-place | Fase 6 (SFTP) | — |
| Bronzes do SFTP dependem dos nomes normalizados pelo `raw_para_staging` | Fase 6 (SFTP) | Mover a normalização para a prata dessas bronzes. |
| Prod sem MinIO quebra o bronze via `read_parquet` (9.1) | Fase 7 | — |
| ~~Versão do Postgres de homologação e do pg_duckdb~~ | — | Resolvido: Postgres 15, pg_duckdb 1.2.0. Produção a confirmar antes da Fase 7. |

## 10. Ordem de migração

| # | DAG(s) | Por quê nessa posição |
|---|---|---|
| 1 | **`fgv/incc_m` (piloto proposto)** | Um xlsx público, sem credencial, `overwrite`, com `bronze_fgv_incc_m` dependente. **Diferença esperada:** os nomes passam a ser o cabeçalho do xlsx, então a prata do INCC precisa renomear. |
| 2 | `fgv/icst` | Mesmo extrator, mais login e TLS legado. |
| 3 | `fipe`, `mrv` ×2, `abecip_poupanca`, `abecip_financiamentos` | `http_file` com `url_resolver`. |
| 4 | `bacen_sgs`, `credito_pib`, `infomoney` | API JSON. O `bacen_sgs` é `overwrite` (série inteira); o primeiro `merge` real é o `infomoney`, se o `compact` se confirmar. |
| 5 | `ibge`, `ibge_sidra` | JSON aninhado; tira o `Variable.get` do parse. |
| 6 | `novo_caged` ×3 | PowerBI DSR. |
| 7 | `tesouro_gerencial/mcid` ×3 | E-mail (provider `imap`). |
| 8 | `abecip_instituicoes` | `object_storage`. |
| 9 | `sftp` + `minio_transform` | Depende dos conflitos de mascaramento e normalização. |

## 11. Etapa final: detecção de drift (Fase 8 do guia)

O detalhamento das tarefas e o critério de aceite estão na Fase 8 do guia.

- **8.1 Raw → staging (Python).** O `FileConverter` grava o retrato `_schema.json` de cada partição e o compara com o da ingestão anterior. Detecta coluna nova ou sumida, ordem, cabeçalho, volume, formato, encoding e aba. A política por dataset fica no `DatasetSpec` (`on_schema_drift`: `warn`/`fail`). Com `fail`, a partição não é publicada e o bronze segue com a última ingestão boa.
  - O Template Method do converter já nasce na Fase 3 com o passo reservado, para a Fase 8 não precisar reabrir o contrato.
- **8.2 Bronze → prata (dbt).** Parte do que já existe e generaliza para todos os bronzes migrados:
  - `sem_drift_de_colunas`;
  - `conjuntura_contrato_do_staging` (piso de linhas);
  - retratos `ouro_conjuntura_qualidade_schema` e `_schema_drift`.
  - **O que é novo:** retrato de perfil por coluna da prata e um modelo de drift de dado (volume, nulos, categorias, faixas, frescor), com testes genéricos de limiar, `warn` por padrão.
- **Decisões que ficam para o início da Fase 8:**
  - padrão de `on_schema_drift` (proposta: coluna sumida = `fail`, coluna nova = `warn`);
  - limiares por modelo;
  - se vale adotar um pacote dbt (elementary, dbt_expectations). Hoje o projeto não tem `packages.yml`.

## 12. Compatibilidade com o lakehouse (Iceberg, Delta, Hudi)

Restrição de desenho para todas as fases. A migração em si continua fora de escopo (seção 8 do guia).

- **Carga:** `LoadMode`, `LoadResult` e o `LoaderFactory` existem em Python, mesmo com o bronze no dbt hoje. Um `IcebergLoader`, `DeltaLoader` ou `HudiLoader` entra como estratégia registrada, sem mexer em extratores, conversores nem DAGs.
- **Staging:** Parquet texto, particionado por `<AAAA-MM-DD>/<HHMMSS>`, é a entrada natural desses loaders. A partição por data vira a partição da tabela.
- **Drift:** o retrato `_schema.json` da 8.1 vira a entrada da política de *schema evolution* dos formatos de tabela. Iceberg e Delta evoluem schema nativamente; o retrato decide se a evolução é aceita.
- **Storage:** a interface `StorageBackend` não amarra ao MinIO nem ao `S3Hook`. Um catálogo ou storage de lakehouse entra como outra implementação.
- **Iceberg (Fase 9):** `CatalogBackend` `iceberg_rest` (Polaris) e `iceberg_sql` (SqlCatalog, testes offline), `TableBackend` Iceberg e `IcebergLoader` portados da PoC sem singleton. O `IcebergLoader` escreve por record batch numa transação, em vez da `pa.Table` inteira do `Transformer` da PoC.
- **dbt:** o LoadMode como materialização dbt é portável (`table`/`incremental` existem em dbt-trino e dbt-spark), mas o `fonte_lake`/`read_parquet` é específico do pg_duckdb. Na migração ao lakehouse, o bronze passa a ler a tabela Iceberg/Delta/Hudi, não o Parquet.
