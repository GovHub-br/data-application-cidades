# Refatoração da ingestão — data-application-cidades

## 1. Objetivo

Reorganizar a ingestão em três etapas com responsabilidades separadas, seguindo o desenho da PoC
`govhub-data-lakehouse` (https://github.com/bottinolucas/govhub-data-lakehouse), adaptado ao cidades,
onde o bronze vive no Postgres:

```
fonte --Extractor--> raw/<domínio>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/       formato original, intocado (MinIO)
      --FileConverter--> staging/<domínio>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/  Parquet (MinIO)
      --Loader--> bronze (Postgres)                         -> dbt: prata -> ouro
```

| Etapa | Padrão | Responsabilidade |
|---|---|---|
| Extração | Strategy + Factory | Copiar o dado da fonte para a raw no formato original |
| Conversão | Template Method + Factory | Qualquer arquivo (csv, txt, json, xlsx, mdb...) -> Parquet |
| Carga | Template Method + Factory | Parquet da staging -> tabela bronze no Postgres |

O que NÃO muda: raw e staging no MinIO organizados por domínio (data mesh); bronze, prata e ouro no
Postgres; bronze e prata em schemas únicos; ouro por linha de negócio (`gold_conjuntura`, `gold_far`...);
toda tipagem e regra de negócio no dbt.

## 2. O que portar da PoC e o que adaptar

| Componente da PoC | Ação no cidades |
|---|---|
| `extractors/` (`Extractor`, `ExtractorConfig`, `RawFile`, `ExtractorFactory`, `api`, `postgres`, `object_storage`) | Portar o contrato. Criar estratégias para as fontes reais que faltarem (descobrir na Fase 0). |
| `raw/landing.py` (`RawLanding`) + `layout.py` | Portar. O pouso virou a função `storage.land` (helper do storage, sem pacote próprio). Respeitar a organização por domínio que já existe no MinIO. |
| `converters/` (base + csv, txt, json, xlsx, mdb, parquet) | Portar inteiro. Substitui a conversão raw -> staging atual. |
| `loaders/base_loader.py` + `load_types.py` (`LoadMode`, `LoadResult`) + `loader_registry.py` | Portar o template e os modos. |
| `IcebergLoader`, `DeltaLoader`, `HudiLoader` | Iceberg entra na Fase 9 (fase final, catálogo Polaris). Delta e Hudi ficam para depois; a Factory já deixa o caminho aberto. |
| (não existe na PoC) | Criar `PostgresCopyLoader` e `PgDuckdbLoader` (seção 4). |
| `pipeline/steps.py` | Portar (`extract_to_raw`, `convert_to_staging`, `load_to_bronze`). |
| Singleton nas classes base | NÃO portar para extractors, converters e loaders. Só clientes de conexão podem ter cache. |

## 3. Regras não negociáveis (performance e robustez)

1. **Nada de dataset inteiro em memória.**
   - Extractors gravam cada parte em disco (`extract(work_dir) -> Iterator[RawFile]`), e a raw sobe e
     apaga a parte antes da próxima.
   - Extração de Postgres/SQL Server via `COPY ... TO STDOUT` ou cursor server-side, nunca `fetchall()`.
   - Conversores e loaders trabalham por record batch (`pyarrow`), nunca por `DataFrame` inteiro.
2. **Raw é imutável.** Byte a byte o que a fonte entregou. Reprocessar nunca exige chamar a fonte de novo.
3. **Conversão não tipa.** Texto vira coluna `string`; inferência de tipo do CSV desligada (ela só olha o
   primeiro bloco e quebra com arquivos heterogêneos). JSON aninhado vira texto JSON e é desempacotado
   no dbt. Os casos de borda a cobrir:
   - NULL distinto de string vazia (`quoted_strings_can_be_null=False`);
   - BOM no cabeçalho;
   - cabeçalho vazio vira `column_<n>`; cabeçalho repetido vira `<nome>_2`;
   - encoding configurável por dataset (muitas fontes BR são `latin-1` com `;`).
4. **Modo de carga explícito** (`LoadMode`):
   - `overwrite` (padrão, Full Loader): substitui a tabela atomicamente; deleções na fonte se propagam;
   - `merge` + `keys`: upsert para extração parcial (uma janela, um ano); deleções NÃO se propagam,
     então documente isso na DAG;
   - `append`: só para extração incremental que nunca reentrega linha.
   Validação: `merge` sem `keys` é erro; `keys` fora de `merge` é erro; chave inexistente no dado é erro.
5. **Atomicidade.** Leitor nunca vê tabela vazia ou pela metade.
6. **DAG sem lógica de dados.** A DAG só liga os passos de `pipeline/`. Entre tasks trafegam caminhos
   (XCom pequeno), nunca dados. Nada de achatar JSON, renomear ou filtrar dentro da DAG.
7. **Config da DAG sem consulta ao banco no parse.** Use `os.environ.get()` para o que é lido no
   topo do arquivo; `Variable.get()` só dentro de task.
8. **Partição por data de ingestão.** O caminho usa a data e a hora da execução (`run_after` da
   run, no fuso America/Sao_Paulo), em `<AAAA-MM-DD>/<HHMMSS>/`, nunca o `run_id` (que tem `:` e
   `+`). Duas execuções no mesmo dia ficam em subpastas de horário, e vale sempre a última
   ingestão. Segmentos vindos de fora passam por `layout.safe_segment`.

## 4. Loader para Postgres (o que é novo em relação à PoC)

Mesmo template da PoC: `Loader.load()` fixo (valida modo/keys -> baixa os Parquet da run -> unifica
schemas -> stream de batches -> `_ensure_table` -> `_overwrite` / `_merge` / `_append`). Duas
estratégias registradas no `LoaderFactory`:

### `pg_duckdb` — o Postgres lê o Parquet direto do MinIO

O dado não passa pelo worker do Airflow; o Postgres faz tudo no servidor. Mais rápido quando o Postgres
alcança o MinIO (é o caminho atual do bronze, via pg_duckdb).

```sql
-- overwrite, numa transação
BEGIN;
TRUNCATE bronze.<tabela>;
INSERT INTO bronze.<tabela> (<colunas>)
SELECT <colunas normalizadas + colunas técnicas>
FROM read_parquet('s3://<bucket>/staging/<domínio>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/*.parquet');
COMMIT;
```

### `postgres_copy` — streaming do worker com COPY

Para quando o Postgres não alcança o object storage (ex.: prod sem MinIO). O loader lê cada batch do
Parquet e envia com `COPY bronze.<tabela> (...) FROM STDIN` (psycopg3, `cursor.copy()` +
`copy.write_row()` ou CSV em buffer por batch). Substitui o caminho atual do `ClientPostgresDB`, que
carrega tudo em DataFrame.

### Semântica dos modos no Postgres

| Modo | Implementação |
|---|---|
| `overwrite` | `BEGIN; TRUNCATE; COPY/INSERT; COMMIT` numa transação. Leitores esperam o lock, não veem tabela vazia. Não usar `DROP` + `RENAME` (quebra views do dbt que dependem da tabela). |
| `merge` | `COPY` para tabela temporária, depois `MERGE` (PG 15+) ou `INSERT ... ON CONFLICT (<keys>) DO UPDATE` (exige constraint única nas keys), tudo numa transação. Chave duplicada dentro da mesma carga é erro. |
| `append` | `COPY`/`INSERT` direto. |

### `_ensure_table`

- Cria `bronze.<tabela>` se não existir: colunas `text` + colunas técnicas.
- Coluna nova na fonte vira `ALTER TABLE ... ADD COLUMN ... text`.
- Coluna que sumiu da fonte continua existindo e recebe NULL.
- Identificadores sempre com aspas (`psycopg.sql.Identifier`), nunca f-string em SQL.

### Normalização de nomes e colunas técnicas

O cidades já tem uma convenção: colunas técnicas e normalização de nomes para o padrão Postgres
feitas por CTE na carga do bronze, e a macro `add_metadata_timestamps` na prata/ouro. O loader
**reproduz a convenção existente** (descobrir na Fase 0), sem inventar outra.

- Normalização: snake_case, sem acento, sem caractere especial.
- Colisão após normalizar, ou nome acima de 63 bytes (limite do Postgres), é erro explícito, não
  truncamento silencioso.
- As colunas `_source_file` e `_ingested_at` da PoC só entram se não duplicarem o que já existe.

## 5. Ambiente de produção sem MinIO

Hoje prod vai direto ao DW. Com raw/staging, há três opções:

1. **Provisionar o MinIO em prod antes.** Mantém um único fluxo; depende de infra.
2. **`StorageBackend` local + as três etapas numa única task em prod.** No Kubernetes cada task roda
   num pod diferente e o disco local não é compartilhado, então caminhos entre tasks quebrariam. Um
   `run_ingestion()` chama os três passos no mesmo pod, e a DAG escolhe a topologia por variável de
   ambiente (`PIPELINE_ENV`): 3 tasks em dev, 1 task em prod. Carga com `postgres_copy`.
3. **Volume compartilhado (PVC ReadWriteMany)** entre os pods do Airflow.

Recomendação: opção 2 como ponte até o MinIO de prod existir. **Decisão do Lucas antes da Fase 7.**

## 6. Metodologia

- **Branch.** `refactor/ingestao-raw-staging-bronze`, ou reaproveitar uma branch existente nessa
  direção, rebaseada na `main`.
- **TDD estrito.** Teste falhando -> implementação mínima -> refatoração. Um commit por ciclo
  verde (ou por classe).
- **Testes de contrato.** Uma suíte parametrizada que roda contra TODAS as implementações de cada
  família (todo `Extractor` gera `RawFile` em disco; todo `FileConverter` gera Parquet só com strings
  a partir de texto; todo `Loader` respeita os três modos). Implementação nova entra na lista e herda
  os testes.
- **Testes unitários offline.** `LocalStorageBackend`, servidor HTTP local, `tmp_path`, mdbtools
  simulado.
- **Testes de integração** (Postgres com pg_duckdb, MinIO) marcados com `@pytest.mark.integration`
  e pulados quando o serviço não responde.
- **Commits** no padrão Conventional Commits, em português, como o repositório já usa (confirmar).
- **Lint.** O mesmo do repositório (black/isort/ruff, conferir) passando em todo commit.
- **Paradas.** Cada fase termina com um resumo e PARA para revisão. Nada de seguir para a próxima
  fase sem aprovação.

## 7. Plano de desenvolvimento

### Fase 0 — Diagnóstico (sem código de produção)

- [ ] Mapear a ingestão atual: DAGs por domínio, como chegam à raw, como viram Parquet, como o
  bronze é carregado (pg_duckdb, CTE, DAG geral que dispara o dbt), `ClientPostgresDB` e quem o usa.
- [ ] Listar fontes e formatos reais (IBGE, BACEN, ABECIP, FGV, FipeZAP, CAGED, SharePoint...) e
  o encoding/delimitador de cada uma.
- [ ] Levantar: convenção de nomes e colunas técnicas do bronze, versão do Postgres (MERGE?),
  versão do Airflow, cliente de storage existente, estrutura de testes, lint, CI.
- [ ] Entregar `docs/ingestao/diagnostico.md` com a tabela "código atual -> classe nova" e a lista
  de DAGs a migrar, da mais simples para a mais complexa.

**Critério de aceite:** diagnóstico revisado pelo Lucas; DAG piloto escolhida.

### Fase 1 — Fundação

- [ ] Branch criada.
- [ ] Estrutura de pastas: `extractors/`, `raw/`, `converters/`, `loaders/`, `pipeline/`, `layout.py`.
- [ ] Storage: reaproveitar o cliente que existe ou portar `StorageBackend` (local + s3) da PoC.
- [ ] Fixtures de teste, marcador `integration`, testes de contrato vazios.

**Critério de aceite:** suíte roda verde e vazia no CI; nenhuma DAG alterada.

### Fase 2 — Extratores (Strategy + Factory)

- [ ] Contrato: `Extractor.extract(work_dir) -> Iterator[RawFile]`, `from_config`,
  `ExtractorFactory.register(...)`, sem singleton.
- [ ] Teste de contrato primeiro, depois cada estratégia: `api` (com paginação opcional),
  `postgres` (COPY), `object_storage`, e as fontes específicas levantadas na Fase 0.
- [ ] `storage.land`: sobe para `raw/<domínio>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/` e apaga a cópia local.

**Critério de aceite:** extrair uma tabela de 2 milhões de linhas sem crescimento de memória
proporcional (medir RSS no teste de integração); raw idêntica byte a byte ao que a fonte entregou.

### Fase 3 — Conversores (Template Method + Factory)

- [ ] `FileConverter.convert()` fixo (baixa -> `_read()` em batches -> ParquetWriter -> sobe para
  staging). Cada formato implementa só `_read`.
- [ ] csv, txt (delimitador obrigatório), json/ndjson (`record_path`), xlsx (aba configurável),
  mdb/accdb (mdbtools), parquet (passa direto).
- [ ] `mdbtools` instalado na imagem do Airflow.

**Critério de aceite:** todos os casos de borda da seção 3 cobertos por teste; arquivo só com
cabeçalho gera Parquet vazio com schema; formato desconhecido lista os registrados no erro.

### Fase 4 — Loaders para Postgres (Template Method + Factory)

- [ ] `Loader.load()` fixo + `LoadMode` + `LoadResult`.
- [ ] `PostgresCopyLoader` primeiro (funciona em todo ambiente), depois `PgDuckdbLoader`.
- [ ] Normalização de nomes e colunas técnicas seguindo a convenção atual (seção 4).

**Critério de aceite:** testes de integração provando:

- `overwrite` remove linhas que sumiram da fonte;
- um leitor concorrente nunca vê a tabela vazia;
- `merge` atualiza e insere sem duplicar;
- coluna nova na fonte é adicionada;
- `ClientPostgresDB` não é mais usado no caminho de ingestão.

### Fase 5 — Pipeline + DAG piloto

- [ ] `pipeline/steps.py` (`extract_to_raw`, `convert_to_staging`, `load_to_bronze`).
- [ ] Migrar UMA DAG (a mais simples da Fase 0) para três tasks que só chamam esses passos.
- [ ] Rodar em dev lado a lado com a versão antiga e comparar o bronze: contagem de linhas,
  colunas e um checksum por coluna.
- [ ] `dbt build` dos modelos dependentes passando.

**Critério de aceite:** bronze novo equivalente ao antigo (ou diferenças explicadas, ex.: tipos
agora texto, deleções agora propagadas); `dbt build` verde.

### Fase 6 — Migração dos demais domínios

- [ ] Uma DAG (ou um domínio) por PR, com a mesma comparação da Fase 5.
- [ ] Escolher o `LoadMode` de cada dataset e justificar na docstring da DAG.
- [ ] Ajustar modelos dbt que dependiam de tipos ou nomes que agora mudam.
- [ ] Remover o código antigo de ingestão quando nenhuma DAG o usar.

**Critério de aceite:** nenhuma DAG com lógica de dados; código antigo removido; testes e dbt verdes.

### Fase 7 — Produção

- [ ] Implementar a opção escolhida na seção 5.
- [ ] Validar em homologação (Airflow na VM `mcid-airflow`) antes de prod.

**Critério de aceite:** uma execução completa em homologação com o mesmo resultado de dev.

### Fase 8 — Detecção de drift

Última fase de implementação. Duas frentes, uma em cada fronteira do pipeline.

**8.1 Drift estrutural raw -> staging (Python, no converter).**

- [ ] O `FileConverter.convert()` ganha um passo fixo depois de escrever o Parquet: grava o retrato
  `_schema.json` da partição (colunas na ordem, número de linhas, formato, encoding e delimitador
  detectados, abas do xlsx, tabelas do mdb, chaves do json) e o compara com o retrato da ingestão
  anterior do mesmo dataset.
- [ ] `SchemaDriftReport`: coluna nova, coluna sumida, ordem alterada, cabeçalho vazio ou repetido
  novo, variação de linhas fora da faixa, mudança de formato, encoding ou aba.
- [ ] Política por dataset no `DatasetSpec` (`on_schema_drift`: `warn` ou `fail`). Com `fail`, a
  partição não é publicada na staging, e o bronze continua com a última ingestão boa.
- [ ] Relatório em `staging/<domínio>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/_drift.json` e no log da task.
- [ ] Teste de contrato: todo `FileConverter` gera o retrato, e o comparador acusa cada tipo de drift.

**8.2 Drift de dado bronze -> prata (dbt).**

- [ ] Generalizar o que já existe no conjuntura para todos os bronzes migrados:
  `sem_drift_de_colunas` (contrato de colunas), `conjuntura_contrato_do_staging` (piso de
  linhas) e os retratos `ouro_conjuntura_qualidade_schema` / `_schema_drift`.
- [ ] Retrato de perfil por coluna da prata a cada execução (taxa de nulos, distintos, mínimo,
  máximo e média dos numéricos, faixa de datas), materializado como incremental.
- [ ] Modelo de drift de dado comparando o retrato atual com o anterior, mais testes genéricos com
  limiar por modelo (variação de volume, de taxa de nulos, categoria nova fora do domínio, frescor),
  `warn` por padrão e `error` onde a quebra tiver de ser dura.
- [ ] Resultados gravados em `lake._dbt_log` pelo `on-run-end` que já existe.

**Critério de aceite:** um arquivo da raw com coluna removida falha a conversão (dataset `fail`) ou
gera `_drift.json` (dataset `warn`), sem publicar staging quebrada; uma mudança artificial de
distribuição na prata dispara o teste de drift de dado no `dbt build`.

### Fase 9 — Suporte a Iceberg (Polaris)

Última fase de implementação: o pipeline já funciona sem Iceberg, e esta fase acrescenta o formato
de tabela como mais uma estratégia de carga. Delta e Hudi continuam fora.

- [ ] Apache Polaris como catálogo (API REST do Iceberg, metadados no Postgres via JDBC), como
  serviço no compose/infra.
- [ ] `CatalogBackend` portado da PoC, sem singleton: `iceberg_rest` (Polaris, OAuth2 client
  credentials) e `iceberg_sql` (pyiceberg `SqlCatalog`, para testes offline com sqlite e disco).
- [ ] `TableBackend` Iceberg portado da PoC: append, overwrite, upsert, evolução de schema e time
  travel.
- [ ] `IcebergLoader` registrado no `LoaderFactory`, no lugar do `Transformer` da PoC: lê a staging
  por record batch e escreve numa transação (`overwrite` atômico, `merge` por `keys` com chave
  duplicada = erro, `append`). Colunas `string`; coluna nova na fonte vira `add_column`.
- [ ] Dependência `pyiceberg==0.12.0` (constraints do Airflow 3.3.2).

**Critério de aceite:** contrato do `Loader` verde para o `IcebergLoader` contra o `iceberg_sql`
offline e contra o Polaris (integração); carga de 2 milhões de linhas sem crescimento de memória
proporcional. Migrar o bronze/dbt para ler as tabelas Iceberg continua fora de escopo.

### Fase 10 — Documentação

- [ ] README da ingestão: como adicionar fonte, formato e destino (um módulo + `register`, nada mais).
- [ ] Diagrama atualizado (Extração / Conversão / Carga).
- [ ] ADR curto registrando: modos de carga, ausência de singleton, tipagem só no dbt, decisão de prod.

## 8. Fora de escopo deste refactor

- Atualizar a versão do Airflow (PR separado).
- Migrar o bronze/dbt para ler tabelas Iceberg, e Trino (o suporte a Iceberg em si é a Fase 9).
- Catálogo/OpenMetadata.

## 9. Decisões em aberto (perguntar ao Lucas, não assumir)

1. Topologia de prod sem MinIO (seção 5).
2. Onde ficam as colunas técnicas e a normalização de nomes, se a convenção atual for ambígua.
3. `LoadMode` de cada dataset (overwrite vs merge) quando a extração não for claramente completa.
4. Reaproveitar uma branch existente ou criar uma nova.