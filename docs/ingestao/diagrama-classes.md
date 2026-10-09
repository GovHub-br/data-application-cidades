# Diagrama de classes da ingestão (`plugins/ingestion`)

Documentação técnica viva do pacote `plugins/ingestion`. Os diagramas estão em [Mermaid](https://mermaid.js.org/syntax/classDiagram.html) (`classDiagram`): o GitHub e o VS Code os renderizam direto, e o texto é editado como código, com diff no PR.

**Regra de manutenção:** toda mudança de classe, assinatura ou relação no pacote atualiza este arquivo no mesmo commit.

## Notação (UML)

| Elemento | Sintaxe Mermaid | Significado |
|---|---|---|
| `<<abstract>>` | `<<abstract>>` | Classe abstrata (ABC); métodos abstratos terminam em `*` |
| `<<interface>>` | `<<interface>>` | Protocolo estrutural (`typing.Protocol`) |
| `<<dataclass>>` | `<<dataclass>>` | `@dataclass(frozen=True)`, objeto de valor imutável |
| `<<enumeration>>` | `<<enumeration>>` | `Enum` (aqui, `StrEnum`) |
| `<<module>>` | `<<module>>` | Funções de módulo, sem classe |
| `<<external>>` | `<<external>>` | Classe de biblioteca (providers do Airflow, pyarrow…) |
| Herança | `A <\|-- B` | B herda de A |
| Realização | `A <\|.. B` | B satisfaz a interface A |
| Composição | `A *-- B` | A contém B (B não existe sem A) |
| Agregação | `A o-- B` | A referencia B |
| Dependência | `A ..> B` | A usa B |
| Classe | `metodo()$` | Método de classe/estático |

## Visão geral: o fluxo

```mermaid
classDiagram
    direction LR
    class Extractor {
        <<abstract>>
        +extract(work_dir) Iterator~RawFile~*
    }
    class StorageBackend {
        <<abstract>>
    }
    class landing {
        <<module>>
        +land(storage, parts, prefix) LandingResult
    }
    class FileConverter {
        <<abstract>>
    }
    class layout {
        <<module>>
        +raw_prefix(domain, dataset, partition) str
        +staging_prefix(domain, dataset, partition) str
    }

    Extractor ..> landing : RawFile (uma parte por vez)
    landing ..> StorageBackend : put_file / _SUCCESS
    landing ..> layout : prefixo da partição
    FileConverter ..> StorageBackend : raw → staging → latest/
```

## `layout`: caminhos do lake

```mermaid
classDiagram
    class layout {
        <<module>>
        +TIMEZONE: ZoneInfo = America/Sao_Paulo
        +safe_segment(value: str) str
        +ingestion_partition(run_after: datetime) str
        +raw_prefix(domain, dataset, partition) str
        +staging_prefix(domain, dataset, partition) str
        +latest_prefix(domain, dataset) str
    }
```

- **Partição:** `AAAA-MM-DD/HHMMSS` da ingestão, no horário de Brasília.
- **Prefixos:** `raw|staging/<domain>/<dataset>/<partição>/`.

## `storage`: object storage do lake

```mermaid
classDiagram
    class StorageBackend {
        <<abstract>>
        +put_file(key: str, local_path: Path)*
        +get_file(key: str, local_path: Path)*
        +list(prefix: str) list~str~*
        +delete(key: str)*
        +copy(src_key: str, dst_key: str)*
        +exists(key: str) bool*
    }
    class LocalStorageBackend {
        +root: Path
        -_resolve(key) Path
        -_existing(key) Path
    }
    class S3StorageBackend {
        +bucket: str
        +conn_id: str | None = "minio_lake"
        +page_size: int | None
        +hook: S3Hook
    }
    class PrefixedStorage {
        +inner: StorageBackend
        +prefix: str
    }
    class StorageFactory {
        -_registry: dict~str, type~
        +register(name)$ decorator
        +create(name, **kwargs)$ StorageBackend
    }
    class config_storage {
        <<module>>
        +storage_from_env(env) StorageBackend
    }
    class landing {
        <<module>>
        +SUCCESS_MARKER = "_SUCCESS"
        +land(storage, parts: Iterable~Part~, prefix, details) LandingResult
        +publish_latest(storage, partition_prefix, latest_prefix) list~str~
    }
    class Part {
        <<interface>>
        +name: str
        +path: Path
        +size: int
        +sha256: str
    }
    class LandingResult {
        <<dataclass>>
        +prefix: str
        +keys: tuple~str~
        +total_bytes: int
    }
    class StorageError
    class ObjectNotFoundError
    class S3Hook {
        <<external>>
    }

    StorageBackend <|-- LocalStorageBackend
    StorageBackend <|-- S3StorageBackend
    S3StorageBackend o-- S3Hook
    StorageBackend <|-- PrefixedStorage
    PrefixedStorage o-- StorageBackend : embrulha
    StorageFactory ..> StorageBackend : cria
    config_storage ..> StorageFactory
    landing ..> StorageBackend
    landing ..> Part
    landing ..> LandingResult
    StorageError <|-- ObjectNotFoundError
    StorageBackend ..> ObjectNotFoundError : levanta
```

- **Registro:** `local` e `s3` se registram na `StorageFactory` com `@StorageFactory.register`.
- **Sem singleton:** cada `create` devolve uma instância nova.
- **`PrefixedStorage`:**
  - embrulha um backend e enxerga só uma pasta dele;
  - `storage_from_env` o aplica quando há `INGESTION_STORAGE_PREFIX` (ex.: `tests/`, para rodar o pipeline real fora de `raw/` e `staging/`);
  - recusa `raw/` e `staging/` como prefixo.
- **`land`:** sobe uma parte por vez, apaga a cópia local e grava `_SUCCESS` com o manifesto por último. `details` acrescenta campos ao manifesto de cada parte.
- **`publish_latest`:**
  - espelha uma partição com `_SUCCESS` em `latest/<AAAA-MM-DD>/<HHMMSS>/`, que é o que o bronze lê (a partição no caminho dá o `dt_ingest`);
  - o `_SUCCESS` fica na raiz do `latest/`;
  - ordem: copia os novos, remove os que sobraram e copia o `_SUCCESS` por último.

## `extractors`: fonte → disco, no formato original

```mermaid
classDiagram
    class Extractor {
        <<abstract>>
        +config: ExtractorConfig
        +ingestion_time: datetime
        +from_config(config, ingestion_time)$ Extractor
        +extract(work_dir: Path) Iterator~RawFile~*
    }
    class ApiExtractor
    class HttpFileExtractor
    class EmailAttachmentExtractor {
        +hook_class = ImapHook
    }
    class HttpSessionExtractor
    class ObjectStorageExtractor
    class ExtractorFactory {
        -_registry: dict~str, type~
        +register(name)$ decorator
        +create(config, ingestion_time)$ Extractor
    }
    class ExtractorConfig {
        <<dataclass>>
        +source: str
        +conn_id: str = ""
        +base_url: str | None
        +requests: tuple~HttpRequest~
        +adapter: HTTPAdapter | None
        +mail: MailQuery | None
        +session: HttpSession | None
        +objects: ObjectQuery | None
    }
    class ObjectQuery {
        <<dataclass>>
        +prefix: str
        +pattern: str
        +rename: str | None
    }
    class HttpSession {
        <<dataclass>>
        +steps: Sequence~Step~
        +variables: Mapping~str, str~
        +mounts: Mapping~str, HTTPAdapter~
    }
    class Step {
        <<interface>>
        +execute(run) RawFile | None
    }
    class Request {
        <<dataclass>>
        +name, method, url
        +params, data, json, headers
        +capture: Mapping~str, Capture~
        +expect: tuple~Check~
        +drop_empty: tuple~str~
        +check_status: bool
    }
    class Download {
        +filename: str
        +content_types: tuple~str~
    }
    class SetHeaders
    class DropHeaders
    class Capture {
        <<interface>>
        Regex, JsonField, Cookie, Header
        +default, required
        +extract(response, session)
    }
    class Check {
        <<interface>>
        Contains, JsonEquals
        +holds(response) bool
    }
    class LegacyTlsAdapter {
        +seclevel: int = 1
    }
    class resolvers {
        <<module>>
        +link_in_page(page, contains, headers) Callable
        +latest_in_json_listing(endpoint, method, json, params, items, where, order_by, pick, attempts) Callable
    }
    class HttpRequest {
        <<dataclass>>
        +name: str
        +endpoint: str
        +method: str = "GET"
        +params: Mapping
        +json: Any
        +headers: Mapping
        +resolve: Callable | None
    }
    class MailQuery {
        <<dataclass>>
        +subject: str
        +sender: str | None
        +attachment_pattern: str
        +folder: str = "INBOX"
        +credentials_variable: str | None
    }
    class RawFile {
        <<dataclass>>
        +name: str
        +path: Path
        +size: int
        +sha256: str
    }
    class base_extractor {
        <<module>>
        +write_stream(chunks, path, name) RawFile
        +describe_file(path, name) RawFile
    }
    class http_common {
        <<module>>
        +RETRY_ATTEMPTS = 3
        +fetch(hooks, request, expect_json) Response
        +save(response, path, name) RawFile
        +require_source(config)
    }
    class HttpHooks {
        +conn_id: str
        +adapter: HTTPAdapter | None
        +base_url: str | None
        +from_config(config)$ HttpHooks
        +for_method(method) HttpHook
    }
    class ExtractionError
    class SourceNotFoundError
    class HttpHook {
        <<external>>
    }
    class ImapHook {
        <<external>>
    }
    class Part {
        <<interface>>
    }

    Extractor <|-- ApiExtractor
    Extractor <|-- HttpFileExtractor
    Extractor <|-- EmailAttachmentExtractor
    Extractor <|-- HttpSessionExtractor
    HttpSessionExtractor o-- HttpSession
    HttpSession *-- Step
    Step <|.. Request
    Request <|-- Download
    Step <|.. SetHeaders
    Step <|.. DropHeaders
    Request o-- Capture
    Request o-- Check
    HttpSession o-- LegacyTlsAdapter : mounts
    Extractor <|-- ObjectStorageExtractor
    ExtractorConfig *-- ObjectQuery
    ObjectStorageExtractor ..> base_extractor
    Extractor o-- ExtractorConfig
    ExtractorConfig *-- HttpRequest
    ExtractorConfig *-- MailQuery
    ExtractorConfig *-- HttpSession
    HttpRequest ..> resolvers : resolve
    ExtractorFactory ..> Extractor : cria por config.source
    Extractor ..> RawFile : produz
    Part <|.. RawFile
    ApiExtractor ..> http_common
    HttpFileExtractor ..> http_common
    http_common ..> HttpHooks
    HttpHooks o-- HttpHook
    http_common ..> base_extractor
    EmailAttachmentExtractor ..> ImapHook
    EmailAttachmentExtractor ..> base_extractor
    ExtractionError <|-- SourceNotFoundError
```

- **Estratégias registradas:** `api`, `http_file`, `email`, `http_session` e `object_storage`. Nenhuma tem nome de fonte: o que é de cada fonte é configuração na DAG.
- **`object_storage`:** copia para a raw os objetos que outro processo gravou no bucket (`ObjectQuery`: prefixo, padrão da chave, nome na raw). Lê o bucket sem o prefixo de teste (`storage_from_env(with_prefix=False)`): a fonte não muda com `INGESTION_STORAGE_PREFIX`.
- **Fonte pública × fonte com segredo:** fonte HTTP pública declara `base_url` na
  DAG, e o `HttpHooks` monta a Connection em memória; `conn_id` fica para fonte
  com credencial. Nada de Connection por fonte no `.env`.
- **`api` não guarda página de erro:** 2xx que declara HTML/XML (a página que o
  SGS do BACEN devolve com 200) entra no retry e, se persistir, falha. JSON
  servido como `text/plain` (Power BI público) passa.
- **`http_session`:** fluxo numa `requests.Session` própria (o HttpHook abre
  sessão nova a cada chamada), declarado como passos na DAG, como um navegador:
  `Request` (com capturas e conferências), `Download`, `SetHeaders` e
  `DropHeaders`. Capturas `Regex`, `JsonField`, `Cookie` e `Header`, com
  `default` ou `required=False` (mantém o valor anterior); valores e Variables
  entram por `{nome}`. Ex.: o login OutSystems e a navegação ASP.NET do ICST.
- **`resolvers`:** o `resolve` de uma `HttpRequest` para link que muda a cada
  edição: `link_in_page` (primeiro `<a href>` com o padrão) e
  `latest_in_json_listing` (item mais recente numa listagem JSON, com filtro,
  ordenação e tentativas; ex.: o catálogo de RI da MZ, para a MRV).
- **`email` com credencial em Variable:** com `credentials_variable`, a Connection do IMAP (e o remetente) é montada em runtime a partir da Variable JSON (`imap_server`, `email`, `password`, `sender_email`), sem Connection cadastrada.
- **`http_common`:**
  - retry com backoff só em 5xx e falha de rede;
  - 404 vira `SourceNotFoundError`, outro 4xx vira `ExtractionError`;
  - URL absoluta usa a sessão do hook.

## `converters`: raw → Parquet só texto

```mermaid
classDiagram
    class FileConverter {
        <<abstract>>
        +config: ConverterConfig
        +memory_pool: MemoryPool | None
        +convert(path, out_dir) Iterator~ConvertedFile~
        #_read(path) Iterator~Source~*
        #_after_write(converted)
        -_write(source, target) ConvertedFile
    }
    class ConverterFactory {
        -_registry: dict~str, type~
        -_extensions: dict~str, str~
        +register(name, extensions)$ decorator
        +for_file(path, config)$ FileConverter
    }
    class ConverterConfig {
        <<dataclass>>
        +format: str | None
        +encoding: str = "utf-8"
        +delimiter: str | None
        +skip_rows: int = 0
        +sheet: str | None
        +header_row: int = 1
        +record_path: str = "item"
        +key_column: str | None
        +include: str | None
    }
    class Source {
        <<dataclass>>
        +suffix: str | None
        +header: Sequence~str~
        +batches: Iterable~RecordBatch~
        +schema: Schema | None
    }
    class ConvertedFile {
        <<dataclass>>
        +name: str
        +path: Path
        +rows: int
        +columns: tuple~str~
    }
    class base_converter {
        <<module>>
        +batches_from_rows(rows, width, batch_rows) Iterator~RecordBatch~
    }
    class columns {
        <<module>>
        +fix_header(names) list~str~
    }
    class ConversionError
    class partition {
        <<module>>
        +convert_partition(storage, raw_prefix, staging_prefix, latest_prefix, config, work_dir) ConversionResult
    }
    class StagedFile {
        <<dataclass>>
        +name: str
        +path: Path
        +size: int
        +sha256: str
        +source: str
        +rows: int
        +columns: tuple~str~
    }
    class ConversionResult {
        <<dataclass>>
        +staging_prefix: str
        +latest_prefix: str
        +keys: tuple~str~
    }
    class Part {
        <<interface>>
    }
    class landing {
        <<module>>
    }
    class ParquetWriter {
        <<external>>
    }
    class CsvConverter {
        +default_delimiter: str | None = ","
        #_read(path) Iterator~Source~
        -_width(path, delimiter) int
        -_open(path, delimiter, width) CSVStreamingReader
    }
    class TxtConverter {
        +default_delimiter = None
    }
    class open_csv {
        <<external>>
    }
    class JsonConverter {
        #_read(path) Iterator~Source~
        -_records(path) Iterator~dict~
    }
    class ijson {
        <<external>>
    }
    class XlsxConverter {
        #_read(path) Iterator~Source~
        -_sheet_names(path, available) list~str~
        -_width(sheet) int
        -_rows(sheet, width, skip_header) Iterator~list~
    }
    class openpyxl {
        <<external>>
    }
    class MdbConverter {
        #_read(path) Iterator~Source~
        -_tables(path) list~str~
        -_table(path, table) Source
        -_batches(path, table, process, width) Iterator~RecordBatch~
    }
    class mdbtools {
        <<external>>
        mdb-tables
        mdb-export
    }
    class ParquetConverter {
        #_read(path) Iterator~Source~
    }
    class ZipConverter {
        #_read(path) Iterator~Source~
        -_members(archive) Iterator~ZipInfo~
    }
    class PowerBiDsrConverter {
        #_read(path) Iterator~Source~
    }

    FileConverter <|-- CsvConverter
    CsvConverter <|-- TxtConverter
    CsvConverter ..> open_csv : blocos de 1 MiB, tudo string
    FileConverter <|-- JsonConverter
    JsonConverter ..> ijson : duas passadas em streaming
    FileConverter <|-- XlsxConverter
    XlsxConverter ..> openpyxl : read_only, data_only
    FileConverter <|-- MdbConverter
    MdbConverter ..> mdbtools : pipe, sem arquivo intermediário
    MdbConverter ..> open_csv
    FileConverter <|-- ParquetConverter
    FileConverter <|-- ZipConverter
    ZipConverter ..> ConverterFactory : delega cada membro
    FileConverter <|-- PowerBiDsrConverter
    PowerBiDsrConverter ..> ijson : um resultado por vez
    partition ..> ConverterFactory : um arquivo da raw por vez
    partition ..> StagedFile : produz
    partition ..> ConversionResult
    Part <|.. StagedFile
    partition ..> landing : land + publish_latest

    FileConverter o-- ConverterConfig
    ConverterFactory ..> FileConverter : cria por formato ou extensão
    FileConverter ..> Source : _read produz
    FileConverter ..> ConvertedFile : convert produz
    FileConverter ..> columns : fix_header
    FileConverter ..> ParquetWriter : lote a lote
    FileConverter ..> ConversionError : levanta
```

- **Template Method:** `convert` é fixo. Cada formato implementa só `_read`, que devolve as tabelas do arquivo com o cabeçalho e as linhas em lotes de texto.
- **Fixo para todo formato:**
  - conserta o cabeçalho;
  - força `string` em todas as colunas;
  - escreve o Parquet lote a lote;
  - confere as linhas gravadas;
  - chama `_after_write`, gancho reservado para o drift (Fase 8).
- **Formatos registrados:**
  - `csv` (`.csv`, delimitador padrão `,`);
  - `txt` (`.txt`, `.tsv`): delimitador obrigatório, e o `.tsv` assume tabulação;
  - `json` (`.json`): registros em `record_path`, colunas = união das chaves, aninhado vira texto JSON; com `key_column`, as chaves de um objeto viram linhas (a data do pregão do Alpha Vantage); com `nested`, `explode_keys` e `columns` (`Field`, caminho com `[*]`, `{keys}`, `{values}` e `join`), JSON aninhado no estilo do `pandas.json_normalize` (ex.: o formato da API de agregados do IBGE, declarado na DAG);
  - `xlsx` (`.xlsx`, `.xlsm`): uma saída por aba, `header_row`, valor calculado da fórmula, célula vira texto por regra fixa;
  - `mdb` (`.mdb`, `.accdb`): uma saída por tabela (`<arquivo>__<tabela>`), `mdb-export` lido por pipe;
  - `parquet` (`.parquet`): mantém o schema original (`Source.schema`), a exceção ao "tudo string";
  - `zip` (`.zip`): um membro por vez, delegado ao conversor da extensão (`<zip>__<membro>`);
  - `powerbi_dsr` (só por `format`): o DSR do `querydata` do Power BI público, com as máscaras `Ø` (nulo) e `R` (repete a linha anterior) e os nomes do `descriptor`.
- **Nome repetido:** dois Parquet com o mesmo nome vindos de um mesmo arquivo são `ConversionError`.
- **`convert_partition`:**
  - exige o `_SUCCESS` da raw;
  - converte um arquivo da raw por vez e sobe os Parquet pelo `land`, que recusa nome repetido entre arquivos (`a.csv` e `a.json`);
  - grava o `_SUCCESS` da staging com origem, linhas e colunas de cada Parquet;
  - só então publica o `latest/`. Uma falha apaga o que já tinha subido para a partição (o glob do merge/append não lê Parquet órfão) e deixa o `latest/` como estava.
- **Nome de saída:** o do arquivo da raw, com aba, tabela ou membro como sufixo (`relatorio__dotacao.parquet`).

## `dataset`: o que a DAG declara

```mermaid
classDiagram
    class DatasetSpec {
        <<dataclass>>
        +domain: str
        +dataset: str
        +extractor: ExtractorConfig | Callable
        +converter: ConverterConfig
        +load_mode: LoadMode = overwrite
        +keys: tuple~str~
        +extractor_config() ExtractorConfig
    }
    class ExtractorConfig {
        <<dataclass>>
    }
    class ConverterConfig {
        <<dataclass>>
    }
    class LoadMode {
        <<enumeration>>
    }

    DatasetSpec o-- ExtractorConfig
    DatasetSpec o-- ConverterConfig
    DatasetSpec o-- LoadMode
```

- **`DatasetSpec`:** fica no topo de cada DAG, só com literais.
- **Extrator preguiçoso:** `extractor` pode ser uma função que monta a configuração dentro da task (lendo uma Variable, como a lista de séries do BACEN). Nada é consultado no parse.
- **Validação:** `domain` e `dataset` viram pastas e precisam ser segmentos seguros; `load_mode` e `keys` passam por `validate_load`.

## `pipeline`: os passos das DAGs

```mermaid
classDiagram
    class steps {
        <<module>>
        +extract_to_raw(spec, ingestion_time) str
        +convert_to_staging(spec, raw_prefix) str
    }
    class DatasetSpec {
        <<dataclass>>
    }
    class ExtractorFactory
    class landing {
        <<module>>
    }
    class partition {
        <<module>>
    }
    class config_storage {
        <<module>>
    }
    class AirflowSkipException {
        <<external>>
    }

    steps ..> DatasetSpec
    steps ..> ExtractorFactory : extract
    steps ..> landing : land na raw
    steps ..> partition : convert_partition
    steps ..> config_storage : storage_from_env
    steps ..> AirflowSkipException : fonte ausente
```

- **Contrato entre tasks:** cada passo recebe e devolve só o prefixo de uma partição, um XCom pequeno.
  - `extract_to_raw` devolve `raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/`;
  - `convert_to_staging` devolve `staging/<domain>/<dataset>/latest/`.
- **Resolução em runtime:** storage, Connection e Variable (o extrator preguiçoso do `DatasetSpec`) só são resolvidos dentro dos passos.
- **Skip:** fonte sem o dado vira `AirflowSkipException`, não falha.
- **Diretório de trabalho:** `LAKE_TMPDIR`.

## `loaders`: modos de carga

```mermaid
classDiagram
    class LoadMode {
        <<enumeration>>
        OVERWRITE = "overwrite"
        MERGE = "merge"
        APPEND = "append"
    }
    class LoadResult {
        <<dataclass>>
        +mode: LoadMode
        +table: str
        +rows: int
    }
    class load_types {
        <<module>>
        +validate_load(mode, keys) tuple~str~
    }

    LoadResult o-- LoadMode
    load_types ..> LoadMode
```

- **Modos:**
  - `overwrite`: a última ingestão substitui tudo, e deleções na fonte se propagam;
  - `merge`: por chave, vale a ingestão mais recente, e deleções não se propagam;
  - `append`: empilha todas as ingestões.
- **Regras** (iguais no `fonte_lake` do dbt): `merge` exige chaves, sem repetição; `overwrite` e `append` não aceitam chaves.
- **Onde o modo é aplicado:** no bronze, pelo macro `fonte_lake` (`dbt/mcid/macros/fonte_lake.sql`), a partir de `meta.load_mode` e `meta.keys` da fonte no `sources.yml`. Ele não é classe Python, então fica fora do diagrama.

## Próximas classes

Entram neste arquivo conforme forem implementadas:

| Fase | Classes |
|---|---|
| 7 | `Loader`, `PostgresCopyLoader` |
| 8 | Retrato e relatório de drift |
| 9 | `CatalogBackend`, `TableBackend`, `IcebergLoader` |
