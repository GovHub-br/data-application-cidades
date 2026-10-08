# Diagrama de classes da ingestão (`plugins/ingestion`)

Documentação técnica viva do pacote `plugins/ingestion`. Os diagramas estão em [Mermaid](https://mermaid.js.org/syntax/classDiagram.html) (`classDiagram`): o GitHub e o VS Code os renderizam direto, e o texto é editado como código, com diff no PR.

**Regra de manutenção:** toda mudança de classe, assinatura ou relação no pacote atualiza este arquivo no mesmo commit.

## Notação (UML)

| Elemento | Sintaxe Mermaid | Significado |
|---|---|---|
| `<<abstract>>` | `<<abstract>>` | Classe abstrata (ABC); métodos abstratos terminam em `*` |
| `<<interface>>` | `<<interface>>` | Protocolo estrutural (`typing.Protocol`) |
| `<<dataclass>>` | `<<dataclass>>` | `@dataclass(frozen=True)`, objeto de valor imutável |
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
    FileConverter ..> StorageBackend : raw → staging (Fase 3)
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
        +land(storage, parts: Iterable~Part~, prefix) LandingResult
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
- **`land`:** sobe uma parte por vez, apaga a cópia local e grava `_SUCCESS` com o manifesto por último.

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
    class ExtractorFactory {
        -_registry: dict~str, type~
        +register(name)$ decorator
        +create(config, ingestion_time)$ Extractor
    }
    class ExtractorConfig {
        <<dataclass>>
        +source: str
        +conn_id: str
        +requests: tuple~HttpRequest~
        +adapter: HTTPAdapter | None
        +mail: MailQuery | None
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
        +sender: str
        +subject: str
        +attachment_pattern: str
        +folder: str = "INBOX"
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
        +fetch(hooks, request) Response
        +save(response, path, name) RawFile
    }
    class HttpHooks {
        +conn_id: str
        +adapter: HTTPAdapter | None
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
    Extractor o-- ExtractorConfig
    ExtractorConfig *-- HttpRequest
    ExtractorConfig *-- MailQuery
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

- **Estratégias registradas:** `api`, `http_file` e `email`.
- **`http_common`:**
  - retry com backoff só em 5xx e falha de rede;
  - 404 vira `SourceNotFoundError`, outro 4xx vira `ExtractionError`;
  - URL absoluta usa a sessão do hook.

## `converters`: raw → Parquet só texto (Fase 3, em construção)

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
        +include: str | None
    }
    class Source {
        <<dataclass>>
        +suffix: str | None
        +header: Sequence~str~
        +batches: Iterable~RecordBatch~
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

    FileConverter <|-- CsvConverter
    CsvConverter <|-- TxtConverter
    CsvConverter ..> open_csv : blocos de 1 MiB, tudo string
    FileConverter <|-- JsonConverter
    JsonConverter ..> ijson : duas passadas em streaming

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
  - `json` (`.json`): registros em `record_path`, colunas = união das chaves, aninhado vira texto JSON.
- **Nome de saída:** o do arquivo da raw, com aba, tabela ou membro como sufixo (`relatorio__dotacao.parquet`).

## Próximas classes

Entram neste arquivo conforme forem implementadas:

| Fase | Classes |
|---|---|
| 3 | Conversores xlsx, mdb, parquet e zip; `convert_partition`, `publish_latest` |
| 4 | `LoadMode`, `LoadResult` |
| 5 | `DatasetSpec`, `pipeline.steps` |
| 7 | `Loader`, `PostgresCopyLoader` |
| 8 | Retrato e relatório de drift |
| 9 | `CatalogBackend`, `TableBackend`, `IcebergLoader` |
