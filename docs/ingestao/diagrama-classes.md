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
        +land(storage, parts: Iterable~Part~, prefix, details, summary) LandingResult
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
- **`land`:** sobe uma parte por vez, apaga a cópia local e grava `_SUCCESS` com o manifesto por último. `details` acrescenta campos ao manifesto de cada parte; `summary`, calculado depois da última, ao topo do manifesto (`sources`, `duplicates`).
- **`publish_latest`:**
  - espelha uma partição com `_SUCCESS` em `latest/<AAAA-MM-DD>/<HHMMSS>/`, que é o que o bronze lê (a partição no caminho dá o `dt_ingest`);
  - a partição anterior sai do `latest/` e continua inteira no lugar dela (o antepassado);
  - o `_SUCCESS` fica na raiz do `latest/`: o manifesto da partição mais `particao` e `antecessor` (a partição que ela substituiu; republicar a mesma mantém a antecessora);
  - ordem: copia os novos, remove os que sobraram e grava o `_SUCCESS` por último.

## `extractors`: fonte → disco, no formato original

```mermaid
classDiagram
    class Extractor {
        <<abstract>>
        +config: ExtractorConfig
        +ingestion_time: datetime
        +from_config(config, ingestion_time)$ Extractor
        +already_landed: frozenset~str~
        +extract(work_dir: Path) Iterator~RawFile~*
    }
    class ApiExtractor
    class HttpFileExtractor
    class EmailAttachmentExtractor {
        +hook_class = ImapHook
    }
    class HttpSessionExtractor
    class ObjectStorageExtractor
    class SftpExtractor
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
        +remote: RemoteFiles | None
    }
    class RemoteFiles {
        <<dataclass>>
        +root: str
        +pattern: str
        +exclude: tuple~str~
        +recursive: bool = True
        +prefer_extensions: tuple~str~
        +bundles: str | None
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
        +source_id: str | None
        +details: Mapping
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
    class SFTPHook {
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
    Extractor <|-- SftpExtractor
    ExtractorConfig *-- ObjectQuery
    ExtractorConfig *-- RemoteFiles
    SftpExtractor ..> SFTPHook
    SftpExtractor ..> base_extractor
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

- **Estratégias registradas:** `api`, `http_file`, `email`, `http_session`, `object_storage` e `sftp`. Nenhuma tem nome de fonte: o que é de cada fonte é configuração na DAG.
- **`sftp`:** sobre o `SFTPHook`, lista a árvore (`RemoteFiles`: raiz, padrão no caminho relativo, exclusões) e baixa em janelas (`readv`). A mesma entrega em outra pasta ou embrulho (`X.TXT`, `X.TXT.zip`, `X.zip`) pousa uma vez, pela precedência de `prefer_extensions`; `bundles` são pacotes com várias famílias. Os arquivos saem na ordem de chegada na fonte (data de modificação). `source_id` = nome, tamanho e data na fonte.
- **Extração incremental:** com `DatasetSpec.incremental`, o extrator recebe em `already_landed` os `source_id`s que os manifestos da raw já registram e pula o que já pousou.
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
        +bad_rows: str = "error"
    }
    class Source {
        <<dataclass>>
        +suffix: str | None
        +header: Sequence~str~
        +batches: Iterable~RecordBatch~
        +schema: Schema | None
        +skipped_rows: Callable
    }
    class ConvertedFile {
        <<dataclass>>
        +name: str
        +path: Path
        +rows: int
        +columns: tuple~str~
        +skipped_rows: int
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
        +skipped_rows: int
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
  - `csv` (`.csv`, delimitador padrão `,`); só o campo vazio é nulo (`NULL`, `NA`, `N/A` da fonte ficam como texto); `encoding="auto"` e `delimiter="auto"` detectam o dialeto numa amostra de 64 KB (`ingestion.text`) e decodificam com `errors="replace"`; `bad_rows="skip"` descarta e conta a linha com campos a mais ou a menos (`skipped_rows` no manifesto);
  - `txt` (`.txt`, `.tsv`): delimitador obrigatório (ou `"auto"`), e o `.tsv` assume tabulação;
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
        +incremental: bool = False
        +prepare: Sequence~Prepare~
        +extractor_config() ExtractorConfig
    }
    class Prepare {
        <<interface>>
        +apply(part: RawFile, work_dir) Iterator~RawFile~
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
    DatasetSpec o-- Prepare
```

- **`DatasetSpec`:** fica no topo de cada DAG, só com literais.
- **Extrator preguiçoso:** `extractor` pode ser uma função que monta a configuração dentro da task (lendo uma Variable, como a lista de séries do BACEN). Nada é consultado no parse.
- **Validação:** `domain` e `dataset` viram pastas e precisam ser segmentos seguros; `load_mode` e `keys` passam por `validate_load`.
- **`incremental`:** só baixa o que os manifestos da raw não registram, e cada entrega nova vira uma ingestão própria.
- **`prepare`:** transformações de arquivo antes do pouso, em ordem e em disco (ver `prepare`).

## `prepare`: antes do pouso na raw

```mermaid
classDiagram
    class Prepare {
        <<interface>>
        +apply(part: RawFile, work_dir) Iterator~RawFile~
    }
    class Unpack {
        <<dataclass>>
        +members: str = ".*"
        +prefix_with_archive: bool = False
        +require_match: bool = True
    }
    class MaskPii {
        <<dataclass>>
        +positions: Mapping~str, Mapping~int, str~~
        +secret_env: str = "MASKING_HMAC_SECRET"
    }
    class masking {
        <<module>>
        +MaskingKeys(secret, token_len, redaction)
        +classificar(header, encoding)
        +mascarar_tabular(...)
        +mascarar_xlsx(src, dst, keys)
    }
    class text {
        <<module>>
        +detectar_encoding(sample) str
        +detectar_dialeto(sample, encoding)
        +normalizar_colunas(header)
    }
    class RawFile {
        <<dataclass>>
    }

    Prepare <|.. Unpack
    Prepare <|.. MaskPii
    MaskPii ..> masking
    MaskPii ..> text
    Prepare ..> RawFile : RawFile para RawFile
```

- **`Unpack`:** zip e gzip pelo conteúdo (bytes mágicos), não pela extensão: há `.zip` que é gzip, e o `.xlsx` (zip com `[Content_Types].xml`) passa como está. Do zip saem os membros que casam `members`, um por vez; `require_match=False` aceita pacote sem a família procurada. Os membros herdam o `source_id` da entrega.
- **`MaskPii`:** a regra do antigo `mascarar_minio` (saída idêntica byte a byte): classificação pelo cabeçalho, HMAC-SHA256 em CPF e NIS, redação de nome, endereço, CEP e nascimento; CSV/TXT reescritos em latin-1 e XLSX por XML em fluxo; `positions` para arquivo sem cabeçalho. Linha com número de campos diferente do cabeçalho é redigida inteira (a posição das colunas não vale). A auditoria vai para o manifesto (`details.masking`). A raw nunca guarda PII e não é reescrita depois.

## `pipeline`: os passos das DAGs

```mermaid
classDiagram
    class steps {
        <<module>>
        +extract_to_raw(spec, ingestion_time) list~str~
        +convert_to_staging(spec, raw_prefixes) str
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

- **Contrato entre tasks:** os passos trocam só prefixos de partição, um XCom pequeno.
  - `extract_to_raw` devolve as partições gravadas, `raw/<domain>/<dataset>/<AAAA-MM-DD>/<HHMMSS>/`, em ordem: uma por execução, ou, com `incremental`, uma por entrega nova (na ordem de chegada na fonte, com a data do pouso);
  - `convert_to_staging` converte cada uma em ordem, publica o `latest/` a cada partição (ele termina com a última, apontando a anterior como antecessora) e devolve `staging/<domain>/<dataset>/latest/`.
- **Manifesto da raw:** `sources` registra as origens consumidas, inclusive a que os preparos descartaram inteira (na ingestão seguinte), para a extração incremental não baixá-la de novo; `duplicates`, o mesmo nome repetido numa ingestão (fica o primeiro).
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
