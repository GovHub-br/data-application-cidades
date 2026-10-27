# OpenMetadata DAG

Esta pasta concentra a DAG de ingestão do OpenMetadata e o código de suporte para renderizar recipes, preparar artefatos do dbt e executar cada workflow.

## Estrutura

- `openmetadata_ingestion_dag.py`: define a DAG e o encadeamento entre tasks.
- `config.py`: catálogo de recipes, replacements e liga/desliga por recipe.
- `execution.py`: renderização e execução dos workflows.
- `recipes/`: recipes YAML usadas pelo OpenMetadata.
- `glossary.py`: validação e sincronização idempotente via API.
- `glossaries/`: raiz YAML e termos CSV do glossário MCID.
- `semantic_relationships.py`: validação, renderização e publicação das
  relações semânticas em Custom Properties de Table.
- `semantic_relationships/mcid.yaml`: catálogo legível por máquina para
  RAG/GraphRAG.
- `DOCUMENTATION_COVERAGE.md`: critério, resultado e allowlist auditada do MCID.
- `RELATIONSHIP_COVERAGE.md`: linhagem, JOINs, chaves e riscos auditados.

## Glossário MCID

O glossário de negócio não fica em `models/`, pois não representa uma relação
materializada pelo dbt. A fonte versionada fica em `glossaries/mcid.yaml` e
`glossaries/mcid.csv`. A task `sync_mcid_glossary` cria ou atualiza a raiz e os
termos antes de `postgres_metadata` e `dbt_metadata`, permitindo que os FQNs de
`meta.openmetadata.glossary` sejam resolvidos durante a ingestão dbt.

A sincronização é somente de upsert: ela não apaga automaticamente termos que
deixarem de existir no CSV.

O `INGESTION_TOKEN` também precisa ter permissão para criar e editar glossários.
O pacote de ingestão (`openmetadata-ingestion==1.13.3.2`, em `requirements.txt`)
acompanha a linha do servidor, hoje 1.13.3: cliente e servidor de linhas
diferentes divergem no schema das entidades.

## Escopo documental do Postgres

As recipes `postgres_metadata`, `postgres_profiler` e `postgres_classifier`
compartilham uma allowlist exata de 74 tabelas nos cinco schemas aprovados. O
critério e o inventário estão em `DOCUMENTATION_COVERAGE.md`. Ao adicionar ou
renomear um modelo, atualize as três listas em conjunto; o teste
`tests/test_openmetadata_mcid.py` detecta divergências.

O classifier mantém `storeSampleData: false`. Ele pode analisar até 50 linhas
dentro do worker para detectar PII, mas não persiste essas linhas na aba Sample
Data do OpenMetadata. Antes de ampliar a allowlist, avalie não só a documentação,
mas também se a nova tabela pode expor dados sensíveis no OpenMetadata.

## Relações semânticas para RAG/GraphRAG

O catálogo em `semantic_relationships/mcid.yaml` mantém chaves de ligação,
relações observadas, contratos dbt, candidatos de associação, campos de
pesquisa, lógica dos JOINs e cautelas de PII. Ele não cria FKs físicas nem
arestas artificiais de lineage.

Depois de `dbt_metadata`, a task `sync_mcid_semantic_relationships` cria ou
atualiza duas Custom Properties nas 74 tabelas:

- `mcidSemanticRelationships`: conteúdo Markdown específico da tabela;
- `mcidRelatedTables`: referências navegáveis para tabelas relacionadas.

A task usa upsert para as definições, aplica JSON Patch granular para preservar
outras propriedades existentes e só atualiza tabelas cujo conteúdo mudou. Todas
as tabelas e colunas são validadas antes do primeiro PATCH de tabela. Como a API
não oferece uma transação para 74 entidades, uma falha de rede no meio pode
deixar um prefixo já atualizado; o retry é idempotente e converge o restante.

Para conferir na interface:

1. abra **Explore > Databases > Cidades > cidades**;
2. escolha um dos cinco schemas MCID e abra uma tabela;
3. em **Custom Properties**, confira `Relações semânticas MCID` e
   `Tabelas relacionadas no MCID`;
4. use **Lineage** para `ref()`/`source()` reais;
5. use **Glossary Terms/Tags** para conceitos como MCMV, FAR, FDS, APF, CNPJ,
   Código IBGE e UF.

## Onde o pacote roda

O `openmetadata-ingestion` está na imagem do Airflow, pelo lock
`infra/docker/airflow/requirements.lock.txt` (gerado por `compile-lock.sh`, o
mesmo modelo do `data-application-minc`), e as tasks são `@task` comuns.

Até outubro rodava num `@task.virtualenv` isolado, com o Airflow 3.2.2 que o
extra `airflow` do pacote instala. O source `airflow` do OpenMetadata lê o banco
de metadados pelo ORM do próprio Airflow, e no homolog (servidor 3.3.2) a recipe
`airflow_metadata` falhava todo dia no teste de conexão, ao inicializar o
mapper (`Callback` não acha `Trigger`). Fora daquele venv o erro não se
reproduziu — o 3.2.2 isolado lê um banco 3.3.2 sem problema —, então o gatilho
está no ambiente da task virtualenv. Na imagem não há venv: o conector roda no
mesmo processo e no mesmo Airflow 3.3.2 da task, e foi validado contra um banco
de metadados 3.3.2 (teste de conexão e listagem das DAGs).

Por isso o pacote entra **sem** o extra `airflow` (ele fixaria o 3.2.2) e sem o
`pii-processor`, que exige `numpy<2` e rebaixaria o pandas 3 que a ingestão do
lake usa. Sem o `pii-processor`, a classificação automática de PII do
`postgres_classifier` não roda.

A recipe `airflow_metadata` lê o banco de metadados via `Postgres` e roda
em-process com `MetadataWorkflow.create(...)`, em vez de `metadata ingest -c ...`
(no subprocesso do CLI o source falha com um "missing plugin [airflow]"
genérico). Ela é a última da fila: nenhuma outra recipe depende dela.

## Logs esperados

Quando `airflow_metadata` estiver saudável, o log tende a mostrar:

- `Executing workflow em-process via MetadataWorkflow.create(...)`
- `Running CheckAccess...`
- `Running PipelineDetailsAccess...`
- `Running TaskDetailAccess...`
- `Workflow Success %: 100.0`

## Envio em lotes para o OpenMetadata

O sink `metadata-rest` do OpenMetadata 1.12.1 usa lote padrao de 100 entidades.
Uma carga inicial com dezenas de tabelas pode concentrar tudo em um unico `PUT` e
ultrapassar o timeout do proxy na frente do servidor. Por isso, todas as recipes
limitam `bulk_sink_batch_size` a 10. O Airflow continua responsavel pelos retries
da task caso uma falha de rede realmente transitoria ainda aconteca.

O profiler e o classifier tem outro tipo de lote: antes de calcular metricas ou
classificar amostras, o cliente 1.12.1 lista as tabelas do banco no OpenMetadata
em paginas de 100 entidades, incluindo colunas, tags e configuracoes de
profiling. Essa pagina pode gerar uma resposta grande e lenta. Como a versao
1.12.1 nao expoe o tamanho dessa pagina na recipe, `execution.py` executa o
`ProfilerWorkflow` e o `AutoClassificationWorkflow` em-process e aplica pagina
de 20 tabelas somente a essa listagem. O cursor do SDK continua buscando todas
as paginas; nenhuma tabela e descartada por esse limite.

Esses dois workflows também são executados com limiar de sucesso de 100%. O
SDK 1.12.1 força 80% por padrão e pode marcar uma execução parcial como verde;
no DAG, qualquer falha contabilizada aciona o erro/retry. Avisos sobre métricas
opcionais sem implementação continuam não sendo tratados como falha.

## Por que `dbt_metadata` parece repetir ou ficar parado

O workflow dbt do OpenMetadata 1.12.1 processa modelos, descricoes, lineage,
definicoes de testes, casos de teste e resultados em etapas distintas. O
contador `dbt: Processed` pode continuar subindo enquanto
`OpenMetadata: Processed` permanece temporariamente no mesmo valor porque a
criacao de `TestCase` e o envio de `TestCaseResult` fazem chamadas individuais,
fora do lote do sink. Na primeira carga isso e especialmente lento; o servidor
precisa criar a suite/caso e depois gravar o resultado de cada teste.

`bulk_sink_batch_size` nao acelera essas chamadas. Enquanto os logs do servidor
mostrarem `PUT /api/v1/dataQuality/testCases` e
`POST /api/v1/dataQuality/testCases/testCaseResults/...`, a task esta
progredindo e nao reexecutando o `dbt build`.

O aviso `Unable to ingest owner ... cidades` tambem nao e falha. O valor
`cidades` vem de `catalog.json` como owner fisico do PostgreSQL, mas nao existe
como User/Team no OpenMetadata. Nesta versao, o conector tenta resolver esse
owner mesmo com `dbtUpdateOwners: false`; a busca negativa fica em cache, mas o
aviso e emitido para cada modelo.

A DAG aceita somente uma execução ativa (`max_active_runs=1`), evitando que uma
carga agendada e uma manual publiquem os mesmos testes em paralelo. Cada task
tem limite de 3 horas e o DagRun completo, 8 horas; assim, uma chamada HTTP
realmente congelada não ocupa o worker indefinidamente nem bloqueia as próximas
execuções.

## Shim de lineage para APIs

Algumas DAGs precisam anotar lineage de uma API externa para uma tabela, por exemplo:

```text
compras_gov_api -> api_contratos_dag -> IPEA.analytics.compras_gov.contratos
```

O formato completo de lineage do OpenMetadata usa imports como:

```python
from metadata.generated.schema.entity.services.apiService import ApiService
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity
```

Esses imports normalmente vêm do pacote `openmetadata-ingestion`, mas esse pacote não deve ser instalado no runtime principal do Airflow porque pode conflitar com as dependências do scheduler/webserver. Para evitar isso, criamos um shim mínimo em `airflow_lappis/helpers/metadata`.

O `docker-compose.yml` já inclui `/opt/airflow/helpers` no `PYTHONPATH`, então esses módulos ficam disponíveis para o parse das DAGs sem instalar a lib completa:

```text
airflow_lappis/helpers/metadata/generated/schema/entity/services/apiService.py
airflow_lappis/helpers/metadata/generated/schema/entity/data/apiEndpoint.py
airflow_lappis/helpers/metadata/ingestion/source/pipeline/airflow/lineage_parser.py
```

Esse shim não conversa com o OpenMetadata. Ele só cria classes mínimas para que a DAG serialize as anotações no formato esperado pela ingestão de Airflow. Exemplo:

```python
inlets=[
    OMEntity(entity=ApiService, fqn="compras_gov_api", key="apiService")
]
outlets=[
    {
        "entity": "table",
        "fqn": "IPEA.analytics.compras_gov.contratos",
        "key": "apiService",
    }
]
```

A `key` precisa ser a mesma no inlet e no outlet para agrupar as duas pontas na mesma relação de lineage. Se a key divergir, o OpenMetadata pode criar arestas incompletas ou self-loops.

Antes de usar uma entidade não-table, confirme o FQN e o tipo real dela no OpenMetadata. No caso testado, `compras_gov_api` era um `ApiService`, não um `APIEndpoint`.

## Quando atualizar

Atualize esta pasta quando houver:

- nova recipe de metadata/profiler/classifier;
- mudança no banco do Airflow ou nas variáveis de conexão;
- mudança de versão do OpenMetadata;
- novos schemas/tabelas relevantes para as recipes de Postgres metadata e profiler.
