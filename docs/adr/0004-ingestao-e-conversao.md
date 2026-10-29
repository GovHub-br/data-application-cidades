# 0004. Ingestão e conversão para a staging

- **Status:** Proposto
- **Data:** 2026-10-19
- **Revisão:** 2026-10-29 (providers do Airflow na `extract`; DAGs de fonte deixam de gravar no Postgres; contagens conferidas; entrada manual pela `manual_ingest_dag`; drift estrutural na task `verificar_drift`)
- **Relacionados:**
  - ADR-0001 (arquitetura de referência), ADR-0002 (contrato entre camadas: R0 a R2),
    ADR-0005 (layout do lake), ADR-0013 (orquestração)
  - Issues #243 (F1), #244 (refatoração das ingestões) e #248 (staging das APIs sobrescrita)
  - `plugins/ingestor_lake.py`, `plugins/cliente_minio.py`,
  `scripts/raw_para_staging.py`, `scripts/mascarar_minio.py`,
  `dags/data_ingest/minio/minio_transform_dag.py`, `dags/data_ingest/sftp/sftp_ingest_dag.py`

## Contexto

O ADR-0001 põe duas etapas no lake: a fonte grava na raw (etapa 1) e o dado é
convertido para Parquet na staging (etapa 2). Hoje isso acontece por dois caminhos
diferentes:

- **DAGs de API** (IBGE, BACEN, CAGED, ABECIP, FGV etc.) convertem na própria task,
  com `registros_para_staging_parquet` (15 DAGs) ou com o `IngestorLake`. Essa
  conversão não normaliza nomes de coluna nem grava colunas de linhagem, e o hook
  `_tipar` converte números antes de gravar.
- **SFTP:** a `sftp_ingest_dag` só grava a raw. A `minio_transform_dag` roda depois,
  em horário próprio: mascara a PII reescrevendo a raw, varre a `raw/` inteira e
  converte com `scripts/raw_para_staging.py`, que detecta encoding e separador, lê
  MDB e XLSX, processa em blocos de 200 mil linhas, grava a linhagem e é idempotente
  por hash (`lake._staging_log`). No fim, dispara a DAG do dbt. Como varre a `raw/`
  inteira, ela também converte a raw das APIs e regrava, só em texto, a staging que
  as DAGs de API tinham gravado no mesmo caminho (issue #248).
- **Dados manuais:** scripts SQL de `scripts/database/` inserem direto no schema
  `bronze`, e o `IngestorBalancoEmpresas`, chamado pela `conjuntura_dag`, gera
  parquet tipado de um arquivo já posto na raw.

Há ainda dois pontos que nenhum ADR tratava:

- **Postgres.** Antes de gravar o lake, 16 DAGs de API gravam cada execução em
  tabelas do Postgres (`db.insert_data` em `abecip`, `bacen`, `fgv`, `fipe`, `ibge`,
  `infomoney`, `mrv`, `novo_caged` e `siafi`). Nenhum modelo dbt lê essas tabelas: as
  sources desses schemas em `models/sources.yml` não têm nenhum `source()`, e os
  scripts do Superset no repositório não as citam. Só a DAG do infomoney relê a
  própria tabela, para remontar a staging com o histórico acumulado.
- **Clientes.** Cada fonte tem um cliente próprio em `plugins/`, sobre três pilhas
  HTTP (httpx, requests e requests com SSL rebaixado). O acesso ao lake é montado de
  cinco jeitos, o Postgres de dois e o SFTP tem quatro implementações.

O ADR-0002 decidiu que o mascaramento acontece antes de gravar a raw (R0) e que a
conversão é a mesma para todas as fontes (R2). Falta decidir onde a conversão roda.

## Decisão

### A DAG da fonte tem três tasks: `extract`, `verificar_drift` e `transform`

1. **`extract`**, escrita para cada fonte: extrai, mascara a PII (ADR-0002 R0) e
   grava a raw. Devolve as chaves dos objetos que gravou.
2. **`verificar_drift`**, reutilizável (`plugins/lake_tasks.py`): recebe essas
   chaves, compara cada arquivo com o contrato da fonte (drift estrutural,
   ADR-0006) e devolve só as aprovadas. Se algum arquivo foi barrado, manda o
   e-mail e termina em falha, sem impedir a conversão dos outros.
3. **`transform`**, reutilizável (`plugins/lake_tasks.py`): converte para
   Parquet as chaves aprovadas. Implementada uma vez sobre a função
   `processar_objeto()` de `scripts/raw_para_staging.py`.

A `verificar_drift` e a `transform` são as mesmas em todas as DAGs.

A conversão é sempre uma task **separada** da que grava a raw. Assim, um retry da
conversão lê o que já está na raw, e a extração na fonte não roda de novo. Fonte sem
arquivo novo termina `extract` como `skipped`, e as tasks seguintes são puladas
junto.

A `transform` roda no pool `lake_conversao`, que limita quantas conversões rodam ao
mesmo tempo no Airflow inteiro, qualquer que seja a DAG. Os arquivos pesados (os dois
do CadÚnico somam 54 GB) ficam sob esse limite.

Esboço (Airflow 3):

```python
# plugins/lake_tasks.py: escritas uma vez, usadas por todas as DAGs de fonte
@task
def verificar_drift(chaves: list[str]) -> list[str]:
    """Drift estrutural (ADR-0006): devolve só as chaves aprovadas.

    Registra o status de cada objeto em lake._staging_log e, se algum foi
    barrado, manda o e-mail e falha no fim; a transform roda mesmo assim.
    """
    ...


@task(pool="lake_conversao")
def transform(chaves: list[str]) -> dict:
    """Parquet em staging/ para os objetos aprovados no drift estrutural."""
    from raw_para_staging import converter_chaves

    return converter_chaves(chaves)  # registra cada objeto em lake._staging_log


# numa DAG de fonte
@task
def extract() -> list[str]:
    conteudo = ...  # extrai da fonte; nada novo -> AirflowSkipException
    conteudo = mascarar_bytes(conteudo, ext="xlsx")  # ADR-0002 R0
    return [upload_raw_bytes("fgv", "incc_m", conteudo, ext="xlsx")]

transform(verificar_drift(extract()))
```

Para escrever uma DAG de fonte nova, basta `extract` devolver as chaves e encadear
`verificar_drift` e `transform`. Ninguém escreve conversão de novo. Entre as tasks passam só as chaves,
nunca o conteúdo.

### Como a `extract` fala com a fonte

1. **Primeiro, os hooks dos providers do Airflow:** `HttpHook` para APIs,
   `SFTPHook` para o SFTP, `S3Hook` para o lake, `ImapHook` para e-mail e
   `PostgresHook` para o banco. Endereço, credencial e parâmetros de conexão vêm da
   Connection do Airflow (ADR-0013).
2. **Cliente próprio só onde o hook não basta:** autenticação em várias etapas
   (FGVDados), consulta montada pela API da fonte (Power BI do Novo CAGED) ou página
   que precisa ser lida para achar o arquivo (ABECIP, FIPE). Mesmo nesses casos, o
   cliente usa o hook para a conexão e só acrescenta o que é específico da fonte.
3. **Operators de transferência não são usados** (`SFTPToS3Operator`,
   `HttpToS3Operator` e semelhantes). Eles gravam no destino sem passar pelo
   mascaramento (ADR-0002 R0), e alguns levam o conteúdo pela memória do worker ou
   pelo XCom.

### As DAGs de fonte gravam só no lake

O `insert_data` nas tabelas das APIs sai. Os schemas `abecip`, `bacen`, `fgv`,
`fipe`, `ibge`, `infomoney`, `mrv`, `novo_caged` e `siafi` deixam de ser escritos e,
depois do período de conferência, são removidos. O histórico que essas tabelas
acumulavam passa a existir na raw, com uma carga por execução (ADR-0005). O
infomoney, que hoje remonta a staging a partir do Postgres, passa a ler as cargas na
bronze.

As tabelas de controle do schema `lake` (`_ingest_minio_log`, `_staging_log`,
`_masking_log`) continuam: são controle da ingestão, não dado da fonte.

### Entrada manual

A entrada manual (ADR-0002 R7) não tem fonte externa: o arquivo chega à raw pelo
`scripts/enviar_manual.py`, já na pasta `carga=` (ADR-0005). A DAG
`manual_ingest_dag` mantém as mesmas tasks:

- a `extract` não extrai nada. Ela lista em `raw/manual/` os objetos sem registro
  em `lake._staging_log` e devolve as chaves. Sem objeto novo, termina `skipped`.
  A listagem cobre só esse prefixo, que é pequeno, e não a raw inteira;
- a `verificar_drift` e a `transform` são as comuns, com o drift estrutural
  contra o contrato da base.

### DAG de manutenção do lake

A `raw_para_staging_dag`, disparada à mão ou em horário próprio, percorre a `raw/` e
converte o que não tem registro em `lake._staging_log`. Serve para backfill, para
reprocessar em lote o que foi barrado no drift, e como conferência de que nenhuma
DAG de fonte deixou de converter. Não é o caminho normal.

### Relação com a DAG geral do dbt

A DAG geral do dbt roda no horário próprio e não é disparada pelas conversões. Com a
conversão em cada DAG de fonte, disparar o dbt a cada conversão, como a
`minio_transform_dag` faz hoje, rodaria o dbt inteiro a cada fonte.

O que isso garante e o que não garante:

- **Nenhum Parquet pela metade.** Cada objeto da staging só fica visível no object
  storage quando o upload termina, então a bronze nunca lê um arquivo incompleto.
- **Cargas parciais são possíveis.** Uma fonte que grava vários arquivos pode estar
  convertida só em parte quando a DAG geral roda. A bronze lê o que já está pronto,
  e a carga seguinte completa o resto. Para reduzir a chance disso, as DAGs de fonte
  são agendadas para terminar antes da DAG geral. Um período incompleto que chegue à
  prata é pego pelo teste de volume do drift semântico (ADR-0006).

## Alternativas consideradas

**Conversão na mesma task que grava a raw.** É a forma mais curta de escrever, mas
uma falha na conversão faria o retry da task extrair de novo de uma fonte externa,
que pode nem manter o histórico.

**DAG de conversão única, disparada por evento (Assets do Airflow 3).** A DAG da
fonte publicaria um evento com as chaves gravadas, e uma DAG só converteria tudo.
Tem a vantagem de reunir num histórico só toda a conversão do lake. Em retry e em
concorrência não ganha nada: a task separada já evita a reextração, e o pool limita
a concorrência em qualquer DAG (os workers e a imagem são os mesmos, então a DAG
separada não isolaria memória). Em troca, tira o fluxo da fonte de dentro da própria
DAG, dificulta achar onde um arquivo falhou e traz conceitos novos para o time
(Assets, `outlet_events`, eventos agrupados num run só).

**Conversão por varredura periódica da raw (prática atual do SFTP).** A staging só
fica pronta no horário da varredura, cada execução lista o lake inteiro (custo que
cresce com o lake), e a falha de um arquivo se perde no meio da varredura. Fica como
DAG de manutenção. Ela também é a única que vê o lake inteiro e trata os "gêmeos" (o
mesmo arquivo em formatos ou pastas diferentes). Com uma chave por recebimento e um
canal por fonte (ADR-0005), gêmeos novos deixam de existir.

**Um cliente próprio por fonte (prática atual).** Cada fonte resolve conexão,
retry e credencial do seu jeito, e é o que levou a três pilhas HTTP e a quatro
implementações de SFTP. Os hooks já resolvem essa parte, configurada pela
Connection.

**Operators de transferência dos providers.** São a forma mais curta de levar um
arquivo do SFTP ou de uma URL para o lake, mas não há onde mascarar entre a leitura
e a gravação.

**Manter as tabelas Postgres das APIs como cópia.** Não há quem as leia, e elas
já divergem do lake: o Postgres acumula o histórico enquanto a staging de várias
fontes guarda só a última janela consultada. Com a raw imutável, o histórico fica no
lake.

## Trade-offs

| Ganha | Paga |
|---|---|
| Staging pronta logo depois do pouso | A DAG geral pode pegar uma fonte convertida só em parte |
| Fluxo de cada fonte visível na própria DAG | A conversão fica espalhada pelas DAGs de fonte; a visão do lake inteiro depende da DAG de manutenção |
| Uma implementação de conversão para todas as fontes | As DAGs de API perdem a conversão própria e precisam ser migradas uma a uma |
| Retry da conversão sem nova extração | Três tasks por fonte em vez de uma |
| Conexão, credencial e retry configurados num lugar só, a Connection | Os clientes atuais são reescritos sobre os hooks |
| Um caminho só para o dado da fonte, o lake | Quem consulta hoje as tabelas das APIs no Postgres precisa ser reapontado |

## Consequências

- A `minio_transform_dag` dá lugar à `raw_para_staging_dag`, de manutenção. A
  tarefa de mascaramento sai dela (vai para as DAGs de fonte, ADR-0002 R0), e a
  varredura deixa de ser o caminho normal do SFTP.
- As DAGs de API trocam a conversão própria (`registros_para_staging_parquet`,
  `IngestorLake.gerar_staging_parquet`) pela task comum `transform`.
- Os clientes de `plugins/` passam a usar os hooks. Os que só fazem uma chamada HTTP
  simples dão lugar ao `HttpHook`. A imagem já traz os providers `http`, `sftp`,
  `amazon` e `postgres`; o `imap` precisa entrar para a DAG do Tesouro.
- O `ClientPostgresDB.insert_data` deixa de ser usado pelas DAGs de fonte, e as
  sources Postgres sem uso de `models/sources.yml` saem.
- Os scripts de `scripts/database/` e o `IngestorBalancoEmpresas` dão lugar à
  `manual_ingest_dag` (ADR-0002 R7).

## Plano de migração

1. Extrair de `scripts/raw_para_staging.py` a função `converter_chaves(chaves)`, que
   é o `run()` sem a varredura (mesmo controle em `lake._staging_log`), e criar a
   task `transform` em `plugins/lake_tasks.py`, com o pool `lake_conversao`. A task
   `verificar_drift` entra antes dela quando o ADR-0006 for aceito.
2. Ligar a task nas DAGs de fonte, uma a uma, começando pela `sftp_ingest_dag`
   (junto com o mascaramento do ADR-0002; o `run()` de `sftp_para_minio.py` passa a
   devolver as chaves gravadas). Em cada DAG de API, trocar a conversão própria pela
   task comum.
3. Quando todas as fontes converterem pela task, renomear a `minio_transform_dag`
   para `raw_para_staging_dag`, tirar dela o mascaramento e o disparo do dbt, e
   desligar o horário automático.
4. Antes de desligar o Postgres das APIs, listar os datasets do Superset da
   homologação que leem os schemas das APIs e reapontá-los para a ouro. Depois,
   tirar o `insert_data` de cada DAG no mesmo PR que a migrar para `extract` →
   `transform`, junto com a troca do cliente pelo hook.
5. Remover os schemas das APIs depois de um mês sem leitura nem escrita
   (`pg_stat_user_tables`).

## Validação operacional

- A `raw_para_staging_dag`, rodando em conferência (sem converter), não encontra
  objeto da raw sem registro em `lake._staging_log`. Se encontrar, alguma DAG de
  fonte não encadeou a `transform`.
- Nenhuma DAG de fonte grava em `staging/` a não ser pela task `transform`.
- Nenhuma DAG de fonte importa `ClientPostgresDB`, chama `insert_data` ou usa um
  operator de transferência.
- Nenhum script ou DAG insere dado direto numa tabela de bronze.
- Os schemas das APIs não recebem escrita depois da migração (`pg_stat_user_tables`).

## Questões em aberto

Nenhuma.
