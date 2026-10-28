# 0004. Ingestão e conversão para a staging

- **Status:** Proposto
- **Data:** 2026-10-19
- **Relacionados:**
  - ADR-0001 (arquitetura de referência), ADR-0002 (contrato entre camadas: R0 a R2)
  - `plugins/ingestor_lake.py`, `plugins/cliente_minio.py`,
  `scripts/raw_para_staging.py`, `scripts/mascarar_minio.py`,
  `dags/data_ingest/minio/minio_transform_dag.py`, `dags/data_ingest/sftp/sftp_ingest_dag.py`

## Contexto

O ADR-0001 põe duas etapas no lake: a fonte grava na raw (etapa 1) e o dado é
convertido para Parquet na staging (etapa 2). Hoje isso acontece por dois caminhos
diferentes:

- **DAGs de API** (IBGE, BACEN, CAGED, ABECIP, FGV etc.) convertem na própria task,
  com `registros_para_staging_parquet` (30 chamadas) ou com o `IngestorLake`. Essa
  conversão não normaliza nomes de coluna nem grava colunas de linhagem, e o hook
  `_tipar` converte números antes de gravar.
- **SFTP:** a `sftp_ingest_dag` só grava a raw. A `minio_transform_dag` roda depois,
  em horário próprio: mascara a PII reescrevendo a raw, varre a `raw/` inteira e
  converte com `scripts/raw_para_staging.py`, que detecta encoding e separador, lê
  MDB e XLSX, processa em blocos de 200 mil linhas, grava a linhagem e é idempotente
  por hash (`lake._staging_log`). No fim, dispara a DAG do dbt.

O ADR-0002 decidiu que o mascaramento acontece antes de gravar a raw (R0) e que a
conversão é a mesma para todas as fontes (R2). Falta decidir onde a conversão roda.

## Decisão

### A DAG da fonte tem duas tasks: `extract` e `transform`

1. **`extract`**, escrita para cada fonte: extrai, mascara a PII (ADR-0002 R0) e
   grava a raw. Devolve as chaves dos objetos que gravou.
2. **`transform`**, reutilizável (`plugins/lake_tasks.py`): recebe essas chaves,
   faz o drift estrutural (ADR-0006) e a conversão para Parquet. É a mesma task em
   todas as DAGs, implementada uma vez sobre a função `processar_objeto()` de
   `scripts/raw_para_staging.py`.

A conversão é sempre uma task **separada** da que grava a raw. Assim, um retry da
conversão lê o que já está na raw, e a extração na fonte não roda de novo. Fonte sem
arquivo novo termina `extract` como `skipped`, e `transform` é pulada junto.

A `transform` roda no pool `lake_conversao`, que limita quantas conversões rodam ao
mesmo tempo no Airflow inteiro, qualquer que seja a DAG. Os arquivos pesados (os dois
do CadÚnico somam 54 GB) ficam sob esse limite.

Esboço (Airflow 3):

```python
# plugins/lake_tasks.py: escrita uma vez, usada por todas as DAGs de fonte
@task(pool="lake_conversao")
def transform(chaves: list[str]) -> dict:
    """Drift estrutural + Parquet em staging/ para os objetos recém-gravados."""
    from raw_para_staging import converter_chaves

    return converter_chaves(chaves)  # registra cada objeto em lake._staging_log


# numa DAG de fonte
@task
def extract() -> list[str]:
    conteudo = ...  # extrai da fonte; nada novo -> AirflowSkipException
    conteudo = mascarar_bytes(conteudo, ext="xlsx")  # ADR-0002 R0
    return [upload_raw_bytes("fgv", "incc_m", conteudo, ext="xlsx")]

transform(extract())
```

Para escrever uma DAG de fonte nova, basta `extract` devolver as chaves e encadear
`transform`. Ninguém escreve conversão de novo. Entre as tasks passam só as chaves,
nunca o conteúdo.

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

## Trade-offs

| Ganha | Paga |
|---|---|
| Staging pronta logo depois do pouso | A DAG geral pode pegar uma fonte convertida só em parte |
| Fluxo de cada fonte visível na própria DAG | A conversão fica espalhada pelas DAGs de fonte; a visão do lake inteiro depende da DAG de manutenção |
| Uma implementação de conversão para todas as fontes | As DAGs de API perdem a conversão própria e precisam ser migradas uma a uma |
| Retry da conversão sem nova extração | Duas tasks por fonte em vez de uma |

## Consequências

- A `minio_transform_dag` dá lugar à `raw_para_staging_dag`, de manutenção. A
  tarefa de mascaramento sai dela (vai para as DAGs de fonte, ADR-0002 R0), e a
  varredura deixa de ser o caminho normal do SFTP.
- As DAGs de API trocam a conversão própria (`registros_para_staging_parquet`,
  `IngestorLake.gerar_staging_parquet`) pela task comum `transform`.

## Plano de migração

1. Extrair de `scripts/raw_para_staging.py` a função `converter_chaves(chaves)`, que
   é o `run()` sem a varredura (mesmo controle em `lake._staging_log`), e criar a
   task `transform` em `plugins/lake_tasks.py`, com o pool `lake_conversao`. O drift
   estrutural entra nela quando o ADR-0006 for aceito.
2. Ligar a task nas DAGs de fonte, uma a uma, começando pela `sftp_ingest_dag`
   (junto com o mascaramento do ADR-0002; o `run()` de `sftp_para_minio.py` passa a
   devolver as chaves gravadas). Em cada DAG de API, trocar a conversão própria pela
   task comum.
3. Quando todas as fontes converterem pela task, renomear a `minio_transform_dag`
   para `raw_para_staging_dag`, tirar dela o mascaramento e o disparo do dbt, e
   desligar o horário automático.

## Validação operacional

- A `raw_para_staging_dag`, rodando em conferência (sem converter), não encontra
  objeto da raw sem registro em `lake._staging_log`. Se encontrar, alguma DAG de
  fonte não encadeou a `transform`.
- Nenhuma DAG de fonte grava em `staging/` a não ser pela task `transform`.

## Questões em aberto

Nenhuma.
