# 0001. Arquitetura de referência: data lake e data warehouse

- **Status:** Proposto
- **Data:** 2026-10-19
- **Relacionados:** 
  - Issue #117 (ADR de arquitetura de produção) 
  - Issue #119
  (DuckDB lendo a staging)
  - Deck *GovHub — Arquitetura Medalhão de Referência*
  seções 1, 2 e 6, `infra/playbooks/`, `dbt/mcid/models/docs/arquitetura-medalhao-mcid.md`
  - ADR-0002 (contrato entre camadas), ADR-0003 (ambientes, targets e dialetos),
  ADR-0004 (ingestão e conversão para a staging)

## Contexto

A arquitetura foi decidida em partes, issue a issue, e nunca registrada como um
todo. A issue #117 pede esse registro desde a POC com MinIO/SFTP.

Nesse caminho a arquitetura mudou de rumo mais de uma vez:

- A issue #119 definiu que a silver MCMV seria gerada por DuckDB standalone
  lendo direto a `staging/` do MinIO, com o Postgres só como destino. Os
  domínios FAR, FDS e Rural chegaram a rodar apenas no target `staging_duckdb` e
  saíram da carga noturna por um tempo. Hoje voltaram para a DAG geral, em SQL de
  Postgres.
- Depois, o pg_duckdb passou a rodar dentro do Postgres. A bronze hoje é
  carregada no próprio warehouse com `read_parquet('s3://...')`, e prata e ouro
  rodam com `duckdb.force_execution = true` (`dbt/mcid/dbt_project.yml`).
- O object storage trocou de MinIO para RustFS (`infra/playbooks/rustfs/`). Boa
  parte dos documentos e nomes de variáveis ainda fala em MinIO.

Restaram documentos que descrevem estados diferentes e uma pergunta sem dono: onde
cada camada mora e quem lê de quem.

Este ADR decide a forma geral. O que cada camada faz está no ADR-0002; os
ambientes e motores que executam o dbt, no ADR-0003; como o dado chega à staging,
no ADR-0004.

## Decisão

### 1. Dois componentes

| Componente | Tecnologia | O que guarda |
|---|---|---|
| Data lake | RustFS (compatível com S3), bucket `data-lake-mcid` | `raw/`: o dado como a fonte entregou, com a PII já mascarada. `staging/`: o mesmo conteúdo em Parquet. |
| Data warehouse | PostgreSQL 15 + pg_duckdb, banco `cidades` | Bronze, prata e ouro como tabelas físicas. |

O Data Lake guarda o dado no formato da fonte. O Data Warehouse organiza o dado para
análise. Nenhuma camada do Data Warehouse fica no Data Lake, e nenhum arquivo bruto fica no
Data Warehouse.

### 2. Um caminho só, em seis etapas e dois portões

```text
Fonte
  → 1. DAG da fonte mascara a PII e grava em raw/            (lake, ADR-0002 R0)
       ⤷ drift estrutural: o layout do arquivo mudou?        (ADR-0006)
  → 2. conversão para Parquet em staging/                    (lake, ADR-0004)
  → 3. bronze: select * from read_parquet da staging         (warehouse)
       ⤷ drift semântico: os valores mudaram de formato?     (ADR-0006)
  → 4. prata: lê a bronze                                    (warehouse)
  → 5. ouro: lê a prata                                      (warehouse)
  → 6. publicação: Superset, planilhas, boletins, OpenMetadata
```

Os dois portões existem porque a staging guarda tudo como texto. Uma mudança na
fonte não quebra o Parquet; ela só aparece mais adiante, como erro ou, pior, como
número errado num painel. Cada portão barra um tipo de mudança no ponto mais cedo
em que ela pode ser vista:

- **Drift estrutural**, entre a raw e a staging. Compara a estrutura do arquivo
  novo com o schema esperado da fonte. Coluna nova: converte e alerta. Coluna
  removida ou renomeada: o arquivo não vira staging, vai para quarentena, e a
  staging anterior continua valendo. A mesma verificação identifica o tipo de carga
  da fonte: completa (a base inteira a cada envio) ou incremental (só o que é novo
  ou mudou).
- **Drift semântico**, entre a bronze e a prata. Compara os valores com os formatos
  esperados: data que mudou de `dd/mm/aaaa` para `aaaa-mm-dd`, separador decimal
  trocado, encoding diferente, código de domínio novo. Se a fonte mudou o formato, a
  prata não roda, porque a tipagem transformaria em nulo, sem aviso, o que não
  reconhece.

O arquivo barrado continua na raw. Depois de ajustar o contrato ou o modelo, ele é
reprocessado sem nova extração. O que conta como mudança que quebra, em cada
portão, é definido no ADR-0006.

### 3. Consumo só pelo warehouse

Painéis, planilhas, boletins e qualquer outro consumidor leem o warehouse, nunca o
lake. Como todas as camadas do warehouse são tabelas físicas, uma indisponibilidade
do RustFS não derruba o consumo; só atrasa a próxima carga.

### 4. Orquestração

O Airflow agenda e encadeia as etapas e registra as falhas. As DAGs de fonte fazem
as etapas 1 e 2 (detalhe no ADR-0004). Uma DAG geral
(`dags/dbt/mcid_cosmos_dag.py`, via Cosmos) executa o dbt de todas as fontes, nas
etapas 3 a 5, com o drift semântico como testes da bronze, de modo que a mesma
lógica não se repete em cada DAG.

Dentro de cada DAG, uma task só começa quando a anterior terminou bem. Entre as
DAGs de fonte e a DAG geral não há essa garantia: a DAG geral roda no horário
próprio e lê o que já estiver convertido. O que isso significa para cargas
parciais está no ADR-0004.

### 5. O que este ADR substitui

A regra da issue #119, de que "`staging/` é a única fonte autorizada para gerar a
silver", deixa de valer. Agora a staging alimenta só a bronze; a prata lê a bronze.
A intenção da regra continua atendida: a prata segue sem ler tabelas do Postgres
que não venham da staging.

## Alternativas consideradas

**Arquivo DuckDB como banco intermediário da silver, Postgres só para ouro
(estado da #119).**
O arquivo DuckDB vira um ponto único de falha fora do backup do banco, e o
consumo passa a depender de dois bancos. As cargas grandes estouraram memória (os
guardas em `profiles.yml` existem por isso). O que se descarta é o DuckDB como
*destino*. Como motor e como ferramenta de teste local, ele continua (ADR-0003).

**Bronze como view sobre a staging, sem materializar.**
Toda consulta da prata acessaria o RustFS, e o consumo passaria a depender da
disponibilidade do lake. Sai mais barato em disco e mais caro em latência e em
acoplamento.

**Motor federado (Trino) lendo a staging e alimentando vários bancos.**
É a proposta 2 do deck. O ADR-0010 dividiu o warehouse por schema, num banco só,
então o motor federado não é necessário. Esta arquitetura não fecha essa porta: a
proposta 2 trocaria o motor da etapa 3, mas manteria as camadas.

**Lakehouse com todas as camadas em Iceberg no object storage.**
É o passo seguinte natural, porque tira a duplicação entre staging e bronze e traz
transações e histórico de versões. Exige operar um engine e um catálogo (Lakekeeper
ou Polaris), mais compactação e limpeza de versões, e não compensa para as cargas
pequenas e frequentes que temos hoje. Fica como evolução futura (ver Consequências).

## Trade-offs

| Ganha | Paga |
|---|---|
| Um warehouse só, onde todo consumo acontece | O Postgres concentra toda a carga analítica |
| Consumo isolado do lake | Staging e bronze guardam o mesmo conteúdo duas vezes |
| Reprocessamento a partir da raw, sem nova extração | O pg_duckdb é compilado de um commit fixo e precisa ser atualizado à mão |
| Linhagem contínua da fonte à ouro no dbt e no OpenMetadata | Recargas grandes de Parquet ainda precisam ser medidas (ADR-0008) |

## Consequências

- `mcmv_silver_dbt/` (20 modelos, todos com `enabled=false`, e o bloco já comentado
  no `dbt_project.yml`) é legado da #119 e sai do projeto.
- Os documentos que descrevem a #119 como regra vigente
  (`arquitetura-medalhao-mcid.md`, `issue-119-*`) passam a apontar para este ADR.
- **Evolução para lakehouse:** o contrato da bronze (ADR-0002) é definido sobre
  staging e bronze como uma entidade só. Se a bronze passar a ser uma tabela Iceberg
  no lake, quem consome a prata em diante não percebe.

## Plano de migração

1. Remover `mcmv_silver_dbt/` e o bloco comentado no `dbt_project.yml`.
2. Trocar "MinIO" por "RustFS" nos documentos. Variáveis de ambiente
   (`MINIO_ENDPOINT` etc.) mudam junto com a infra, não neste ADR.
3. Marcar a regra da #119 como substituída nos documentos em `dbt/mcid/models/docs/`.

## Validação operacional

- Nenhum modelo de prata ou ouro usa `source()`, `read_parquet` ou `fonte_lake`.
  Essa checagem é detalhada no ADR-0002.
- O Superset tem conexão apenas com o banco `cidades` e com usuário de leitura.

## Questões em aberto

Nenhuma.
