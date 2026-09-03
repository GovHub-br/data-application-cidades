# Arquitetura de Dados MCID — Medalhão (RAW → Staging → Bronze → Silver → Gold)

> Data: 2026-09-02. Documento de referência. Consolida o diagrama de arquitetura
> anexado à issue #130 com o que já está implementado no projeto dbt `mcid`
> (`airflow_lappis/dags/dbt/mcid/`).
> Documentos relacionados: `docs/entregas/issue-117-status-adr-pendente.md` (ADR
> formal ainda pendente), `docs/entregas/issue-119-correcao-arquitetura-duckdb-staging.md`,
> `docs/entregas/issue-118-entrega-final.md`, `docs/glossario-mcid.md`.

## 1. Contexto

O projeto é uma aplicação de dados sobre o programa **Minha Casa Minha Vida
(MCMV)** do Ministério das Cidades. O objetivo é consolidar dados de múltiplas
fontes (operacionais, históricas e de conjuntura), tratá-los de forma governada e
entregar indicadores para dashboards — em especial o **reloginho de metas** (grupo
A) e a **mesa de alertas de gargalo/desempenho** (grupo B).

A arquitetura adota o padrão **medalhão** (medallion): camadas sucessivas que
elevam a qualidade e a semântica do dado, do estado bruto (bronze) ao pronto para
consumo (gold).

## 2. Domínios / Fontes

| Domínio | Exemplos de conteúdo |
|---|---|
| **SFTP** (fábrica GEFUS, GEAVO) | Tabelas de interface INT040/INT054/INT059/INT057/INT065 — empreendimentos FAR, FDS/Entidades e Rural/PNHR |
| **Dados Históricos** | Dump histórico do MCMV: série mensal SNH (`historico_recente_*`), séries anuais OGU/FGTS, bases legadas (BB 2013, CGU, CAIXA BEXT) |
| **SharePoint** | Snapshots do "Novo MCMV" (`novo_mcmv_far_*`, `novo_mcmv_fds_*`, `novo_mcmv_rural_*`) |
| **Tesouro Gerencial (SIAFI)** | Execução orçamentária / financeira |
| **Transfere Gov** | Repasses e convênios |
| **IBGE** | PIB construção civil, SINAPI, PNAD Contínua, referência de UF/município |
| **BACEN** | Financiamentos imobiliários, taxas de juros, IPCA |
| **FIPE / FipeZap** | Índice de locação residencial |
| **Construtoras** | Dados de obra/empreendimento |
| **Email** | Recebimento de bases pontuais |

## 3. Camadas da arquitetura

### 3.1 RAW / Landing Zone (MinIO)

- **Zona de pouso**. Arquivos gravados no **formato original da fonte** (JSON, CSV,
  TXT, XLSX).
- **Imutável**, com **histórico de carga** (cada recebimento é preservado).
- Bucket `data-lake-mcid`, prefixo `raw/`.
- Nunca é lido pelos modelos dbt — serve como backup e origem de reprocessamento.

### 3.2 Staging (MinIO)

- **Área de preparação**. Arquivos brutos da RAW **unificados, particionados e
  convertidos para Parquet** (formato colunar, mais performático para leitura).
- Bucket `data-lake-mcid`, prefixo `staging/` (ex.: `staging/dados_historicos/`,
  `staging/sftp/fabrica/GEFUS/`, `staging/abecip/`, ...).
- **Regra de arquitetura (issue #119): `staging/` é a única fonte autorizada para
  gerar Silver.** Postgres não deve ser usado como fonte de leitura da Silver MCMV.
- Metas de canonicalização: `dados_historicos` < 500 tabelas; `sftp` = 1.453
  tabelas (ver `docs/evidencias/issue-130-gap-staging-indicadores.md`).

### 3.3 Data Quality (gate)

Portão entre Staging e Bronze. Verifica requisitos de qualidade **antes** de o dado
entrar no banco. Se **passa** → segue para a Bronze. Se **não passa** → **emite
alerta** (não materializa).

Requisitos de qualidade (da issue #130):

1. Padronização de colunas
2. Padronização de dados (domínios, formatos)
3. Dicionário de dados
4. Catálogo de tabelas
5. Identificação de colunas que deixaram de existir e de novas colunas (drift de schema)
6. Campos obrigatórios para preenchimento
7. Percentual de completude das bases
8. Identificar quais bases seguem full-refresh e quais seguem incremental (podem ter novos layouts)
9. Identificar cruzamentos de dados de bases antigas com bases novas
10. Linhagem do dado (fonte a fonte, serviço a serviço)
11. Tratamento de dados nulos e duplicados (cuidado com a regra do APF)

Maiores dificuldades declaradas: **empreendimentos sem APF**, **dados nulos**,
**bases novas** (TransfereGov/FNHIS/FAR/FDS).

### 3.4 Bronze (PostgreSQL)

- **Recebe a cópia exata ou a leitura direta da Staging.**
- **Ainda é bruto**: mantém a **estrutura original**, tipos genéricos (tudo `text`
  ou `JSONB`), **sem aplicação de regras de negócio**.
- **Agrega os dados históricos de diversos meses** — a Bronze é onde a série
  temporal multi-mês é consolidada (um snapshot por mês, empilhado).
- Carrega a **documentação de colunas e tabelas, catálogo e dicionário**.
- É "Bronze" justamente porque o dado continua em estado bruto no banco.

### 3.5 Silver (PostgreSQL)

- **União de tabelas** (ex.: FAR CAIXA + FAR BB → linha FAR).
- **Linhas MCMV** — contrato semântico comum por frente (FAR, Entidades/FDS,
  Rural/PNHR, FNHIS/SUB50, Classe Média/Faixa 3, Reforma, Cidades, Conjuntura).
- **Tipagem** — text → date/numeric/int, normalização de domínio, parse de valores
  em formato brasileiro (`13.898.046,25`).
- Dados tratados, padronizados e **governados**.

### 3.6 Gold / Marts (PostgreSQL)

- **Indicadores** prontos para consumo.
- Alimenta **Superset / dashboards / planilhas / relatórios**.
- Regra: dashboards consomem Gold, **não** a Silver diretamente (evita acoplamento
  a tabelas instáveis).
- Exemplos: `indicadores_reloginho`, `resumo_reloginho_dashboard`,
  `indicadores_gargalo_desempenho`, `resumo_gargalo_desempenho_dashboard`,
  `gold_*` de conjuntura.

## 4. Implementação em dbt (`profile: mcid`)

O projeto usa **dois motores**, selecionados por `--target`:

| Target | Motor | `database` | Uso |
|---|---|---|---|
| `prod` | PostgreSQL (`cidades`) | `cidades` | Bronze/Silver/Gold materializadas; ambiente analítico; destino do Superset |
| `staging_duckdb` | DuckDB + `httpfs` (S3/MinIO) | `mcid_staging` | Leitura de `staging/*.parquet` do MinIO para a Silver MCMV e modelos que dependem do dump histórico |

O `+database` é resolvido dinamicamente:
`{{ 'cidades' if target.type == 'postgres' else 'mcid_staging' }}`.

### 4.1 Sources (`models/sources.yml`)

- `mcmv_staging` — external source dbt-duckdb (`meta.external_location` →
  `s3://<bucket>/staging/sharepoint/{name}.parquet`), 28 tabelas FAR/FDS/Rural + compartilhadas.
  Substitui o antigo `raw` (`__dados_brutos`), **removido** (`migracao-bronze-minio-mcmv`).
- `ibge`, `fgv`, `bacen`, `novo_caged`, `infomoney`, `fipe`, `abecip` — conjuntura.
- `conjuntura_bronze` — schema bronze já materializado da conjuntura.

### 4.2 Macros de leitura da Staging (MinIO via DuckDB)

- `minio_staging_uri(object_name)` → `s3://data-lake-mcid/staging/<object_name>`
- `read_minio_staging_parquet(object_name)` → `read_parquet(..., union_by_name=true)`
- `read_minio_staging_parquet_series(glob)` → idem, com `filename=true` (permite
  derivar `dt_referencia` do nome do arquivo)
- `assert_duckdb_staging_only()` → barra execução da Silver MCMV fora do DuckDB

### 4.3 Estrutura de modelos (`models/`)

| Pasta | Schema | Camadas | Motor |
|---|---|---|---|
| `conjuntura_dbt/` | `conjuntura_{bronze,silver,gold}` | bronze + silver + gold | Postgres |
| `empreendimento_far_dbt/` | `bronze` / `silver` / `gold` | bronze + silver + gold | **DuckDB only** (migrado — `migracao-bronze-minio-mcmv`) |
| `empreendimento_fds_dbt/` | `bronze` / `silver` / `gold` | bronze + silver + gold | **DuckDB only** (era `entidades_dbt` / `entidades_fds`) |
| `empreendimento_rural_dbt/` | `bronze` / `silver` / `gold` | bronze + silver + gold | **DuckDB only** (frente nova) |
| `mcmv_silver_dbt/` | `mcmv_silver` | silver (por frente) | **DuckDB only** (`+enabled: target.type == 'duckdb'`) |
| `mcmv_historico_dbt/` | `bronze` / `silver` / `gold` (+ `mcmv_historico` seed piloto) | histórico/snapshot | seed / DuckDB |
| `indicadores_mcmv_dbt/` | `mcmv_indicadores` | gold (reloginho + gargalo) | **DuckDB only** (gargalo passou a depender dos golds FAR/FDS) |
| `metadata/` | `metadata` | incremental | Postgres |

### 4.4 Materialização

- Padrão do projeto: `+materialized: table` (bronze, silver, gold).
- `metadata`: `incremental`.
- Conjuntura separa schema por camada (`conjuntura_bronze`, `conjuntura_silver`,
  `conjuntura_gold`).

## 5. Campos técnicos de auditoria e histórico

Padrão definido no piloto da issue #118 (`historico_mcmv_serie_temporal_snapshot`)
e reaproveitado nos modelos históricos:

| Campo | Função |
|---|---|
| `id_historico_snapshot` | Chave técnica única da versão do registro |
| `id_negocio_historico` | Chave lógica estável (programa + linha + período) |
| `dt_referencia` | Período do snapshot (derivado do **nome do arquivo**, mais confiável que `dt_movimento`) |
| `snapshot_date` / `dt_ingest` | Momento da carga |
| `dt_valid_from` / `dt_valid_to` | Janela de validade (SCD2) |
| `is_current` | Indicador de registro corrente |
| `hash_linha` | Hash do conteúdo para detecção de mudança / dedup |
| `source_file` | Arquivo de origem na Staging |
| `estrategia_versionamento` / `regra_retencao` | Metadados de governança |

Estratégia histórica: **snapshot completo** por período (não incremental), retenção
indeterminada para auditoria e backtest.

## 6. Fluxo de execução (alvo)

```text
Fonte
  → MinIO raw/               (formato original, imutável)
  → MinIO staging/ parquet   (unificado, colunar)
  → Data Quality gate        (dicionário, catálogo, drift, nulos, APF, completude)
        ├── falha → Emite Alerta
        └── ok
  → Bronze (Postgres)        (cópia fiel da staging, text/JSONB, multi-mês empilhado)
  → Silver (Postgres)        (união de tabelas, linhas MCMV, tipagem, domínio)
  → Gold / marts (Postgres)  (indicadores)
  → Superset / planilhas / relatórios
```

Orquestração: Airflow (Cosmos → dbt). Leitura da Staging: DuckDB (`httpfs` + S3
MinIO).

## 7. Estado atual vs. alvo

| Item | Situação |
|---|---|
| Convenção medalhão em dbt (bronze/silver/gold) | **Implementada** em `conjuntura_dbt`, `empreendimento_far_dbt`, `empreendimento_fds_dbt`, `empreendimento_rural_dbt`, `mcmv_historico_dbt` |
| Silver MCMV lendo `staging/` via DuckDB | Diretriz definida (#119); refatoração fonte-a-fonte **parcial** |
| Bronze materializada no Postgres como cópia fiel da Staging | **Parcial** — existe para conjuntura. FAR/FDS/Rural e o MCMV histórico têm bronze fiel **em DuckDB** lendo `staging/` via `mcmv_staging` (`migracao-bronze-minio-mcmv`, `separacao-silver-historico-por-frente`); promoção para Postgres pendente do ADR #117 |
| Data Quality como gate antes da Bronze, com alerta | **Não implementado** como portão; hoje são testes dbt de saída |
| Série histórica multi-mês na Bronze | Reloginho (grupo A) **refatorado** em bronze → silver → gold em `indicadores_mcmv_dbt/` (ver `issue-130-refatoracao-medalhao-reloginho.md`). Eixo histórico de empreendimentos **migrado para o padrão medalhão** (`separacao-silver-historico-por-frente.md`): bronze fiel SFTP+SNH → silver por frente (`silver_mcmv_historico_empreendimento_far`/`_fds`/`_rural` + consolidado) → gold snapshot corrente; série executiva pré-2024 renomeada (`bronze/silver/gold_mcmv_historico_serie_*`). Tudo em DuckDB; promoção para Postgres pendente do ADR #117 |
| Dicionário / catálogo / drift / completude | Existem como scripts e CSVs em `data-science/dados-historicos-tratamento/` e `openspec/`; não materializados como modelos |
| ADR formal de arquitetura de produção | **Pendente** (issue #117) — este documento é insumo, não substitui o ADR |

## 8. Decisões arquiteturais em aberto (para o ADR — issue #117)

1. Bronze apenas como cópia/projeção da Staging **no MinIO** vs. Bronze
   materializada **no Postgres**.
2. Consumo direto da Staging vs. cópia Bronze rastreável.
3. Histórico por **snapshot completo** vs. **incremental / hash de linha**.
4. Gold/marts como **tabelas** materializadas vs. **views**.
5. Regra oficial de meta do ciclo (hoje só existe "meta visual" 2.214.810).
6. Regra de deduplicação do APF — em qual camada aplicar (Bronze / Silver / Gold).

## 9. Glossário rápido

| Termo | Significado |
|---|---|
| **APF** | Autorização Para Financiamento — chave de empreendimento (pode variar por fase) |
| **Reloginho** | Painel executivo de acompanhamento de metas de UH (grupo A) |
| **Gargalo/desempenho** | Mesa de alertas de risco de obra (grupo B) |
| **SNH** | Secretaria Nacional de Habitação — origem da série mensal de dados prioritários |
| **GEFUS / GEAVO** | Áreas da fábrica CAIXA que enviam as tabelas de interface (INT0xx) |
| **FAR / FDS / PNHR** | Fundo de Arrendamento Residencial / Fundo de Desenvolvimento Social (Entidades) / Programa Nacional de Habitação Rural |
| **UH** | Unidade Habitacional |
| **Frente** | Modalidade/linha do MCMV (FAR, Entidades, Rural, FNHIS/SUB50, Faixa 3, Reforma, ...) |
| **Staging canônica** | Conjunto reduzido e deduplicado de tabelas Parquet em `staging/` |
