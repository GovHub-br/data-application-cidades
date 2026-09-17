# Issue #130 — Analise de Aderencia dos Indicadores do Reloginho a Arquitetura Medalhao

> Data: 2026-09-02. Escopo: avaliar se os indicadores do reloginho (grupo A da #130)
> respeitam a arquitetura medalhao (RAW -> Staging -> Data Quality -> Bronze ->
> Silver -> Gold), com foco na exigencia de que a camada **Bronze agregue os dados
> historicos de diversos meses**.
> Referencias: diagrama de arquitetura anexado a #130; issue
> [#130](https://github.com/GovHub-br/data-application-cidades/issues/130);
> `openspec/changes/reloginho-dados-historicos/`.

> **Atualização 2026-09-02:** os desvios 1, 2 e a inconsistência do gating foram
> endereçados — o reloginho foi quebrado em bronze → silver → gold. Ver
> `issue-130-refatoracao-medalhao-reloginho.md`. Este documento fica como registro
> do diagnóstico original.

## Veredito

**Os indicadores do reloginho NAO estao aderentes a arquitetura medalhao como
desenhada.** O modelo `indicadores_mcmv_dbt/gold/indicadores_reloginho.sql` e um
**Gold que le direto da `staging/` do MinIO**, concentrando num unico arquivo a
ingestao, a tipagem, a deduplicacao e a regra de negocio. Nao existe camada Bronze
nem Silver para a serie mensal SNH, e a **agregacao historica multi-mes — que a
arquitetura posiciona explicitamente na Bronze — acontece dentro do Gold**.

Os indicadores de **gargalo/desempenho (grupo B)**, no mesmo diretorio, seguem
Bronze -> Silver -> Gold no Postgres corretamente e servem de referencia do padrao
esperado.

## O que a arquitetura exige

```
Fonte (SFTP, Dados Historicos, SharePoint, Tesouro, TransfereGov, IBGE, BACEN, ...)
  -> RAW / Landing Zone (MinIO)      : formato original, imutavel, historico de carga
  -> Staging (MinIO)                 : arquivos unificados / particionados em Parquet
  -> DATA QUALITY (gate)             : sem APF, nulos, bases novas, drift...  -> falha: Emite Alerta
  -> BRONZE (PostgreSQL)             : copia exata / leitura direta da staging;
                                       estrutura original, tipos genericos (text/JSONB);
                                       SEM regra de negocio;
                                       AGREGA OS DADOS HISTORICOS DE DIVERSOS MESES;
                                       documentacao de colunas/tabelas, catalogo e dicionario
  -> SILVER (PostgreSQL)             : uniao de tabelas, linhas MCMV, tipagem
  -> GOLD (PostgreSQL)               : indicadores
```

## O que esta implementado hoje

| Artefato | Camada declarada | Materializacao | Fonte de dados |
|---|---|---|---|
| `indicadores_mcmv_dbt/gold/indicadores_reloginho.sql` | Gold | table, schema `mcmv_indicadores` (DuckDB `mcid_staging`) | `staging/dados_historicos/historico_recente_*.parquet` via `read_minio_staging_parquet_series` (DuckDB + MinIO) |
| `indicadores_mcmv_dbt/gold/resumo_reloginho_dashboard.sql` | Gold | table | `ref('indicadores_reloginho')` |
| `mcmv_historico_dbt/piloto/historico_mcmv_serie_temporal_snapshot.sql` | (historico) | table | **seed** `issue_118_mcmv_serie_temporal_piloto` (serie anual OGU/FGTS) |
| `mcmv_historico_dbt/empreendimentos/historico_mcmv_empreendimentos_snapshot.sql` | (historico) | table (DuckDB) | `staging/sftp/.../INT0**.parquet` direto |

O `indicadores_reloginho.sql` executa, em um unico arquivo:

1. **Ingestao** — glob de `historico_recente_*.parquet`, `dt_referencia` derivada do
   nome do arquivo (regex `YYYYMM`).
2. **Tipagem** — `try_cast(nullif(trim(col::text), '') as bigint)` para colunas
   numericas, parse de data.
3. **Deduplicacao** — `row_number() over (partition by agente_financeiro, apf,
   dt_referencia)` para neutralizar a duplicacao 2x por APF.
4. **Regra de negocio** — `sum()` mensal de UH contratadas/entregues/vigentes,
   `count(distinct apf)`, `count(*) over (...)` para meses observados.

## Desvios em relacao a arquitetura

### 1. Gold le a Staging direto; Bronze e Silver inexistentes para o reloginho

Nao ha `bronze_snh_*` nem `silver_snh_*`. Ingestao + tipagem + dedup + agregacao de
negocio estao no mesmo `.sql` de Gold. Compare com `conjuntura_dbt/`
(bronze/silver/gold completos) e `empreendimento_far_dbt/bronze/obra_mensal.sql`.

### 2. Nao ha Bronze que "agregue os dados historicos de diversos meses" (questao principal)

A consolidacao dos 22 meses (2024-06 -> 2026-03) existe, mas esta no glob
`historico_recente_*` **dentro do Gold**, com `dt_referencia` extraida do nome do
arquivo ali mesmo. A arquitetura quer essa consolidacao multi-mes na Bronze
("o dado continua em seu estado bruto no banco... BRONZE: dados brutos ja em SQL,
documentacao de colunas e tabelas, catalogo e dicionario").

Os modelos `mcmv_historico_dbt/` ja implementam o padrao de serie historica
versionada (`id_historico_snapshot`, `dt_valid_from/to`, `is_current`, `hash_linha`)
— o bloco de construcao correto —, mas **o reloginho nao os consome**. O
`historico_mcmv_serie_temporal_snapshot` ainda le de um **seed** (piloto #118), nao
da staging.

### 3. Bronze deveria estar no Postgres; o reloginho e um pipeline paralelo so-DuckDB

O caminho do reloginho vive em `mcid_staging` (DuckDB contra MinIO) e **nunca
aterrissa no Postgres `cidades`**. O grupo A fica fora do medalhao-em-Postgres onde
o grupo B esta. O `issue-130-gap-staging-indicadores.md` assume isso como decisao
("construir a gold do reloginho lendo staging via DuckDB") e marca o grupo A como
"bloqueado" ate a canonicalizacao da base.

### 4. O gate de Data Quality nao esta entre Staging e Bronze

Os testes (`not_null`, `accepted_values`, `assert_reloginho_grain_unique`,
`assert_reloginho_reconcilia_66`) rodam **depois** do Gold materializado — sao
testes de saida, nao portao de entrada. Nao existe o ramo "nao passou nos testes de
qualidade -> Emite Alerta".

### 5. Requisitos transversais da #130 fora do pipeline

Dicionario de colunas, catalogo de tabelas, campos obrigatorios, % de completude,
drift de schema (colunas que sumiram / novas), tratamento de nulos e duplicados
existem como **scripts e CSVs** em `data-science/dados-historicos-tratamento/` e
specs em `openspec/`, mas nao como modelos Bronze/Silver dbt alimentando o
reloginho.

## O que esta aderente

- Convencao de pastas medalhao existe no projeto; `conjuntura_dbt`,
  `empreendimento_far_dbt` e `entidades_dbt` a seguem corretamente.
- O Gold do reloginho esta **posicionado e tagueado** em `gold/`, materializado como
  tabela, com schema proprio.
- Le de `staging/`, nunca de `raw/` — consistente com a regra "dbt so le staging"
  (issue #119).
- **Deduplicacao 2x por APF** tratada e documentada, com teste de reconciliacao
  contra a referencia #66 (diff 0,000%, PASS) — ver
  `issue-130-reconciliacao-reloginho-dados-historicos.md`.
- `resumo_reloginho_dashboard` constroi sobre `ref('indicadores_reloginho')`
  (Gold sobre Gold, correto).
- Cobertura historica multi-mes **e atingida** (22 meses) — na camada errada, mas
  atingida.
- Limitacoes documentadas com honestidade (meta oficial pendente, ponteiro SNH
  202606 ausente, lacunas da serie BB).

## Recomendacoes

1. **Quebrar `indicadores_reloginho.sql` em tres camadas:**
   - `bronze` (ou rotear pelo `mcmv_historico_dbt`): landing de `historico_recente_*`
     + `o_recente_*`, uma linha por linha de origem, `filename -> dt_referencia`,
     colunas preservadas ou minimamente tipadas, metadados `source_file`/`dt_ingest`,
     **acumulacao multi-mes**. Materializar no Postgres.
   - `silver`: dedup por APF, coercao de tipos, uniao de agentes/frentes,
     normalizacao de linha MCMV e de dominio.
   - `gold/indicadores_reloginho`: apenas a regra de negocio (totais mensais,
     `n_apf`, `n_meses_observados`, `ritmo_medio_mensal`).
2. **Aterrissar Bronze/Silver no Postgres `cidades`** para o grupo A entrar no mesmo
   medalhao do grupo B. Se o DuckDB-only for interino (SNH ainda nao canonicalizada,
   ver `issue-130-gap-staging-indicadores.md`), registrar como excecao temporaria com
   issue de rastreio.
3. **Ligar os checks de qualidade** (APF nulo, nulos, drift de schema, campos
   obrigatorios) como testes sobre Bronze/Silver **antes** do Gold, com caminho de
   alerta ("Emite Alerta").
4. **Reusar o padrao de snapshot do `mcmv_historico_dbt`** (`is_current`,
   `dt_valid_from/to`, `hash_linha`) para a serie mensal SNH, em vez de um glob novo
   dentro do Gold.

## Inconsistencia tecnica a corrigir (working tree, nao commitada)

O `dbt_project.yml` teve o gating removido:

```diff
-        indicadores_reloginho:
-          +enabled: "{{ target.type == 'duckdb' }}"
-        resumo_reloginho_dashboard:
-          +enabled: "{{ target.type == 'duckdb' }}"
```

O cabecalho de `indicadores_reloginho.sql` ainda diz *"Target obrigatorio:
staging_duckdb (gating em dbt_project.yml)"*. Sem o `+enabled`, um `dbt run` com
target Postgres tenta materializar esses modelos e falha no `read_parquet`/DuckDB.
Reintroduzir o gating **ou** atualizar o comentario e garantir degradacao limpa.

## Resumo executivo (1 paragrafo para a issue)

O Gold do reloginho foi entregue funcional e reconciliado contra a referencia #66,
porem fora do padrao medalhao: le a `staging/` do MinIO direto via DuckDB,
concentra ingestao/tipagem/dedup/negocio num unico modelo Gold, e nao possui
camadas Bronze e Silver. A exigencia de que a **Bronze agregue os dados historicos
de diversos meses nao esta atendida** — a agregacao multi-mes ocorre dentro do
Gold e o resultado nunca aterrissa no Postgres. Recomenda-se decompor em
Bronze/Silver no Postgres (reusando `mcmv_historico_dbt`), mover os checks de
qualidade para antes do Gold com alerta, e alinhar o gating do `dbt_project.yml`
com o cabecalho do modelo.
