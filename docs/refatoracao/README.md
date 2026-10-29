# Guia das refatorações do data-application-cidades

> Documento vivo. Atualize o status e as decisões aqui quando uma issue for aberta, um PR entrar ou uma decisão mudar.
> Levantamento feito em 29/10/2026 sobre a `main` em `13a2253` (depois do #241).

## 1. Para que serve

O repositório acumulou problemas em quatro áreas:
- **padrões diferentes:** dbt com dois adaptadores, cinco jeitos de ler o lake e quatro listas de dependências;
- **código morto:** sobras dos DAGs do IPEA e do PR #206;
- **riscos reais:** PII, staging sobrescrita e coleta com prazo de validade;
- **CI que não barra nada.**

Este guia reúne o que vamos refatorar, em que ordem e com quais regras, para que cada issue e cada PR saia daqui e não de um levantamento novo.

Cada frente (F1–F5) e cada candidato (B1–B15) tem um documento próprio nesta pasta, com o problema, a correção proposta, as dependências e o critério de aceite. O painel de status (seção 4) leva a cada um.

## 2. Regras de trabalho

1. **Issue antes de código.** Cada item deste guia vira uma issue com critério de aceite. Nada é implementado fora de uma issue.
2. **Plano antes de PR.** Cada issue começa em modo planejamento (plano → aprovação → código).
3. **Um PR por item.** Na frente de dbt, um PR por domínio.
4. **ADRs na branch deles.** Decisão que muda contrato (staging, Postgres raw, nomenclatura) passa por ADR. Os ADRs são escritos em outra branch: não criar nem editar `docs/adr` aqui.
5. **Diagnosticar sobre a `origin/main`.** Antes de analisar, conferir a distância da branch local (`git rev-list --left-right --count origin/main...HEAD`).
6. **Testes proporcionais ao risco.** Teste em `tests/` para o que muda comportamento; nada de montar um ambiente pesado para provar o que já funciona.
7. **Lint só nos arquivos tocados.** O hook atual formata o repositório inteiro e falha (item B7). Até ele ser trocado, rodar ruff e black só nos arquivos do PR.
8. **Commits e PRs sem atribuição a ferramentas** (sem `Co-Authored-By`, sem "Generated with").
9. **Validação fora dos dados reais.** `raw/` e `staging/` reais não são tocados durante refatoração; a validação usa `tests/`.

## 3. Ordem de execução

| Ordem | O quê | Por quê |
|---|---|---|
| 1 | **P0 (B1–B5)** | Riscos reais, correções pequenas, não dependem de ADR. B1 e B2 mexem no mesmo script. |
| 2 | **Frente 4 (poetry → uv)** com B6, B7 e B10 | Tooling e CI blocante antes das refatorações grandes, para que elas já nasçam checadas. |
| 3 | **Frente 1 (ADRs)** | Fecha o contrato da staging, o destino do Postgres raw e a nomenclatura. |
| 4 | **Frente 2 (ingestão)** com B11, B12 e B13 | Depende dos ADRs do contrato da staging. |
| 5 | **Frente 3 (dbt)**, por domínio, com a **Frente 5 (OpenMetadata)** | O OM depende do catálogo completo que sai do dbt. |
| 6 | **B8, B9, B14 e B15** | Issues avulsas, sem bloquear nada. |

## 4. Painel de status

Status: `a abrir` → `issue aberta` → `em andamento` → `PR aberto` → `feito`.

| ID | Item | Status | Issue | PR |
|---|---|---|---|---|
| F1 | [ADRs](f1-adrs.md) | issue aberta | | |
| F2 | [Ingestão: modelagem nova](f2-ingestao.md) | a abrir | | |
| F3 | [dbt: padrão único, seletores, homolog](f3-dbt.md) | a abrir | | |
| F4 | [poetry → uv](f4-uv.md) | a abrir | | |
| F5 | [OpenMetadata](f5-openmetadata.md) | a abrir | | |
| B1 | [`raw_para_staging` só em `raw/sftp/`](b01-raw-para-staging-so-sftp.md) | a abrir | | |
| B2 | [Máscara como trava da staging](b02-mascara-como-trava.md) | a abrir | | |
| B3 | [Novo CAGED com anos fixos](b03-caged-anos-fixos.md) | a abrir | | |
| B4 | [`local.env` fora do git](b04-local-env-versionado.md) | a abrir | | |
| B5 | [`no_log` nos playbooks](b05-ansible-no-log.md) | a abrir | | |
| B6 | [CI blocante (ruff, dbt parse, DagBag)](b06-ci-blocante.md) | a abrir | | |
| B7 | [pre-commit só nos arquivos tocados](b07-pre-commit.md) | a abrir | | |
| B8 | [Imagem única da CI para VM e k8s](b08-imagem-unica.md) | a abrir | | |
| B9 | [GitHub Pages e dependabot](b09-pages-dependabot.md) | a abrir | | |
| B10 | [`tests/` com `conftest.py`](b10-conftest.md) | a abrir | | |
| B11 | Código morto da ingestão | a abrir | | |
| B12 | Um escritor de staging | a abrir | | |
| B13 | Orquestração dos DAGs | a abrir | | |
| B14 | Infra sem duplicação | a abrir | | |
| B15 | Higiene do repositório | a abrir | | |

## 5. As cinco frentes previstas

### F1. Organização do desenvolvimento via ADRs

Três decisões precisam de ADR antes da F2 e da F3: contrato da staging, destino do Postgres raw e nomenclatura. Detalhe em [f1-adrs.md](f1-adrs.md).

### F2. Refatoração das ingestões: primeiro providers, depois clientes próprios

O #206 foi descartado; a ingestão é modelada do zero, providers primeiro. Lista as regras do #206 a revalidar e o que o modelo novo precisa resolver. Detalhe em [f2-ingestao.md](f2-ingestao.md).

### F3. Refatoração do dbt: problemas do OpenMetadata, seletores, prod e homolog

Padrão único do `dbt/mcid`, ordem por domínio, o reloginho lendo o bronze da execução anterior, o eixo histórico fora do OM e a falta de target de homolog. Detalhe em [f3-dbt.md](f3-dbt.md).

### F4. Troca do poetry pelo uv

Arquivos que mudam na troca e o que resolver junto: fonte de verdade única das dependências, drift do sqlglot e imports não declarados. Detalhe em [f4-uv.md](f4-uv.md).

### F5. Melhorias no OpenMetadata

Catálogo completo (vem da F3), versão fixada em quatro lugares e valores fixos nos templates. Detalhe em [f5-openmetadata.md](f5-openmetadata.md).

## 6. Candidatos novos (fora das cinco frentes)

✔ = conferido no código. Os demais vêm da varredura e devem ser confirmados ao abrir a issue.

### P0: risco real

| ID | Item |
|---|---|
| B1 | [✔ O `raw_para_staging` sobrescreve a staging das APIs](b01-raw-para-staging-so-sftp.md) |
| B2 | [✔ A máscara não trava o caminho da PII](b02-mascara-como-trava.md) |
| B3 | [✔ O Novo CAGED para de coletar em janeiro de 2027](b03-caged-anos-fixos.md) |
| B4 | [✔ `local.env` versionado](b04-local-env-versionado.md) |
| B5 | [Senhas em texto puro na saída do Ansible](b05-ansible-no-log.md) |

### P1: CI, qualidade e tooling

| ID | Item |
|---|---|
| B6 | [A CI não barra nada](b06-ci-blocante.md) |
| B7 | [Pre-commit quebrado](b07-pre-commit.md) |
| B8 | [Imagem da CI que ninguém usa](b08-imagem-unica.md) |
| B9 | [GitHub Pages e dependabot](b09-pages-dependabot.md) |
| B10 | [`tests/` sem `conftest.py`](b10-conftest.md) |

### P2: limpeza e estrutura

**B11. Código morto da ingestão**

- os métodos de `ClientPostgresDB` para compras_gov, transfere_gov, pessoas e siorg, sobras do #237 (`plugins/cliente_postgres.py:183-569`);
- funções sem uso em `cliente_email` e `cliente_minio`;
- `IngestorLake.subir_raw`;
- `scripts/sftp_ingest.py` e `scripts/sftp_rename_tables.py`;
- os `typers=` mortos, que geram warning em todo run;
- os `__pycache__` de refactors antigos em `plugins/`.

**B12. Um escritor de staging**

- **Problema:** hoje são três, com regras diferentes:
  - `registros_para_staging_parquet` grava só texto e ignora `typers`;
  - `gera_parquet_tipado` é o do IBGE;
  - o pandas do `abecip_instituicoes`.
- **Depende de:** B1 e do ADR do contrato.

**B13. Orquestração**

- **Slots presos:** o `conjuntura_dag` segura 13 slots em polling; `deferrable=True` resolve.
- **Disparo em dobro:** o `mcid_cosmos_dag` não tem `max_active_runs` e é disparado pelo cron e pelo `minio_transform_dag`.
- **Lista incompleta:** abecip_financiamentos e abecip_instituicoes estão fora de `INGEST_DAG_IDS`.
- **Variables sem controle:** `dynamic_schedules`, `IBGE_CONFIGURACOES` e `BACEN_SERIES` não estão no repositório nem são validadas. Uma vírgula a mais no JSON derruba o DAG no parse.

**B14. Infra sem duplicação**

- **Envs copiadas:** as envs do Airflow estão repetidas no compose, na VM e no k8s. O k8s ainda diverge em `LAKE_TMPDIR=/tmp`.
- **Playbooks sem roles:** a instalação do Docker, o bloco UFW e as `allowed_networks` estão copiados quatro vezes.
- **`infra/env/.env.example`:** traz MinIO e bucket do IPEA e um `AIRFLOW_IMAGE_NAME` sem uso, e não tem `MASKING_HMAC_SECRET`.
- **Texto desatualizado:** `infra/README.md` diz "três VMs", mas são quatro.

**B15. Higiene do repositório**

- `README.md` genérico do IPEA, com GitLab e targets de make que não existem;
- três `.user.yml` do dbt versionados;
- `superset/__init__.py` vazio, fazendo sombra ao módulo `superset`;
- `Plano-Trabalho.md` parado;
- `docs-pages/src/_data/airflow.json` com 74 DAGs, quando só 9 existem;
- `scripts/snapshots/*.json` versionados, com nomes de arquivo do CadÚnico;
- `data-science/`: 74% do repositório (32 MB de CSV, PDF e zip), mas está no escopo (base do TCC). Decidir se fica, vira LFS ou vai para o lake.

## 7. Decisões em aberto

| Decisão | Onde se decide | Bloqueia |
|---|---|---|
| Contrato da `staging/` (tipado ou texto, um escritor) | ADR | F2, B12 |
| Destino das tabelas Postgres raw das APIs | ADR, depois de conferir o Superset | F2 (histórico truncado) |
| Nomenclatura `prata/ouro` × `silver/gold` | ADR | F3 |
| Quais regras do #206 valem no modelo novo | Modelagem da F2 | F2 |
| DAGs quase idênticas: fábrica ou não | Modelagem da F2 | F2 |
| Target de homolog e roteamento de schema | Issue da F3 | F3 |
| `data-science/`: repositório, LFS ou lake | Issue da B15 | — |

## 8. Registro de decisões

| Data | Decisão |
|---|---|
| 29/10/2026 | O levantamento é aprovado como base do trabalho. |
| 29/10/2026 | O projeto de ingestão do PR #206 é descartado; a F2 é modelada do zero, providers primeiro e depois clientes próprios. |
