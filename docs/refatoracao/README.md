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
| F3 | dbt: padrão único, seletores, homolog | a abrir | | |
| F4 | poetry → uv | a abrir | | |
| F5 | OpenMetadata | a abrir | | |
| B1 | `raw_para_staging` só em `raw/sftp/` | a abrir | | |
| B2 | Máscara como trava da staging | a abrir | | |
| B3 | Novo CAGED com anos fixos | a abrir | | |
| B4 | `local.env` fora do git | a abrir | | |
| B5 | `no_log` nos playbooks | a abrir | | |
| B6 | CI blocante (ruff, dbt parse, DagBag) | a abrir | | |
| B7 | pre-commit só nos arquivos tocados | a abrir | | |
| B8 | Imagem única da CI para VM e k8s | a abrir | | |
| B9 | GitHub Pages e dependabot | a abrir | | |
| B10 | `tests/` com `conftest.py` | a abrir | | |
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

Ordem já decidida:
1. a DAG do OM roda verde no homolog;
2. depois, um PR por domínio, por issue, após os ADRs.

O padrão-alvo proposto, ainda a fechar:
- bronze só `select * from fonte_lake()`;
- prata e ouro em SQL de Postgres, sem `describe` nem `run_query` na compilação;
- um target só.

Os domínios entram nesta ordem: `linha_financiada_dbt`, `reforma_casa_brasil_dbt`, `mcmv_historico_dbt`.

**Antes de migrar o histórico:** o pg_duckdb 1.2.0 está com `max_temp_directory_size` vazio e `threads=-1`. O piloto derrubou o prod em 05/09. Definir esses dois parâmetros antes.

O que a varredura acrescenta:

- **O reloginho lê o bronze da execução anterior** (conferido).
  - `indicadores_mcmv_dbt` está no grupo `dominios_postgres`, mas faz `ref(f.modelo)` nos `bronze_dhist_*` do `eixo_historico`. Esse grupo roda depois (`mcid_cosmos_dag.py:127`). Num banco zerado, quebra.
  - Também usa `try_cast` do DuckDB no target `prod` sem `force_execution`. Conferir.
- **O conjuntura roda duas vezes:** no `conjuntura_dag` (`select=["conjuntura_dbt"]`) e no `mcid_cosmos_dag`, cujo seletor cobre o projeto todo.
- **O eixo histórico fica fora do OpenMetadata.**
  - `helpers/openmetadata/dbt_artifacts.py:18` roda `docs generate --selector dominios_postgres`, então mcmv_historico, reforma e linha_financiada ficam sem catálogo.
  - O docs da CI (`scripts/conjuntura/gerar_docs_seguros.py:81-90`) roda sem seletor e quebra no `describe`.
- **Não existe target de homolog.** Homolog e prod rodam `prod` e só mudam as env vars. `macros/get_custom_schema.sql:19` só roteia `prod`, `prod_duckdb` e `staging_duckdb`: um target `homolog` ingênuo jogaria tudo num schema só.
- **Linhagem:**
  - far, fds e rural (24 bronzes) leem `s3://data-lake-mcid/...` literal, sem `source()`, e por isso a linhagem no OM começa no bronze;
  - o bucket é configurado de três jeitos: `var`, `env_var` e literal.
- **Cinco padrões de bronze e ingestão duplicada.** INT057, INT059, INT065 e MONIT_MOV_OBRA alimentam duas cadeias: a do histórico e a de far, fds e rural.
- **Código morto:**
  - `models/mcmv_silver_dbt/`: 10 modelos desabilitados, com refs inexistentes;
  - `mcmv_historico_dbt/piloto/`;
  - 13 sources Postgres sem uso;
  - `analysis-paths` apontando para uma pasta que não existe;
  - `dbt deps` na CI sem `packages.yml`.
- **Padronização:**
  - `config(materialized=...)` inline nos 278 modelos;
  - `tests:` e `data_tests:` misturados;
  - 21 snapshots com `target_schema='ouro'` fixo;
  - 16 colunas do boletim com espaço e acento no nome.
- **Qualidade e documentação:**
  - 104 modelos sem teste em YAML e cerca de 200 sem teste de unicidade;
  - nenhuma source com `freshness`;
  - dois modelos sem entrada no YAML (`ouro_conjuntura_boletim_p6_ogu`, `ouro_linha_financiada_mapa_uf`);
  - os 36 `.md` de `models/docs/` não são blocos `{% docs %}`, então não chegam ao catálogo.
- **SQL duplicado:**
  - `prata_rural_prioritarios_bb` e `_caixa` são idênticos;
  - os 14 `prata_conjuntura_ibge_*` são passthroughs que cabem numa macro;
  - far e fds (`financeiro_mensal`, `evolucao_financeira`, `obra_mensal`) têm 70–80% de semelhança.

### F4. Troca do poetry pelo uv

Arquivos que mudam:
- **`pyproject.toml`:** sai `[tool.poetry]` e muda o `[build-system]`. A descrição ainda diz "IPEA".
- **`.gitignore:10`:** o `**/*.lock` também ignoraria o `uv.lock`, e é por isso que o `poetry.lock` nunca foi versionado.
- **`Makefile`:** todo `poetry run`, mais o `poetry lock` que roda a cada `make setup`.
- **CI:** `.github/actions/setup-poetry/`, `.github/workflows/main.yaml:19-60` e `.github/dependabot.yml:7`.
- **`tests/test_dependencias_sincronizadas.py:31-39`:** lê `tool.poetry.dependencies`.
- **`README.md:63-72`.**
- **Já usam uv e ficam como estão:** `infra/docker/airflow/Dockerfile` e `compile-lock.sh`.

O que resolver junto:
- **Fonte de verdade das dependências.** Hoje são quatro listas: `pyproject.toml`, `requirements.txt`, `infra/docker/airflow/requirements.lock.txt` e o `requirements.generated.txt`, que está morto (Airflow 2.8.1, só local).
- **Drift do sqlglot:** os testes usam `^30` e a imagem usa `29.0.1`. A exceção `SO_FORA_DA_IMAGEM` esconde a diferença.
- **Imports diretos não declarados:** `yaml`, `psycopg2`, `paramiko`, `httpx`, `bs4`, `chardet`, `pytz` e `packaging`.
- **Grupo dev:**
  - cerca de 17 stubs `types-*` que não têm a ver com o código (uWSGI, gevent, WTForms…);
  - tudo sem pin;
  - `jupyter` incluído.
- **Cache da CI:** a chave `hashFiles('**/poetry.lock')` é sempre vazia, porque o lock é ignorado. O cache nunca renova.
- **Workspace:** `data-science/dados-historicos-tratamento` já é um projeto uv e pode entrar como membro.

### F5. Melhorias no OpenMetadata

- **Eixo histórico no catálogo:** vem da F3.
- **Versão 1.13.3 fixada em quatro lugares:**
  - `requirements.txt:50`;
  - `infra/playbooks/openmetadata/group_vars/openmetadata.yaml:9`;
  - `infra/k8s/openmetadata/values.yaml`;
  - o Elasticsearch em `infra/k8s/openmetadata/elasticsearch.yaml:37`.
- **Valores fixos:** o serviço "Cidades" e o e-mail de admin estão em `infra/playbooks/airflow/templates/airflow.env.j2:39` e `infra/k8s/openmetadata/bootstrap.yaml`.
- **Leitura no parse:** os toggles `OM_INGEST_*` são lidos no parse do DAG.
- **Comentários desatualizados:** dizem que o `openmetadata-ingestion` roda num venv isolado (`pyproject.toml:147-149`, `infra/env/.env.example:58-62`), mas ele está na imagem.
- **Exemplo do IPEA:** `helpers/openmetadata/README.md:168-205` ainda usa `compras_gov_api`.

## 6. Candidatos novos (fora das cinco frentes)

✔ = conferido no código. Os demais vêm da varredura e devem ser confirmados ao abrir a issue.

### P0: risco real

**B1. ✔ O `raw_para_staging` sobrescreve a staging das APIs**

- **Problema:**
  - `scripts/raw_para_staging.py` varre todo o `raw/` e só ignora `dados_historicos` (`:99`, `:1085`).
  - Grava `staging/<mesmo caminho>.parquet` só com texto e acrescenta `_source_file`, `_ingested_at` e `_source_hash`.
  - São exatamente as colunas do "job do outro time" que quebrou o FGV e o infomoney em agosto. Hipótese forte: a colisão nasce do nosso próprio `minio_transform_dag` diário.
- **Correção:** restringir a varredura a `raw/sftp/`, sem mudar o caminho de ninguém. Criar um namespace próprio já foi rejeitado porque quebra o data mesh.
- **Antes:** confirmar no `lake._staging_log` se há chaves fora de `sftp/`.
- **Aceite:** um run do `minio_transform_dag` não reescreve `staging/fgv`, `staging/ibge`, `staging/bacen` nem `staging/infomoney`.

**B2. ✔ A máscara não trava o caminho da PII**

- **Problema:**
  - `mascarar_minio.run` registra a falha por objeto e retorna normalmente.
  - `raw_para_staging` não confere a tag `masked`.
  - `ClienteMinio.get_tagging` engole o erro (`plugins/cliente_minio.py:371-376`).
  - `sftp_ingest_dag` e `minio_transform_dag` têm schedules independentes.
- **Correção:** a staging exige a tag (ou o registro no `lake._masking_log`), e a task falha quando a máscara tiver erro.
- **Aceite:** teste em `tests/` mostrando que um objeto sem `masked` não vira parquet e que um erro de máscara falha a task.

**B3. ✔ O Novo CAGED para de coletar em janeiro de 2027**

- **Problema:**
  - `anos = [2024, 2025, 2026]` está fixo em `plugins/cliente_novo_caged.py:255`;
  - o corte de mês está errado por dois (`:282`);
  - o mês que falha some da staging, que é regravada inteira (`:288`).
- **Aceite:** teste que deriva anos e meses a partir de uma data fixa.

**B4. ✔ `local.env` versionado**

- **Problema:**
  - O arquivo está no `.gitignore:36`, mas foi re-adicionado em `e1c1ed3`.
  - Tem a Fernet key e credenciais de dev.
  - Três scripts carregam o arquivo: `scripts/sftp_minio_diff.py:38`, `scripts/superset/linha_financiada.py:724` e `dbt/mcid/_run-common.sh:52`. Se alguém preencher com as credenciais reais do DW ou do SFTP, elas vão para o git.
- **Aceite:** `git rm --cached local.env`, mais um `local.env.example`, e os três scripts conferidos.

**B5. Senhas em texto puro na saída do Ansible**

- **Problema:** os `debug` finais não têm `no_log`:
  - `install-airflow.yaml:232`;
  - `install-postgres.yaml:401-403`;
  - `install-rustfs.yaml:283`;
  - `install-openmetadata.yaml:349`.
- **Aceite:** `no_log: true` nos `debug`, conferido com `ansible-playbook --check`.

### P1: CI, qualidade e tooling

**B6. A CI não barra nada**

- **Problema:**
  - `main.yaml:42` roda `make lint-ci || true`;
  - black, ruff e mypy não rodam;
  - o ruff acusa 950 erros, 660 deles em `data-science/`;
  - não há `dbt parse` nem teste de import dos DAGs (DagBag).
- **Proposta:** ruff blocante só em `dags/`, `plugins/`, `helpers/` e `scripts/`, mais `dbt parse` e um teste de DagBag.

**B7. Pre-commit quebrado**

- **Problema:**
  - `setup-git-hooks.sh` roda `make format` no repositório inteiro, sem re-stage, e com `set -e` falha sempre.
  - O pre-push roda `mypy --install-types` (rede, altera o venv) e pula o sqlfluff pela flag `GITLAB_CI`.
- **Proposta:** `.pre-commit-config.yaml` só com os arquivos tocados. Cabe junto com a F4.

**B8. Imagem da CI que ninguém usa**

- **Problema:**
  - **Três nomes de imagem:** a CI publica `data-application-cidades-airflow`, o k8s espera `cidades-airflow:main` (push manual) e a VM builda local.
  - **Build em dobro:** a CI builda a imagem duas vezes.
  - **Contexto inchado:** não há `.dockerignore`, então o contexto leva `data-science/` (32 MB) e o `.git`.
  - **Conteúdo da imagem:** carrega a senha padrão `airflow/airflow` e libs de sistema sem uso (gdal, odbc, gtk).

**B9. GitHub Pages e dependabot**

- **Problema:**
  - `docs-pages.yaml` e o job `dbt_docs` do `main.yaml` publicam no mesmo Pages, e ganha o último.
  - O dependabot aponta para `/infra/docker/airflow-mcp`, que não existe.
  - O PR de pip do dependabot quebra o teste do lock por construção.

**B10. `tests/` sem `conftest.py`**

- **Problema:**
  - cada módulo faz `sys.path.insert` ou `spec_from_file_location`;
  - `test_foo.py` é placeholder;
  - `tests/ingestion/` só tem `__pycache__`;
  - os cerca de 22 testes de `data-science/` não rodam em lugar nenhum.

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
