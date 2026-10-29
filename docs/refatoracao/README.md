# Guia das refatorações do data-application-cidades

> Documento vivo. Atualize o status e as decisões aqui quando uma issue for aberta, um PR entrar ou uma decisão mudar.
> Issue de acompanhamento: [#242](https://github.com/GovHub-br/data-application-cidades/issues/242), com cada item como sub-issue.
> Levantamento feito em 29/10/2026 sobre a `main` em `13a2253` (depois do #241).

## 1. Para que serve

O repositório acumulou problemas em quatro áreas:
- **padrões diferentes:** dbt com dois adaptadores, cinco jeitos de ler o lake e quatro listas de dependências;
- **código morto:** sobras dos DAGs do IPEA e do PR #206;
- **riscos reais:** PII, staging sobrescrita e coleta com prazo de validade;
- **CI que não barra nada.**

Este guia reúne o que vamos refatorar, em que ordem e com quais regras, para que cada issue e cada PR saia daqui e não de um levantamento novo.

Cada frente (F1–F5) e cada candidato (B1–B15) tem um documento próprio nesta pasta, com o problema, a correção proposta, as dependências e o critério de aceite. O painel de status (seção 4) leva a cada um.

Para retomar o trabalho noutra máquina ou sessão, comece por [RETOMADA.md](RETOMADA.md): regras de trabalho, decisões vigentes, armadilhas conhecidas e próximos passos.

## 2. Regras de trabalho

1. **Issue antes de código.** Cada item deste guia tem uma sub-issue da [#242](https://github.com/GovHub-br/data-application-cidades/issues/242). Nada é implementado fora dela, e o PR a referencia (`Closes #N`).
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
| F1 | [ADRs](f1-adrs.md) | PR aberto | [#243](https://github.com/GovHub-br/data-application-cidades/issues/243) | [#264](https://github.com/GovHub-br/data-application-cidades/pull/264) |
| F2 | [Ingestão: modelagem nova](f2-ingestao.md) | issue aberta | [#244](https://github.com/GovHub-br/data-application-cidades/issues/244) | |
| F3 | [dbt: padrão único, seletores, homolog](f3-dbt.md) | issue aberta | [#245](https://github.com/GovHub-br/data-application-cidades/issues/245) | |
| F4 | [poetry → uv](f4-uv.md) | issue aberta | [#246](https://github.com/GovHub-br/data-application-cidades/issues/246) | |
| F5 | [OpenMetadata](f5-openmetadata.md) | issue aberta | [#247](https://github.com/GovHub-br/data-application-cidades/issues/247) | |
| B1 | [`raw_para_staging` só em `raw/sftp/`](b01-raw-para-staging-so-sftp.md) | issue aberta | [#248](https://github.com/GovHub-br/data-application-cidades/issues/248) | |
| B2 | [Máscara como trava da staging](b02-mascara-como-trava.md) | issue aberta | [#249](https://github.com/GovHub-br/data-application-cidades/issues/249) | |
| B3 | [Novo CAGED com anos fixos](b03-caged-anos-fixos.md) | issue aberta | [#250](https://github.com/GovHub-br/data-application-cidades/issues/250) | |
| B4 | [`local.env` fora do git](b04-local-env-versionado.md) | issue aberta | [#251](https://github.com/GovHub-br/data-application-cidades/issues/251) | |
| B5 | [`no_log` nos playbooks](b05-ansible-no-log.md) | issue aberta | [#252](https://github.com/GovHub-br/data-application-cidades/issues/252) | |
| B6 | [CI blocante (ruff, dbt parse, DagBag)](b06-ci-blocante.md) | issue aberta | [#253](https://github.com/GovHub-br/data-application-cidades/issues/253) | |
| B7 | [pre-commit só nos arquivos tocados](b07-pre-commit.md) | issue aberta | [#254](https://github.com/GovHub-br/data-application-cidades/issues/254) | |
| B8 | [Imagem única da CI para VM e k8s](b08-imagem-unica.md) | issue aberta | [#255](https://github.com/GovHub-br/data-application-cidades/issues/255) | |
| B9 | [GitHub Pages e dependabot](b09-pages-dependabot.md) | issue aberta | [#256](https://github.com/GovHub-br/data-application-cidades/issues/256) | |
| B10 | [`tests/` com `conftest.py`](b10-conftest.md) | issue aberta | [#257](https://github.com/GovHub-br/data-application-cidades/issues/257) | |
| B11 | [Código morto da ingestão](b11-codigo-morto-ingestao.md) | issue aberta | [#258](https://github.com/GovHub-br/data-application-cidades/issues/258) | |
| B12 | [Um escritor de staging](b12-escritor-unico-staging.md) | issue aberta | [#259](https://github.com/GovHub-br/data-application-cidades/issues/259) | |
| B13 | [Orquestração dos DAGs](b13-orquestracao.md) | issue aberta | [#260](https://github.com/GovHub-br/data-application-cidades/issues/260) | |
| B14 | [Infra sem duplicação](b14-infra-duplicada.md) | issue aberta | [#261](https://github.com/GovHub-br/data-application-cidades/issues/261) | |
| B15 | [Higiene do repositório](b15-higiene-repositorio.md) | issue aberta | [#262](https://github.com/GovHub-br/data-application-cidades/issues/262) | |

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

| ID | Item |
|---|---|
| B11 | [Código morto da ingestão](b11-codigo-morto-ingestao.md) |
| B12 | [Um escritor de staging](b12-escritor-unico-staging.md) |
| B13 | [Orquestração dos DAGs](b13-orquestracao.md) |
| B14 | [Infra sem duplicação](b14-infra-duplicada.md) |
| B15 | [Higiene do repositório](b15-higiene-repositorio.md) |

## 7. Decisões em aberto

| Decisão | Onde se decide | Bloqueia |
|---|---|---|
| Quais regras do #206 valem no modelo novo | Modelagem da F2 | F2 |
| DAGs quase idênticas: fábrica ou não | Modelagem da F2 | F2 |
| `uv.lock` × constraints da imagem base do Airflow | ADR-0014, na #246 | F4 |
| `data-science/`: repositório, LFS ou lake | Issue da B15 | — |

## 8. Registro de decisões

| Data | Decisão |
|---|---|
| 29/10/2026 | O levantamento é aprovado como base do trabalho. |
| 29/10/2026 | O projeto de ingestão do PR #206 é descartado; a F2 é modelada do zero, providers primeiro e depois clientes próprios. |
| 29/10/2026 | Cada documento vira uma sub-issue da #242 (#243–#262); a F1 ganha issue nova, porque não havia uma aberta. |
| 29/10/2026 | ADRs revisados e propostos no PR #264. Contrato da staging (0002/0004), Postgres raw das APIs fora (0004), nomenclatura (0007), padrão único do dbt (0003), lake espelhando a fonte com `carga=` (0005), providers na `extract` (0004), orquestração (0013) e dependências, imagem e CI (0014). |
| 29/10/2026 | A entrada manual passa pela raw: CSV ou XLSX em `raw/manual/<base>/carga=.../`, enviado pelo `scripts/enviar_manual.py` e convertido pela `manual_ingest_dag`, com carga completa e só dado sem PII (ADR-0002 R7, 0004, 0005). A coluna `_carga` ordena as cargas, e a migração da raw antiga só copia objetos sem PII (0005). |
| 29/10/2026 | A homologação é também o ambiente de desenvolvimento: o time testa direto nela, sem dbt local; a produção é o ambiente do ministério. Os limites do pg_duckdb ficam só no playbook do Postgres, e os scripts shell de `dbt/mcid/` saem na migração (ADR-0003). |
| 29/10/2026 | O drift estrutural vira a task `verify_structural_data_drift`, entre `extract` e `transform`; o semântico segue nos testes da bronze, rodados pelo Cosmos como tasks da DAG geral. Todo alerta de drift é e-mail (ADR-0006, ADR-0004). |
| 29/10/2026 | Sem schema `comum`: cada secretaria carrega as fontes que usa no próprio schema, e o compartilhamento fica com o ministério quando outra secretaria chegar (ADR-0007, 0010, 0011). Só a nomenclatura do medalhão fica em português; todo nome técnico de rastreio vai para inglês: `_carga`/`carga=` viram `_load_id`/`load_id=`, e os status, `quarantined`, `uncataloged` e `additive_drift` (ADR-0007). |
