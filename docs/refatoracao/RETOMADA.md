# Retomada das refatorações

> Referência para continuar o trabalho das issues de refatoração em outra máquina ou sessão do Claude Code.
> Estado de 29/10/2026. Guia: [README.md](README.md). ADRs: `docs/adr/` (PR #264).

Para começar uma sessão, peça ao Claude Code:

```text
Leia docs/refatoracao/RETOMADA.md, docs/refatoracao/README.md e o documento da issue
que vamos trabalhar. Siga as regras de trabalho da RETOMADA. Vamos na issue #NNN:
comece em modo planejamento.
```

## 1. Onde as coisas estão

| O quê | Onde | Estado |
|---|---|---|
| Guia e documentos por item | `docs/refatoracao/`, branch `docs/guia-refatoracao` | PR [#263](https://github.com/GovHub-br/data-application-cidades/pull/263), aberto |
| ADRs 0001–0014 | `docs/adr/`, branch `feat/adrs-arquiteturais` | PR [#264](https://github.com/GovHub-br/data-application-cidades/pull/264), aberto, todos `Proposto`; fecha a #243 |
| Issue-mãe | [#242](https://github.com/GovHub-br/data-application-cidades/issues/242) | 20 sub-issues: F1–F5 = #243–#247, B1–B15 = #248–#262 |
| Projeto de ingestão anterior | PR #206, branch `refactor/ingestao-raw-staging-bronze` | **Descartado** em 29/10. Ainda não foi fechado; fechar só com confirmação do Lucas. |

Enquanto o #263 não entrar na `main`, os links `blob/main/docs/refatoracao/...` das issues ficam quebrados. Para ler os documentos antes disso, use a branch `docs/guia-refatoracao`.

## 2. Regras de trabalho

1. **Issue antes de código.** Todo trabalho sai de uma sub-issue da #242, e o PR usa `Closes #N`.
2. **Modo planejamento primeiro.** Plano, aprovação do Lucas e só então código. Na dúvida sobre uma decisão dele, pergunte; não assuma.
3. **Um PR por item.** Na frente de dbt, um PR por domínio.
4. **Fechar o ciclo.** Ao abrir o PR e ao fechar a issue, atualize o status no documento do item (cabeçalho), no painel do guia (seção 4) e no registro de decisões (seção 8) quando houver decisão nova.
5. **Diagnosticar sobre a `origin/main`.** Antes de analisar, rode `git fetch` e `git rev-list --left-right --count origin/main...HEAD`. Uma base velha já levou a análises erradas.
6. **Commits e PRs:**
   - em português, no formato `tipo(escopo): descrição`;
   - **nunca** com `Co-Authored-By` nem "Generated with Claude Code", nem outra atribuição.
7. **O hook do git está quebrado** (B7, #254): ele formata o repositório inteiro e falha.
   - Até a #254 ser resolvida, use `git commit --no-verify` e `git push --no-verify`.
   - Rode ruff e black só nos arquivos que você tocou.
8. **Testes proporcionais ao risco.** Teste em `tests/` para o que muda comportamento. Não monte um ambiente pesado para provar o que já funciona.
9. **Dados reais:**
   - nunca valide escrevendo em `raw/` ou `staging/` reais;
   - não rode inserções manuais (`scripts/database/`) sem pedido explícito;
   - não apague nada do lake sem confirmação.
10. **ADRs:** enquanto estão `Proposto`, mudanças entram no PR #264. Depois de `Aceito`, um ADR não é editado para mudar a decisão; escreve-se outro que o substitui (`docs/adr/README.md`).
11. **Ações irreversíveis ou públicas** (fechar PR, apagar branch, force push em branch de outra pessoa, comentar em issue de outra pessoa): só com confirmação na hora.

## 3. Decisões vigentes

Detalhe em `docs/adr/`.

| Tema | Decisão | ADR |
|---|---|---|
| Arquitetura | Lake (RustFS: `raw/` e `staging/`) e warehouse (Postgres 15 com pg_duckdb). Consumo só pelo warehouse. | 0001 |
| Camadas | Raw imutável e já mascarada. Staging toda em texto, por uma conversão comum. Bronze = `select * from fonte_lake()`. Prata genérica, no grão da fonte. Toda regra de negócio fica na ouro. | 0002 |
| PII | Mascarar **antes** de gravar a raw, com HMAC determinístico. A chave nunca muda e fica só no ambiente do Airflow. | 0002 R0 |
| dbt | Padrão único: SQL de Postgres, com o pg_duckdb só na leitura do lake, e um target por ambiente (`homolog`, `prod`). A homologação é também o ambiente de desenvolvimento: sem dbt local. Saem o `prod_duckdb`, o `staging_duckdb` e os scripts shell de `dbt/mcid/`; os limites do pg_duckdb ficam só no playbook. A ordem de migração é `linha_financiada` → `reforma_casa_brasil` → `mcmv_historico`. | 0003 |
| Ingestão | Duas tasks por DAG de fonte: `extract` (escrita por fonte, mascara e grava a raw) e `transform` (comum: drift estrutural e Parquet). Hooks dos providers primeiro, sem operators de transferência. As DAGs de fonte não gravam no Postgres. | 0004 |
| Lake | Espelha a fonte, com uma pasta `carga=<UTC>` antes do arquivo. A API usa `<fonte>/<dado>/carga=.../`. Arquivo reenviado sem mudança não gera carga, pelo controle por hash. A coluna `_carga` ordena as cargas; o `_ingested_at` não. | 0005 |
| Entrada manual | CSV ou XLSX em `raw/manual/<base>/carga=.../`, pelo `scripts/enviar_manual.py`, convertido pela `manual_ingest_dag`. Carga completa, só dado sem PII. Substitui os scripts de `scripts/database/`. | 0002 R7 |
| Drift | O contrato da fonte fica no `sources.yml`. Drift estrutural na `transform`, que põe o arquivo em quarentena; drift semântico nos testes da bronze, com `error`. | 0006 |
| Nomes | `bronze_`/`prata_`/`ouro_`, um schema por secretaria (`snh`, `comum`) e colunas técnicas com `_`. | 0007, 0010 |
| Orquestração | Nada no parse, cron literal, configuração versionada, segredo em Connection, sem dado nunca é verde, `deferrable`, `max_active_runs=1` no dbt. | 0013 |
| Tooling | uv com `uv.lock` versionado, imagem única publicada pela CI, CI blocante (ruff, `dbt parse`, DagBag) e pre-commit só nos arquivos tocados. | 0014 |

**Já descartado; não proponha de novo:**
- **O pacote `plugins/ingestion` do #206.** A F2 é modelada do zero.
- **Namespace próprio na staging** (por exemplo, `staging/conjuntura_fgv/`). Quebra o data mesh.
- **`materialized_view`.** Derrubava o banco; o time a abandonou em 29/08.
- **DuckDB como destino da prata** (o estado da #119).

## 4. Fatos que já custaram caro

- **Staging das APIs sobrescrita.** O `scripts/raw_para_staging.py`, rodado pela `minio_transform_dag`, varre a `raw/` inteira e regrava `staging/<fonte>/<dado>.parquet` só em texto, com `_source_file`/`_ingested_at`/`_source_hash`. É a suspeita principal do "job do outro time" que quebrou o FGV e o infomoney em agosto (B1, #248). Antes de corrigir, confirme no `lake._staging_log` as chaves de origem fora de `sftp/`.
- **IBGE:**
  - o `IBGE_CONFIGURACOES` é um JSON frágil: uma vírgula sobrando derruba a DAG no parse;
  - a API v3 devolve o valor com ponto decimal, e não se remove o ponto na tipagem, porque isso corrompe o número;
  - o `ClienteIBGE` aceita uma classificação por bloco; duas dão HTTP 500.
- **BACEN SGS:**
  - ignora `ultimos` na query e devolve a série inteira;
  - as séries diárias exigem `dataInicial` (sem ele, HTTP 406);
  - às vezes responde 200 com HTML.
- **Tesouro:** o e-mail da dotação chega de terça a sábado, entre 4h e 8h. O cron é `0 9 * * *`.
- **SharePoint** é inserção manual, sem DAG. O `.mdb` CCI_CCA da raw antiga tem PII.
- **Testes:**
  - o `dbt-core` faz `load_dotenv()` no import e vaza o `.env` para os testes; limpe o env com uma fixture autouse;
  - venv local: um upgrade do Airflow no lugar apaga o `airflow/__init__.py`, e é preciso reinstalar com `--force-reinstall`.
- **Superset:** o bearer JWT da API é "cego" para algumas rotas; use o cookie de sessão.
- **pg_duckdb:** o piloto derrubou o banco em 05/09. Os limites ficam só no playbook do Postgres (`duckdb.max_memory` 4096 MB, `duckdb.max_temp_directory_size` 50 GB), mas em 29/10 o banco de homologação estava com `max_temp_directory_size` vazio. Reaplique o playbook antes de migrar o eixo histórico (ADR-0003). Os scripts shell de `dbt/mcid/` limitam só o processo DuckDB e saem na migração; não os use como referência.
- **Reloginho:** lê o bronze da execução anterior, porque está no grupo do Postgres e os `bronze_dhist_*` são refeitos depois, no grupo do DuckDB. Isso se resolve com o padrão único.
- **Homologação:** o Airflow roda num container único, com git-sync da `main`. Ele escreve no mesmo banco e lake configurados no `.env`, então tudo que entra na `main` roda lá.

## 5. Próximos passos, em ordem

1. **Revisão dos PRs.** O #264 (ADRs) passa por revisão do time, os ADRs mudam para `Aceito`, e o PR entra na `main`. O #263 (guia) entra na `main`.
2. **P0, à parte** (não dependem de ADR):
   - [#248](https://github.com/GovHub-br/data-application-cidades/issues/248) B1 e [#249](https://github.com/GovHub-br/data-application-cidades/issues/249) B2 juntos, porque é o mesmo script: restringir o `raw_para_staging` a `raw/sftp/` e fazer a máscara travar a conversão;
   - [#250](https://github.com/GovHub-br/data-application-cidades/issues/250) B3: anos do CAGED derivados da data;
   - [#251](https://github.com/GovHub-br/data-application-cidades/issues/251) B4: `local.env` fora do git;
   - [#252](https://github.com/GovHub-br/data-application-cidades/issues/252) B5: `no_log` no Ansible.
3. **Tooling:** [#246](https://github.com/GovHub-br/data-application-cidades/issues/246) F4 (uv), com [#253](https://github.com/GovHub-br/data-application-cidades/issues/253) B6, [#254](https://github.com/GovHub-br/data-application-cidades/issues/254) B7 e [#257](https://github.com/GovHub-br/data-application-cidades/issues/257) B10, seguindo o ADR-0014. Isso acaba com o `--no-verify`.
4. **Ingestão:** [#244](https://github.com/GovHub-br/data-application-cidades/issues/244) F2. A modelagem nova segue os ADRs 0004, 0005 e 0013. Mostre uma DAG de exemplo antes de replicar. Absorve as #258, #259 e #260.
5. **dbt e OpenMetadata:** [#245](https://github.com/GovHub-br/data-application-cidades/issues/245) F3, um PR por domínio, depois de a DAG do OM rodar verde no homolog. A [#247](https://github.com/GovHub-br/data-application-cidades/issues/247) F5 vai junto.
6. **Avulsos:** #255, #256, #261 e #262.

## 6. Pendências que dependem do Lucas

- Fechar o PR #206 e apagar a branch `refactor/ingestao-raw-staging-bronze`.
- Os 66 alertas do dependabot na `main` (3 críticos): incluir na F4 (#246) ou abrir uma issue própria.
- O ADR do TCC (`0001-arquitetura-mlops-prevencao-paralisacao.md`) existe só como arquivo local, fora do git, numa das máquinas. Ele colide com o número 0001 e vai precisar de um número livre quando entrar.
- As questões em aberto dos ADRs, listadas em [f1-adrs.md](f1-adrs.md).
