# Constituição do data-application-cidades

## Core Principles

### I. Mínimo de código novo

- Antes de escrever código, reaproveite nesta ordem: o que já existe no repositório, a stdlib,
  os hooks dos providers do Airflow e as dependências já instaladas. Dependência nova para
  poucas linhas é proibida.
- Nada de abstração, opção, wrapper ou configuração que nenhum requisito pede. Apagar vale mais
  que acrescentar.
- O menor diff que resolve o problema inteiro vence, incluindo os chamadores, testes e
  fixtures que a mudança quebra.
- Atalho com limite conhecido leva o comentário `shortcut: <limite>, <quando evoluir>`.

Por quê: o repositório já acumula padrões duplicados e código morto (guia das refatorações).

### II. PII nunca pousa sem máscara

- A PII é mascarada antes de gravar a `raw/`, com HMAC determinístico. A chave nunca muda e
  fica só no ambiente do Airflow.
- Falha na máscara MUST parar o caminho do dado: um objeto sem máscara confirmada nunca vira
  staging.
- O time não guarda o valor original em nenhum ambiente próprio.
- Segredos ficam em Connection ou variável de ambiente, nunca versionados. Logs, saídas de
  Ansible, snapshots e commits não carregam senha nem PII.

### III. Dados reais intocados na validação

- A validação usa `tests/`. Nunca se valida escrevendo em `raw/` ou `staging/` reais.
- Inserções manuais (`scripts/database/`) só rodam com pedido explícito, e nada se apaga do
  lake sem confirmação.
- A homologação roda a `main` com o banco e o lake reais: só entra na `main` o que pode rodar
  lá.

### IV. Testes proporcionais ao risco

- Mudança de comportamento leva teste em `tests/`. Lógica nova não trivial (ramo, parser,
  segurança, máscara) leva ao menos um teste pequeno.
- Não se monta ambiente pesado para provar o que já funciona.

### V. Contrato muda por ADR

- Mudança de contrato (staging, raw, nomenclatura, padrão do dbt, orquestração, tooling) passa
  por um ADR em `docs/adr/`.
- Um ADR `Aceito` não é editado para mudar a decisão: escreve-se outro que o substitui.
- A spec e o plano de cada item citam os ADRs que seguem.

## Restrições técnicas

- **Airflow:** a release mais nova, com as dependências pinadas pelas constraints oficiais dela.
- **dbt:** SQL de Postgres, com o pg_duckdb só na leitura do lake. Targets `homolog` (também é
  o ambiente de desenvolvimento) e `prod` (ministério). Não há dbt local.
- **Lake:** RustFS com `raw/` e `staging/`. O consumo é só pelo warehouse.
- **Nomes:** `bronze_`/`prata_`/`ouro_` em português. Colunas técnicas, status, chaves do
  contrato e ids de DAG e task ficam em inglês. O código segue a língua do código ao redor.

## Fluxo de desenvolvimento

1. **Issue antes de código.** Todo trabalho sai de uma issue, e o PR a fecha com `Closes #N`.
2. **Spec Kit por item:**
   - `/speckit-specify` (entrada: a issue e, na refatoração, o documento de
     `docs/refatoracao/`);
   - `/speckit-clarify`, quando houver ambiguidade;
   - `/speckit-plan`, `/speckit-tasks` e `/speckit-analyze`;
   - **aprovação do Lucas**;
   - `/speckit-implement`.

   Correções triviais (typo, uma linha) dispensam o fluxo.
3. **Ponytail no plano e no código.** O plano justifica cada arquivo novo. Antes do PR, uma
   revisão (`/ponytail-review`) corta o que não é necessário.
4. **Um PR por item.** No dbt, um PR por domínio.
5. **Base atual.** Diagnosticar sobre a `origin/main` depois de `git fetch`.
6. **Branches, commits e PRs:**
   - branches `tipo/escopo`;
   - commits em português, no formato `tipo(escopo): descrição`;
   - nunca com atribuição a ferramentas.
7. **Lint só nos arquivos tocados** enquanto o pre-commit não for corrigido (#254).
8. **Fechar o ciclo.** Ao abrir o PR e ao fechar a issue, atualizar o status no documento do
   item e no painel do guia.

## Governance

- Esta constituição prevalece sobre outras práticas do repositório. O guia de execução do
  dia a dia é `docs/refatoracao/RETOMADA.md`.
- Toda mudança na constituição entra por PR revisado pelo time. A versão segue semver:
  - MAJOR remove ou redefine um princípio;
  - MINOR acrescenta um princípio ou seção;
  - PATCH só ajusta a redação.
- Todo `plan.md` passa pelo Constitution Check. Uma violação só entra justificada na tabela de
  complexidade do plano.

**Version**: 1.0.0 | **Ratified**: 2026-11-10 | **Last Amended**: 2026-11-10
