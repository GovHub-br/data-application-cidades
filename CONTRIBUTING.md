# Guia de Contribuição — Gov Hub BR

Antes de começar, obrigado por considerar contribuir com o **Gov Hub BR**!

O GovHub BR é uma plataforma open-source com o propósito de transformar dados públicos em ativos estratégicos para a administração pública e a sociedade. Toda contribuição — seja código, pipelines de dados, documentação, ideias ou feedback — é bem-vinda.

---

## Índice

- [Código de Conduta](#código-de-conduta)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Configurando o Ambiente Local](#configurando-o-ambiente-local)
- [Convenção de Branches](#convenção-de-branches)
- [Padrão de Commits](#padrão-de-commits)
- [Fluxo de Pull Request](#fluxo-de-pull-request)
- [Como Pegar ou Atribuir Issues](#como-pegar-ou-atribuir-issues)
- [Processo de Code Review](#processo-de-code-review)
- [Executando os Testes](#executando-os-testes)
- [Padrões de Código e Lint](#padrões-de-código-e-lint)
- [Boas Práticas Gerais](#boas-práticas-gerais)

---
## Código de Conduta
Por favor, leia nosso [Código de Conduta](CODE_OF_CONDUCT.md). Ele está em vigor o tempo todo. Esperamos que seja respeitado por todos que contribuem para este projeto. Comportamentos inadequados não serão tolerados.

## Estrutura do Projeto

```
.
├── airflow/
│   ├── dags/
│   └── plugins/
├── dbt/
│   └── models/
├── jupyter/
│   └── notebooks/
├── superset/
│   └── dashboards/
├── docker-compose.yml
├── Makefile
├── CONTRIBUTING.md
└── README.md
```

> Consulte a [documentação de arquitetura](https://gov-hub.io/govhub/documentacao/arquitetura/) para entender o fluxo completo dos dados.

---

## Configurando o Ambiente Local

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/)
- [Git](https://git-scm.com/) >= 2.x
- Python = 3.11
- Make
- Acesso às credenciais dos sistemas estruturantes (quando necessário) — veja o [guia de credenciais](https://gov-hub.io/govhub/documentacao/tutoriais/sistemas-estruturantes/acesso-apis-siafi-siape/)

### Passo a passo

#### 1. Faça o fork pelo GitHub e clone o seu fork
```bash
git clone https://github.com/<seu_usuario>/data-application-gov-hub.git
cd data-application-gov-hub
```

#### 2. Adicione o repositório original como remote "upstream"
```bash
git remote add upstream https://github.com/GovHub-br/data-application-gov-hub.git
```

#### 3. Execute a configuração usando Make:
```bash
make setup
```

Isso irá:
- Criar os ambientes virtuais necessários
- Instalar as dependências
- Configurar os hooks de pre-commit
- Preparar o ambiente de desenvolvimento

#### 4. Copie as variáveis de ambiente e configure conforme necessário

```bash
cp local.env .env
```

#### 5. Suba o ambiente com Docker Compose
```bash
docker compose up -d
```

#### 6. Acesse os serviços locais
```bash
# Apache Airflow:  http://localhost:8080
# Apache Superset: http://localhost:8088
# Jupyter:         http://localhost:8888
```

Para instruções detalhadas de cada componente, consulte a [documentação de instalação](https://gov-hub.io/govhub/documentacao/instalacao/).

---

## Convenção de Branches

Crie sua branch a partir de `main` com um nome descritivo seguindo o padrão:

```
<tipo>/<descricao-curta>
```

| Tipo | Quando usar |
|------|-------------|
| `feat` | Nova funcionalidade ou pipeline |
| `fix` | Correção de bug ou inconsistência de dados |
| `docs` | Alterações apenas em documentação |
| `refactor` | Refatoração sem mudança de comportamento |
| `ci` | Mudanças em CI/CD ou infraestrutura |
| `test` | Adição ou ajuste de testes (DBT ou unitários) |
| `chore` | Tarefas de manutenção gerais |

**Exemplos:**

```bash
git checkout -b feat/integracao-siafi-despesas
git checkout -b fix/corrigir-modelo-silver-servidores
git checkout -b docs/atualizar-dicionario-siape
git checkout -b ci/ajustar-pipeline-kubernetes
```

---

## Padrão de Commits

As mensagens de commit devem seguir o padrão [Conventional Commits](https://www.conventionalcommits.org/pt-br/), conforme adotado pelo projeto.

### Formato

```
<tipo>(<escopo opcional>): <descrição clara e objetiva no imperativo>
```

### Tipos aceitos

| Tipo | Descrição |
|------|-----------|
| `feat` | Nova funcionalidade ou novo modelo/pipeline |
| `fix` | Correção de bug ou erro em transformação |
| `docs` | Documentação (MkDocs, docstrings, README) |
| `ci` | Integração contínua, Docker, Kubernetes, Airflow |
| `refactor` | Melhoria de código sem alteração de comportamento |
| `test` | Testes DBT (`schema.yml`) ou testes unitários |
| `chore` | Manutenção geral, atualização de dependências |
| `perf` | Melhoria de performance em queries ou pipelines |

### Exemplos

```bash
# Nova funcionalidade
git commit -m "feat(dbt): adicionar modelo gold de execução orçamentária por UG"

# Correção de bug
git commit -m "fix(dag): corrigir timeout na DAG de ingestão do SIAPE"

# Documentação
git commit -m "docs: adicionar dicionário de dados para domínio de pessoal"

# CI/Infraestrutura
git commit -m "ci: ajustar configuração do Astronomer Cosmos para DBT 1.8"

# Referenciando issue
git commit -m "feat(dbt): criar snapshot de cargos e funções

Closes #42"
```

Quando necessário, utilize a descrição estendida do commit para detalhar motivações, impactos e decisões técnicas importantes. Isso facilita o entendimento histórico das mudanças e contribui para uma base de código mais sustentável e auditável.

---

## Fluxo de Pull Request

```
fork → branch → commits → push → Pull Request → review → merge
```

### Passo a passo

```bash
# 1. Mantenha sua branch atualizada com o upstream antes de enviar
git fetch upstream
git rebase upstream/main

# 2. Faça push da sua branch para o seu fork
git push origin feat/integracao-siafi-despesas

# 3. Abra um Pull Request no GitHub apontando para a branch main do repositório principal
```

Antes de enviar, certifique-se de que sua alteração está funcionando corretamente, sem quebrar funcionalidades existentes, e que segue os padrões definidos pelo projeto.

### Checklist antes de abrir o PR

- [ ] As alterações funcionam corretamente no ambiente local (Docker Compose)
- [ ] Os testes DBT passam (`dbt test`)
- [ ] A branch está atualizada com `upstream/main`
- [ ] O título do PR segue o padrão Conventional Commits
- [ ] O PR referencia a issue relacionada (`Closes #<número>`)
- [ ] Documentação atualizada, se aplicável
- [ ] Prints, logs ou exemplos de output incluídos quando relevante

### Template de Pull Request

Utilize o modelo disponível em `.github/PULL_REQUEST_TEMPLATE.md` ao abrir um PR:

```markdown
## Descrição
<!-- O que esse PR faz? Por que essa mudança é necessária? -->

## Tipo de mudança
- [ ] Nova funcionalidade / pipeline
- [ ] Correção de bug ou inconsistência de dados
- [ ] Refatoração de modelo DBT
- [ ] Documentação
- [ ] Infraestrutura / CI
- [ ] Outro: ___

## Issues relacionadas
Closes #

## Como testar / validar
<!-- Passo a passo para reproduzir e validar a mudança -->
1.
2.

## Evidências
<!-- Prints, logs, resultados de query ou link de documentação -->

## Checklist
- [ ] Testes DBT adicionados/atualizados
- [ ] Documentação atualizada
- [ ] Sem dados sensíveis ou credenciais no código
```

---

## Como Pegar ou Atribuir Issues

Toda solicitação de mudança, correção de bug ou sugestão de melhoria deve ser registrada por meio de uma issue, usando os templates disponíveis em `.github/ISSUE_TEMPLATE/`. Certifique-se de preencher todos os campos obrigatórios com informações precisas: contexto, impacto e possíveis caminhos de solução.

1. Navegue até a aba [**Issues**](https://github.com/GovHub-br/govhub/issues) do repositório.
2. Filtre por labels como `good first issue` ou `help wanted` para começar.
3. Leia a descrição completa e verifique se a issue já tem alguém atribuído.
4. Comente na issue manifestando interesse: **"Posso trabalhar nessa issue?"**.
5. Aguarde a confirmação de um mantenedor antes de iniciar.

> **Importante:** não abra PRs para issues já atribuídas a outro contribuidor sem combinar antes.

### Templates disponíveis

- `bug_report.md` — erros em modelos DBT, DAGs ou infraestrutura
- `feature_request.md` — novas integrações ou funcionalidades
- `documentation.md` — melhorias na documentação ou dicionário de dados

---

## Processo de Code Review

### Para quem submete o PR

- Responda a todos os comentários de revisão de forma objetiva.
- Implemente as mudanças solicitadas em novos commits (evite `push --force` após o início do review).
- Aguarde nova aprovação antes de solicitar merge.

### Para quem faz o review

- Seja construtivo e específico — explique o *porquê* da sugestão.
- Diferencie bloqueadores (`X`) de sugestões opcionais.
- Verifique especialmente: qualidade dos modelos DBT, linhagem de dados, testes de qualidade e impacto em pipelines existentes.
- O merge é responsabilidade dos mantenedores do projeto.

---

## Executando os Testes

### Testes DBT

```bash
# Executar todos os testes de qualidade de dados
dbt test

# Executar testes de um modelo específico
dbt test --select modelo_gold_orcamento

# Compilar os modelos sem executar (validação de SQL)
dbt compile

# Gerar e visualizar a documentação DBT
dbt docs generate
dbt docs serve
```

Consulte a documentação de [testes DBT](https://gov-hub.io/govhub/documentacao/tutoriais/dbt/testes/) para mais detalhes.
###
### Validação de DAGs (Airflow)

```bash
# Verificar sintaxe de todas as DAGs
python -m pytest tests/ -v

# Testar execução de uma DAG específica
airflow dags test <nome_da_dag> <data_execucao>
```

> Toda contribuição que adiciona ou altera modelos DBT deve incluir testes de qualidade no `schema.yml` correspondente (ex.: `not_null`, `unique`, `accepted_values`).

---

## Padrões de Código e Lint

### DBT / SQL

- Siga a [Arquitetura Medallion](https://gov-hub.io/govhub/documentacao/tutoriais/dbt/arquitetura-medallion/) (bronze → silver → gold).
- Nomeie modelos com prefixo da camada: `bronze_`, `silver_`, `gold_`.
- Documente cada modelo e coluna no arquivo `schema.yml` correspondente.
- Use [macros DBT](https://gov-hub.io/govhub/documentacao/tutoriais/dbt/macros/) para lógica reutilizável.

### Python (DAGs e scripts)

```bash
# Instalar dependências de desenvolvimento
pip install -r requirements-dev.txt

# Verificar lint
flake8 dags/ --max-line-length=120

# Formatar código
black dags/
```

### Documentação (MkDocs)

```bash
# Visualizar documentação localmente
mkdocs serve

# Construir site estático
mkdocs build
```

---

## Boas Práticas Gerais

- **PRs pequenos e focados:** um PR por funcionalidade ou correção facilita o review.
- **Sem credenciais no código:** nunca commite tokens, senhas ou chaves de API — use variáveis de ambiente. Veja o [guia de credenciais](https://gov-hub.io/govhub/documentacao/tutoriais/sistemas-estruturantes/acesso-apis-siafi-siape/).
- **Sem dados sensíveis:** não inclua dados reais de servidores públicos ou cidadãos em testes ou exemplos.
- **Documente junto ao código:** atualize `schema.yml` e a documentação no mesmo PR da mudança.
- **Compatibilidade:** valide que sua contribuição não quebra pipelines ou modelos existentes.
- **Novas dependências:** discuta em uma issue antes de adicionar novos pacotes ou serviços.
- **Idioma:** código e comentários técnicos em **inglês**; issues, PRs e documentação em **português**.

## FAQ

**Posso contribuir sem ter sido atribuído a uma issue?**
> Sim, para correções triviais como typos na documentação. Para mudanças em modelos DBT, DAGs ou infraestrutura, abra ou comente uma issue antes.

**Não tenho acesso aos sistemas estruturantes (SIAFI, SIAPE). Posso contribuir?**
> Sim! Muitas contribuições não exigem acesso a APIs governamentais — documentação, testes DBT com dados sintéticos, melhorias de infraestrutura e dashboards no Superset são exemplos.

**Onde posso tirar dúvidas técnicas sobre DBT, Airflow ou Superset?**
> Consulte os [tutoriais da documentação](https://gov-hub.io/govhub/documentacao/instalacao/) ou abra uma [Discussion](https://github.com/GovHub-br/govhub/discussions) no repositório.

**Meu PR foi fechado sem merge. O que faço?**
> Leia o motivo no comentário de fechamento, corrija os pontos indicados e abra um novo PR referenciando o anterior.

---

<div align="center">
  Feito com 💜 pela comunidade GovHub BR &nbsp;·&nbsp;
  <a href="https://github.com/GovHub-br/govhub/issues">Reportar problema</a> &nbsp;·&nbsp;
  <a href="https://gov-hub.io/govhub/">Documentação</a>
</div>