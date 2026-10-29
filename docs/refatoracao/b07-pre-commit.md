# B7 · Pre-commit quebrado

> Parte do [guia das refatorações](README.md) · Prioridade P1 · Status: a abrir · Issue: — · PR: —

## Problema

Não existe `.pre-commit-config.yaml`. O `setup-git-hooks.sh` grava hooks direto em `.git/hooks`.

- **O pre-commit sempre falha.**
  - Ele roda `make format`, que faz `black .`, `ruff check --fix .` e `sqlfmt ./dbt` no repositório inteiro, incluindo `data-science/`.
  - Não re-adiciona ao stage o que formatou.
  - Com `set -e`, os erros que o ruff não corrige sozinho fazem o hook falhar em todo commit. Quando não falha, reescreve arquivos que nada têm a ver com o commit.
- **O pre-push é lento e incompleto.**
  - Roda `mypy . --install-types`, que acessa a rede e altera o venv, e depois a suíte de testes inteira.
  - Pula o sqlfluff por causa da flag `GITLAB_CI`, herdada do GitLab.
- **O script mente sobre si mesmo.** As mensagens usam `${GREEN}` e `${NC}`, que não estão definidos, e as descrições dos hooks não batem com o que eles fazem.

Até isto ser resolvido, o time commita com `--no-verify` e roda o lint só nos arquivos tocados.

## Correção proposta

- Um `.pre-commit-config.yaml` com ruff, black e sqlfmt, rodando só nos arquivos do commit.
- Remover o `setup-git-hooks.sh`.

## Critério de aceite

Um commit que toca um arquivo só roda os hooks nesse arquivo e passa.

## Relação

Vai junto com a [F4](f4-uv.md) e o [B6](b06-ci-blocante.md).
