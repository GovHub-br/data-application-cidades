# B9 · GitHub Pages e dependabot

> Parte do [guia das refatorações](README.md) · Prioridade P1 · Status: issue aberta · Issue: [#256](https://github.com/GovHub-br/data-application-cidades/issues/256) · PR: —

## Problema

- **Dois workflows publicam no mesmo GitHub Pages.** São o `.github/workflows/docs-pages.yaml:41-55` e o job `dbt_docs` do `main.yaml:119-212`, e ganha o último a rodar. O `dbt_docs` ainda exige VPN e espera o `docker_build` sem necessidade.
- **O dependabot aponta para caminhos que não servem:**
  - `/infra/docker/airflow-mcp`, que não existe (`.github/dependabot.yml:51`);
  - `/docs-pages`, que não tem manifesto (`:20`).
- **Os PRs de pip do dependabot quebram por construção.** O update de pip na raiz mexe no `requirements.txt` sem regenerar o lock da imagem, e o teste do lock falha.
- **Versões de actions misturadas:** `checkout@v7`/`@v4`, `setup-python@v7`/`@v5`.

## Correção proposta

- Um site só no Pages, com o dbt docs como subcaminho do docs-pages ou em outro destino.
- Dependabot só nos manifestos reais.
- O update de pip regenerando o lock, ou desligado.

## Critério de aceite

- Um deploy não sobrescreve o outro.
- O dependabot não abre PR para caminho inexistente.
