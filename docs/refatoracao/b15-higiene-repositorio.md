# B15 · Higiene do repositório

> Parte do [guia das refatorações](README.md) · Prioridade P2 · Status: issue aberta · Issue: [#262](https://github.com/GovHub-br/data-application-cidades/issues/262) · PR: —

## O que arrumar

- **`README.md`.** É o texto genérico do Gov Hub/IPEA:
  - URL de clone do GitLab (`:59`);
  - targets de make que não existem: `tests`, `clean` e `build` (`:133-137`);
  - passos de GPG do GitLab (`:154`).
- **`.user.yml` do dbt versionados.** São três: na raiz, em `dbt/` e em `dbt/mcid/`. Guardam o ID anônimo do dbt e devem ir para o `.gitignore`.
- **`superset/__init__.py` vazio.** É um pacote da era Siape/Siafi que faz sombra ao módulo `superset`.
- **`Plano-Trabalho.md` parado.** É o checklist do boletim, com os "Status:" vazios, sem mudança desde 19/08/2026.
- **`docs-pages/src/_data/airflow.json` desatualizado.** Lista 74 DAGs com o prefixo `airflow_lappis/`, mas só 9 existem depois do #237. Rodar `make docs-collect`.
- **`scripts/snapshots/*.json` versionados.** Listam a árvore do SFTP, com nomes de arquivo do CadÚnico.
- **`requirements.generated.txt`.** É ignorado e só existe localmente; sai junto com a [F4](f4-uv.md).
- **`data-science/`.**
  - Ocupa 32 MB dos 43 MB versionados (74%): 128 CSVs, PDFs, zip, xlsx e PNGs.
  - Está no escopo, porque é a base do TCC de prevenção de paralisação.
  - Decidir se fica no repositório, vai para o Git LFS ou vai para o lake.

## Critério de aceite

- O README descreve o projeto do MCid e os comandos que existem.
- Nenhum `.user.yml` versionado.
- A decisão sobre o `data-science/` está anotada no [registro de decisões](README.md#8-registro-de-decisões).

## Relação

Avulso. Não bloqueia nada.
