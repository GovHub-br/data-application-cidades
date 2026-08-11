# docs-pages

Site de roadmap e documentação narrativa do projeto, publicado no GitHub Pages.
Todo o conteúdo factual é lido do próprio repositório a cada publicação; a
narrativa é escrita à mão e versionada aqui.

O design completo está em [plans/2026-08-11-docs-roadmap-design.md](plans/2026-08-11-docs-roadmap-design.md).

## Como rodar

```bash
make docs-serve     # gera o site e serve em http://localhost:8000
```

O primeiro `make docs-*` cria um virtualenv próprio em `docs-pages/.venv` com
`jinja2` e `pyyaml`. Não é preciso ter Airflow, dbt nem Poetry instalados.

| Comando | O que faz | Precisa de rede |
|---|---|---|
| `make docs-collect` | Atualiza o acervo em `src/_data/` a partir de git, `gh`, dbt e DAGs | sim |
| `make docs-build` | Renderiza o site em `docs-pages/site/` | não |
| `make docs-serve` | Build e servidor local | não |
| `make docs-clean` | Apaga o site gerado | não |

`docs-collect` e `docs-build` são separados de propósito: o build no CI nunca
depende de rede nem de banco, e o diff de uma coleta mostra exatamente o que
mudou no acervo.

## Estrutura

```
docs-pages/
├── tooling/          código do pipeline (coleta e build)
│   ├── collect.py    orquestra os coletores
│   ├── collectors/   git+PRs, dbt, Airflow, acervo
│   ├── build.py      renderiza os templates
│   └── graficos.py   gera os SVG dos gráficos
├── src/
│   ├── _data/        acervo coletado (JSON versionado)
│   ├── templates/    Jinja2 — uma página por template
│   ├── assets/       tema.css e app.js
│   └── acervo/       documentos enviados pela equipe (a criar)
├── plans/            design e planos
└── site/             saída do build (ignorada pelo git)
```

## O que é gerado e o que é escrito

Nenhum número é digitado à mão nos templates: contagens, inventários e gráficos
vêm de `src/_data/*.json`. A prosa — o problema que cada entrega resolveu — é
escrita à mão nos templates e, a partir da fatia 2, no `roadmap.yml`.

Se um coletor falhar, o JSON anterior é preservado e o site publica com o último
dado bom. O build só falha em duas situações: link interno quebrado e template
inválido.

## Estado atual

Fatia 1 (esqueleto vivo) entregue: coletores, build, tema GovHub, capa com
números reais e as três abas. As páginas de fase (`roadmap.yml`), as páginas de
domínio com linhagem Mermaid e o acervo de documentos vêm nas fatias 2 a 4.
