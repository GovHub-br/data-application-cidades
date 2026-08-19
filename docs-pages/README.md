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
│   ├── dados.py      agrega acervo e curadoria, aplica o escopo
│   ├── graficos.py   gera os SVG dos gráficos
│   └── mermaid.py    gera e renderiza a linhagem das tabelas gold
├── src/
│   ├── _data/        acervo coletado (JSON versionado)
│   ├── _diagramas/   SVG das linhagens, em cache por hash (versionado)
│   ├── dominios.yml  curadoria: escopo, programa e contexto dos domínios
│   ├── templates/    Jinja2 — uma página por template
│   ├── assets/       tema.css e app.js
│   └── acervo/       documentos enviados pela equipe (a criar)
├── plans/            design e planos
└── site/             saída do build (ignorada pelo git)
```

## Diagramas de linhagem

Cada tabela gold tem um diagrama do fluxo que a constrói, da fonte até ela. O
texto Mermaid é gerado dos `ref()` e `source()` do próprio SQL — não é desenhado
à mão e não sai do lugar quando o modelo muda.

O SVG é renderizado pelo `mermaid-cli`, que precisa de Node e de um Chrome, e
fica em cache em `src/_diagramas/<hash>.svg`, versionado. A chave é o hash do
texto do diagrama: um diagrama só é re-renderizado quando a linhagem muda. É o
que permite ao CI construir o site sem Node instalado.

Sem Node na máquina, o build segue adiante: a página mostra a definição Mermaid
no lugar do desenho, e avisa quantos diagramas ficaram sem SVG.

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
