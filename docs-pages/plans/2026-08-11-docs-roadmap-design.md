# Design — Site de roadmap e documentação narrativa

**Data:** 2026-08-11
**Repositório:** `GovHub-br/data-application-cidades`
**Status:** design validado; fatia 1 (esqueleto vivo) implementada em `docs-pages/`

## Objetivo

Publicar no GitHub Pages um site que conte a história do que foi construído neste
repositório, em três leituras — gestão, técnica e vitrine — a partir dos artefatos
que o próprio projeto já produz: commits e PRs, modelos dbt, DAGs e clientes de
ingestão, além de documentos enviados pela equipe.

O site não é um changelog nem um manual. É um roadmap narrado, em que cada
afirmação de valor vem acompanhada da evidência que a sustenta.

## Decisões

| Decisão | Escolha |
|---|---|
| Público | três abas: Gestão, Técnico, Vitrine |
| Eixo narrativo | timeline (espinha) + domínios (profundidade) |
| Produção de conteúdo | narrativa à mão, dados factuais gerados |
| Gerador | HTML estático próprio, montado com Jinja2 |
| Fontes | git/PRs, artefatos dbt, DAGs/clientes, documentos enviados |
| Diagramas | Mermaid, renderizado para SVG no CI |
| Ambição visual | rica: timeline interativa, diagramas, gráficos |
| Fases do roadmap | por período |
| Publicação | GitHub Pages via Actions (repositório é público) |

## Arquitetura

Três estágios separados por contrato de dados.

### 1. Coleta

Quatro coletores independentes em `docs-pages/tooling/collectors/`, cada um com uma
responsabilidade e uma saída própria:

| Coletor | Lê | Produz |
|---|---|---|
| `git_pr.py` | `git log` e `gh pr list --json` | commits e PRs com data, autor, labels, corpo |
| `dbt_models.py` | árvore de `models/` e `schema.yml` dos três projetos | modelos, camadas, linhagem, testes |
| `airflow_dags.py` | AST dos arquivos em `dags/` e `plugins/` | DAGs, schedules, clientes e fontes |
| `assets.py` | `docs-pages/src/acervo/` | documentos enviados, com metadados |

Cada coletor grava um JSON em `docs-pages/src/_data/`. Rodam isolados: a falha de um não
impede os demais.

### 2. Curadoria

`docs-pages/src/` guarda a narrativa escrita à mão, versionada em Markdown e YAML.
`roadmap.yml` define as fases e as amarra aos dados coletados por consulta.

### 3. Build

`docs-pages/tooling/build.py` resolve as consultas, renderiza os templates Jinja2 e
escreve HTML estático em `docs-pages/site/`. O GitHub Actions publica em Pages a cada push
na `main`.

**Invariante:** nenhum número é digitado à mão em HTML. A prosa nunca é gerada;
os fatos nunca são manuais.

## Estrutura do site

O acervo é um só; as abas são três leituras dele.

| Aba | Pergunta que responde | Fonte dominante |
|---|---|---|
| Gestão | O que foi entregue e que problema resolveu? | narrativa, números agregados, documentos |
| Técnico | Como funciona e como eu mexo nisso? | dbt, DAGs, linhagem, PRs |
| Vitrine | Por que importa e como replicar? | narrativa institucional, arquitetura |

```
/                     capa: hero, números vivos, três portas de entrada
/gestao/              roadmap negocial
/gestao/fases/<id>/   fase: contexto, entregas, evidências
/tecnico/             arquitetura, stack, como rodar
/tecnico/fontes/      clientes de ingestão e schedules
/tecnico/modelos/     inventário dbt navegável
/dominios/<slug>/     história fim a fim de um domínio
/vitrine/             narrativa institucional e replicabilidade
/acervo/              documentos, apresentações, dashboards
```

### Páginas de domínio

São o núcleo do site. Um único template renderiza todos os domínios a partir de
`dominios.yml`, contando: problema de negócio, fontes, caminho bronze/silver/gold,
uso concreto e marcos históricos. As três abas apontam para a mesma página,
variando apenas a seção destacada.

Recorte inicial, a confirmar com a equipe: `conjuntura`, `ted`, `pessoas`,
`contratos`, `orcamento`, `entidades`, `mcmv-far`, `emendas`, `dados-abertos`.

## O contrato de costura

`roadmap.yml` é o arquivo mantido à mão. Declara fases e consulta o acervo em vez
de listar entregas:

```yaml
fases:
  - id: fundacao
    periodo: [2024-12, 2025-03]
    titulo: "Fundação da plataforma"
    negocio: |
      Antes, cada análise exigia extração manual de sistema em sistema.
      Esta fase montou a infraestrutura que tornou os dados dos sistemas
      estruturantes consultáveis num só lugar.
    tecnico: |
      Airflow 2.8, dbt e Superset em Docker, arquitetura Medallion,
      Postgres como warehouse.
    evidencias:
      prs: { desde: 2024-12-01, ate: 2025-03-31 }
      dbt: { projetos: [ipea], camadas: [bronze] }
      dags: { caminho: "data_ingest/**" }
    destaques: [12, 18, 27]
    documentos: [ata-kickoff.pdf]
```

O build resolve cada consulta contra os JSONs coletados e injeta contagens, PRs,
modelos e DAGs no template.

### Regras de integridade

1. **Cobertura** — todo PR mergeado deve cair em alguma fase. PR órfão gera aviso,
   não erro: impede buracos silenciosos na história conforme o projeto avança.
2. **Referência viva** — destaque ou documento inexistente falha o build. Não se
   publica link morto em página de prestação de contas.

## Camada visual

A identidade GovHub é aplicada com a skill `govhub-visual-identity` (paleta,
tipografia e uso de marca), com o roxo `#7A34F3` como cor principal.

**Princípio: zero dependência externa no site publicado.** Sem CDN, sem bundler,
sem `node_modules` versionado.

- **Diagramas** — os coletores geram texto Mermaid (`.mmd`) a partir do
  `manifest.json`; o CI converte para SVG com `mermaid-cli` via `npx`. Node existe
  apenas no runner. O diagrama de arquitetura geral é o único escrito à mão.
- **Gráficos** — SVG gerado no build (modelos por trimestre, PRs por fase, fontes
  por sistema). Sem biblioteca de charts.
- **Interatividade** — um `assets/app.js` em JS puro: filtro da timeline, troca de
  abas, busca sobre um `search-index.json` gerado no build, realce na linhagem.
  Progressive enhancement: sem JS, a página segue completa.
- **Tema** — `assets/tema.css` com custom properties para cor, espaçamento e
  tipografia. Dark mode e `print.css` derivam daí; qualquer página vira PDF
  apresentável.

Cabeçalho, abas, rodapé e cards vivem em parciais Jinja.

## Build, preview e publicação

Coletar e construir são comandos distintos:

```
make docs-collect   # usa rede: gh e git → docs-pages/src/_data/*.json
make docs-build     # offline: Jinja2 e mermaid-cli → docs-pages/site/
make docs-serve     # build e servidor local em http://localhost:8000
```

Os alvos criam e usam um virtualenv próprio em `docs-pages/.venv`, com apenas
`jinja2` e `pyyaml`. Rodar o site não exige Airflow, dbt nem Poetry instalados.

Os JSONs coletados ficam **versionados**. Assim o build no CI não depende de rede
nem de banco, o diff de uma coleta mostra o que mudou no acervo, e um build falho
nunca destrói a última versão boa dos dados.

`docs-pages/site/` entra no `.gitignore`: o deploy é por Actions, não por pasta.

### Preview local

`make docs-serve` roda o build e sobe `python -m http.server` sobre
`docs-pages/site/` — sem dependência adicional. O visual pode ser conferido
inteiro antes de qualquer push.

Para que o mesmo HTML funcione em `localhost:8000/` e em
`govhub-br.github.io/data-application-cidades/`, cada página recebe do build a
variável `rel` — o prefixo relativo à sua profundidade. Nenhuma URL absoluta é
escrita nos templates.

O `mermaid-cli` exige Node. Localmente, se `npx` não estiver disponível, o build
emite um aviso e insere um placeholder no lugar do diagrama, seguindo adiante — o
restante do visual continua conferível.

### CI

Workflow próprio em `.github/workflows/docs-pages.yaml`, disparado em push na
`main` e em PRs que tocam `docs-pages/`:

1. `make docs-build` (Python 3.11 e `npx mmdc`)
2. em PR: publica o site como artefato para revisão visual antes do merge
3. em `main`: `upload-pages-artifact` e `deploy-pages`

### Resiliência

Coletor que falha registra o erro e preserva o JSON anterior; o site publica com o
dado da última coleta. O build sempre produz site. As duas exceções que devem
falhar são referência morta e template quebrado.

### Verificação

Testes em `tests/docs/` cobrem os coletores (com fixtures de `manifest.json` e de
saída do `gh`) e o resolvedor de consultas do `roadmap.yml`, que concentra a lógica
real. Um smoke test renderiza o site completo e valida os links internos.

## Premissa resolvida

A dependência de `dbt parse` — e portanto de conexão ao Postgres — foi eliminada
na implementação. A convenção de pastas do repositório
(`<projeto>/models/<domínio>_dbt/<camada>/<modelo>.sql`) já carrega projeto,
domínio e camada, e os `ref()`/`source()` no SQL dão a linhagem. O coletor lê a
árvore de arquivos e o `schema.yml`, rodando offline e sem dbt instalado.

## Plano de entrega

Quatro fatias, cada uma publicável por si:

| # | Fatia | Entrega | Valida |
|---|---|---|---|
| 1 | Esqueleto vivo | coletores, `build.py`, tema, capa com números reais, deploy | o pipeline funciona ponta a ponta |
| 2 | Roadmap | `roadmap.yml`, timeline interativa, páginas de fase nas três abas | a costura narrativa e evidência se sustenta |
| 3 | Domínios | template único, páginas de domínio, linhagem Mermaid, inventário dbt | o template se dilui como previsto |
| 4 | Acervo e acabamento | documentos, vitrine, busca, `print.css`, revisão de texto | serve como prestação de contas |

## Pendências com a equipe

Nenhuma bloqueia a fatia 1.

1. **Documentos** — apresentações, atas, planilhas de entrega e prints de
   dashboards do Superset, colocados em `docs-pages/src/acervo/`.
2. **Recorte de fases** — os períodos que fazem sentido para a gestão. Rascunho
   proposto a partir do histórico de PRs, depois corrigido pela equipe.
3. **Domínios** — confirmar se o recorte de nove bate com o produto real.
4. **Texto negocial** — o problema resolvido em cada fase. Primeira versão inferida
   dos PRs, ajustada depois na voz institucional.
